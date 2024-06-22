use once_cell::sync::Lazy;
use std::{
    fs::File,
    net::{Ipv4Addr, SocketAddrV4},
};
use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use redis_starter_rust::{CommandHandler, Handler, Role, ARGUMENTS, DB};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Lazy::force(&ARGUMENTS);
    let _guard = init_log(ARGUMENTS.port);
    tracing::debug!("{:#?}", *ARGUMENTS);

    let listener = {
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, ARGUMENTS.port);
        TcpListener::bind(addr).await?
    };

    load_rdb()?;

    if let Role::Slave(slave) = &ARGUMENTS.role {
        tokio::spawn(async move { slave.connect(ARGUMENTS.port).await });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    CommandHandler::new(Handler::new(stream), &ARGUMENTS.role)
                        .handle_commands()
                        .await
                        .inspect_err(|e| tracing::error!("{e}"))
                });
            }
            Err(e) => {
                tracing::error!("{e}");
            }
        }
    }
}

fn load_rdb() -> anyhow::Result<()> {
    ARGUMENTS
        .dir
        .as_ref()
        .zip(ARGUMENTS.db_filename.as_ref())
        .map(|(dir, name)| dir.join(name))
        .map_or(Ok(()), |rdb_path| DB.load_rdb(rdb_path))
}

fn init_log(port: u16) -> WorkerGuard {
    let console_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_filter(EnvFilter::from_default_env());

    let (file_layer, guard) = File::create(format!(
        "{dir}{sep}{name}_{port}.log",
        dir = env!("CARGO_MANIFEST_DIR"), // FIXME
        sep = std::path::MAIN_SEPARATOR,
        name = env!("CARGO_CRATE_NAME")
    ))
    .map(tracing_appender::non_blocking)
    .map(|(file, guard)| {
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(file)
            .with_file(true)
            .with_line_number(true)
            .with_ansi(false)
            .with_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::TRACE.into())
                    .with_env_var("FILE_LOG")
                    .from_env_lossy(),
            );
        (layer, guard)
    })
    .unwrap();

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();
    guard
}
