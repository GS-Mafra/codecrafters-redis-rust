use std::{
    fs::File,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};
use tokio::{net::TcpListener, sync::broadcast::Sender};
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use redis_starter_rust::{connect_slave, Arguments, Command, Handler, Resp, Role};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parser();
    let _guard = init_log(arguments.port);

    tracing::debug!("{:#?}", arguments);
    let Arguments { port, role } = arguments;

    let role = Arc::new(role);

    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let listener = TcpListener::bind(addr).await.unwrap();

    if let Some(slave) = connect_slave(&role, port).await? {
        let role = role.clone();
        tokio::spawn(async move { handle_connection(slave, &role, None).await });
    }

    let master = role.get_slaves();

    loop {
        let sender = master.cloned();
        let role = role.clone();

        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(Handler::new(stream), &role, sender)
                        .await
                        .inspect_err(|e| tracing::error!("{e}"))
                });
            }
            Err(e) => {
                tracing::error!("error: {e}");
            }
        }
    }
}

async fn handle_connection(
    mut handler: Handler,
    role: &Role,
    sender: Option<Sender<Resp>>,
) -> anyhow::Result<()> {
    loop {
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };
        role.increase_offset(handler.offset);
        handler.offset = 0;

        let response = Command::parse(&resp, role)?;

        if !response.silent {
            handler.write(&response.resp).await?;
        }

        if let Some(sender) = &sender {
            if let Some(data) = response.data {
                let mut slave = sender.subscribe();
                handler.write(&data).await?;
                while let Ok(recv) = slave.recv().await {
                    handler.write(&recv).await?;
                }
            }

            if let Some(propagation) = response.slave_propagation {
                let _ = sender.send(propagation);
            }
        }
    }
}

fn init_log(port: u16) -> WorkerGuard {
    let console_layer = tracing_subscriber::fmt::layer()
        .without_time()
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
            .without_time()
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
