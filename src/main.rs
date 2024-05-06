use anyhow::Context;
use once_cell::sync::Lazy;
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::net::{TcpListener, TcpStream};

use redis_starter_rust::{connect_slave, debug_print, Command, Handler, ARGUMENTS};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Lazy::force(&ARGUMENTS);
    debug_print!("{:#?}", &*ARGUMENTS);

    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, ARGUMENTS.port);
    let listener = TcpListener::bind(addr).await.unwrap();

    let _slave = connect_slave(&ARGUMENTS.role, ARGUMENTS.port)
        .await
        .with_context(|| format!("Failed to connect to master at {addr}"))?;

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(stream)
                        .await
                        .inspect_err(|e| eprintln!("{e}"))
                });
            }
            Err(e) => {
                eprintln!("error: {e}");
            }
        }
    }
}

async fn handle_connection(stream: TcpStream) -> anyhow::Result<()> {
    let mut handler = Handler::new(stream);
    loop {
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };
        let response = Command::parse(&resp)?;
        handler.write(&response).await?;
    }
}
