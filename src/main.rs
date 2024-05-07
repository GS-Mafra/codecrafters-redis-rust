use once_cell::sync::Lazy;
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::{net::TcpListener, sync::broadcast::Sender};

use redis_starter_rust::{connect_slave, debug_print, Command, Handler, Resp, Role, ARGUMENTS};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Lazy::force(&ARGUMENTS);
    debug_print!("{:#?}", &*ARGUMENTS);

    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, ARGUMENTS.port);
    let listener = TcpListener::bind(addr).await.unwrap();

    if let Some(slave) = connect_slave(&ARGUMENTS.role, ARGUMENTS.port).await? {
        tokio::spawn(async move { handle_connection(slave, &ARGUMENTS.role, None).await });
    }

    let master = ARGUMENTS.role.get_slaves();

    loop {
        let sender = master.cloned();

        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(Handler::new(stream), &ARGUMENTS.role, sender)
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

async fn handle_connection(
    mut handler: Handler,
    role: &Role,
    sender: Option<Sender<Resp>>,
) -> anyhow::Result<()> {
    loop {
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };
        let response = Command::parse(&resp, role)?;

        handler.write(&response.resp).await?;

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
