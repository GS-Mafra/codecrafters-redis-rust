use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};
use tokio::{net::TcpListener, sync::broadcast::Sender};

use redis_starter_rust::{connect_slave, debug_print, Arguments, Command, Handler, Resp, Role};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = {
        let arguments = Arguments::parser();
        debug_print!("{:#?}", arguments);
        arguments
    };
    let Arguments { port, role } = arguments;
    let role = Arc::new(role);

    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let listener = TcpListener::bind(addr).await.unwrap();

    if let Some(mut slave) = connect_slave(&role, port).await? {
        slave.offset = 0;
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
        debug_print!("role: {role:#?}");
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };
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
        role.increase_offset(handler.offset);
        handler.offset = 0;
    }
}
