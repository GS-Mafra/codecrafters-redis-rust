use anyhow::{bail, Context};
use bytes::Buf;
use std::{
    io::Cursor,
    net::SocketAddrV4,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::net::TcpStream;

use crate::{
    commands::{Ping, Psync, ReplConf},
    Command, Handler, Resp,
};

#[derive(Debug)]
pub struct Slave {
    pub addr: SocketAddrV4,
    offset: AtomicU64,
}

impl Slave {
    pub(crate) const fn new(addr: SocketAddrV4) -> Self {
        Self {
            addr,
            offset: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    #[inline]
    fn increase_offset(&self, by: u64) {
        let prev = self.offset.fetch_add(by, Ordering::Relaxed);
        tracing::info!("Increased offset of {prev} to {}", by + prev);
    }

    pub async fn connect(&self, port: u16) -> anyhow::Result<()> {
        tracing::info!("Connecting slave to master at {}", self.addr);
        let master = TcpStream::connect(self.addr)
            .await
            .with_context(|| format!("Failed to connect to master at {}", self.addr))?;
        let handler = self.handshake(master, port).await?;

        self.handle_connection(handler).await
    }

    async fn handle_connection(&self, mut handler: Handler) -> anyhow::Result<()> {
        loop {
            let Some(resp) = handler.read().await? else {
                return Ok(());
            };
            let parsed_cmd = match Command::parse(&resp) {
                Ok((cmd, _)) => cmd,
                Err(e) => {
                    tracing::error!("{}", e);
                    continue;
                }
            };
            match parsed_cmd {
                Command::Set(set) => set.apply(),
                Command::Del(del) => {
                    del.apply();
                }
                Command::ReplConf(replconf) => {
                    replconf.apply_and_respond_slave(&mut handler, self).await?;
                }
                _ => (),
            };
            self.increase_offset(resp.len() as u64);
        }
    }

    async fn handshake(&self, stream: TcpStream, port: u16) -> anyhow::Result<Handler> {
        let mut handler = Handler::new(stream);
        tracing::info!("Starting handshake");

        tracing::info!("Sending PING to master");
        handler.write(&Ping::new(None).into_resp()).await?;
        check_handshake(&mut handler, "PONG").await?;

        tracing::info!("Sending first REPLCONF to master");
        handler
            .write(&ReplConf::ListeningPort(port).into_resp())
            .await?;
        check_handshake(&mut handler, "OK").await?;

        tracing::info!("Sending second REPLCONF to master");
        handler
            .write(&ReplConf::Capa("psync2".into()).into_resp())
            .await?;
        check_handshake(&mut handler, "OK").await?;

        tracing::info!("Sending PSYNC to master");
        handler
            .write(&Psync::new("?".into(), -1).into_resp())
            .await?;
        let recv = handler.read().await?;
        tracing::info!("Received: {recv:?}");

        if handler.buf.is_empty() {
            handler.read_bytes().await?;
        }

        // TODO do something with the rdb
        let _rdb = {
            let mut cur = Cursor::new(handler.buf.as_ref());
            let rdb = Resp::parse_rdb(&mut cur)?;
            handler.buf.advance(cur.position().try_into()?);
            rdb
        };

        Ok(handler)
    }
}

async fn check_handshake(handler: &mut Handler, msg: &str) -> anyhow::Result<()> {
    let recv = handler.read().await?;
    if !recv.is_some_and(|x| x.as_simple().is_some_and(|x| x == msg)) {
        bail!("Expected {msg}")
    }
    Ok(())
}
