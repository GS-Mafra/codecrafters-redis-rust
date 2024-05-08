use anyhow::{bail, Context};
use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    args::{Role, Slave},
    Resp,
};

pub struct Handler {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
    pub offset: u64,
}

impl Handler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(1024 * 4),
            offset: 0,
        }
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<Resp>> {
        loop {
            if let Some(resp) = self.parse()? {
                return Ok(Some(resp));
            }

            if 0 == self.stream.read_buf(&mut self.buf).await? {
                return Ok(None);
            }
        }
    }

    async fn read_bytes(&mut self) -> anyhow::Result<()> {
        self.stream.read_buf(&mut self.buf).await?;
        Ok(())
    }

    fn parse(&mut self) -> anyhow::Result<Option<Resp>> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let mut cur = Cursor::new(self.buf.as_ref());

        match Resp::check(&mut cur) {
            Ok(()) => {
                let len = cur.position().try_into()?;
                cur.set_position(0);
                let resp = Resp::parse(&mut cur).map(Option::Some);
                self.buf.advance(len);
                self.offset += u64::try_from(len)?;
                resp
            }
            Err(crate::resp::Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_checked(&mut self, resp: &Resp, role: &Role) -> anyhow::Result<()> {
        match role {
            Role::Master(_) => self.write(resp).await,
            Role::Slave(_) => Ok(())
        }
    }

    pub async fn write(&mut self, resp: &Resp) -> anyhow::Result<()> {
        tracing::debug!("Writing: {resp:?}");
        match resp {
            Resp::Simple(inner) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(inner.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Resp::Bulk(inner) => self.write_bulk(inner, true).await?,
            Resp::Array(elems) => {
                self.stream.write_u8(b'*').await?;
                self.stream
                    .write_all(elems.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                for resp in elems {
                    Box::pin(self.write(resp)).await?;
                }
            }
            Resp::Integer(inner) => {
                self.stream.write_u8(b':').await?;
                self.stream.write_all(inner.to_string().as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Resp::Data(inner) => self.write_bulk(inner, false).await?,

            Resp::Null => self.stream.write_all(b"$-1\r\n").await?,
        };
        self.stream.flush().await?;
        Ok(())
    }

    async fn write_bulk(&mut self, bulk: &Bytes, crlf: bool) -> anyhow::Result<()> {
        self.stream.write_u8(b'$').await?;
        self.stream
            .write_all(bulk.len().to_string().as_bytes())
            .await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(bulk).await?;
        if crlf {
            self.stream.write_all(b"\r\n").await?;
        }
        Ok(())
    }
}

pub async fn connect_slave(role: &Role, port: u16) -> anyhow::Result<Option<Handler>> {
    if let Role::Slave(Slave { addr, .. }) = role {
        tracing::info!("Connecting slave to master at {addr}");
        let master = TcpStream::connect(addr)
            .await
            .with_context(|| format!("Failed to connect to master at {addr}"))?;
        let handler = handshake(master, port).await?;
        Ok(Some(handler))
    } else {
        Ok(None)
    }
}

async fn handshake(stream: TcpStream, port: u16) -> anyhow::Result<Handler> {
    let mut handler = Handler::new(stream);
    tracing::info!("Starting handshake");

    let resp = Resp::Array(vec![Resp::bulk("PING")]);
    tracing::info!("Sending PING to master");
    handler.write(&resp).await?;
    check_handshake(&mut handler, "PONG").await?;

    let resp = {
        let replconf = Resp::bulk("REPLCONF");
        let listening_port = Resp::bulk("listening-port");
        let port = Resp::bulk(port.to_string());
        Resp::Array(vec![replconf, listening_port, port])
    };
    tracing::info!("Sending first REPLCONF to master");
    handler.write(&resp).await?;
    check_handshake(&mut handler, "OK").await?;

    let resp = {
        let replconf = Resp::bulk("REPLCONF");
        let capa = Resp::bulk("capa");
        let the_capas = Resp::bulk("psync2");
        Resp::Array(vec![replconf, capa, the_capas])
    };
    tracing::info!("Sending second REPLCONF to master");
    handler.write(&resp).await?;
    check_handshake(&mut handler, "OK").await?;

    let resp = {
        let psync = Resp::bulk("PSYNC");
        let id = Resp::bulk("?");
        let offset = Resp::bulk("-1");
        Resp::Array(vec![psync, id, offset])
    };
    tracing::info!("Sending PSYNC to master");
    handler.write(&resp).await?;
    let recv = handler.read().await?;
    tracing::info!("Received: {recv:?}");

    // TODO do something with the data in handler.buf
    handler.read_bytes().await?;
    handler.buf.clear();

    handler.offset = 0;

    Ok(handler)
}

async fn check_handshake(handler: &mut Handler, msg: &str) -> anyhow::Result<()> {
    let recv = handler.read().await?;
    if !recv.is_some_and(|x| Resp::simple(msg) == x) {
        bail!("Expected {msg}")
    }
    Ok(())
}
