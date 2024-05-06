use anyhow::{bail, Context};
use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{args::Role, debug_print, Resp};

pub struct Handler {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl Handler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(1024 * 4),
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
                resp
            }
            Err(crate::resp::Error::Incomplete) => {
                debug_print!("Resp was incomplete");
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write(&mut self, resp: &Resp) -> anyhow::Result<()> {
        match resp {
            Resp::Simple(inner) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(inner.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Resp::Bulk(inner) => {
                self.stream.write_u8(b'$').await?;
                self.stream
                    .write_all(inner.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(inner).await?;
                self.stream.write_all(b"\r\n").await?;
            }
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
            Resp::Null => self.stream.write_all(b"$-1\r\n").await?,
        };
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_bytes(&mut self, bytes: Bytes) -> anyhow::Result<()> {
        self.stream.write_u8(b'$').await?;
        self.stream.write_all(bytes.len().to_string().as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(bytes.as_ref()).await?;
        Ok(())
    }
}

pub async fn connect_slave(role: &Role, port: u16) -> anyhow::Result<Option<Handler>> {
    if let Role::Slave(addr) = role {
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

    let resp = Resp::Array(vec![Resp::Bulk("PING".into())]);
    handler.write(&resp).await?;
    check_handshake(&mut handler, "PONG").await?;

    let resp = {
        let replconf = Resp::Bulk("REPLCONF".into());
        let listening_port = Resp::Bulk("listening-port".into());
        let port = Resp::Bulk(port.to_string().into());
        Resp::Array(vec![replconf, listening_port, port])
    };
    handler.write(&resp).await?;
    check_handshake(&mut handler, "OK").await?;

    let resp = {
        let replconf = Resp::Bulk("REPLCONF".into());
        let capa = Resp::Bulk("capa".into());
        let the_capas = Resp::Bulk("psync2".into());
        Resp::Array(vec![replconf, capa, the_capas])
    };
    handler.write(&resp).await?;
    check_handshake(&mut handler, "OK").await?;

    let resp = {
        let psync = Resp::Bulk("PSYNC".into());
        let id = Resp::Bulk("?".into());
        let offset = Resp::Bulk("-1".into());
        Resp::Array(vec![psync, id, offset])
    };
    handler.write(&resp).await?;

    Ok(handler)
}

async fn check_handshake(handler: &mut Handler, msg: &str) -> anyhow::Result<()> {
    let recv = handler.read().await?;
    if !recv.is_some_and(|x| Resp::Simple(msg.into()) == x) {
        bail!("Expected {msg}")
    }
    Ok(())
}
