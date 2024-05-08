use anyhow::{bail, Context};
use bytes::{Buf, Bytes, BytesMut};
use std::{io::Cursor, net::SocketAddrV4};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::broadcast::Sender,
};

use crate::{args::Role, Command, Resp};

pub struct Handler {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    buf: BytesMut,
    offset: u64,
}

impl Handler {
    #[must_use]
    pub fn new((reader, writer): (OwnedReadHalf, OwnedWriteHalf)) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            buf: BytesMut::with_capacity(1024 * 4),
            offset: 0,
        }
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<Resp>> {
        loop {
            if let Some(resp) = self.parse()? {
                return Ok(Some(resp));
            }

            if 0 == self.reader.read_buf(&mut self.buf).await? {
                return Ok(None);
            }
        }
    }

    async fn read_bytes(&mut self) -> anyhow::Result<()> {
        self.reader.read_buf(&mut self.buf).await?;
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
            Role::Slave(_) => Ok(()),
        }
    }

    pub async fn write(&mut self, resp: &Resp) -> anyhow::Result<()> {
        tracing::debug!("Writing: {resp:?}");
        match resp {
            Resp::Simple(inner) => {
                self.writer.write_u8(b'+').await?;
                self.writer.write_all(inner.as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Resp::Bulk(inner) => self.write_bulk(inner, true).await?,
            Resp::Array(elems) => {
                self.writer.write_u8(b'*').await?;
                self.writer
                    .write_all(elems.len().to_string().as_bytes())
                    .await?;
                self.writer.write_all(b"\r\n").await?;
                for resp in elems {
                    Box::pin(self.write(resp)).await?;
                }
            }
            Resp::Integer(inner) => {
                self.writer.write_u8(b':').await?;
                self.writer.write_all(inner.to_string().as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Resp::Data(inner) => self.write_bulk(inner, false).await?,

            Resp::Null => self.writer.write_all(b"$-1\r\n").await?,
        };
        self.writer.flush().await?;
        Ok(())
    }

    async fn write_bulk(&mut self, bulk: &Bytes, crlf: bool) -> anyhow::Result<()> {
        self.writer.write_u8(b'$').await?;
        self.writer
            .write_all(bulk.len().to_string().as_bytes())
            .await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.write_all(bulk).await?;
        if crlf {
            self.writer.write_all(b"\r\n").await?;
        }
        Ok(())
    }
}

pub async fn handle_connection(
    mut handler: Handler,
    role: &Role,
    sender: Option<Sender<Resp>>,
) -> anyhow::Result<()> {
    loop {
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };
        Command::new(&mut handler, role, sender.as_ref())
            .parse(&resp)
            .await?;

        role.increase_offset(handler.offset);
        handler.offset = 0;
    }
}

pub async fn connect_slave(addr: SocketAddrV4, role: &Role, port: u16) -> anyhow::Result<()> {
    tracing::info!("Connecting slave to master at {addr}");
    let master = TcpStream::connect(addr)
        .await
        .with_context(|| format!("Failed to connect to master at {addr}"))?;
    let handler = handshake(master, port).await?;
    handle_connection(handler, role, None).await?;
    Ok(())
}

async fn handshake(stream: TcpStream, port: u16) -> anyhow::Result<Handler> {
    let mut handler = Handler::new(stream.into_split());
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
