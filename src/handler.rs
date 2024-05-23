use bytes::{Buf, Bytes, BytesMut};
use std::{io::Cursor, net::SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

use crate::{Command, Resp, Role};

#[derive(Debug)]
pub struct Handler {
    pub(crate) addr: SocketAddr,
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    pub(crate) buf: BytesMut,
}

impl Handler {
    pub fn new(stream: TcpStream) -> Self {
        let addr = stream.peer_addr().unwrap();
        let (reader, writer) = stream.into_split();
        Self {
            addr,
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            buf: BytesMut::with_capacity(1024 * 4),
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

    pub(crate) async fn read_bytes(&mut self) -> anyhow::Result<()> {
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
                resp
            }
            Err(crate::resp::Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write(&mut self, resp: &Resp) -> Result<(), std::io::Error> {
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

    async fn write_bulk(&mut self, bulk: &Bytes, crlf: bool) -> Result<(), std::io::Error> {
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

    pub(crate) fn disconnected(e: &std::io::Error) -> bool {
        use std::io::ErrorKind::{ConnectionAborted, ConnectionReset, UnexpectedEof};
        matches!(
            e.kind(),
            ConnectionAborted | UnexpectedEof | ConnectionReset
        )
    }
}

pub async fn handle_connection(mut handler: Handler, role: &Role) -> anyhow::Result<()> {
    loop {
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };

        let (parsed_cmd, raw_cmd) = Command::parse(&resp)?;

        match parsed_cmd {
            Command::Ping(ping) => ping.apply_and_respond(&mut handler).await?,
            Command::Echo(echo) => echo.apply_and_respond(&mut handler).await?,
            Command::Get(get) => get.apply_and_respond(&mut handler).await?,
            Command::Set(set) => {
                set.apply_and_respond(&mut handler).await?;
                propagate(role, raw_cmd).await;
            }
            Command::Del(del) => {
                del.apply_and_respond(&mut handler).await?;
                propagate(role, raw_cmd).await;
            }
            Command::Info(info) => info.apply_and_respond(&mut handler, role).await?,
            Command::ReplConf(replconf) => replconf.apply_and_respond(&mut handler).await?,
            Command::Wait(wait) => wait.apply_and_respond(&mut handler, role).await?,
            Command::Config(config) => config.apply_and_respond(&mut handler).await?,
            Command::Keys(keys) => keys.apply_and_respond(&mut handler).await?,
            Command::Psync(psync) => 'psync: {
                let Role::Master(master) = role else {
                    break 'psync;
                };
                psync.apply_and_respond(&mut handler, master).await?;
                master.add_slave(handler).await;
                return Ok(());
            }
        };
    }
}

async fn propagate(role: &Role, command: &[Resp]) {
    if let Role::Master(master) = role {
        let command = Resp::Array(command.to_owned());
        master.propagate(&command, true).await;
    }
}
