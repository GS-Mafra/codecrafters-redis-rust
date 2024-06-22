use bytes::{Buf, Bytes, BytesMut};
use std::{io::Cursor, net::SocketAddr};
use thiserror::Error;
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
            buf: BytesMut::with_capacity(1024),
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

    pub async fn write(&mut self, resp: &Resp) -> std::io::Result<()> {
        tracing::debug!("Writing: {resp:?}");
        match resp {
            Resp::Simple(inner) => self.write_simple(inner, '+').await?,
            Resp::Err(inner) => self.write_simple(inner, '-').await?,
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

    async fn write_bulk(&mut self, bulk: &Bytes, crlf: bool) -> std::io::Result<()> {
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

    async fn write_simple(&mut self, simple: &str, c: char) -> std::io::Result<()> {
        self.writer.write_u8(c as u8).await?;
        self.writer.write_all(simple.as_bytes()).await?;
        self.writer.write_all(b"\r\n").await?;
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

#[allow(clippy::module_name_repetitions)]
pub struct CommandHandler<'a> {
    handler: Option<Handler>,
    role: &'a Role,
    queued: Vec<(Command, Vec<Resp>)>,
    transaction: bool,
}

#[allow(clippy::unused_async)]
impl<'a> CommandHandler<'a> {
    pub const fn new(handler: Handler, role: &'a Role) -> Self {
        Self {
            handler: Some(handler),
            role,
            queued: Vec::new(),
            transaction: false,
        }
    }

    pub async fn handle_commands(&mut self) -> anyhow::Result<()> {
        loop {
            match self.handle_command().await {
                Ok(()) => (),
                Err(CommandError::Finished | CommandError::Replicated) => return Ok(()),
                Err(e) => {
                    unsafe { self.handler.as_mut().unwrap_unchecked() }
                        .write(&Resp::Err(e.to_string()))
                        .await?;
                }
            }
        }
    }

    async fn handle_command(&mut self) -> Result<(), CommandError> {
        let handler = unsafe { self.handler.as_mut().unwrap_unchecked() };

        let Some(resp) = handler.read().await? else {
            return Err(CommandError::Finished);
        };

        let (parsed_cmd, raw_cmd) = Command::parse(&resp)?;

        if self.transaction {
            match parsed_cmd {
                Command::Exec => self.apply_exec().await?,
                Command::Multi(_) => {
                    return Err(anyhow::anyhow!("ERR MULTI calls can not be nested").into())
                }
                Command::Discard(discard) => {
                    self.queued.clear();
                    self.transaction = false;
                    handler.write(&discard.execute()).await?;
                }
                other => {
                    self.queued.push((other, raw_cmd));
                    handler.write(&Resp::simple("QUEUED")).await?;
                }
            }
            return Ok(());
        };

        let resp = self.apply_commands(parsed_cmd, raw_cmd).await?;
        unsafe { self.handler.as_mut().unwrap_unchecked() }
            .write(&resp)
            .await?;
        Ok(())
    }

    async fn apply_commands(
        &mut self,
        parsed_cmd: Command,
        raw_cmd: Vec<Resp>,
    ) -> Result<Resp, CommandError> {
        let resp = match parsed_cmd {
            Command::Exec => {
                return Err(anyhow::anyhow!("ERR EXEC without MULTI").into());
            }
            Command::Discard(_) => {
                return Err(anyhow::anyhow!("ERR DISCARD without MULTI").into());
            }

            Command::Set(set) => {
                let resp = set.execute();
                propagate(self.role, raw_cmd).await;
                resp
            }
            Command::Del(del) => {
                let resp = del.execute()?;
                propagate(self.role, raw_cmd).await;
                resp
            }
            Command::Xadd(xadd) => {
                let resp = xadd.execute()?;
                propagate(self.role, raw_cmd).await;
                resp
            }
            Command::Incr(incr) => {
                let resp = incr.execute()?;
                propagate(self.role, raw_cmd).await;
                resp
            }

            Command::Ping(ping) => ping.execute(),
            Command::Echo(echo) => echo.execute(),
            Command::Get(get) => get.execute()?,
            Command::Config(config) => config.execute(),
            Command::Keys(keys) => keys.execute(),
            Command::Type(r#type) => r#type.execute(),
            Command::Xrange(xrange) => xrange.execute()?,
            Command::Xread(xread) => xread.execute().await?,

            Command::Info(info) => info.execute(self.role).await?,
            Command::Wait(wait) => wait.execute(self.role).await?,

            Command::Multi(multi) => {
                let resp = multi.execute();
                self.transaction = true;
                resp
            }

            Command::ReplConf(replconf) => replconf.execute(),
            Command::Psync(psync) => {
                if self.transaction {
                    return Err(
                        anyhow::anyhow!("ERR Command not allowed inside a transaction").into(),
                    );
                }

                let Role::Master(master) = self.role else {
                    return Err(anyhow::anyhow!("").into()); // FIXME
                };
                match psync.execute(master) {
                    Ok((resp, data)) => {
                        let mut handler = self.handler.take().unwrap();
                        handler.write(&resp).await?;
                        handler.write(&data).await?;
                        master.add_slave(handler).await;
                        return Err(CommandError::Replicated);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        };
        Ok(resp)
    }

    async fn apply_exec(&mut self) -> anyhow::Result<()> {
        let mut queue_res = Vec::with_capacity(self.queued.len());
        let queue = std::mem::take(&mut self.queued); // FIXME use Vec::drain

        for (parsed_cmd, raw_cmd) in queue {
            let resp = self
                .apply_commands(parsed_cmd, raw_cmd)
                .await
                .unwrap_or_else(|e| Resp::Err(e.to_string()));
            queue_res.push(resp);
        }

        self.transaction = false;
        unsafe { self.handler.as_mut().unwrap_unchecked() }
            .write(&Resp::Array(queue_res))
            .await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("No more bytes to read from tcpstream")]
    Finished,
    #[error("Handler was taken for replication")]
    Replicated,
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

async fn propagate(role: &Role, command: Vec<Resp>) {
    if let Role::Master(master) = role {
        let command = Resp::Array(command);
        master.propagate(&command, true).await;
    }
}
