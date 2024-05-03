use std::{fmt::Display, io::Cursor};

use anyhow::Context;
use atoi::FromRadix10SignedChecked;
use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

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
    let mut handler = RespHandler::new(stream);
    loop {
        let Some(resp) = handler.read().await? else {
            return Ok(());
        };
        let response = Command::parse(&resp)?;
        handler.write(&response).await?;
    }
}

struct Command;

impl Command {
    fn parse(value: &Resp) -> anyhow::Result<Resp> {
        match value {
            Resp::Array(values) => {
                let mut values = values.iter().take(2);
                let command = values.next().context("No command")?;
                let value = values.next();

                let Resp::Bulk(resp) = command else {
                    return Err(anyhow::anyhow!("Expected bulk string"));
                };

                match resp.as_ref() {
                    s if s.eq_ignore_ascii_case(b"ping") => Ok(Resp::Simple("PONG".into())),

                    s if s.eq_ignore_ascii_case(b"echo") => {
                        Ok(value.cloned().context("Missing ECHO value")?)
                    }
                    _ => unimplemented!(),
                }
            }
            _ => Err(anyhow::anyhow!("Unsupported")),
        }
    }
}

struct RespHandler {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl RespHandler {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(1024),
        }
    }

    async fn read(&mut self) -> anyhow::Result<Option<Resp>> {
        loop {
            if let Some(resp) = self.parse()? {
                return Ok(Some(resp));
            }

            if 0 == self.stream.read_buf(&mut self.buf).await? {
                return Ok(None);
            }
        }
    }

    async fn write(&mut self, resp: &Resp) -> anyhow::Result<()> {
        self.stream.write_all(format!("{resp}").as_bytes()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    fn parse(&mut self) -> anyhow::Result<Option<Resp>> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let mut cur = Cursor::new(self.buf.as_ref());
        let resp = Resp::parse(&mut cur).map(Option::Some);
        self.buf.advance(cur.position().try_into()?);
        resp
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Resp {
    Simple(String),
    Bulk(Bytes),
    Array(Vec<Self>),
}

impl Resp {
    fn parse(cur: &mut Cursor<&[u8]>) -> anyhow::Result<Self> {
        match cur.get_u8() {
            b'*' => {
                let len = slice_to_int::<usize>(read_line(cur)?)?;
                let mut elems = Vec::with_capacity(len);

                for _ in 0..len {
                    elems.push(Self::parse(cur)?);
                }
                Ok(Self::Array(elems))
            }
            b'+' => String::from_utf8(read_line(cur)?.to_vec())
                .map(Self::Simple)
                .map_err(anyhow::Error::from),
            b'$' => {
                let len = slice_to_int::<usize>(read_line(cur)?)?;

                let data = Bytes::copy_from_slice(&cur.chunk()[..len]);
                cur.advance(len + b"\r\n".len());

                Ok(Self::Bulk(data))
            }
            c => unimplemented!("{:?}", c as char),
        }
    }
}

fn read_line<'a>(cur: &mut Cursor<&'a [u8]>) -> anyhow::Result<&'a [u8]> {
    let start = cur.position().try_into()?;
    let end = cur.get_ref().len();

    for i in start..end - 1 {
        if cur.get_ref()[i] == b'\r' && cur.get_ref()[i + 1] == b'\n' {
            cur.set_position((i + 2) as u64);
            return Ok(&cur.get_ref()[start..i]);
        }
    }

    Err(anyhow::anyhow!("Failed to read line"))
}

fn slice_to_int<T>(slice: &[u8]) -> anyhow::Result<T>
where
    T: FromRadix10SignedChecked,
{
    atoi::atoi::<T>(slice).context("Failed to parse length")
}

impl Display for Resp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Simple(resp) => write!(f, "+{resp}\r\n")?,
            Self::Bulk(resp) => {
                if let Ok(resp) = std::str::from_utf8(resp) {
                    write!(f, "${}\r\n{}\r\n", resp.len(), resp)?;
                };
            }
            Self::Array(elems) => {
                write!(f, "*{}\r\n", elems.len())?;
                for resp in elems {
                    resp.fmt(f)?;
                }
            }
        };
        Ok(())
    }
}
