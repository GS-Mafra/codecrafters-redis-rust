use std::{
    collections::HashMap,
    io::Cursor,
    sync::RwLock,
    time::{Duration, Instant},
};

use anyhow::Context;
use atoi::FromRadix10SignedChecked;
use bytes::{Buf, Bytes, BytesMut};
use once_cell::sync::Lazy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
};

static DB: Lazy<RwLock<HashMap<String, Value>>> = Lazy::new(|| RwLock::new(HashMap::new()));

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

#[derive(Debug, Clone)]
struct Value {
    inner: Bytes,
    created: Instant,
    expiration: Option<Duration>,
}

struct Command;

impl Command {
    fn set(k: String, v: Bytes, px: Option<Duration>) {
        let value = Value {
            inner: v,
            created: Instant::now(),
            expiration: px,
        };

        DB.write().unwrap().insert(k, value);
        #[cfg(debug_assertions)]
        eprintln!("{:?}", DB.read().unwrap());
    }

    fn get(k: &str) -> Option<Value> {
        DB.read().unwrap().get(k).cloned()
    }

    fn del(k: &str) {
        DB.write().unwrap().remove(k);
    }

    fn parse(value: &Resp) -> anyhow::Result<Resp> {
        match value {
            Resp::Array(values) => {
                let mut values = values.iter();
                let Some(Resp::Bulk(command)) = values.next() else {
                    return Err(anyhow::anyhow!("Expected bulk string"));
                };

                Ok(match command.to_ascii_lowercase().as_slice() {
                    b"ping" => Resp::Simple("PONG".into()),
                    b"echo" => values.next().cloned().context("Missing ECHO value")?,
                    b"get" => {
                        let key = values.next().context("Missing Key")?.as_string()?;
                        Self::get(&key)
                            .and_then(|val| {
                                let expired = val.expiration.is_some_and(|px| {
                                    let time_passed = val.created.elapsed();
                                    #[cfg(debug_assertions)]
                                    eprintln!("{time_passed:.02?} passed");
                                    px <= time_passed
                                });
                                if expired {
                                    eprintln!("{key} expired");
                                    Self::del(&key);
                                    None
                                } else {
                                    Some(val)
                                }
                            })
                            .map_or(Resp::Null, |v| Resp::Bulk(v.inner))
                    }
                    b"set" => {
                        let key = values.next().context("Missing Key")?.as_string()?;
                        let value = values.next().context("Missing Value")?.as_string()?;
                        let px = {
                            values
                                .position(|x| *x == Resp::Bulk(b"px".as_ref().into()))
                                .and_then(|pos| {
                                    values
                                        .nth(pos)
                                        .and_then(|x| Resp::as_string(x).ok())
                                        .and_then(|x| x.parse::<u64>().ok())
                                        .map(Duration::from_millis)
                                })
                        };
                        Self::set(key, value.into(), px);
                        Resp::Simple("OK".into())
                    }
                    _ => unimplemented!(),
                })
            }
            _ => Err(anyhow::anyhow!("Unsupported RESP for command")),
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

    fn parse(&mut self) -> anyhow::Result<Option<Resp>> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let mut cur = Cursor::new(self.buf.as_ref());
        let resp = Resp::parse(&mut cur).map(Option::Some);
        self.buf.advance(cur.position().try_into()?);
        resp
    }

    async fn write(&mut self, resp: &Resp) -> anyhow::Result<()> {
        match resp {
            Resp::Simple(inner) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(inner.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Resp::Bulk(inner) => {
                self.stream.write_u8(b'$').await?;
                self.write_int(inner.len()).await?;
                self.stream.write_all(inner).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Resp::Array(elems) => {
                self.stream.write_u8(b'*').await?;
                self.write_int(elems.len()).await?;
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

    async fn write_int(&mut self, int: usize) -> anyhow::Result<()> {
        self.stream.write_all(int.to_string().as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Resp {
    Simple(String),
    Bulk(Bytes),
    Array(Vec<Self>),
    // NullBulk
    Null,
}

impl Resp {
    fn parse(cur: &mut Cursor<&[u8]>) -> anyhow::Result<Self> {
        #[cfg(debug_assertions)]
        eprintln!("Parsing: {:?}", std::str::from_utf8(cur.chunk()));

        let resp = match cur.get_u8() {
            b'*' => {
                let len = slice_to_int::<usize>(read_line(cur)?)?;
                let mut elems = Vec::with_capacity(len);

                for _ in 0..len {
                    elems.push(Self::parse(cur)?);
                }
                Self::Array(elems)
            }
            b'+' => String::from_utf8(read_line(cur)?.to_vec())
                .map(Self::Simple)
                .map_err(anyhow::Error::from)?,
            b'$' => 'bulk: {
                if &cur.chunk()[..2] == b"-1" {
                    cur.advance(b"-1\r\n".len());
                    break 'bulk Self::Null;
                }
                let len = slice_to_int::<usize>(read_line(cur)?)?;

                let data = Bytes::copy_from_slice(&cur.chunk()[..len]);
                cur.advance(len + b"\r\n".len());
                Self::Bulk(data)
            }
            c => unimplemented!("{:?}", c as char),
        };
        #[cfg(debug_assertions)]
        eprintln!("Parsed {resp:?}");

        Ok(resp)
    }

    fn as_string(&self) -> anyhow::Result<String> {
        match self {
            Self::Bulk(resp) => String::from_utf8(resp.to_vec()).context("Invalid String"),
            Self::Simple(resp) => Ok(resp.clone()),
            _ => Err(anyhow::anyhow!("Not valid RESP for a string")),
        }
    }
}

fn read_line<'a>(cur: &mut Cursor<&'a [u8]>) -> anyhow::Result<&'a [u8]> {
    let chunk = cur.chunk();
    let start = cur.get_ref().len() - chunk.len();

    if let Some(pos) = chunk.windows(2).position(|b| b == b"\r\n") {
        cur.advance(pos + 2);
        return Ok(&cur.get_ref()[start..pos + start]);
    }
    Err(anyhow::anyhow!("Failed to read line"))
}

fn slice_to_int<T>(slice: &[u8]) -> anyhow::Result<T>
where
    T: FromRadix10SignedChecked,
{
    atoi::atoi::<T>(slice).context("Failed to parse length")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resp() {
        let echo = b"*2\r\n$4\r\necho\r\n$3\r\nhey\r\n";
        let ping = b"*1\r\n$4\r\nping\r\n";

        let resp = [echo.as_ref(), ping.as_ref()].concat();

        let mut cur = Cursor::new(resp.as_ref());
        let resp = |cur: &mut Cursor<&[u8]>| Resp::parse(cur).unwrap();

        {
            let expected = Resp::Array(vec![Resp::Bulk("echo".into()), Resp::Bulk("hey".into())]);
            pretty_assertions::assert_eq!(resp(&mut cur), expected);
        }
        assert!(cur.remaining() == ping.len());
        {
            let expected = Resp::Array(vec![Resp::Bulk("ping".into())]);
            pretty_assertions::assert_eq!(resp(&mut cur), expected);
        }

        assert!(!cur.has_remaining());
    }
}
