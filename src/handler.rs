use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{io::{AsyncReadExt, AsyncWriteExt, BufWriter}, net::TcpStream};

use crate::Resp;


pub struct Handler {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl Handler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(1024),
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
        let resp = Resp::parse(&mut cur).map(Option::Some);
        self.buf.advance(cur.position().try_into()?);
        resp
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
                self.write_int(inner.len()).await?;
                self.stream.write_all(inner).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Resp::Array(elems) => {
                self.stream.write_u8(b'*').await?;
                self.write_int(elems.len()).await?;
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
