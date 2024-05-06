use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{args::Role, Resp};

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
        write_to(&mut self.stream, resp).await
    }
}

async fn write_to<S>(stream: &mut S, resp: &Resp) -> anyhow::Result<()>
where
    S: AsyncWriteExt + std::marker::Unpin + std::marker::Send,
{
    match resp {
        Resp::Simple(inner) => {
            stream.write_u8(b'+').await?;
            stream.write_all(inner.as_bytes()).await?;
            stream.write_all(b"\r\n").await?;
        }
        Resp::Bulk(inner) => {
            stream.write_u8(b'$').await?;
            stream.write_all(inner.len().to_string().as_bytes()).await?;
            stream.write_all(b"\r\n").await?;
            stream.write_all(inner).await?;
            stream.write_all(b"\r\n").await?;
        }
        Resp::Array(elems) => {
            stream.write_u8(b'*').await?;
            stream.write_all(elems.len().to_string().as_bytes()).await?;
            stream.write_all(b"\r\n").await?;
            for resp in elems {
                Box::pin(write_to(stream, resp)).await?;
            }
        }
        Resp::Null => stream.write_all(b"$-1\r\n").await?,
    };
    stream.flush().await?;
    Ok(())
}

pub async fn connect_slave(role: &Role) -> anyhow::Result<Option<TcpStream>> {
    if let Role::Slave(addr) = role {
        let mut master = TcpStream::connect(addr).await?;
        let resp = Resp::Array(vec![Resp::Bulk("PING".into())]);
        write_to(&mut master, &resp).await?;
        Ok(Some(master))
    } else {
        Ok(None)
    }
}
