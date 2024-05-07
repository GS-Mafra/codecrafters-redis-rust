use anyhow::Context;
use atoi::FromRadix10SignedChecked;
use bytes::{Buf, Bytes};
use std::io::Cursor;
use thiserror::Error;

use crate::debug_print;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Incomplete resp")]
    Incomplete,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Resp {
    Simple(String),
    Bulk(Bytes),
    Array(Vec<Self>),
    Integer(i64),
    Data(Bytes),
    // NullBulk
    Null,
}

impl Resp {
    pub fn parse(cur: &mut Cursor<&[u8]>) -> anyhow::Result<Self> {
        debug_print!("Parsing: {:?}", std::str::from_utf8(cur.chunk()));

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
                    advance(cur, b"-1\r\n".len())?;
                    break 'bulk Self::Null;
                }
                let len = slice_to_int::<usize>(read_line(cur)?)?;

                let data = Bytes::copy_from_slice(&cur.chunk()[..len]);
                advance(cur, len + b"\r\n".len())?;
                Self::Bulk(data)
            }
            b':' => Self::Integer(slice_to_int::<i64>(read_line(cur)?)?),
            c => unimplemented!("{:?}", c as char),
        };
        debug_print!("Parsed {resp:?}");

        Ok(resp)
    }

    pub fn check(cur: &mut Cursor<&[u8]>) -> Result<(), Error> {
        debug_print!("Checking: {:?}", std::str::from_utf8(cur.chunk()));

        match get_u8(cur)? {
            b'*' => {
                let len = slice_to_int::<usize>(read_line(cur)?)?;

                for _ in 0..len {
                    Self::check(cur)?;
                }
            }
            b'+' | b':' => {
                read_line(cur)?;
            }
            b'$' => 'bulk: {
                if &cur.chunk()[..2] == b"-1" {
                    advance(cur, b"-1\r\n".len())?;
                    break 'bulk;
                }
                let len = slice_to_int::<usize>(read_line(cur)?)?;

                advance(cur, len + b"\r\n".len())?;
            }
            c => unimplemented!("{:?}", c as char),
        }
        Ok(())
    }

    pub(crate) fn as_string(&self) -> anyhow::Result<String> {
        match self {
            Self::Bulk(resp) => String::from_utf8(resp.to_vec()).context("Invalid String"),
            Self::Simple(resp) => Ok(resp.clone()),
            _ => Err(anyhow::anyhow!("Not valid RESP for a string")),
        }
    }

    #[inline]
    pub fn bulk(b: impl Into<Bytes>) -> Self {
        Self::Bulk(b.into())
    }

    #[inline]
    pub fn simple(s: impl Into<String>) -> Self {
        Self::Simple(s.into())
    }
}

fn get_u8(cur: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !cur.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(cur.get_u8())
}

fn advance(cur: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if cur.remaining() < n {
        return Err(Error::Incomplete);
    }
    cur.advance(n);
    Ok(())
}

fn read_line<'a>(cur: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let chunk = cur.chunk();
    let start = cur.get_ref().len() - chunk.len();
    if let Some(pos) = chunk.windows(2).position(|b| b == b"\r\n") {
        advance(cur, pos + 2)?;
        return Ok(&cur.get_ref()[start..pos + start]);
    }
    Err(Error::Incomplete)
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
            let expected = Resp::Array(vec![Resp::bulk("echo"), Resp::bulk("hey")]);
            pretty_assertions::assert_eq!(resp(&mut cur), expected);
        }
        assert!(cur.remaining() == ping.len());
        {
            let expected = Resp::Array(vec![Resp::bulk("ping")]);
            pretty_assertions::assert_eq!(resp(&mut cur), expected);
        }

        assert!(!cur.has_remaining());
    }
}
