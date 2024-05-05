use std::io::Cursor;

use anyhow::Context;
use atoi::FromRadix10SignedChecked;
use bytes::{Buf, Bytes};

use crate::debug_print;

#[derive(Debug, PartialEq, Clone)]
pub enum Resp {
    Simple(String),
    Bulk(Bytes),
    Array(Vec<Self>),
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
        debug_print!("Parsed {resp:?}");

        Ok(resp)
    }

    pub(crate) fn as_string(&self) -> anyhow::Result<String> {
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
