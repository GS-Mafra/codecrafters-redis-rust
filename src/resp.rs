use anyhow::{bail, Context};
use atoi::FromRadix10SignedChecked;
use bytes::{Buf, Bytes};
use std::io::Cursor;
use thiserror::Error;

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
    Null,
}

impl Resp {
    const CRLF_LEN: usize = b"\r\n".len();

    pub fn parse_rdb(cur: &mut Cursor<&[u8]>) -> anyhow::Result<Bytes> {
        if get_u8(cur)? != b'$' {
            bail!("Not a rdb");
        }
        let len = slice_to_int::<usize>(read_line(cur)?)?;
        let data = Bytes::copy_from_slice(&cur.chunk()[..len]);
        advance(cur, len)?;
        Ok(data)
    }

    pub fn parse(cur: &mut Cursor<&[u8]>) -> anyhow::Result<Self> {
        tracing::trace!("Parsing: {:?}", Bytes::copy_from_slice(cur.chunk()));

        let resp = match get_u8(cur)? {
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
                advance(cur, len + Self::CRLF_LEN)?;
                Self::Bulk(data)
            }
            b':' => Self::Integer(slice_to_int::<i64>(read_line(cur)?)?),
            c => unimplemented!("{:?}", c as char),
        };
        tracing::debug!("Parsed {resp:?}");

        Ok(resp)
    }

    pub fn check(cur: &mut Cursor<&[u8]>) -> Result<(), Error> {
        tracing::trace!("Checking: {:?}", Bytes::copy_from_slice(cur.chunk()));

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

                advance(cur, len + Self::CRLF_LEN)?;
            }
            c => unimplemented!("{:?}", c as char),
        }
        Ok(())
    }

    pub(crate) fn as_string(&self) -> anyhow::Result<String> {
        match self {
            Self::Bulk(resp) => String::from_utf8(resp.to_vec()).context("Invalid String"),
            Self::Simple(resp) => Ok(resp.clone()),
            resp => bail!("Not valid RESP for a string: {resp:?}"),
        }
    }

    pub(crate) fn as_bytes(&self) -> anyhow::Result<Bytes> {
        Ok(match self {
            Self::Bulk(inner) => inner.clone(),
            Self::Simple(inner) => Bytes::copy_from_slice(inner.as_bytes()),
            resp => bail!("Not valid RESP for bytes {resp:?}"),
        })
    }

    #[inline]
    pub(crate) const fn as_bulk(&self) -> Option<&Bytes> {
        match self {
            Self::Bulk(inner) => Some(inner),
            _ => None,
        }
    }

    #[inline]
    pub(crate) const fn as_array(&self) -> Option<&Vec<Self>> {
        match self {
            Self::Array(elems) => Some(elems),
            _ => None,
        }
    }

    #[inline]
    pub(crate) const fn as_simple(&self) -> Option<&String> {
        match self {
            Self::Simple(inner) => Some(inner),
            _ => None,
        }
    }

    pub(crate) fn to_int<T: FromRadix10SignedChecked>(&self) -> anyhow::Result<T> {
        Ok(match self {
            Self::Bulk(resp) => slice_to_int(resp)?,
            Self::Simple(resp) => slice_to_int(resp.as_bytes())?,
            // Self::Integer(resp) => *resp,
            resp => bail!("Not valid RESP for a int {resp:?}"),
        })
    }

    #[inline]
    pub(crate) fn bulk(b: impl Into<Bytes>) -> Self {
        Self::Bulk(b.into())
    }

    #[inline]
    pub(crate) fn simple(s: impl Into<String>) -> Self {
        Self::Simple(s.into())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        let mut len = 1_usize;
        let int_len = |int: usize| int.checked_ilog10().unwrap_or(0) as usize + 1;

        match self {
            Self::Simple(inner) => len += inner.len() + Self::CRLF_LEN,
            Self::Bulk(inner) => {
                len += int_len(inner.len()) + Self::CRLF_LEN + inner.len() + Self::CRLF_LEN;
            }
            Self::Array(elems) => {
                len += int_len(elems.len())
                    + Self::CRLF_LEN
                    + elems.iter().fold(0, |acc, x| acc + Self::len(x));
            }
            #[allow(clippy::cast_possible_truncation)]
            Self::Integer(inner) => {
                if inner.is_negative() {
                    len += 1;
                }
                len += int_len(inner.unsigned_abs() as usize) + Self::CRLF_LEN;
            }
            Self::Data(inner) => len += int_len(inner.len()) + Self::CRLF_LEN + inner.len(),
            Self::Null => len += b"-1".len() + Self::CRLF_LEN,
        };
        len
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

pub fn slice_to_int<T>(slice: impl AsRef<[u8]>) -> anyhow::Result<T>
where
    T: FromRadix10SignedChecked,
{
    atoi::atoi::<T>(slice.as_ref()).context("Failed to parse int from slice")
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
        pretty_assertions::assert_eq!(cur.remaining(), ping.len());
        {
            let expected = Resp::Array(vec![Resp::bulk("ping")]);
            pretty_assertions::assert_eq!(resp(&mut cur), expected);
        }

        assert!(!cur.has_remaining());
    }

    #[test]
    fn len() {
        let to_resp = |bytes: &[u8]| Resp::parse(&mut Cursor::new(bytes)).unwrap();

        let array_bulk = b"*2\r\n$4\r\necho\r\n$3\r\nhey\r\n";
        pretty_assertions::assert_eq!(to_resp(array_bulk).len(), array_bulk.len());

        let neg_int = format!(":{}\r\n", i64::MIN);
        pretty_assertions::assert_eq!(to_resp(neg_int.as_bytes()).len(), neg_int.len());

        let int = format!(":{}\r\n", i64::MAX);
        pretty_assertions::assert_eq!(to_resp(int.as_bytes()).len(), int.len());

        let null = b"$-1\r\n";
        pretty_assertions::assert_eq!(to_resp(null).len(), null.len());

        let simple = b"+OK\r\n";
        pretty_assertions::assert_eq!(to_resp(simple).len(), simple.len());
    }
}
