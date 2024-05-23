use anyhow::ensure;
use bytes::{Buf, Bytes};
use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{BitAnd, BitOr, Shr},
    str::from_utf8 as str_utf8,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
#[allow(dead_code)]
pub struct Rdb {
    version: u32,
    aux_fields: AuxFields,
    pub(crate) db: Db,
    checksum: Bytes,
}

impl Rdb {
    // https://rdb.fnordig.de/file_format.html
    pub fn parse(mut bytes: Bytes) -> anyhow::Result<Self> {
        tracing::trace!("Parsing rdb: {bytes:?}");

        ensure!(&*bytes.split_to(5) == b"REDIS", "Expected magic string");

        let version = str_utf8(&bytes.split_to(4))?.parse::<u32>()?;
        tracing::debug!("Parsed version: {version:?}");

        let aux_fields = AuxFields::parse(&mut bytes);
        tracing::debug!("Parsed aux_fields: {aux_fields:#?}");

        let db = Db::parse(&mut bytes)?;
        tracing::debug!("Parsed db: {db:#?}");

        ensure!(bytes.get_u8() == 0xff, "End of RDB");
        let checksum = bytes.split_to(8);
        ensure!(bytes.is_empty());

        let rdb = Self {
            version,
            aux_fields,
            db,
            checksum,
        };
        tracing::info!("Parsed rdb: {rdb:#?}");
        Ok(rdb)
    }

    fn parse_string(string: &mut Bytes) -> Bytes {
        tracing::trace!("Parsing string: {string:?}");

        let (len, encoded) = Self::parse_len(string);
        let string = if encoded {
            Self::parse_int_str(string, len)
        } else {
            string.split_to(len as usize)
        };
        tracing::trace!("Parsed string: {string:?}");
        string
    }

    fn parse_len(bytes: &mut Bytes) -> (u32, bool) {
        let encoding = bytes.get_u8();
        // 0b1100_0000 -> 0b______11
        let first_2_bits = encoding.shr(6_u8);
        tracing::trace!("length encoding first 2 bits: {first_2_bits:02b}");

        let mut is_encoded = false;
        let len = match first_2_bits {
            0b00 => u32::from(encoding.bitand(0b00_111_111)),
            0b01 => {
                let byte = bytes.get_u8();
                u32::from(encoding.bitand(0b00_111_111).bitor(byte))
            }
            0b10 => bytes.get_u32(),
            0b11 => {
                is_encoded = true;
                u32::from(encoding.bitand(0b00_111_111))
            }
            _ => unreachable!(),
        };

        tracing::trace!("Parsed len: {len}");
        (len, is_encoded)
    }

    fn parse_int_str(str: &mut Bytes, fmt: u32) -> Bytes {
        match fmt {
            0 => str.get_i8().to_string().into(),
            1 => str.get_i16_le().to_string().into(),
            2 => str.get_i32_le().to_string().into(),
            3 => todo!("lzf"),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AuxFields {
    // FIXME parsed bytes
    redis_ver: Option<Bytes>,
    redis_bits: Option<Bytes>,
    ctime: Option<Bytes>,
    used_mem: Option<Bytes>,
    aof_base: Option<Bytes>,
}

impl AuxFields {
    const AUX_FIELDS: u8 = 0xfa;

    fn parse(bytes: &mut Bytes) -> Self {
        let mut redis_ver = None;
        let mut redis_bits = None;
        let mut ctime = None;
        let mut used_mem = None;
        let mut aof_base = None;

        loop {
            if bytes.chunk()[0] != Self::AUX_FIELDS {
                break;
            }
            bytes.advance(1);

            let (key, value) = {
                let key = Rdb::parse_string(bytes);
                let value = Rdb::parse_string(bytes);
                (key, value)
            };

            match key.as_ref() {
                b"redis-ver" => redis_ver.insert(value),
                b"redis-bits" => redis_bits.insert(value),
                b"ctime" => ctime.insert(value),
                b"used-mem" => used_mem.insert(value),
                b"aof-base" => aof_base.insert(value),
                _ => {
                    tracing::info!("Unknown aux field: {key:?}");
                    continue;
                }
            };
        }
        Self {
            redis_ver,
            redis_bits,
            ctime,
            used_mem,
            aof_base,
        }
    }
}

#[derive(Debug)]
pub struct Db {
    pub(crate) maps: Vec<HashMap<String, Value>>,
}

impl Db {
    const DB_SELECTOR: u8 = 0xFE;
    const RESIZEDB: u8 = 0xFB;

    const EXPIRE_S: u8 = 0xFD;
    const EXPIRE_MS: u8 = 0xFC;

    fn parse(bytes: &mut Bytes) -> anyhow::Result<Self> {
        let mut maps = Vec::new();
        loop {
            if bytes.chunk()[0] != Self::DB_SELECTOR {
                break;
            }
            bytes.advance(1);

            let (_db_num, _) = Rdb::parse_len(bytes);

            ensure!(bytes.get_u8() == Self::RESIZEDB);

            let (db_size, _exp_size) = Self::parse_size(bytes);
            let map = (0..db_size)
                .map(|_| Self::parse_entry(bytes))
                .collect::<anyhow::Result<HashMap<_, _>>>()?;
            maps.push(map);
        }
        Ok(Self { maps })
    }

    fn parse_size(bytes: &mut Bytes) -> (u32, u32) {
        let (db, _) = Rdb::parse_len(bytes);
        tracing::trace!("db size: {db:?}");
        let (exp, _) = Rdb::parse_len(bytes);
        tracing::trace!("expiry size: {exp:?}");
        (db, exp)
    }

    fn parse_entry(bytes: &mut Bytes) -> anyhow::Result<(String, Value)> {
        let expiration: Option<SystemTime>;
        let flag: u8;

        match bytes.get_u8() {
            Self::EXPIRE_S => {
                let dur = u64::from(bytes.get_u32());
                let time = UNIX_EPOCH + Duration::from_secs(dur);
                expiration = Some(time);
                flag = bytes.get_u8();
            }
            Self::EXPIRE_MS => {
                let dur = bytes.get_u64_le();
                let time = UNIX_EPOCH + Duration::from_millis(dur);
                expiration = Some(time);
                flag = bytes.get_u8();
            }
            b => {
                expiration = None;
                flag = b;
            }
        }

        let (key, value) = {
            let key = {
                let string = Rdb::parse_string(bytes);
                str_utf8(&string)?.to_owned()
            };
            let value = {
                let v_type = Type::parse(bytes, flag);
                Value { v_type, expiration }
            };
            (key, value)
        };
        tracing::debug!("Parsed entry: key: {key:?}; value: {value:?}");
        Ok((key, value))
    }
}

// TODO use the one from db.rs
pub struct Value {
    pub(crate) v_type: Type,
    pub(crate) expiration: Option<SystemTime>,
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Value")
            .field("type", &self.v_type)
            .field(
                "expiration",
                &self.expiration.map(chrono::DateTime::<chrono::Local>::from),
            )
            .finish()
    }
}

#[derive(Debug)]
pub enum Type {
    String(Bytes),
    // List,
    // Set,
    // SortedSet,
    // Hash,
    // Zipmap,
    // Ziplist,
    // Intset Encoding,
    // Sorted Set in Ziplist Encoding,
    // Hashmap in Ziplist Encoding,
    // List in Quicklist Encoding,
}

impl Type {
    fn parse(bytes: &mut Bytes, flag: u8) -> Self {
        match flag {
            0 => {
                let string = Rdb::parse_string(bytes);
                Self::String(string)
            }
            _ => unimplemented!("flag: {flag}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;

    use super::*;

    #[test]
    #[traced_test]
    fn parse_len() {
        // 0b00
        {
            let mut bytes = Bytes::from_static(&[0b0010_0000]);
            pretty_assertions::assert_eq!(Rdb::parse_len(&mut bytes).0, 0b10_0000);
        }

        // 0b01
        {
            let mut bytes = Bytes::from_static(&[0b0100_0010, 0b000_0001]);
            pretty_assertions::assert_eq!(Rdb::parse_len(&mut bytes).0, 0b11);
        }

        // 0b10
        {
            let bytes = [[0b1000_0000].as_ref(), 1_u32.to_be_bytes().as_ref()].concat();
            let mut bytes = Bytes::from(bytes);
            pretty_assertions::assert_eq!(Rdb::parse_len(&mut bytes).0, 1_u32);
        }

        // 0b11
        {
            let mut bytes = Bytes::from_static(&[0b1100_0000]);
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 0);

            let mut bytes = Bytes::from_static(&[0b1100_0001]);
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 1);

            let mut bytes = Bytes::from_static(&[0b1100_0010]);
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 2);

            let mut bytes = Bytes::from_static(&[0b1100_0011]);
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 3);
        }
    }

    #[test]
    #[traced_test]
    fn parse_encoded_str() {
        // 0
        {
            let mut bytes =
                Bytes::from([[0b1100_0000].as_ref(), i8::MIN.to_le_bytes().as_ref()].concat());
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 0);
            pretty_assertions::assert_eq!(
                Rdb::parse_int_str(&mut bytes, len),
                Bytes::from(i8::MIN.to_string())
            );
        }
        // 1
        {
            let mut bytes =
                Bytes::from([[0b1100_0001].as_ref(), i16::MIN.to_le_bytes().as_ref()].concat());
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 1);
            pretty_assertions::assert_eq!(
                Rdb::parse_int_str(&mut bytes, len),
                Bytes::from(i16::MIN.to_string())
            );
        }
        // 2
        {
            let mut bytes =
                Bytes::from([[0b1100_0010].as_ref(), i32::MIN.to_le_bytes().as_ref()].concat());
            let (len, encoded) = Rdb::parse_len(&mut bytes);
            assert!(encoded);
            pretty_assertions::assert_eq!(len, 2);
            pretty_assertions::assert_eq!(
                Rdb::parse_int_str(&mut bytes, len),
                Bytes::from(i32::MIN.to_string())
            );
        }
        // TODO
        // 3
        // {
        //     let mut bytes = Bytes::from_static(&[0b1100_0011]);
        //     let (len, encoded) = Rdb::parse_len(&mut bytes);
        //     assert!(encoded);
        //     pretty_assertions::assert_eq!(len, 3);
        // }
    }
}
