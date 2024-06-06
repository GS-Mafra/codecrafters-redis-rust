use std::time::{Duration, SystemTime};

use anyhow::Context;
use bytes::Bytes;

use crate::{db::Type, slice_to_int, Handler, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Set {
    pub(crate) key: String,
    pub(crate) value: Type,
    pub(crate) expiry: Option<SystemTime>,
}

impl Set {
    pub fn new(key: String, value: Bytes, expiry: Option<Duration>) -> Self {
        let value = Type::String(value);
        let expiry = expiry.map(|x| SystemTime::now() + x);
        Self { key, value, expiry }
    }

    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        let value = i.next().context("Missing Value")?.to_bytes()?;
        let expiry = i.next().and_then(|x| {
            let expiry = x.as_bulk()?;
            let dur = i
                .next()
                .and_then(|x| x.to_bytes().and_then(slice_to_int).ok());

            match expiry.to_ascii_lowercase().as_slice() {
                b"px" => dur.map(Duration::from_millis),
                b"ex" => dur.map(Duration::from_secs),
                _ => todo!(),
            }
        });
        Ok(Self::new(key, value, expiry))
    }

    #[inline]
    pub fn apply(self) {
        DB.set(self);
    }

    pub async fn apply_and_respond(self, handler: &mut Handler) -> anyhow::Result<()> {
        self.apply();
        handler.write(&Resp::simple("OK")).await?;
        Ok(())
    }
}
