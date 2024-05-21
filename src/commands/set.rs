use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;

use crate::{resp::slice_to_int, Handler, Resp, DB};

use super::IterResp;

pub struct Set {
    key: String,
    value: Bytes,
    expiry: Option<Duration>,
}

impl Set {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing Key")?.as_string()?;
        let value = i.next().context("Missing Value")?.as_bytes()?;
        let expiry = i.next().and_then(|x| {
            let expiry = x.as_bulk()?;
            let dur = i
                .next()
                .and_then(|x| x.as_bytes().and_then(slice_to_int).ok());

            match expiry.to_ascii_lowercase().as_slice() {
                b"px" => dur.map(Duration::from_millis),
                b"ex" => dur.map(Duration::from_secs),
                _ => todo!(),
            }
        });
        Ok(Self { key, value, expiry })
    }

    pub fn apply(self) {
        DB.set(self.key, self.value, self.expiry);
    }

    pub async fn apply_and_respond(self, handler: &mut Handler) -> anyhow::Result<()> {
        DB.set(self.key, self.value, self.expiry);
        handler.write(&Resp::simple("OK")).await?;
        Ok(())
    }
}
