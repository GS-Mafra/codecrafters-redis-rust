use anyhow::Context;

use crate::{Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Get {
    pub(crate) key: String,
}

impl Get {
    pub const fn new(key: String) -> Self {
        Self { key }
    }

    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        Ok(Self { key })
    }

    pub fn execute(&self) -> anyhow::Result<Resp> {
        let value = DB
            .get(self)
            .map(|v| v.v_type.as_string().context("Invalid type").cloned())
            .transpose()?
            .map_or(Resp::Null, Resp::Bulk);
        Ok(value)
    }
}
