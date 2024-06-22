use anyhow::Context;

use crate::{db::Type as DbType, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Type {
    key: String,
}

impl Type {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        Ok(Self { key })
    }

    pub fn execute(&self) -> Resp {
        let ty = DB
            .inner
            .read()
            .get(&self.key)
            .map_or("none", |v| match v.v_type {
                DbType::String(_) => "string",
                DbType::Stream(_) => "stream",
            });
        Resp::simple(ty)
    }
}
