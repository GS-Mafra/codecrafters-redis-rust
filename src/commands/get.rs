use anyhow::Context;

use crate::{Handler, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        Ok(Self { key })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let value = DB
            .get(&self.key)
            .map(|v| v.v_type.as_string().context("Invalid type").cloned())
            .transpose()?
            .map_or(Resp::Null, Resp::Bulk);
        handler.write(&value).await?;
        Ok(())
    }
}
