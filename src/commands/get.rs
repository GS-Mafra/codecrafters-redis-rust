use anyhow::Context;

use crate::{Handler, Resp, DB};

use super::IterResp;

pub struct Get {
    key: String,
}

impl Get {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing Key")?.as_string()?;
        Ok(Self { key })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let value = DB.get(&self.key).map_or(Resp::Null, Resp::Bulk);
        handler.write(&value).await?;
        Ok(())
    }
}
