use anyhow::{bail, Context};
use bytes::Bytes;

use crate::{Handler, Resp};

use super::IterResp;

pub struct Echo<'a> {
    msg: &'a Bytes,
}

impl<'a> Echo<'a> {
    pub(super) fn parse(mut i: IterResp<'a>) -> anyhow::Result<Self> {
        let Some(msg) = i.next().context("Missing echo value")?.as_bulk() else {
            bail!("Expected bulk string");
        };
        Ok(Self { msg })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        handler.write(&Resp::Bulk(self.msg.clone())).await?;
        Ok(())
    }
}
