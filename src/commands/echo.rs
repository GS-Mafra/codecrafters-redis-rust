use anyhow::anyhow;
use bytes::Bytes;

use crate::{Handler, Resp};

use super::IterResp;

#[derive(Debug)]
pub struct Echo {
    msg: Bytes,
}

impl Echo {
    fn new(msg: Bytes) -> Self {
        Self { msg }
    }

    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        i.next()
            .and_then(Resp::as_bulk)
            .cloned()
            .map(Self::new)
            .ok_or_else(|| anyhow!("Expected bulk string"))
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        handler.write(&Resp::Bulk(self.msg.clone())).await?;
        Ok(())
    }
}
