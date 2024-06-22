use anyhow::Context;
use bytes::Bytes;

use crate::Resp;

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
            .context("Expected bulk string")
    }

    pub fn execute(self) -> Resp {
        Resp::Bulk(self.msg)
    }
}
