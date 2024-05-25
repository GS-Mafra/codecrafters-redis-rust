use anyhow::Context;
use glob_match::glob_match;

use crate::{Handler, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Keys {
    pat: String,
}

impl Keys {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let pat = i
            .next()
            .context("Missing pattern")
            .and_then(Resp::to_string)?;
        Ok(Self { pat })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let keys = DB
            .inner
            .read()
            .keys()
            .filter(|x| glob_match(&self.pat, x))
            .cloned()
            .map(Resp::bulk)
            .collect::<Vec<_>>();
        let resp = Resp::Array(keys);
        handler.write(&resp).await?;
        Ok(())
    }
}
