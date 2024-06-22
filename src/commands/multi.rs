use anyhow::ensure;

use crate::{Handler, Resp};

use super::IterResp;

#[derive(Debug)]
pub struct Multi;

impl Multi {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        ensure!(
            i.next().is_none(),
            "ERR wrong number of arguments for 'multi' command"
        );
        Ok(Self)
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = Resp::simple("OK");
        handler.write(&resp).await?;
        Ok(())
    }
}
