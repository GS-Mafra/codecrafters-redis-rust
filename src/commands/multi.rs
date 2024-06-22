use anyhow::ensure;

use crate::Resp;

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

    #[allow(clippy::unused_self)]
    pub fn execute(&self) -> Resp {
        Resp::simple("OK")
    }
}
