use anyhow::ensure;

use super::IterResp;

pub struct Exec;

impl Exec {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<()> {
        ensure!(
            i.next().is_none(),
            "ERR wrong number of arguments for 'exec' command"
        );
        Ok(())
    }
}
