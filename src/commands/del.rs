use crate::{Handler, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub(super) fn parse(i: IterResp) -> Self {
        Self {
            keys: i.flat_map(Resp::to_string).collect(),
        }
    }

    pub fn apply(&self) -> usize {
        DB.del(&self.keys)
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let deleted = self.apply();
        let resp = Resp::Integer(i64::try_from(deleted)?);
        handler.write(&resp).await?;
        Ok(())
    }
}
