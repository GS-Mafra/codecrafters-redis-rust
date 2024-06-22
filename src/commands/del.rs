use crate::{Resp, DB};

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

    pub fn execute(&self) -> anyhow::Result<Resp> {
        let deleted = DB.del(&self.keys);
        let resp = Resp::Integer(i64::try_from(deleted)?);
        Ok(resp)
    }
}
