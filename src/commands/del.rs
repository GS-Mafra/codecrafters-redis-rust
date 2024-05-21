use crate::{Handler, Resp, DB};

use super::IterResp;

pub struct Del<'a> {
    keys: IterResp<'a>,
}

impl<'a> Del<'a> {
    pub(super) const fn parse(i: IterResp<'a>) -> Self {
        Self { keys: i }
    }

    pub fn apply(&self) -> usize {
        DB.multi_del(self.keys.as_ref().iter().flat_map(Resp::as_string))
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let deleted = DB.multi_del(self.keys.as_ref().iter().flat_map(Resp::as_string));
        let resp = Resp::Integer(i64::try_from(deleted)?);
        handler.write(&resp).await?;
        Ok(())
    }
}
