use anyhow::Context;
use std::{ops::RangeInclusive, str::from_utf8 as str_utf8};

use crate::{
    db::{stream::EntryId, Stream, Type},
    Handler, Resp, DB,
};

use super::IterResp;

#[derive(Debug)]
pub struct Xrange {
    key: String,
    range: RangeInclusive<EntryId>,
}

impl Xrange {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        let start = i
            .next()
            .context("Missing start")
            .and_then(|x| to_entry_id(x, true))?;
        let end = i
            .next()
            .context("Missing end")
            .and_then(|x| to_entry_id(x, false))?;
        let range = start..=end;
        Ok(Self { key, range })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = DB
            .inner
            .read()
            .get(&self.key)
            .map(|x| Type::as_stream(&x.v_type).context("XRANGE on invalid key"))
            .transpose()?
            .map(|stream| stream.inner.range(self.range.start()..=self.range.end()))
            .map_or_else(Vec::new, Stream::format_entries);
        handler.write(&Resp::Array(resp)).await?;
        Ok(())
    }
}

fn to_entry_id(resp: &Resp, start: bool) -> anyhow::Result<EntryId> {
    resp.as_bulk()
        .map(|id| str_utf8(id))
        .transpose()?
        .map(|x| {
            if x == "-" && start {
                return Ok(EntryId::MIN);
            } else if x == "+" && !start {
                return Ok(EntryId::MAX);
            }

            let default_seq = if start { 0 } else { u64::MAX };
            EntryId::split_or_seq(default_seq, x)
        })
        .transpose()?
        .context("Invalid value for range")
}
