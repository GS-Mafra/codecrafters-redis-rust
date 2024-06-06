use anyhow::Context;
use std::{ops::RangeInclusive, str::from_utf8 as str_utf8};

use crate::{
    db::{stream::EntryId, Stream},
    slice_to_int, Handler, Resp, DB,
};

use super::IterResp;

#[derive(Debug)]
pub struct Xrange {
    key: String,
    range: RangeInclusive<EntryId>,
    count: Option<usize>,
}

impl Xrange {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        let range = {
            let start = i
                .next()
                .context("Missing start")
                .and_then(|x| to_entry_id(x, true))?;
            let end = i
                .next()
                .context("Missing end")
                .and_then(|x| to_entry_id(x, false))?;
            start..=end
        };

        let count = i
            .next()
            .and_then(Resp::as_bulk)
            .filter(|x| x.eq_ignore_ascii_case(b"count"))
            .and_then(|_| i.next().and_then(Resp::as_bulk).map(slice_to_int::<usize>))
            .transpose()?;

        Ok(Self { key, range, count })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = DB
            .inner
            .read()
            .get(&self.key)
            .map(|x| {
                x.v_type
                    .as_stream()
                    .with_context(|| format!("XRANGE on invalid key: \"{}\"", self.key))
            })
            .transpose()?
            .map(|stream| stream.iter_with_count(self.count, self.range.start()..=self.range.end()))
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
