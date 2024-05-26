use anyhow::Context;
use std::{ops::RangeInclusive, str::from_utf8 as str_utf8, time::Duration};

use crate::{
    db::{stream::EntryId, Type},
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
            .map_or_else(Vec::new, |values| {
                values.fold(Vec::new(), |mut acc, (id, key_values)| {
                    let mut values = vec![Resp::bulk(id.to_string())];

                    let k_v = key_values.iter().fold(Vec::new(), |mut acc, (key, value)| {
                        acc.push(Resp::bulk(key.clone()));
                        acc.push(Resp::bulk(value.clone()));
                        acc
                    });

                    values.push(Resp::Array(k_v));

                    acc.push(Resp::Array(values));
                    acc
                })
            });
        handler.write(&Resp::Array(resp)).await?;
        Ok(())
    }
}

fn to_entry_id(resp: &Resp, start: bool) -> anyhow::Result<EntryId> {
    resp.as_bulk()
        .map(|id| str_utf8(id))
        .transpose()?
        .map(|x| {
            if let Some((ms_time, sq_num)) = x.rsplit_once('-') {
                let ms_time = Duration::from_millis(ms_time.parse::<u64>()?);
                let sq_num = sq_num.parse::<u64>()?;
                anyhow::Ok(EntryId::new(ms_time, sq_num))
            } else {
                let ms_time = Duration::from_millis(x.parse::<u64>()?);
                let sq_num: u64 = if start { 0 } else { u64::MAX };
                anyhow::Ok(EntryId::new(ms_time, sq_num))
            }
        })
        .transpose()?
        .context("Invalid value for range")
}
