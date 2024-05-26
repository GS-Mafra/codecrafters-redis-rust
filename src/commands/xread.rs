use std::str::from_utf8 as str_utf8;

use anyhow::{ensure, Context};

use crate::{
    db::{stream::EntryId, Stream},
    Handler, Resp, DB,
};

use super::IterResp;

#[derive(Debug)]
pub struct Xread {
    keys_ids: Vec<(String, EntryId)>,
}

impl Xread {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        ensure!(
            i.next().is_some_and(|x| x
                .as_bulk()
                .is_some_and(|x| x.eq_ignore_ascii_case(b"streams"))),
            "Expected \"streams\" argument"
        );

        let slice = i.as_slice();
        ensure!(slice.len() % 2 == 0, "Invalid number of arguments");

        let half = slice.len() / 2;
        let keys_ids = slice[..half].iter().zip(slice[half..].iter()).try_fold(
            Vec::with_capacity(half),
            |mut acc, (key, id)| {
                let key = key.to_string()?;
                let id = id
                    .as_bulk()
                    .map(|id| str_utf8(id))
                    .transpose()?
                    .map(|id| EntryId::split_or_seq(0, id))
                    .transpose()?
                    .context("Invalid id")?;
                acc.push((key, id));
                anyhow::Ok(acc)
            },
        )?;

        Ok(Self { keys_ids })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = {
            let lock = DB.inner.read();

            let resp = self
                .keys_ids
                .iter()
                .try_fold(Vec::new(), |mut acc, (key, id)| {
                    let stream = lock
                        .get(key)
                        .and_then(|x| x.v_type.as_stream())
                        .with_context(|| format!("XREAD on invalid key: \"{key}\""))?;

                    let mut key_entries = Vec::with_capacity(2);
                    key_entries.push(Resp::bulk(key.clone()));

                    let entries = Stream::format_entries(stream.inner.range(id..).skip(1));
                    key_entries.push(Resp::Array(entries));

                    acc.push(Resp::Array(key_entries));
                    anyhow::Ok(acc)
                })?;
            Resp::Array(resp)
        };

        handler.write(&resp).await?;
        Ok(())
    }
}
