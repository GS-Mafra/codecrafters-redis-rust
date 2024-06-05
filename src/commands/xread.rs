use std::{
    ops::Bound::{Excluded, Unbounded},
    str::from_utf8 as str_utf8,
    time::Duration,
};

use anyhow::{bail, ensure, Context};

use crate::{
    db::{stream::EntryId, Stream},
    Handler, Resp, DB,
};

use super::IterResp;

#[derive(Debug)]
pub struct Xread {
    block_time: Option<Duration>,
    keys_ids: Vec<(String, EntryId)>,
}

impl Xread {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let mut block_time = None;

        while let Some(arg) = i.next().and_then(Resp::as_bulk) {
            match arg.to_ascii_lowercase().as_slice() {
                b"block" => {
                    block_time = i
                        .next()
                        .and_then(Resp::as_bulk)
                        .map(|x| str_utf8(x))
                        .transpose()?
                        .map(str::parse::<u64>)
                        .transpose()?
                        .map(Duration::from_millis);
                }
                b"count" => unimplemented!("count"),
                b"streams" => break,
                _ => bail!("Expected \"streams\" argument"),
            }
        }

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

        Ok(Self {
            block_time,
            keys_ids,
        })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = 'resp: {
            let resp = self.get_key_entries()?;
            if resp != Resp::Null {
                break 'resp resp;
            }

            if let Some(block_time) = self.block_time {
                if self.first_unblocked(block_time).await {
                    break 'resp self.get_key_entries()?;
                }
            }
            Resp::Null
        };

        handler.write(&resp).await?;
        Ok(())
    }

    fn get_key_entries(&self) -> anyhow::Result<Resp> {
        let lock = DB.inner.read();

        let mut v = Vec::new();
        for (key, id) in &self.keys_ids {
            let Some(stream) = lock
                .get(key)
                .map(|x| {
                    x.v_type
                        .as_stream()
                        .with_context(|| format!("XREAD on invalid key: \"{key}\""))
                })
                .transpose()?
            else {
                continue;
            };

            let range = (Excluded(id), Unbounded);
            let entries = Stream::format_entries(stream.inner.range(range));
            if entries.is_empty() {
                continue;
            }

            let key_entries = vec![Resp::bulk(key.clone()), Resp::Array(entries)];
            v.push(Resp::Array(key_entries));
        }
        drop(lock);

        Ok(if v.is_empty() {
            Resp::Null
        } else {
            Resp::Array(v)
        })
    }

    async fn first_unblocked(&self, block_time: Duration) -> bool {
        let mut set = self
            .keys_ids
            .iter()
            .cloned()
            .map(|(key, id)| async move {
                let mut rx = DB.added_stream.subscribe();
                loop {
                    rx.changed().await.expect("Sender alive");
                    if let Some((added_key, added_id)) = &*rx.borrow_and_update() {
                        if *added_key == key && *added_id > id {
                            break;
                        }
                    }
                }
            })
            .collect::<tokio::task::JoinSet<_>>();

        let sleep = tokio::time::sleep(block_time);
        tokio::pin!(sleep);

        tokio::select! {
            _ = set.join_next() => {
                true
            }
            () = &mut sleep, if block_time.as_millis() != 0 => {
                println!("timed out");
                false
            }
        }
    }
}
