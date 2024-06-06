use std::{
    ops::Bound::{self, Excluded, Included, Unbounded},
    str::from_utf8 as str_utf8,
    time::Duration,
};

use anyhow::{bail, ensure, Context};

use crate::{
    db::{stream::EntryId, Stream},
    slice_to_int, Handler, Resp, DB,
};

use super::IterResp;

#[derive(Debug)]
pub struct Xread {
    block_time: Option<Duration>,
    count: Option<usize>,
    keys_ids: Vec<(String, MaybeTopId)>,
}

impl Xread {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let mut block_time = None;
        let mut count = None;

        while let Some(arg) = i.next().and_then(Resp::as_bulk) {
            match arg.to_ascii_lowercase().as_slice() {
                b"block" => {
                    block_time = i
                        .next()
                        .and_then(Resp::as_bulk)
                        .map(slice_to_int::<u64>)
                        .transpose()?
                        .map(Duration::from_millis);
                }
                b"count" => {
                    count = i
                        .next()
                        .and_then(Resp::as_bulk)
                        .map(slice_to_int::<usize>)
                        .transpose()?;
                }
                b"streams" => break,
                _ => bail!("Expected \"streams\" argument"),
            }
        }

        let slice = i.as_slice();
        ensure!(
            !slice.is_empty() && slice.len() % 2 == 0,
            "Invalid number of arguments"
        );

        let half = slice.len() / 2;
        let keys_ids = slice[..half].iter().zip(slice[half..].iter()).try_fold(
            Vec::with_capacity(half),
            |mut acc, (key, id)| {
                let key = key.to_string()?;
                let id = id
                    .as_bulk()
                    .map(|id| {
                        let id = if id.as_ref() == b"$" {
                            MaybeTopId::Top
                        } else {
                            let id = str_utf8(id).map(|id| EntryId::split_or_seq(0, id))??;
                            MaybeTopId::NotTop(id)
                        };
                        anyhow::Ok(id)
                    })
                    .transpose()?
                    .context("Invalid id")?;
                acc.push((key, id));
                anyhow::Ok(acc)
            },
        )?;

        Ok(Self {
            block_time,
            count,
            keys_ids,
        })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = {
            let sync = {
                let iter = self.keys_ids.iter().filter_map(|(key, id)| match id {
                    MaybeTopId::NotTop(id) => Some((key, (Excluded(*id), Unbounded))),
                    MaybeTopId::Top => None,
                });

                self.get_keys_entries(iter)?
            };

            if sync != Resp::Null {
                sync
            } else if let Some((key, id)) = self.first_unblocked().await? {
                let iter = std::iter::once((&key, (Included(id), Unbounded)));
                self.get_keys_entries(iter)?
            } else {
                Resp::Null
            }
        };

        handler.write(&resp).await?;
        Ok(())
    }

    fn get_keys_entries<'a, I>(&self, i: I) -> anyhow::Result<Resp>
    where
        I: Iterator<Item = (&'a String, (Bound<EntryId>, Bound<EntryId>))>,
    {
        let lock = DB.inner.read();

        let mut v = Vec::new();
        for (key, range) in i {
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

            let entries = Stream::format_entries(stream.iter_with_count(self.count, range));
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

    async fn first_unblocked(&self) -> anyhow::Result<Option<(String, EntryId)>> {
        let Some(block_time) = self.block_time else {
            return Ok(None);
        };

        let mut set = self
            .keys_ids
            .iter()
            .cloned()
            .map(|(key, id)| async move {
                let mut rx = DB.added_stream.subscribe();
                loop {
                    rx.changed().await.expect("Sender alive");
                    let Some((added_key, added_id)) = &*rx.borrow_and_update() else {
                        continue;
                    };

                    if *added_key != key {
                        continue;
                    }

                    match id {
                        MaybeTopId::Top => (),
                        MaybeTopId::NotTop(id) => {
                            if *added_id < id {
                                continue;
                            }
                        }
                    }
                    break (added_key.clone(), *added_id);
                }
            })
            .collect::<tokio::task::JoinSet<_>>();

        let sleep = tokio::time::sleep(block_time);
        tokio::pin!(sleep);

        tokio::select! {
            res = set.join_next() => {
                let (key, id) = res.expect("1 task")?;
                Ok(Some((key,id)))
            }
            () = &mut sleep, if block_time.as_millis() != 0 => {
                println!("timed out");
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum MaybeTopId {
    Top,
    NotTop(EntryId),
}
