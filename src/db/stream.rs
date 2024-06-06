use anyhow::bail;
use either::Either;
use std::{
    collections::BTreeMap,
    fmt::Display,
    ops::RangeBounds,
    time::{Duration, UNIX_EPOCH},
};

use crate::Resp;

type StreamInner = BTreeMap<EntryId, StreamValues>;
type StreamValues = Vec<(String, String)>;

#[derive(Debug)]
pub struct Stream {
    pub(crate) inner: StreamInner,
}

impl Stream {
    const SMALL_EQ: &'static str =
        "ERR The ID specified in XADD is equal or smaller than the target stream top item";

    pub(crate) const fn new() -> Self {
        let inner = BTreeMap::new();
        Self { inner }
    }

    pub(super) fn xadd(&mut self, id: EntryId, values: StreamValues) -> String {
        let id_res = id.to_string();
        self.inner.insert(id, values);
        id_res
    }

    pub(crate) fn format_entries<'a, I>(entries: I) -> Vec<Resp>
    where
        I: Iterator<Item = (&'a EntryId, &'a StreamValues)>,
    {
        entries.fold(Vec::new(), |mut acc, (id, key_values)| {
            let mut values = Vec::with_capacity(2);
            values.push(Resp::bulk(id.to_string()));

            let k_v = key_values.iter().fold(
                Vec::with_capacity(key_values.len() * 2),
                |mut acc, (key, value)| {
                    acc.push(Resp::bulk(key.clone()));
                    acc.push(Resp::bulk(value.clone()));
                    acc
                },
            );

            values.push(Resp::Array(k_v));

            acc.push(Resp::Array(values));
            acc
        })
    }

    pub(crate) fn iter_with_count<R>(
        &self,
        count: Option<usize>,
        range: R,
    ) -> impl Iterator<Item = (&EntryId, &StreamValues)>
    where
        R: RangeBounds<EntryId>,
    {
        let range = self.inner.range(range);
        match count {
            Some(count) => Either::Left(range.take(count)),
            None => Either::Right(range),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Clone, Copy)]
pub struct EntryId {
    ms_time: Duration,
    sq_num: u64,
}

impl EntryId {
    pub const MIN: Self = Self::new(Duration::from_millis(u64::MIN), u64::MIN);
    pub const MAX: Self = Self::new(Duration::from_millis(u64::MAX), u64::MAX);

    pub const fn new(ms_time: Duration, sq_num: u64) -> Self {
        Self { ms_time, sq_num }
    }

    pub(crate) fn split_or_seq(sq_num: u64, id: &str) -> anyhow::Result<Self> {
        let res = if let Some((ms_time, sq_num)) = id.rsplit_once('-') {
            let ms_time = Duration::from_millis(ms_time.parse::<u64>()?);
            let sq_num = sq_num.parse::<u64>()?;
            Self::new(ms_time, sq_num)
        } else {
            let ms_time = Duration::from_millis(id.parse::<u64>()?);
            Self::new(ms_time, sq_num)
        };
        Ok(res)
    }
}

impl Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ms_time}-{sq_num}",
            ms_time = self.ms_time.as_millis(),
            sq_num = self.sq_num
        )
    }
}

#[derive(Debug)]
pub enum MaybeAuto {
    Auto,
    AutoSeq(Duration),
    Set((Duration, u64)),
}

impl MaybeAuto {
    pub(crate) fn auto_generate(self, stream: &Stream) -> anyhow::Result<EntryId> {
        let last_entry = stream.inner.last_key_value();

        let res = match self {
            Self::Set((ms_time, sq_num)) => {
                let entry_id = EntryId::new(ms_time, sq_num);
                if last_entry.is_some_and(|(key, _)| entry_id <= *key) {
                    bail!(Stream::SMALL_EQ);
                }
                entry_id
            }
            Self::AutoSeq(ms_time) => {
                use std::cmp::Ordering::{Equal, Greater, Less};

                let sq_num = last_entry
                    .map_or(Ok(0), |(key, _)| match ms_time.cmp(&key.ms_time) {
                        Less => bail!(Stream::SMALL_EQ),
                        Equal => Ok(key.sq_num + 1),
                        Greater => Ok(0),
                    })
                    .map(|mut num| {
                        if ms_time.as_millis() == 0 && num == 0 {
                            num = 1;
                        }
                        num
                    })?;

                EntryId::new(ms_time, sq_num)
            }
            Self::Auto => {
                let ms_time = UNIX_EPOCH.elapsed()?;
                let sq_num = last_entry.map_or(0, |(last_key, _)| {
                    if last_key.ms_time == ms_time {
                        last_key.sq_num + 1
                    } else {
                        0
                    }
                });

                EntryId::new(ms_time, sq_num)
            }
        };
        Ok(res)
    }
}
