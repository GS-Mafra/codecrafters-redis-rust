use anyhow::bail;
use std::{
    collections::BTreeMap,
    fmt::Display,
    time::{Duration, UNIX_EPOCH},
};

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
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
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
