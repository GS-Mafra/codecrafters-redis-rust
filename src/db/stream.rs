use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    time::Duration,
};

use anyhow::bail;
use bytes::Bytes;

type StreamInner = BTreeMap<EntryId, StreamValues>;
type StreamValues = HashMap<String, Bytes>;

#[derive(Debug)]
pub struct Stream {
    inner: StreamInner,
}

impl Stream {
    pub(crate) fn new(id: EntryId, values: StreamValues) -> Self {
        let mut inner = BTreeMap::new();
        inner.insert(id, values);
        Self { inner }
    }

    pub(super) fn xadd(&mut self, id: EntryId, values: StreamValues) -> anyhow::Result<()> {
        let last_entry = self.inner.last_entry();
        if last_entry.is_some_and(|entry| id <= *entry.key()) {
            bail!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            );
        }

        self.inner.insert(id, values);
        Ok(())
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct EntryId {
    ms_time: Duration,
    sq_num: u64,
}

impl EntryId {
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
