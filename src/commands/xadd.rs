use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use std::{collections::HashMap, str::from_utf8 as str_utf8, time::Duration};

use crate::{db::stream::EntryId, Handler, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Xadd {
    pub(crate) key: String,
    pub(crate) id: EntryId,
    pub(crate) k_v: HashMap<String, Bytes>,
}

impl Xadd {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;
        let id = i
            .next()
            .context("Missing id")?
            .as_bulk()
            .map(|id| str_utf8(id))
            .transpose()?
            .map(|id| {
                id.rsplit_once('-')
                    .context("Invalid format for key: <millisecondsTime>-<sequenceNumber>")
            })
            .transpose()?
            .map(|(ms_time, sq_num)| {
                let ms_time = Duration::from_millis(ms_time.parse::<u64>()?);
                let sq_num = sq_num.parse::<u64>()?;
                ensure!(sq_num > 0, "ERR The ID specified in XADD must be greater than 0-0");
                let entry_id = EntryId::new(ms_time, sq_num);
                anyhow::Ok(entry_id)
            })
            .transpose()?
            .context("Invalid value for id")?;

        let mut k_v = HashMap::new();
        while let Some(key) = i.next() {
            let Some(value) = i.next() else {
                bail!("No value found for key: {key:?}");
            };
            let k = key
                .as_bulk()
                .map(|k| str_utf8(k).map(str::to_owned))
                .transpose()?;
            let v = value.as_bulk().cloned();
            k.zip(v)
                .context("Invalid values for key-value pair")
                .map(|(k, v)| k_v.insert(k, v))?;
        }
        ensure!(!k_v.is_empty(), "Missing key-value pairs");
        Ok(Self { key, id, k_v })
    }

    #[inline]
    pub fn apply(self) -> anyhow::Result<()> {
        DB.xadd(self)
    }

    pub async fn apply_and_respond(self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = Resp::bulk(self.id.to_string());
        Self::apply(self)?;
        handler.write(&resp).await?;
        Ok(())
    }
}
