use anyhow::{bail, ensure, Context};
use std::{str::from_utf8 as str_utf8, time::Duration};

use crate::{db::stream::MaybeAuto, Handler, Resp, DB};

use super::IterResp;

#[derive(Debug)]
pub struct Xadd {
    pub(crate) key: String,
    pub(crate) id: MaybeAuto,
    pub(crate) k_v: Vec<(String, String)>,
}

impl Xadd {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key")?.to_string()?;

        let id = {
            let id = i
                .next()
                .context("Missing id")?
                .as_bulk()
                .map(|id| str_utf8(id))
                .transpose()?
                .context("Invalid value for id")?;

            match id.rsplit_once('-') {
                Some((ms_time, sq_num)) => {
                    let time = ms_time.parse::<u64>()?;
                    let ms_time = Duration::from_millis(time);
                    if sq_num == "*" {
                        MaybeAuto::AutoSeq(ms_time)
                    } else {
                        let sq_num = sq_num.parse::<u64>()?;
                        if time == 0 {
                            ensure!(
                                sq_num > 0,
                                "ERR The ID specified in XADD must be greater than 0-0"
                            );
                        }
                        MaybeAuto::Set((ms_time, sq_num))
                    }
                }
                None if id.eq("*") => MaybeAuto::Auto,
                None => {
                    bail!("Invalid format for id: <millisecondsTime>-<sequenceNumber> or *")
                }
            }
        };

        let mut k_v = Vec::new();
        while let Some(key) = i.next() {
            let Some(value) = i.next() else {
                bail!("No value found for key: {key:?}");
            };
            let k = key.to_string()?;
            let v = value.to_string()?;
            k_v.push((k, v));
        }
        ensure!(!k_v.is_empty(), "Missing key-value pairs");
        Ok(Self { key, id, k_v })
    }

    #[inline]
    pub fn apply(self) -> anyhow::Result<String> {
        DB.xadd(self)
    }

    pub async fn apply_and_respond(self, handler: &mut Handler) -> anyhow::Result<()> {
        let resp = Resp::bulk(self.apply()?);
        handler.write(&resp).await?;
        Ok(())
    }
}
