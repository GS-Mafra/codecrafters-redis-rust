use std::collections::hash_map::Entry;

use anyhow::Context;

use crate::{
    db::{Type, Value},
    slice_to_int, Handler, Resp, DB,
};

use super::IterResp;

#[derive(Debug)]
pub struct Incr {
    key: String,
}

impl Incr {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let key = i.next().context("Missing key").and_then(Resp::to_string)?;
        Ok(Self { key })
    }

    pub fn apply(self) -> anyhow::Result<i64> {
        let mut lock = DB.inner.write();
        let entry = lock.entry(self.key);
        // TODO store as int? https://redis.io/docs/latest/commands/incr/
        let res = match entry {
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                let value = entry
                    .v_type
                    .as_string()
                    .context("WRONGTYPE Operation against a key holding the wrong kind of value")
                    .and_then(|x| {
                        slice_to_int::<i64>(x)
                            .context("ERR value is not an integer or out of range")
                    })
                    .and_then(|x| x.checked_add(1).context("ERR increment would overflow"))?;
                entry.v_type = Type::String(value.to_string().into());
                value
            }
            Entry::Vacant(entry) => {
                let val = 1;
                entry.insert(Value::new_no_expiry_string(val.to_string().into()));
                val
            }
        };
        drop(lock);
        Ok(res)
    }

    pub async fn apply_and_respond(self, handler: &mut Handler) -> anyhow::Result<()> {
        let res = self.apply()?;
        handler.write(&Resp::Integer(res)).await?;
        Ok(())
    }
}
