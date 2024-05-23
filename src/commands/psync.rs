use anyhow::Context;
use bytes::Bytes;

use crate::{Handler, Master, Resp};

use super::IterResp;

pub struct Psync {
    id: String,
    offset: i64,
}

impl Psync {
    pub(crate) const fn new(id: String, offset: i64) -> Self {
        Self { id, offset }
    }

    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let id = i.next().context("Expected id")?.to_string()?;
        let offset = i.next().context("Expected offset")?.to_int()?;
        Ok(Self { id, offset })
    }

    pub async fn apply_and_respond(
        &self,
        handler: &mut Handler,
        master: &Master,
    ) -> anyhow::Result<()> {
        let master_replid = master.replid();
        let master_repl_offset = master.repl_offset();

        let resp = Resp::Simple(format!("FULLRESYNC {master_replid} {master_repl_offset}"));
        handler.write(&resp).await?;
        handler.write(&get_data()?).await?;
        Ok(())
    }

    pub(crate) fn into_resp(self) -> Resp {
        Resp::Array(vec![
            Resp::bulk("psync"),
            Resp::bulk(self.id),
            Resp::bulk(self.offset.to_string()),
        ])
    }
}

fn get_data() -> anyhow::Result<Resp> {
    // TODO
    const DATA: &str = "524544495330303131fa0972656469732d76657\
    205372e322e30fa0a72656469732d62697473c040fa056374696d65\
    c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d626\
    17365c000fff06e3bfec0ff5aa2";

    hex::decode(DATA)
        .map_err(anyhow::Error::from)
        .map(Bytes::from)
        .map(Resp::Data)
}
