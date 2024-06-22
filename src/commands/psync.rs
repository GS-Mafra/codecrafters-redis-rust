use std::fmt::Display;

use anyhow::Context;
use bytes::Bytes;

use crate::{slice_to_int, Master, Resp};

use super::IterResp;

#[derive(Debug)]
pub struct Psync {
    id: String,
    offset: Offset,
}

impl Psync {
    pub(crate) const fn new(id: String, offset: Offset) -> Self {
        Self { id, offset }
    }

    pub(crate) fn first_sync() -> Self {
        Self::new("?".into(), Offset::Unknown)
    }

    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let id = i.next().context("Expected id")?.to_string()?;
        let offset = i
            .next()
            .and_then(Resp::as_bulk)
            .map(|x| match x.as_ref() {
                b"-1" => Ok(Offset::Unknown),
                _ => slice_to_int(x).map(Offset::Num),
            })
            .transpose()?
            .context("Expected offset")?;
        Ok(Self { id, offset })
    }

    #[allow(clippy::unused_self)]
    pub fn execute(&self, master: &Master) -> anyhow::Result<(Resp, Resp)> {
        let master_replid = master.replid();
        let master_repl_offset = master.repl_offset();

        let resp = Resp::Simple(format!("FULLRESYNC {master_replid} {master_repl_offset}"));
        Ok((resp, get_data()?))
    }

    pub(crate) fn into_resp(self) -> Resp {
        Resp::Array(vec![
            Resp::bulk("psync"),
            Resp::bulk(self.id),
            Resp::bulk(self.offset.to_string()),
        ])
    }
}

#[derive(Debug)]
pub enum Offset {
    Unknown,
    Num(usize),
}

impl Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => f.write_str("-1"),
            Self::Num(num) => num.fmt(f),
        }
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
