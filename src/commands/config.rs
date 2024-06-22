use anyhow::{bail, Context};
use bytes::Bytes;

use crate::{Resp, ARGUMENTS};

use super::IterResp;

#[derive(Debug)]
pub enum Config {
    Get(Vec<Bytes>),
    // TODO
}

impl Config {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let Some(arg) = i.next().context("Missing args")?.as_bulk() else {
            bail!("Expected bulk string");
        };
        Ok(match arg.to_ascii_lowercase().as_slice() {
            b"get" => Self::Get(i.filter_map(Resp::as_bulk).map(Bytes::clone).collect()),
            _ => todo!("{arg:?}"),
        })
    }

    pub fn execute(&self) -> Resp {
        match self {
            Self::Get(params) => Self::handle_get(params),
        }
    }

    fn handle_get(params: &[Bytes]) -> Resp {
        let v = params.iter().fold(Vec::new(), |mut acc, param| {
            match param.to_ascii_lowercase().as_slice() {
                b"dir" => {
                    if let Some(dir) = &ARGUMENTS.dir {
                        acc.push(Resp::Bulk(param.clone()));
                        acc.push(Resp::bulk(dir.as_os_str().as_encoded_bytes()));
                    }
                }
                b"dbfilename" => {
                    if let Some(dbfilename) = &ARGUMENTS.db_filename {
                        acc.push(Resp::Bulk(param.clone()));
                        acc.push(Resp::bulk(dbfilename.as_os_str().as_encoded_bytes()));
                    }
                }
                _ => todo!("{param:?}"),
            }
            acc
        });
        Resp::Array(v)
    }
}
