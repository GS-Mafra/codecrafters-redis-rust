use anyhow::{bail, Context};
use bytes::Bytes;

use crate::{Handler, Resp, ARGUMENTS};

use super::IterResp;

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

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        match self {
            Self::Get(params) => Self::handle_get(params, handler).await?,
        }
        Ok(())
    }

    async fn handle_get(params: &[Bytes], handler: &mut Handler) -> anyhow::Result<()> {
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
        let resp = Resp::Array(v);
        handler.write(&resp).await?;
        Ok(())
    }
}
