use anyhow::{bail, Context};
use bytes::Bytes;

use crate::{Handler, Resp, ARGUMENTS};

use super::IterResp;

pub enum Config<'a> {
    Get(Vec<&'a Bytes>),
    // TODO
}

impl<'a> Config<'a> {
    pub(super) fn parse(mut i: IterResp<'a>) -> anyhow::Result<Self> {
        let Some(arg) = i.next().context("Missing args")?.as_bulk() else {
            bail!("Expected bulk string");
        };
        Ok(match arg.to_ascii_lowercase().as_slice() {
            b"get" => Self::Get(i.filter_map(Resp::as_bulk).collect()),
            _ => todo!("{arg:?}"),
        })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        match self {
            Self::Get(params) => Self::handle_get(params, handler).await?,
        }
        Ok(())
    }

    async fn handle_get(params: &[&Bytes], handler: &mut Handler) -> anyhow::Result<()> {
        for param in params {
            match param.to_ascii_lowercase().as_slice() {
                b"dir" => {
                    if let Some(dir) = &ARGUMENTS.dir {
                        let resp = Resp::Array(vec![
                            Resp::bulk("dir"),
                            Resp::bulk(dir.as_os_str().as_encoded_bytes()),
                        ]);
                        handler.write(&resp).await?;
                    }
                }
                b"dbfilename" => {
                    if let Some(dbfilename) = &ARGUMENTS.db_filename {
                        let resp = Resp::Array(vec![
                            Resp::bulk("dbfilename"),
                            Resp::bulk(dbfilename.as_os_str().as_encoded_bytes()),
                        ]);
                        handler.write(&resp).await?;
                    }
                }
                _ => todo!("{param:?}"),
            }
        }
        Ok(())
    }
}