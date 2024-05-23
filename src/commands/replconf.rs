use anyhow::{bail, ensure, Context};
use bytes::Bytes;

use crate::{Handler, Resp, Slave};

use super::IterResp;

pub enum ReplConf {
    ListeningPort(u16),
    Capa(Bytes),
    GetAck,
    Ack(u64),
}

impl ReplConf {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let Some(arg) = i.next().context("Missing args")?.as_bulk() else {
            bail!("Expected bulk string");
        };
        Ok(match arg.to_ascii_lowercase().as_slice() {
            b"listening-port" => {
                let port = i.next().context("Missing port")?.to_int()?;
                Self::ListeningPort(port)
            }
            b"capa" => {
                let capas = i.next().context("Missing capa")?.to_bytes()?;
                Self::Capa(capas)
            }
            b"getack" => {
                ensure!(i
                    .next()
                    .is_some_and(|x| x.as_bulk().is_some_and(|x| x.as_ref() == b"*")));
                Self::GetAck
            }
            b"ack" => {
                let offset = i.next().context("Missing offset")?.to_int()?;
                Self::Ack(offset)
            }
            _ => todo!(),
        })
    }

    pub async fn apply_and_respond(&self, handler: &mut Handler) -> anyhow::Result<()> {
        handler.write(&Resp::simple("OK")).await?;
        Ok(())
    }

    pub async fn apply_and_respond_slave(
        &self,
        handler: &mut Handler,
        slave: &Slave,
    ) -> anyhow::Result<()> {
        let Self::GetAck = self else {
            bail!("Expected getack");
        };

        let resp = Self::Ack(slave.offset()).into_resp();
        handler.write(&resp).await?;
        Ok(())
    }

    pub(crate) fn into_resp(self) -> Resp {
        let replconf = Resp::bulk("REPLCONF");
        match self {
            Self::GetAck => Resp::Array(vec![replconf, Resp::bulk("GETACK"), Resp::bulk("*")]),
            Self::Capa(capa) => Resp::Array(vec![replconf, Resp::bulk("capa"), Resp::Bulk(capa)]),
            Self::Ack(offset) => Resp::Array(vec![
                replconf,
                Resp::bulk("ACK"),
                Resp::bulk(offset.to_string()),
            ]),
            Self::ListeningPort(port) => Resp::Array(vec![
                replconf,
                Resp::bulk("listening-port"),
                Resp::bulk(port.to_string()),
            ]),
        }
    }
}
