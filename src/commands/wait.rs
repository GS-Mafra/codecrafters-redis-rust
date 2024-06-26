use anyhow::{bail, Context};
use std::time::Duration;

use crate::{Command, Resp, Role};

use super::{IterResp, ReplConf};

#[derive(Debug)]
pub struct Wait {
    min_slaves: i64,
    timeout: Duration,
}

impl Wait {
    pub(super) fn parse(mut i: IterResp) -> anyhow::Result<Self> {
        let min_slaves = i.next().context("Missing num of slaves")?.to_int()?;
        let timeout = i
            .next()
            .context("Missing timeout")?
            .to_int()
            .map(Duration::from_millis)?;
        Ok(Self {
            min_slaves,
            timeout,
        })
    }

    pub async fn execute(&self, role: &Role) -> anyhow::Result<Resp> {
        let Role::Master(master) = role else {
            bail!("Expected master");
        };

        let master_offset = master.repl_offset();
        if master_offset == 0 {
            let count = master.slaves.read().await.len().try_into()?;
            return Ok(Resp::Integer(count));
        }
        // {
        //     let ackreplicas = master
        //         .slaves()
        //         .await
        //         .iter()
        //         .filter(|x| x.offset >= master_offset)
        //         .count()
        //         .try_into()?;
        //     if ackreplicas >= self.min_slaves {
        //         let resp = Resp::Integer(ackreplicas);
        //         handler.write(&resp).await?;
        //         return Ok(());
        //     }
        // }
        master.propagate(&ReplConf::GetAck.into_resp(), false).await;

        let mut slaves = master.slaves.write().await;
        let count = self.min_slaves.min(i64::try_from(slaves.len())?);

        let mut processed = 0_i64;
        let task = async {
            for slave in &mut *slaves {
                let Some(resp) = slave.handler.read().await? else {
                    continue;
                };
                let slave_offset = get_offset(&resp)?;
                tracing::debug!("Slave offset: {slave_offset}; master_offset: {master_offset}");
                if slave_offset >= master_offset {
                    processed += 1;
                }
                if processed >= count {
                    break;
                }
            }
            anyhow::Ok(processed)
        };
        if self.timeout.as_millis() != 0 {
            let _ = tokio::time::timeout(self.timeout, task).await;
        } else {
            task.await?;
        }
        drop(slaves);

        let resp = Resp::Integer(processed);
        Ok(resp)
    }
}

fn get_offset(resp: &Resp) -> anyhow::Result<u64> {
    if let Command::ReplConf(ReplConf::Ack(offset)) = Command::parse(resp)?.0 {
        Ok(offset)
    } else {
        bail!("Expected replconf ack");
    }
}
