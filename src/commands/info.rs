use std::io::Write;

use crate::{Handler, Master, Resp, Role, Slave};

use super::IterResp;

#[derive(Debug)]
pub enum Info {
    Replication,
    // TODO
}

impl Info {
    pub(super) fn parse(mut i: IterResp) -> Self {
        let Some(arg) = i.next().and_then(Resp::as_bulk) else {
            // TODO return all sections
            return Self::Replication;
        };

        let resp = match arg.to_ascii_lowercase().as_slice() {
            b"replication" => Self::Replication,
            _ => todo!("{arg:?}"),
        };
        resp
    }

    pub async fn apply_and_respond(
        &self,
        handler: &mut Handler,
        role: &Role,
    ) -> anyhow::Result<()> {
        match self {
            Self::Replication => {
                let resp = Resp::bulk(Replication::new(role).to_bytes().await?);
                handler.write(&resp).await?;
                Ok(())
            }
        }
    }
}

enum Replication<'a> {
    Master(&'a Master),
    Slave(&'a Slave),
}

impl<'a> Replication<'a> {
    fn new(role: &'a Role) -> Self {
        match role {
            Role::Master(master) => Self::Master(master),
            Role::Slave(slave) => Self::Slave(slave),
        }
    }

    async fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut bytes = Vec::new();

        write!(bytes, "# Replication\r\n")?;
        match self {
            Self::Master(master) => {
                write!(bytes, "role:master\r\n")?;
                {
                    let slaves = master.slaves.read().await;
                    write!(bytes, "connected_slaves:{}\r\n", slaves.len())?;
                    slaves.iter().enumerate().try_for_each(|(i, slave)| {
                        let addr = slave.addr();
                        write!(
                            bytes,
                            "slave{i}:ip={ip},port={port},offset={off}\r\n",
                            ip = addr.ip(),
                            port = addr.port(),
                            off = slave.offset,
                        )
                    })?;
                }
                write!(bytes, "master_replid:{}\r\n", master.replid())?;
                write!(bytes, "master_repl_offset:{}\r\n", master.repl_offset())?;
            }
            Self::Slave(slave) => {
                write!(bytes, "role:slave\r\n")?;
                write!(bytes, "master_host:{}\r\n", slave.addr.ip())?;
                write!(bytes, "master_port:{}\r\n", slave.addr.port())?;
            }
        }
        Ok(bytes)
    }
}
