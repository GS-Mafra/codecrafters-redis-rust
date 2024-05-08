use anyhow::{bail, Context};
use bytes::Bytes;
use std::{fmt::Display, time::Duration};
use tokio::sync::broadcast::Sender;

use crate::{Handler, Resp, Role, DB};

type IterResp<'a> = std::slice::Iter<'a, Resp>;

pub struct Command<'a> {
    handler: &'a mut Handler,
    role: &'a Role,
    sender: Option<&'a Sender<Resp>>,
}

impl<'a> Command<'a> {
    pub fn new(handler: &'a mut Handler, role: &'a Role, sender: Option<&'a Sender<Resp>>) -> Self {
        Self {
            handler,
            role,
            sender,
        }
    }

    pub async fn parse(&mut self, value: &Resp) -> anyhow::Result<()> {
        let Resp::Array(elems) = value else {
            bail!("Unsupported RESP for command");
        };

        let mut values = elems.iter();
        let Some(Resp::Bulk(command)) = values.next() else {
            bail!("Expected bulk string");
        };
        match command.to_ascii_lowercase().as_slice() {
            b"ping" => self.ping().await?,
            b"echo" => self.echo(values).await?,
            b"get" => self.get(values).await?,
            b"set" => {
                self.set(values).await?;
                self.propagate(elems);
            }
            b"del" => {
                self.del(values).await?;
                self.propagate(elems);
            }
            b"info" => self.info(values).await?,
            b"replconf" => self.replconf(values).await?,
            b"psync" => self.psync(values).await?,
            _ => unimplemented!(),
        }

        Ok(())
    }

    fn propagate(&mut self, command: &[Resp]) {
        if let Some(slaves) = self.sender {
            // TODO check err
            let _ = slaves.send(Resp::Array(command.to_owned()));
        }
    }

    async fn ping(&mut self) -> anyhow::Result<()> {
        self.handler
            .write_checked(&Resp::simple("PONG"), self.role)
            .await
    }

    async fn echo(&mut self, mut i: IterResp<'_>) -> anyhow::Result<()> {
        self.handler
            .write_checked(i.next().context("Missing ECHO value")?, self.role)
            .await
    }

    async fn get(&mut self, mut i: IterResp<'_>) -> anyhow::Result<()> {
        let key = i.next().context("Missing Key")?.as_string()?;
        let value = DB.get(&key).map_or(Resp::Null, Resp::Bulk);
        self.handler.write_checked(&value, self.role).await
    }

    async fn set(&mut self, mut i: IterResp<'_>) -> anyhow::Result<()> {
        let key = i.next().context("Missing Key")?.as_string()?;
        let value = i.next().context("Missing Value")?.as_string()?;
        let px = {
            i.position(|x| *x == Resp::bulk("px")).and_then(|pos| {
                i.nth(pos)
                    .and_then(|x| Resp::as_string(x).ok())
                    .and_then(|x| x.parse::<u64>().ok())
                    .map(Duration::from_millis)
            })
        };
        DB.set(key, value.into(), px);
        self.handler
            .write_checked(&Resp::simple("OK"), self.role)
            .await
    }

    async fn del(&mut self, i: IterResp<'_>) -> anyhow::Result<()> {
        let deleted = DB.multi_del(i.flat_map(Resp::as_string));
        let resp = Resp::Integer(i64::try_from(deleted).expect("Deleted to be in range of i64"));
        self.handler.write_checked(&resp, self.role).await
    }

    async fn info(&mut self, mut i: IterResp<'_>) -> anyhow::Result<()> {
        let Some(Resp::Bulk(arg)) = i.next() else {
            // TODO return all sections
            let resp = Info {
                replication: Some(Replication::new(self.role)),
            };
            return self
                .handler
                .write_checked(&Resp::bulk(resp.to_string()), self.role)
                .await;
        };

        let resp = match arg.to_ascii_lowercase().as_slice() {
            b"replication" => Info {
                replication: Some(Replication::new(self.role)),
            },

            _ => todo!("{arg:?}"),
        };
        self.handler
            .write_checked(&Resp::bulk(resp.to_string()), self.role)
            .await
    }

    async fn replconf(&mut self, mut i: IterResp<'_>) -> anyhow::Result<()> {
        // TODO
        match self.role {
            Role::Master(_) => self.handler.write(&Resp::simple("OK")).await,
            Role::Slave(slave) => {
                let conf = i.next();

                let Some(Resp::Bulk(conf)) = conf else {
                    panic!()
                };

                match conf.to_ascii_lowercase().as_slice() {
                    b"getack" => {
                        let resp = Resp::Array(vec![
                            Resp::bulk("REPLCONF"),
                            Resp::bulk("ACK"),
                            Resp::bulk(slave.offset().to_string()),
                        ]);
                        self.handler.write(&resp).await
                    }
                    _ => todo!(),
                }
            }
        }
    }

    async fn psync(&mut self, mut i: IterResp<'_>) -> anyhow::Result<()> {
        let _id = i.next().context("Expected id")?;
        let _offset = i.next().context("Expected offset")?;

        let Role::Master(master) = self.role else {
            panic!("Expected master")
        };

        let master_replid = master.replid();
        let master_repl_offset = master.repl_offset();

        let resp = Resp::Simple(format!("FULLRESYNC {master_replid} {master_repl_offset}"));
        self.handler.write(&resp).await?;
        self.handler.write(&get_data()?).await?;

        let mut slave = self.sender.expect("Checked for master already").subscribe();
        while let Ok(recv) = slave.recv().await {
            self.handler.write(&recv).await?;
        }
        Ok(())
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

#[derive(Debug)]
struct Info<'a> {
    replication: Option<Replication<'a>>,
    // TODO
}

#[derive(Debug)]
struct Replication<'a> {
    role: &'a Role,
    // connected_slaves:
    master_replid: Option<&'a str>,
    master_repl_offset: Option<u64>,
    // second_repl_offset:
    // repl_backlog_active:
    // repl_backlog_size:
    // repl_backlog_first_byte_offset:
    // repl_backlog_histlen:
}

impl<'a> Replication<'a> {
    fn new(role: &'a Role) -> Self {
        match role {
            Role::Master(master) => Self {
                role,
                master_replid: Some(master.replid()),
                master_repl_offset: Some(master.repl_offset()),
            },
            Role::Slave(_) => Self {
                role,
                master_replid: None,
                master_repl_offset: None,
            },
        }
    }
}

impl<'a> Display for Replication<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            role,
            master_replid,
            master_repl_offset,
        } = self;
        write!(f, "# Replication\r\n")?;

        match role {
            Role::Master(_) => write!(f, "role:master")?,
            Role::Slave(_) => write!(f, "role:slave")?,
        }
        f.write_str("\r\n")?;

        if let Some(master_replid) = master_replid {
            write!(f, "master_replid:{master_replid}\r\n")?;
        }
        if let Some(master_repl_offset) = master_repl_offset {
            write!(f, "master_repl_offset:{master_repl_offset}\r\n")?;
        }
        Ok(())
    }
}

impl<'a> Display for Info<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { replication } = self;
        if let Some(replication) = replication {
            write!(f, "{replication}\r\n")?;
        }
        Ok(())
    }
}
