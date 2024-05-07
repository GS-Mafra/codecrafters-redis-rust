use anyhow::{bail, Context};
use bytes::Bytes;
use std::{fmt::Display, time::Duration};

use crate::{Resp, Role, DB};

#[derive(Debug)]
pub struct Command {
    pub resp: Resp,
    pub data: Option<Resp>,
    pub slave_propagation: Option<Resp>,
}

impl Command {
    pub fn parse(value: &Resp, role: &Role) -> anyhow::Result<Self> {
        match value {
            Resp::Array(elems) => {
                let mut values = elems.iter();
                let Some(Resp::Bulk(command)) = values.next() else {
                    bail!("Expected bulk string");
                };
                Ok(match command.to_ascii_lowercase().as_slice() {
                    b"ping" => Self::ping(),
                    b"echo" => Self::echo(values)?,
                    b"get" => Self::get(values)?,
                    b"set" => Self::set(values)?.and_propagate(role, elems),
                    b"del" => Self::del(values).and_propagate(role, elems),
                    b"info" => Self::info(values, role),
                    b"replconf" => Self::replconf(),
                    b"psync" => Self::psync(values, role)?,
                    _ => unimplemented!(),
                })
            }
            _ => Err(anyhow::anyhow!("Unsupported RESP for command")),
        }
    }

    const fn new(resp: Resp) -> Self {
        Self {
            resp,
            data: None,
            slave_propagation: None,
        }
    }

    fn with_data(mut self, data: Resp) -> Self {
        self.data = Some(data);
        self
    }

    fn and_propagate(mut self, role: &Role, commands: &[Resp]) -> Self {
        if let Role::Master { .. } = role {
            let resp = Resp::Array(commands.to_owned());
            self.slave_propagation = Some(resp);
        }
        self
    }

    fn ping() -> Self {
        Self::new(Resp::Simple("PONG".into()))
    }

    fn echo<'a, I>(i: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        Ok(Self::new(
            i.into_iter().next().context("Missing ECHO value")?.clone(),
        ))
    }

    fn get<'a, I>(i: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let key = i.into_iter().next().context("Missing Key")?.as_string()?;
        Ok(Self::new(DB.get(&key).map_or(Resp::Null, Resp::Bulk)))
    }

    fn set<'a, I>(i: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let mut i = i.into_iter();
        let key = i.next().context("Missing Key")?.as_string()?;
        let value = i.next().context("Missing Value")?.as_string()?;
        let px = {
            i.position(|x| *x == Resp::Bulk(b"px".as_ref().into()))
                .and_then(|pos| {
                    i.nth(pos)
                        .and_then(|x| Resp::as_string(x).ok())
                        .and_then(|x| x.parse::<u64>().ok())
                        .map(Duration::from_millis)
                })
        };
        DB.set(key, value.into(), px);
        Ok(Self::new(Resp::Simple("OK".into())))
    }

    fn del<'a, I>(i: I) -> Self
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let mut keys = i.into_iter();
        let deleted = DB.multi_del(keys.by_ref().flat_map(Resp::as_string));
        let resp = Resp::Integer(i64::try_from(deleted).expect("Deleted to be in range of i64"));
        Self::new(resp)
    }

    fn info<'a, I>(i: I, role: &Role) -> Self
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let Some(Resp::Bulk(arg)) = i.into_iter().next() else {
            // TODO return all sections
            let resp = Info {
                replication: Some(Replication::new(role)),
            };
            return Self::new(Resp::Bulk(resp.to_string().into()));
        };

        let resp = match arg.to_ascii_lowercase().as_slice() {
            b"replication" => Info {
                replication: Some(Replication::new(role)),
            },

            _ => todo!("{arg:?}"),
        };
        Self::new(Resp::Bulk(resp.to_string().into()))
    }

    fn replconf() -> Self {
        // TODO
        Self::new(Resp::Simple("OK".into()))
    }

    fn psync<'a, I>(i: I, role: &Role) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let mut i = i.into_iter();
        let _id = i.next().context("Expected id")?;
        let _offset = i.next().context("Expected offset")?;

        let Role::Master(master) = role else {
            panic!("Expected master")
        };

        let master_replid = master.replid();
        let master_repl_offset = master.repl_offset();

        let resp = Resp::Simple(format!("FULLRESYNC {master_replid} {master_repl_offset}"));
        Ok(Self::new(resp).with_data(get_data()?))
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
            Role::Master { .. } => write!(f, "role:master")?,
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
