use crate::{args::Role, DB, Resp, ARGUMENTS};
use anyhow::Context;
use std::{fmt::Display, time::Duration};

pub struct Command;

impl Command {
    pub fn parse(value: &Resp) -> anyhow::Result<Resp> {
        match value {
            Resp::Array(values) => {
                let mut values = values.iter();
                let Some(Resp::Bulk(command)) = values.next() else {
                    return Err(anyhow::anyhow!("Expected bulk string"));
                };
                Ok(match command.to_ascii_lowercase().as_slice() {
                    b"ping" => Self::ping(),
                    b"echo" => Self::echo(values)?,
                    b"get" => Self::get(values)?,
                    b"set" => Self::set(values)?,
                    b"info" => Self::info(values)?,
                    _ => unimplemented!(),
                })
            }
            _ => Err(anyhow::anyhow!("Unsupported RESP for command")),
        }
    }

    fn ping() -> Resp {
        Resp::Simple("PONG".into())
    }

    fn echo<'a, I>(i: I) -> anyhow::Result<Resp>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        Ok(i.into_iter().next().context("Missing ECHO value")?.clone())
    }

    fn get<'a, I>(i: I) -> anyhow::Result<Resp>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let key = i.into_iter().next().context("Missing Key")?.as_string()?;
        Ok(DB.get(&key).map_or(Resp::Null, Resp::Bulk))
    }

    fn set<'a, I>(i: I) -> anyhow::Result<Resp>
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
        Ok(Resp::Simple("OK".into()))
    }

    fn info<'a, I>(i: I) -> anyhow::Result<Resp>
    where
        I: IntoIterator<Item = &'a Resp>,
    {
        let Some(Resp::Bulk(arg)) = i.into_iter().next() else {
            // TODO return all sections
            return Err(anyhow::anyhow!("Missing arg"));
        };
        let resp = match arg.to_ascii_lowercase().as_slice() {
            b"replication" => Info {
                replication: Some(Replication {
                    role: &ARGUMENTS.role,
                }),
            },

            _ => todo!("{arg:?}"),
        };
        Ok(Resp::Bulk(resp.to_string().into()))
    }
}

struct Info<'a> {
    replication: Option<Replication<'a>>,
    // TODO
}

struct Replication<'a> {
    role: &'a Role,
    // connected_slaves:
    // master_replid:
    // master_repl_offset:
    // second_repl_offset:
    // repl_backlog_active:
    // repl_backlog_size:
    // repl_backlog_first_byte_offset:
    // repl_backlog_histlen:
}

impl<'a> Display for Replication<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { role } = self;
        write!(f, "# Replication\r\n")?;
        write!(f, "{role}\r\n")?;
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
