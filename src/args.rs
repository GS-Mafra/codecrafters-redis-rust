use clap::{arg, ArgAction, Args, Command, FromArgMatches, Parser};
use once_cell::sync::Lazy;
use rand::{distributions::Alphanumeric, Rng};
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::sync::broadcast::{self, Sender};

pub static ARGUMENTS: Lazy<Arguments> = Lazy::new(Arguments::parser);

#[derive(Debug, Parser)]
pub struct Arguments {
    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    #[arg(skip)]
    pub role: Role,
}

impl Arguments {
    #[must_use]
    // https://docs.rs/clap/latest/clap/_derive/index.html#using-derived-arguments-in-a-builder-application
    pub fn parser() -> Self {
        let cli = Command::new(env!("CARGO_CRATE_NAME")).arg(
            arg!(--replicaof)
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["MASTER_HOST", "MASTER_PORT"]),
        );

        let cli = Self::augment_args(cli);
        let mut matches = cli.get_matches();

        let role = matches
            .remove_many::<String>("replicaof")
            .and_then(|mut x| {
                let host = {
                    let host = x.next().unwrap();
                    if host == "localhost" {
                        Ipv4Addr::LOCALHOST
                    } else {
                        host.parse().ok()?
                    }
                };
                let port = x.next().unwrap().parse::<u16>().ok()?;
                Some(Role::Slave(SocketAddrV4::new(host, port)))
            })
            .unwrap_or_default();

        Self::from_arg_matches(&matches)
            .map(|mut args| {
                args.role = role;
                args
            })
            .map_err(|e| e.exit())
            .unwrap()
    }
}

#[derive(Debug)]
pub enum Role {
    Master(Master),
    Slave(SocketAddrV4),
}

impl Role {
    #[must_use]
    pub const fn get_slaves(&self) -> Option<&Sender<crate::Resp>> {
        match self {
            Self::Master(master) => Some(&master.channel),
            Self::Slave(_) => None,
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Self::Master(Master::default())
    }
}

#[derive(Debug)]
pub struct Master {
    pub master_replid: String,
    pub master_repl_offset: u64,
    channel: Sender<crate::Resp>,
}

impl Default for Master {
    fn default() -> Self {
        Self {
            master_replid: rand::thread_rng()
                .sample_iter(Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            master_repl_offset: 0,
            channel: broadcast::channel(16).0,
        }
    }
}

impl Master {
    #[inline]
    #[must_use]
    pub fn replid(&self) -> &str {
        &self.master_replid
    }

    #[inline]
    #[must_use]
    pub const fn repl_offset(&self) -> u64 {
        self.master_repl_offset
    }
}
