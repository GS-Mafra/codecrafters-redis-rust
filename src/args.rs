use clap::{arg, ArgAction, Args, Command, FromArgMatches, Parser};
use rand::{distributions::Alphanumeric, Rng};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::broadcast::{self, Sender};

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
                let slave = Slave::new(SocketAddrV4::new(host, port));
                Some(Role::Slave(slave))
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
    Slave(Slave),
}

impl Role {
    #[inline]
    #[must_use]
    pub const fn get_slaves(&self) -> Option<&Sender<crate::Resp>> {
        match self {
            Self::Master(master) => Some(&master.channel),
            Self::Slave(_) => None,
        }
    }

    #[inline]
    pub fn increase_offset(&self, by: u64) {
        match self {
            Self::Master(master) => master.repl_offset.fetch_add(by, Ordering::Relaxed),
            Self::Slave(slave) => slave.offset.fetch_add(by, Ordering::Relaxed),
        };
    }
}

impl Default for Role {
    fn default() -> Self {
        Self::Master(Master::default())
    }
}

#[derive(Debug)]
pub struct Master {
    replid: String,
    repl_offset: AtomicU64,
    channel: Sender<crate::Resp>,
}

impl Default for Master {
    fn default() -> Self {
        Self {
            replid: rand::thread_rng()
                .sample_iter(Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            repl_offset: 0.into(),
            channel: broadcast::channel(16).0,
        }
    }
}

impl Master {
    #[inline]
    #[must_use]
    pub fn replid(&self) -> &str {
        &self.replid
    }

    #[inline]
    #[must_use]
    pub fn repl_offset(&self) -> u64 {
        self.repl_offset.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct Slave {
    pub addr: SocketAddrV4,
    offset: AtomicU64,
}

impl Slave {
    fn new(addr: SocketAddrV4) -> Self {
        Self {
            addr,
            offset: 0.into(),
        }
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }
}
