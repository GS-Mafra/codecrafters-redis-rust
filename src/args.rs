use clap::{arg, ArgAction, Args, Command, FromArgMatches, Parser};
use rand::{distributions::Alphanumeric, Rng};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::broadcast::{self, error::SendError, Receiver, Sender};

use crate::Resp;

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
    pub fn increase_offset(&self, by: u64) {
        let prev = match self {
            Self::Master(master) => master.repl_offset.fetch_add(by, Ordering::Relaxed),
            Self::Slave(slave) => slave.offset.fetch_add(by, Ordering::Relaxed),
        };
        tracing::info!("Increased offset of {prev} by {by}");
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
    channel: Sender<Resp>,
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
    pub fn replid(&self) -> &str {
        &self.replid
    }

    #[inline]
    pub fn repl_offset(&self) -> u64 {
        self.repl_offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn connected_slaves(&self) -> usize {
        self.channel.receiver_count()
    }

    #[inline]
    pub fn send_to_slaves(&self, prop: Resp) -> Result<usize, SendError<Resp>> {
        self.channel.send(prop)
    }

    #[inline]
    pub fn spawn_slave(&self) -> Receiver<Resp> {
        self.channel.subscribe()
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
