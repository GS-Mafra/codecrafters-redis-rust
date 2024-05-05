use std::{
    fmt::Display,
    net::{Ipv4Addr, SocketAddrV4},
};

use clap::{arg, ArgAction, Args, Command, FromArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct Arguments {
    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    #[arg(skip)]
    pub role: Role,
}

impl Arguments {
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    // https://docs.rs/clap/latest/clap/_derive/index.html#using-derived-arguments-in-a-builder-application
    pub fn parser() -> Self {
        let cli = Command::new(env!("CARGO_CRATE_NAME"))
            .arg(arg!(--replicaof).action(ArgAction::Append).num_args(2..3));

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

#[derive(Debug, Clone)]
pub enum Role {
    Master,
    Slave(SocketAddrV4),
}

impl Default for Role {
    fn default() -> Self {
        Self::Master
    }
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master => write!(f, "role: master"),
            Self::Slave(_) => write!(f, "role: slave"),
        }
    }
}
