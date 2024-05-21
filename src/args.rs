use clap::{arg, value_parser, ArgAction, Command};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};

use crate::{Role, Slave};

#[derive(Debug)]
pub struct Arguments {
    pub port: u16,
    pub role: Role,
}

impl Arguments {
    #[must_use]
    pub fn parse() -> Self {
        let mut matches = Command::new(env!("CARGO_CRATE_NAME"))
            .arg(
                arg!(--replicaof)
                    .action(ArgAction::Set)
                    .value_names(["HOST PORT"]),
            )
            .arg(
                arg!(--port)
                    .action(ArgAction::Set)
                    .default_value("6379")
                    .value_parser(value_parser!(u16).range(1..)),
            )
            .get_matches();

        let port = matches.remove_one::<u16>("port").unwrap();

        let role = matches
            .remove_one::<String>("replicaof")
            .and_then(|x| {
                let (host, port) = x.split_once(' ')?;
                let host = if host == "localhost" {
                    Ipv4Addr::LOCALHOST
                } else {
                    host.parse().ok()?
                };
                let port = port.parse::<u16>().ok()?;
                let slave = Slave::new(SocketAddrV4::new(host, port));
                Some(Role::Slave(Arc::new(slave)))
            })
            .unwrap_or_default();

        Self { port, role }
    }
}
