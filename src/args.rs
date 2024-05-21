use clap::{arg, value_parser, ArgAction, Command};
use once_cell::sync::Lazy;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};

use crate::{Role, Slave};

pub static ARGUMENTS: Lazy<Arguments> = Lazy::new(Arguments::parse);

#[derive(Debug)]
pub struct Arguments {
    pub port: u16,
    pub role: Role,
    pub dir: Option<PathBuf>,
    pub db_filename: Option<PathBuf>,
}

impl Arguments {
    #[must_use]
    #[allow(clippy::cognitive_complexity)]
    pub fn parse() -> Self {
        let mut matches = Command::new(env!("CARGO_CRATE_NAME"))
            .arg(
                arg!(--port)
                    .action(ArgAction::Set)
                    .default_value("6379")
                    .value_parser(value_parser!(u16).range(1..)),
            )
            .arg(
                arg!(--replicaof)
                    .action(ArgAction::Set)
                    .value_names(["HOST PORT"])
                    .value_delimiter(' ')
                    .requires("port"),
            )
            .arg(
                arg!(--dir)
                    .action(ArgAction::Set)
                    .value_parser(value_parser!(PathBuf)),
            )
            .arg(
                arg!(--dbfilename)
                    .action(ArgAction::Set)
                    .value_parser(value_parser!(PathBuf)),
            )
            .get_matches();

        let port = matches.remove_one::<u16>("port").unwrap();
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

        let dir = matches.remove_one::<PathBuf>("dir");
        let db_filename = matches.remove_one::<PathBuf>("dbfilename");
        Self {
            port,
            role,
            dir,
            db_filename,
        }
    }
}
