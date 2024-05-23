#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod args;
pub use args::ARGUMENTS;

mod commands;
pub use commands::Command;

mod handler;
pub use handler::{handle_connection, Handler};

pub mod roles;
pub use roles::{Master, Role, Slave};

mod resp;
pub use resp::Resp;

mod db;
pub use db::DB;

mod rdb;
pub use rdb::Rdb;
