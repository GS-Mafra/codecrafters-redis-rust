#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod args;
pub use args::ARGUMENTS;

mod commands;
pub use commands::Command;

mod handler;
pub use handler::{CommandHandler, Handler};

pub mod roles;
pub use roles::{Master, Role, Slave};

mod resp;
pub use resp::Resp;

mod db;
pub use db::DB;

mod rdb;
pub use rdb::Rdb;

#[inline]
pub fn slice_to_int<T>(slice: impl AsRef<[u8]>) -> anyhow::Result<T>
where
    T: atoi::FromRadix10SignedChecked,
{
    use anyhow::Context;
    atoi::atoi::<T>(slice.as_ref()).context("Failed to parse int from slice")
}
