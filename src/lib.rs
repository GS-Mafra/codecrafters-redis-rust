#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod args;
pub(crate) use args::Role;
pub use args::ARGUMENTS;

pub mod commands;
pub use commands::Command;

pub mod handler;
pub use handler::{connect_slave, Handler};

mod resp;
pub(crate) use resp::Resp;

pub(crate) mod db;
pub(crate) use db::DB;

#[macro_export]
macro_rules! debug_print {
    ($($x:tt)*) => {
        {
            #[cfg(debug_assertions)]
            {
                eprintln!($($x)*)
            }
        }
    }
}
