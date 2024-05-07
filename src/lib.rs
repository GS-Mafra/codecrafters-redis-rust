#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod args;
pub use args::{Role, Arguments};

pub mod commands;
pub use commands::Command;

pub mod handler;
pub use handler::{connect_slave, Handler};

mod resp;
pub use resp::Resp;

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
