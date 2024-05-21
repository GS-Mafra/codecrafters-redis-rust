pub mod master;
pub use master::Master;

pub mod slave;
pub use slave::Slave;

use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Role {
    Master(Arc<Master>),
    Slave(Arc<Slave>),
}

impl Default for Role {
    fn default() -> Self {
        Self::Master(Arc::new(Master::default()))
    }
}
