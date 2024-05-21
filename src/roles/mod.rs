pub mod master;
pub use master::Master;

pub mod slave;
pub use slave::Slave;

#[derive(Debug)]
pub enum Role {
    Master(Master),
    Slave(Slave),
}

impl Default for Role {
    fn default() -> Self {
        Self::Master(Master::default())
    }
}
