use rand::{distributions::Alphanumeric, Rng};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::RwLock;

use crate::{Handler, Resp};

#[derive(Debug)]
pub struct Master {
    replid: String,
    repl_offset: AtomicU64,
    pub(crate) slaves: RwLock<Vec<Replica>>,
}

impl Default for Master {
    fn default() -> Self {
        Self {
            replid: rand::thread_rng()
                .sample_iter(Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            repl_offset: AtomicU64::new(0),
            slaves: RwLock::new(Vec::new()),
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
    pub fn increase_offset(&self, by: u64) {
        let prev = self.repl_offset.fetch_add(by, Ordering::Relaxed);
        tracing::info!("Increased offset of {prev} to {}", by + prev);
    }

    // FIXME async closure https://github.com/rust-lang/rust/issues/62290
    pub async fn propagate(&self, resp: &Resp, incr_offset: bool) {
        // FIXME
        let len = if incr_offset { resp.len() } else { 0 };

        let mut lock = self.slaves.write().await;
        let mut to_retain = Vec::<bool>::with_capacity(lock.len());
        for slave in &mut *lock {
            let retain = !slave
                .handler
                .write(resp)
                .await
                .is_err_and(|e| Handler::disconnected(&e));
            slave.offset += len as u64;
            to_retain.push(retain);
        }
        self.increase_offset(len as u64);
        let mut retain = to_retain.into_iter();
        lock.retain(|_| retain.next().unwrap());
    }

    pub async fn add_slave(&self, handler: Handler) {
        let slave = Replica::new(handler);
        self.slaves.write().await.push(slave);
    }
}

#[derive(Debug)]
pub struct Replica {
    pub(crate) handler: Handler,
    pub(crate) offset: u64,
}

impl Replica {
    const fn new(handler: Handler) -> Self {
        Self { handler, offset: 0 }
    }

    #[inline]
    #[must_use]
    pub const fn addr(&self) -> &SocketAddr {
        &self.handler.addr
    }
}
