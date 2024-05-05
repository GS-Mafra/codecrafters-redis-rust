use bytes::Bytes;
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    sync::RwLock,
    time::{Duration, Instant},
};

use crate::debug_print;

pub static DB: Lazy<Db> = Lazy::new(Db::new);

#[derive(Debug, Clone)]
pub struct Value {
    pub inner: Bytes,
    pub created: Instant,
    pub expiration: Option<Duration>,
}

pub struct Db {
    inner: RwLock<HashMap<String, Value>>,
}

impl Db {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, k: String, v: Bytes, px: Option<Duration>) {
        let value = Value {
            inner: v,
            created: Instant::now(),
            expiration: px,
        };

        self.inner.write().unwrap().insert(k, value);
        debug_print!("{:?}", self.inner.read().unwrap());
    }

    pub fn get(&self, k: &str) -> Option<Bytes> {
        // drops read lock
        let value = { self.inner.read().unwrap().get(k).cloned() };

        value
            .and_then(|val| {
                let expired = val.expiration.is_some_and(|px| {
                    let time_passed = val.created.elapsed();
                    debug_print!("{time_passed:.02?} passed of {px:.02?}");
                    px <= time_passed
                });
                if expired {
                    eprintln!("{k} expired");
                    self.del(k);
                    None
                } else {
                    Some(val)
                }
            })
            .map(|v| v.inner)
    }

    pub fn del(&self, k: &str) {
        self.inner.write().unwrap().remove(k);
    }
}


#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;

    #[test]
    fn expires() {
        let db = Db::new();

        let k: String = "test".into();
        let v = b"bytes".as_ref().into();
        let px = Duration::from_millis(2_000);
        db.set(k.clone(), v, Some(px));

        assert!(db.get(&k).is_some());
        sleep(Duration::from_millis(2_000));
        assert!(db.get(&k).is_none());
    }
}