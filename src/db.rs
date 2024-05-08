use bytes::Bytes;
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    sync::RwLock,
    time::{Duration, Instant},
};

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

        tracing::debug!("Adding to db: \"{k}\": {:#?}", value);
        self.inner.write().unwrap().insert(k, value);
    }

    pub fn get(&self, k: &str) -> Option<Bytes> {
        // drops read lock
        let value = { self.inner.read().unwrap().get(k).cloned() };

        value
            .and_then(|val| {
                let expired = val.expiration.is_some_and(|px| {
                    let time_passed = val.created.elapsed();
                    tracing::info!("\"{k}\": {time_passed:.02?} passed out of {px:.02?}");
                    px <= time_passed
                });
                if expired {
                    tracing::info!("\"{k}\" expired");
                    self.del(k);
                    None
                } else {
                    Some(val)
                }
            })
            .map(|v| v.inner)
    }

    fn del(&self, k: &str) {
        tracing::info!("Deleting: \"{k}\"");
        self.inner.write().unwrap().remove(k);
    }

    pub fn multi_del(&self, keys: impl Iterator<Item = impl AsRef<str>>) -> usize {
        let mut lock = self.inner.write().unwrap();
        keys.filter_map(|k| lock.remove(k.as_ref()).map(|_| k))
            .inspect(|k| tracing::info!("Deleted: \"{}\"", k.as_ref()))
            .count()
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

    #[test]
    fn del() {
        let db = Db::new();

        let keys = ["key1", "key2", "key3"].map(String::from);

        keys.clone()
            .into_iter()
            .for_each(|k| db.set(k, "test".into(), None));

        assert_eq!(db.inner.read().unwrap().len(), 3);
        db.del(&keys[0]);
        assert_eq!(db.inner.read().unwrap().len(), 2);
        assert_eq!(db.multi_del(keys[1..=2].iter()), 2);
    }
}
