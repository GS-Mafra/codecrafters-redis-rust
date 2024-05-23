use bytes::Bytes;
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    fmt::Debug,
    path::Path,
    sync::RwLock,
    time::{Duration, SystemTime},
};

use crate::{rdb::Type, Rdb};

pub static DB: Lazy<Db> = Lazy::new(Db::new);

#[derive(Clone)]
pub struct Value {
    pub inner: Bytes,
    pub expiration: Option<SystemTime>,
}

pub struct Db {
    pub(crate) inner: RwLock<HashMap<String, Value>>,
}

impl Db {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, k: String, v: Bytes, exp: Option<Duration>) {
        let value = Value {
            inner: v,
            expiration: exp.map(|x| SystemTime::now() + x),
        };

        tracing::debug!("Adding to db: \"{k}\": {:#?}", value);
        self.inner.write().unwrap().insert(k, value);
    }

    pub fn get(&self, k: &str) -> Option<Bytes> {
        // drops read lock
        let value = { self.inner.read().unwrap().get(k).cloned() };

        value
            .and_then(|val| {
                let expired = val.expiration.is_some_and(|exp| exp <= SystemTime::now());
                if expired {
                    tracing::info!("\"{k}\" expired");
                    self.multi_del(std::iter::once(k));
                    None
                } else {
                    Some(val)
                }
            })
            .map(|v| v.inner)
    }

    pub fn multi_del(&self, keys: impl Iterator<Item = impl AsRef<str>>) -> usize {
        let mut lock = self.inner.write().unwrap();
        keys.filter_map(|k| lock.remove(k.as_ref()).map(|_| k))
            .inspect(|k| tracing::info!("Deleted: \"{}\"", k.as_ref()))
            .count()
    }

    pub fn load_rdb(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let rdb = std::fs::read(path)?;
        let rdb = Rdb::parse(rdb.into())?;
        self.apply_rdb(rdb);
        Ok(())
    }

    pub fn apply_rdb(&self, rdb: Rdb) {
        let mut lock = self.inner.write().unwrap();
        rdb.db
            .maps
            .into_iter()
            .flatten()
            .filter(|(key, v)| {
                let expired = v.expiration.is_some_and(|exp| exp <= SystemTime::now());
                if expired {
                    tracing::info!("key: \"{key}\" from rdb expired");
                }
                !expired
            })
            .for_each(|(key, value)| {
                let Type::String(string) = value.v_type; // else
                let value = Value {
                    inner: string,
                    expiration: value.expiration,
                };
                lock.insert(key, value);
            });
        tracing::debug!("Applied rdb: {lock:#?}");
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Value")
            .field("inner", &self.inner)
            .field(
                "expiration",
                &self.expiration.map(chrono::DateTime::<chrono::Local>::from),
            )
            .finish()
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
        db.multi_del(std::iter::once(keys[0].clone()));
        assert_eq!(db.inner.read().unwrap().len(), 2);
        assert_eq!(db.multi_del(keys[1..=2].iter()), 2);
    }
}
