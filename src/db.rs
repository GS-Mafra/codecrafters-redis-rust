use bytes::Bytes;
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    path::Path,
    sync::RwLock,
    time::{Duration, SystemTime},
};

use crate::Rdb;

pub static DB: Lazy<Db> = Lazy::new(Db::new);

pub struct Db {
    pub(crate) inner: RwLock<HashMap<String, Value>>,
}

impl Db {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, k: String, t: Type, exp: Option<Duration>) {
        let value = Value {
            v_type: t,
            expiration: exp.map(|x| SystemTime::now() + x),
        };

        tracing::debug!("Adding to db: \"{k}\": {:#?}", value);
        self.inner.write().unwrap().insert(k, value);
    }

    pub fn get(&self, k: &str) -> Option<Type> {
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
            .map(|v| v.v_type)
    }

    pub fn multi_del(&self, keys: impl Iterator<Item = impl AsRef<str>>) -> usize {
        let mut lock = self.inner.write().unwrap();
        keys.filter_map(|k| lock.remove(k.as_ref()).map(|_| k))
            .inspect(|k| tracing::info!("Deleted: \"{}\"", k.as_ref()))
            .count()
    }

    pub fn load_rdb(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();

        // FIXME windows doesn't like /tmp :(
        let path = if path.starts_with("/tmp/") {
            let mut tmp = std::env::temp_dir();
            tmp.extend(path.components().skip(2));
            Cow::Owned(tmp)
        } else {
            Cow::Borrowed(path)
        };

        let rdb = match std::fs::read(&path) {
            Ok(rdb) => rdb,
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    tracing::error!("File not found: {}", path.display());
                    return Ok(());
                }
                _ => return Err(e.into()),
            },
        };
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
                let Type::String(_) = &value.v_type else {
                    todo!("{:?}", value.v_type);
                };
                lock.insert(key, value);
            });
        tracing::debug!("Applied rdb: {lock:#?}");
    }
}

#[derive(Clone)]
pub struct Value {
    pub(crate) v_type: Type,
    pub(crate) expiration: Option<SystemTime>,
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Value")
            .field("type", &self.v_type)
            .field(
                "expiration",
                &self.expiration.map(chrono::DateTime::<chrono::Local>::from),
            )
            .finish()
    }
}

#[derive(Debug, Clone)]
#[repr(u8)]
pub enum Type {
    String(Bytes) = 0,
    // List,
    // Set,
    // SortedSet,
    // Hash,
    // Zipmap,
    // Ziplist,
    // Intset Encoding,
    // Sorted Set in Ziplist Encoding,
    // Hashmap in Ziplist Encoding,
    // List in Quicklist Encoding,
    Stream(/* TODO */ ()) = 21,
}

impl Type {
    #[inline]
    pub(crate) fn as_string(&self) -> Option<Bytes> {
        #[allow(clippy::match_wildcard_for_single_variants)]
        match self {
            Self::String(string) => Some(string.clone()),
            _ => None,
        }
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
        let v = Type::String(b"bytes".as_ref().into());
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
            .for_each(|k| db.set(k, Type::String("test".into()), None));

        assert_eq!(db.inner.read().unwrap().len(), 3);
        db.multi_del(std::iter::once(keys[0].clone()));
        assert_eq!(db.inner.read().unwrap().len(), 2);
        assert_eq!(db.multi_del(keys[1..=2].iter()), 2);
    }
}
