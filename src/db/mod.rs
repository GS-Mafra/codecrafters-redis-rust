use anyhow::bail;
use bytes::Bytes;
use once_cell::sync::Lazy;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    path::Path,
    time::SystemTime,
};
use stream::EntryId;
use tokio::sync::watch;

use crate::Rdb;

pub mod r#type;
pub use r#type::Type;

pub mod stream;
pub use stream::Stream;

pub static DB: Lazy<Db> = Lazy::new(Db::new);

type ReadValue<'a> = MappedRwLockReadGuard<'a, Value>;

pub struct Db {
    pub(crate) inner: RwLock<HashMap<String, Value>>,
    pub(crate) added_stream: watch::Sender<Option<(String, EntryId)>>,
}

impl Db {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            added_stream: watch::Sender::new(None),
        }
    }

    pub fn set(&self, set: crate::commands::Set) {
        let value = Value::new(set.value, set.expiry);
        tracing::debug!("Adding to db: \"{}\": {:#?}", set.key, value);
        self.inner.write().insert(set.key, value);
    }

    pub fn xadd(&self, xadd: crate::commands::Xadd) -> anyhow::Result<String> {
        let mut lock = self.inner.write();
        let entry = lock.entry(xadd.key.clone());

        let (res, id) = match entry {
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                let Type::Stream(stream) = &mut entry.v_type else {
                    bail!("XADD on invalid key \"{}\"", xadd.key);
                };
                let id = xadd.id.auto_generate(stream)?;
                let res = stream.xadd(id, xadd.k_v);
                (res, id)
            }
            Entry::Vacant(entry) => {
                let mut stream = Stream::new();
                let id = xadd.id.auto_generate(&stream)?;
                let res = stream.xadd(id, xadd.k_v);
                entry.insert(Value::new_no_expiry(Type::Stream(stream)));
                (res, id)
            }
        };
        drop(lock);
        tracing::debug!(
            "Notifying {qnty} waiters of stream added {key} {id}",
            qnty = self.added_stream.receiver_count(),
            key = xadd.key,
            id = id
        );
        let _ = self.added_stream.send(Some((xadd.key, id)));
        Ok(res)
    }

    pub fn del<I, S>(&self, keys: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut lock = self.inner.write();
        keys.into_iter()
            .filter_map(|k| {
                let k = k.as_ref();
                lock.remove(k)
                    .inspect(|_| tracing::info!("Deleted: \"{}\"", k))
            })
            .count()
    }

    pub fn get(&self, get: &crate::commands::Get) -> Option<ReadValue> {
        let k = &get.key;
        RwLockReadGuard::try_map(self.inner.read(), |lock| lock.get(k))
            .map(|lock| {
                if lock.expiration.is_some_and(|exp| exp <= SystemTime::now()) {
                    drop(lock);
                    tracing::info!("\"{k}\" expired");
                    self.del(std::iter::once(k));
                    None
                } else {
                    Some(lock)
                }
            })
            .ok()?
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
        self.inner
            .write()
            .extend(rdb.db.maps.into_iter().flatten().filter(|(key, v)| {
                let expired = v.expiration.is_some_and(|exp| exp <= SystemTime::now());
                if expired {
                    tracing::info!("key: \"{key}\" from rdb expired");
                }
                !expired
            }));
    }
}

pub struct Value {
    pub(crate) v_type: Type,
    pub(crate) expiration: Option<SystemTime>,
}

impl Value {
    #[inline]
    pub const fn new(r#type: Type, expiration: Option<SystemTime>) -> Self {
        Self {
            v_type: r#type,
            expiration,
        }
    }

    #[inline]
    pub const fn new_no_expiry(r#type: Type) -> Self {
        Self::new(r#type, None)
    }

    #[inline]
    pub const fn new_no_expiry_string(bytes: Bytes) -> Self {
        Self::new_no_expiry(Type::String(bytes))
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use chrono::{DateTime, Local};

        f.debug_struct("Value")
            .field("type", &self.v_type)
            .field("expiration", &self.expiration.map(DateTime::<Local>::from))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use crate::commands::{Get, Set};

    use super::*;

    #[test]
    fn expires() {
        let db = Db::new();

        let key = "test".to_owned();
        let value = b"bytes".as_ref().into();
        let expiry = Some(Duration::from_millis(100));
        let set = Set::new(key.clone(), value, expiry);
        db.set(set);

        assert!(db.get(&Get::new(key.clone())).is_some());
        sleep(Duration::from_millis(100));
        assert!(db.get(&Get::new(key)).is_none());
    }

    #[test]
    fn del() {
        let db = Db::new();

        let keys = ["key1", "key2", "key3"].map(String::from);

        keys.clone()
            .into_iter()
            .map(|k| Set::new(k, "test".into(), None))
            .for_each(|set| {
                db.set(set);
            });

        assert_eq!(db.inner.read().len(), 3);
        assert_eq!(db.del(std::iter::once(keys[0].clone())), 1);
        assert_eq!(db.inner.read().len(), 2);
        assert_eq!(db.del(keys[1..=2].iter()), 2);
    }
}
