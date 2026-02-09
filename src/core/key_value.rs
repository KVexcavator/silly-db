//! key value interface
use crate::core::binary_serializer::Entry;
use crate::core::log_storage::Log;
use crate::model::update_modes::UpdateMode;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct KV {
    log: Log,
    mem: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub enum KVError {
    Io(std::io::Error),
}

impl From<std::io::Error> for KVError {
    fn from(e: std::io::Error) -> Self {
        KVError::Io(e)
    }
}

impl KV {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, KVError> {
        let mut log = Log::open(path)?;
        let mut mem = HashMap::new();

        // read WAL for EOF
        while let Some(entry) = log.read()? {
            if entry.is_deleted() {
                mem.remove(entry.key());
            } else {
                mem.insert(entry.key().to_vec(), entry.value().to_vec());
            }
        }

        Ok(KV { log, mem })
    }

    pub fn close(&mut self) -> Result<(), KVError> {
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KVError> {
        Ok(self.mem.get(key).cloned())
    }

    pub fn set_if_existed(
        &mut self,
        key: &[u8],
        val: &[u8],
        mode: UpdateMode,
    ) -> Result<bool, KVError> {
        let existed = self.mem.contains_key(key);

        let should_write = match mode {
            UpdateMode::Upsert => true,
            UpdateMode::Insert => !existed,
            UpdateMode::Update => existed,
        };

        if !should_write {
            return Ok(false);
        }

        let entry = Entry::new(key.to_vec(), val.to_vec());
        let _ = self.log.write(&entry);

        self.mem.insert(key.to_vec(), val.to_vec());

        Ok(existed)
    }

    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<bool, KVError> {
        self.set_if_existed(key, val, UpdateMode::Insert)
    }

    pub fn del(&mut self, key: &[u8]) -> Result<bool, KVError> {
        let existed = self.mem.contains_key(key);

        if existed {
            let entry = Entry::tombstone(key.to_vec());
            self.log.write(&entry)?;
            self.mem.remove(key);
        }

        Ok(existed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_open_and_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();
        kv.close().unwrap();
    }

    #[test]
    fn get_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let kv = KV::open(&path).unwrap();
        let value = kv.get(b"missing").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn can_set_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();

        let updated = kv.set(b"key", b"value").unwrap();
        assert!(!updated);

        let value = kv.get(b"key").unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn can_delete_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();

        kv.set(b"key", b"value").unwrap();

        let deleted = kv.del(b"key").unwrap();
        assert!(deleted);

        let value = kv.get(b"key").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn cant_delete_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();

        let deleted = kv.del(b"maybe").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn replay_log_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        {
            let mut kv = KV::open(&path).unwrap();
            kv.set(b"a", b"1").unwrap();
            kv.set(b"a", b"2").unwrap();
            kv.del(b"a").unwrap();
            kv.set(b"b", b"3").unwrap();
        }

        // rerun
        let kv = KV::open(&path).unwrap();

        assert!(kv.get(b"a").unwrap().is_none());
        assert_eq!(kv.get(b"b").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn delete_missing_does_not_affect_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        {
            let mut kv = KV::open(&path).unwrap();
            assert!(!kv.del(b"nope").unwrap());
        }

        let kv = KV::open(&path).unwrap();
        assert!(kv.get(b"nope").unwrap().is_none());
    }

    #[test]
    fn kv_recovers_from_partial_wal() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        {
            let mut kv = KV::open(&path).unwrap();
            kv.set(b"a", b"1").unwrap();
        }

        // ruining WAL
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[9, 9, 9]).unwrap();
            f.sync_all().unwrap();
        }

        let kv = KV::open(&path).unwrap();
        assert_eq!(kv.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn insert_mode_does_not_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();

        kv.set(b"k", b"v1").unwrap();
        let updated = kv.set_if_existed(b"k", b"v2", UpdateMode::Insert).unwrap();

        assert!(!updated);
        assert_eq!(kv.get(b"k").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn update_mode_only_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();

        let updated = kv.set_if_existed(b"k", b"v1", UpdateMode::Update).unwrap();
        assert!(!updated);
        assert!(kv.get(b"k").unwrap().is_none());
    }

    #[test]
    fn update_mode_updates_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut kv = KV::open(&path).unwrap();

        kv.set(b"k", b"v1").unwrap();
        let updated = kv.set_if_existed(b"k", b"v2", UpdateMode::Update).unwrap();

        assert!(updated);
        assert_eq!(kv.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }
}
