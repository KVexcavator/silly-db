//! key value interface
use std::collections::HashMap;

pub struct KV {
    mem: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub struct KVError {
    // TODO
}

impl KV {
    pub fn open() -> Result<Self, KVError> {
        Ok(Self {
            mem: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> Result<(), KVError> {
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KVError> {
        Ok(self.mem.get(key).cloned())
    }

    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<bool, KVError> {
        let existed = self.mem.contains_key(key);
        self.mem.insert(key.to_vec(), val.to_vec());
        Ok(existed)
    }

    pub fn del(&mut self, key: &[u8]) -> Result<bool, KVError> {
        Ok(self.mem.remove(key).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_open_and_close() {
        let mut kv = KV::open().unwrap();
        kv.close().unwrap();
    }

    #[test]
    fn get_missing_key() {
        let kv = KV::open().unwrap();
        let value = kv.get(b"missing").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn can_set_and_get() {
        let mut kv = KV::open().unwrap();

        let updated = kv.set(b"key", b"value").unwrap();
        assert!(!updated);

        let value = kv.get(b"key").unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn can_set_update_existing_key() {
        let mut kv = KV::open().unwrap();

        kv.set(b"key", b"value1").unwrap();
        let updated = kv.set(b"key", b"value2").unwrap();
        assert!(updated);

        let value = kv.get(b"key").unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[test]
    fn can_delete_key() {
        let mut kv = KV::open().unwrap();
        kv.set(b"key", b"value").unwrap();

        let deleted = kv.del(b"key").unwrap();
        assert!(deleted);

        let value = kv.get(b"key").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn cant_delete_missing_key() {
        let mut kv = KV::open().unwrap();

        let deleted = kv.del(b"maybe").unwrap();
        assert!(!deleted);
    }
}
