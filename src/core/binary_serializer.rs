//! Binary Serialization
use std::io::{self, Read, Write};
use crc32fast::Hasher;
pub struct Entry {
    key: Vec<u8>,
    val: Vec<u8>,
    deleted: bool,
}


impl Entry {
    pub fn new(key: Vec<u8>, val: Vec<u8>) -> Self {
        Entry {
            key,
            val,
            deleted: false,
        }
    }

    pub fn tombstone(key: Vec<u8>) -> Self {
        Entry {
            key,
            val: Vec::new(),
            deleted: true,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.val
    }
    // serialization
    // native example
    // pub fn encode(&self) -> Vec<u8> {
    //     let key_len = self.key.len() as u32;
    //     let val_len = self.val.len() as u32;

    //     let mut data = Vec::with_capacity(4 + 4 + self.key.len() + self.val.len());

    //     // key length
    //     data.extend_from_slice(&key_len.to_le_bytes());
    //     // value length
    //     data.extend_from_slice(&val_len.to_le_bytes());
    //     // key bytes
    //     data.extend_from_slice(&self.key);
    //     // value bytes
    //     data.extend_from_slice(&self.val);

    //     data
    // }

    // writer for tests
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf).unwrap();
        buf
    }

    // writer data like file, WAL ...
    pub fn encode_into<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut payload = Vec::new();

        payload.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(self.val.len() as u32).to_le_bytes());
        payload.push(self.deleted as u8);
        payload.extend_from_slice(&self.key);
        payload.extend_from_slice(&self.val);

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        w.write_all(&crc.to_le_bytes())?;
        w.write_all(&payload)?;

        Ok(())
    }

    // deserialization
    pub fn decode<R: Read>(r: &mut R) -> io::Result<Self> {
        use std::io::{Error, ErrorKind};

        let mut crc_buf = [0u8; 4];
        r.read_exact(&mut crc_buf)?;
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut header = [0u8; 9]; // key_len(4) + val_len(4) + deleted(1)
        r.read_exact(&mut header)?;

        let key_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let deleted = header[8] != 0;

        let mut key = vec![0u8; key_len];
        let mut val = vec![0u8; val_len];

        r.read_exact(&mut key)?;
        r.read_exact(&mut val)?;

        let mut hasher = Hasher::new();
        hasher.update(&header);
        hasher.update(&key);
        hasher.update(&val);
        let actual_crc = hasher.finalize();

        if actual_crc != expected_crc {
            return Err(Error::new(ErrorKind::InvalidData, "bad checksum"));
        }

        Ok(Entry { key, val, deleted })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode() {
        let ent = Entry {
            key: b"a".to_vec(),
            val: b"bb".to_vec(),
            deleted: false,
        };

        let encoded = ent.encode();

        assert_eq!(encoded, vec![59, 37, 55, 31, 1, 0, 0, 0, 2, 0, 0, 0, 0, 97, 98, 98]);
    }

    #[test]
    fn encode_then_decode() {
        let entry = Entry {
            key: b"barbambia".to_vec(),
            val: b"kergudu".to_vec(),
            deleted: false,
        };

        let data = entry.encode();

        let mut cursor = std::io::Cursor::new(data);
        let decoded = Entry::decode(&mut cursor).unwrap();

        assert_eq!(decoded.key, b"barbambia");
        assert_eq!(decoded.val, b"kergudu");
        assert!(!decoded.deleted);
    }

    #[test]
    fn encode_into_then_decode() {
        let entry = Entry {
            key: b"barbambia".to_vec(),
            val: b"kergudu".to_vec(),
            deleted: false,
        };

        let mut buf = std::io::Cursor::new(Vec::new());
        entry.encode_into(&mut buf).unwrap();

        buf.set_position(0);

        let decoded = Entry::decode(&mut buf).unwrap();

        assert_eq!(decoded.key, b"barbambia");
        assert_eq!(decoded.val, b"kergudu");
        assert!(!decoded.deleted);
    }

    #[test]
    fn encode_then_decode_delete() {
        let entry = Entry {
            key: b"to-delete".to_vec(),
            val: Vec::new(),
            deleted: true,
        };

        let data = entry.encode();
        let mut cursor = std::io::Cursor::new(data);
        let decoded = Entry::decode(&mut cursor).unwrap();

        assert_eq!(decoded.key, b"to-delete");
        assert!(decoded.val.is_empty());
        assert!(decoded.deleted);
    }
}
