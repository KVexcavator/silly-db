//! Binary Serialization
use std::io::{self, Read, Write};
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
    pub fn key(&self) -> &[u8] {
        &self.key
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
        w.write_all(&(self.key.len() as u32).to_le_bytes())?;
        w.write_all(&(self.val.len() as u32).to_le_bytes())?;
        w.write_all(&[self.deleted as u8])?;
        w.write_all(&self.key)?;
        w.write_all(&self.val)?;
        Ok(())
    }

    // deserialization
    pub fn decode<R: Read>(reader: &mut R) -> io::Result<Self> {
        // read key length
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;
        // read value length
        reader.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;
        // deleted flag
        let mut flag = [0u8; 1];
        reader.read_exact(&mut flag)?;
        let deleted = flag[0] != 0;
        // read key
        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;
        // read value
        let mut val = vec![0u8; val_len];
        reader.read_exact(&mut val)?;

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

        assert_eq!(encoded, vec![1, 0, 0, 0, 2, 0, 0, 0, 0, b'a', b'b', b'b',]);
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
