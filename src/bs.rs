//! Binary Serialization

pub struct Entry {
    key: Vec<u8>,
    val: Vec<u8>,
}

impl Entry {
    // serialization
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u32;
        let val_len = self.val.len() as u32;

        let mut data = Vec::with_capacity(4 + 4 + self.key.len() + self.val.len());

        // key length
        data.extend_from_slice(&key_len.to_le_bytes());
        // value length
        data.extend_from_slice(&val_len.to_le_bytes());
        // key bytes
        data.extend_from_slice(&self.key);
        // value bytes
        data.extend_from_slice(&self.val);

        data
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
        };

        let encoded = ent.encode();

        assert_eq!(encoded, vec![1, 0, 0, 0, 2, 0, 0, 0, b'a', b'b', b'b',]);
    }
}
