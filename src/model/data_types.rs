use std::convert::TryInto;

#[derive(Debug, Clone, PartialEq)]
pub enum CellType {
    I64(i64),
    Str(Vec<u8>),
}

const TYPE_I64: u8 = 1;
const TYPE_STR: u8 = 2;

#[derive(Debug)]
pub enum DecodeError {
    UnexpectedEOF,
    UnknownType(u8),
}

impl CellType {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            CellType::I64(v) => {
                out.push(TYPE_I64);
                out.extend_from_slice(&v.to_le_bytes());
            }
            CellType::Str(s) => {
                out.push(TYPE_STR);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
    }

    pub fn decode(mut data: &[u8]) -> Result<(CellType, &[u8]), DecodeError> {
        if data.len() < 1 {
            return Err(DecodeError::UnexpectedEOF);
        }

        let data_types = data[0];
        data = &data[1..];

        match data_types {
            TYPE_I64 => {
                if data.len() < 8 {
                    return Err(DecodeError::UnexpectedEOF);
                }
                let v = i64::from_le_bytes(data[..8].try_into().unwrap());
                Ok((CellType::I64(v), &data[8..]))
            }

            TYPE_STR => {
                if data.len() < 4 {
                    return Err(DecodeError::UnexpectedEOF);
                }
                let len =
                    u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
                data = &data[4..];

                if data.len() < len {
                    return Err(DecodeError::UnexpectedEOF);
                }

                let s = data[..len].to_vec();
                Ok((CellType::Str(s), &data[len..]))
            }

            other => Err(DecodeError::UnknownType(other)),
        }
    }

    pub fn same_type(&self, other: &CellType) -> bool {
        matches!(
            (self, other),
            (CellType::I64(_), CellType::I64(_))
                | (CellType::Str(_), CellType::Str(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_i64() {
        let cell = CellType::I64(-123456789);
        let mut buf = Vec::new();

        cell.encode(&mut buf);

        let (decoded, rest) = CellType::decode(&buf).unwrap();
        assert_eq!(decoded, cell);
        assert!(rest.is_empty());
    }

    #[test]
    fn encode_decode_str() {
        let cell = CellType::Str(b"hello world".to_vec());
        let mut buf = Vec::new();

        cell.encode(&mut buf);

        let (decoded, rest) = CellType::decode(&buf).unwrap();
        assert_eq!(decoded, cell);
        assert!(rest.is_empty());
    }

    #[test]
    fn encode_multiple_cells() {
        let cells = vec![
            CellType::I64(42),
            CellType::Str(b"abc".to_vec()),
            CellType::I64(-1),
        ];

        let mut buf = Vec::new();
        for c in &cells {
            c.encode(&mut buf);
        }

        let mut data = buf.as_slice();
        for expected in cells {
            let (cell, rest) = CellType::decode(data).unwrap();
            assert_eq!(cell, expected);
            data = rest;
        }

        assert!(data.is_empty());
    }
}