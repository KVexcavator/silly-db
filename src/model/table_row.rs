use crate::model::data_types::{CellType, DecodeError};
use crate::model::table_schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub cells: Vec<CellType>,
}

impl Row {
    pub fn encode_key(&self, schema: &Schema) -> Vec<u8> {
        assert_eq!(self.cells.len(), schema.cols.len());

        let mut key = Vec::new();
        key.extend_from_slice(schema.table.as_bytes());
        key.push(0x00);

        for &idx in &schema.pkey {
            let col = &schema.cols[idx];
            let cell = &self.cells[idx];

            assert!(
                col.data_types.same_type(cell),
                "column {} type mismatch",
                col.name
            );

            cell.encode(&mut key);
        }

        key
    }

    pub fn decode_key(
        &mut self,
        schema: &Schema,
        mut key: &[u8],
    ) -> Result<(), DecodeError> {
        let prefix_len = schema.table.len() + 1;
        key = &key[prefix_len..];

        for &idx in &schema.pkey {
            let (cell, rest) = CellType::decode(key)?;
            self.cells[idx] = cell;
            key = rest;
        }

        Ok(())
    }

    pub fn encode_val(&self, schema: &Schema) -> Vec<u8> {
        assert_eq!(self.cells.len(), schema.cols.len());

        let mut val = Vec::new();

        for (idx, col) in schema.cols.iter().enumerate() {
            if schema.pkey.contains(&idx) {
                continue;
            }

            let cell = &self.cells[idx];

            assert!(
                col.data_types.same_type(cell),
                "column {} type mismatch",
                col.name
            );

            cell.encode(&mut val);
        }

        val
    }

    pub fn decode_val(
        &mut self,
        schema: &Schema,
        mut val: &[u8],
    ) -> Result<(), DecodeError> {
        for (idx, col) in schema.cols.iter().enumerate() {
            if schema.pkey.contains(&idx) {
                continue;
            }

            let (cell, rest) = CellType::decode(val)?;

            assert!(
                col.data_types.same_type(&cell),
                "column {} type mismatch",
                col.name
            );

            self.cells[idx] = cell;
            val = rest;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::table_schema::Column;

    fn schema() -> Schema {
        Schema {
            table: "link".into(),
            cols: vec![
                Column { name: "time".into(), data_types: CellType::I64(0) },
                Column { name: "src".into(), data_types: CellType::Str(vec![]) },
                Column { name: "dst".into(), data_types: CellType::Str(vec![]) },
            ],
            pkey: vec![1, 2],
        }
    }

    #[test]
    fn encode_decode_row() {
        let schema = schema();

        let row = Row {
            cells: vec![
                CellType::I64(123),
                CellType::Str(b"a".to_vec()),
                CellType::Str(b"b".to_vec()),
            ],
        };

        let key = row.encode_key(&schema);
        let val = row.encode_val(&schema);

        let mut decoded = schema.new_row();
        decoded.decode_key(&schema, &key).unwrap();
        decoded.decode_val(&schema, &val).unwrap();

        assert_eq!(row, decoded);
    }
}