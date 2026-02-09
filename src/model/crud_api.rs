use crate::core::key_value::{KV, KVError};
use crate::model::data_types::DecodeError;
use crate::model::table_row::Row;
use crate::model::table_schema::Schema;
use crate::model::update_modes::UpdateMode;

pub struct DB {
    pub kv: KV,
}

#[derive(Debug)]
pub enum DBError {
    KV(KVError),
    Decode(DecodeError),
}

impl From<KVError> for DBError {
    fn from(err: KVError) -> Self {
        DBError::KV(err)
    }
}

impl From<DecodeError> for DBError {
    fn from(err: DecodeError) -> Self {
        DBError::Decode(err)
    }
}

impl DB {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, DBError> {
        Ok(DB {
            kv: KV::open(path)?,
        })
    }

    pub fn close(&mut self) -> Result<(), DBError> {
        self.kv.close()?;
        Ok(())
    }

    fn write_row(&mut self, schema: &Schema, row: &Row, mode: UpdateMode) -> Result<bool, DBError> {
        let key = row.encode_key(schema);
        let val = row.encode_val(schema);

        Ok(self.kv.set_if_existed(&key, &val, mode)?)
    }

    pub fn insert(&mut self, schema: &Schema, row: &Row) -> Result<bool, DBError> {
        self.write_row(schema, row, UpdateMode::Insert)
    }

    pub fn upsert(&mut self, schema: &Schema, row: &Row) -> Result<bool, DBError> {
        self.write_row(schema, row, UpdateMode::Upsert)
    }

    pub fn update(&mut self, schema: &Schema, row: &Row) -> Result<bool, DBError> {
        self.write_row(schema, row, UpdateMode::Update)
    }

    pub fn delete(&mut self, schema: &Schema, row: &Row) -> Result<bool, DBError> {
        let key = row.encode_key(schema);
        Ok(self.kv.del(&key)?)
    }

    pub fn select(&self, schema: &Schema, row: &mut Row) -> Result<bool, DBError> {
        let key = row.encode_key(schema);

        let Some(val) = self.kv.get(&key)? else {
            return Ok(false);
        };

        row.decode_val(schema, &val)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::data_types::CellType;
    use crate::model::table_schema::{Column, Schema};

    fn schema() -> Schema {
        Schema {
            table: "t".into(),
            cols: vec![
                Column {
                    name: "a".into(),
                    data_types: CellType::I64(0),
                },
                Column {
                    name: "b".into(),
                    data_types: CellType::I64(0),
                },
            ],
            pkey: vec![1],
        }
    }

    fn row(a: i64, b: i64) -> Row {
        Row {
            cells: vec![CellType::I64(a), CellType::I64(b)],
        }
    }

    #[test]
    fn insert_and_select() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut db = DB::open(&path).unwrap();
        let schema = schema();

        db.insert(&schema, &row(10, 1)).unwrap();

        let mut r = row(0, 1);
        let ok = db.select(&schema, &mut r).unwrap();

        assert!(ok);
        assert_eq!(r, row(10, 1));
    }

    #[test]
    fn insert_duplicate_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut db = DB::open(&path).unwrap();
        let schema = schema();

        db.insert(&schema, &row(10, 1)).unwrap();
        let updated = db.insert(&schema, &row(20, 1)).unwrap();

        assert!(!updated);
    }

    #[test]
    fn update_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut db = DB::open(&path).unwrap();
        let schema = schema();

        db.insert(&schema, &row(10, 1)).unwrap();
        db.update(&schema, &row(20, 1)).unwrap();

        let mut r = row(0, 1);
        db.select(&schema, &mut r).unwrap();

        assert_eq!(r, row(20, 1));
    }

    #[test]
    fn delete_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.log");

        let mut db = DB::open(&path).unwrap();
        let schema = schema();

        db.insert(&schema, &row(10, 1)).unwrap();
        db.delete(&schema, &row(0, 1)).unwrap();

        let mut r = row(0, 1);
        let ok = db.select(&schema, &mut r).unwrap();

        assert!(!ok);
    }
}
