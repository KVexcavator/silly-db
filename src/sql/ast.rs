use crate::model::data_types::CellType;
use crate::model::table_schema::Column;

#[derive(Debug, PartialEq)]
pub struct NamedCell {
    pub column: String,
    pub value: CellType,
}

impl NamedCell {
    pub fn new(column: String, value: CellType) -> Self {
        Self { column, value }
    }
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select(StmtSelect),
    CreateTable(StmtCreateTable),
    Insert(StmtInsert),
    Update(StmtUpdate),
    Delete(StmtDelete),
}

#[derive(Debug, PartialEq)]
pub struct StmtSelect {
    pub table: String,
    pub cols: Vec<String>,
    pub keys: Vec<NamedCell>,
}

#[derive(Debug, PartialEq)]
pub struct StmtCreateTable {
    pub table: String,
    pub cols: Vec<Column>,
    pub pkey: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct StmtInsert {
    pub table: String,
    pub values: Vec<CellType>,
}

#[derive(Debug, PartialEq)]
pub struct StmtUpdate {
    pub table: String,
    pub keys: Vec<NamedCell>,
    pub values: Vec<NamedCell>,
}

#[derive(Debug, PartialEq)]
pub struct StmtDelete {
    pub table: String,
    pub keys: Vec<NamedCell>,
}
