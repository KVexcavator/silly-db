use crate::model::data_types::CellType;

#[derive(Debug, PartialEq)]
pub  struct StmtSelect {
  pub table: String,
  pub cols: Vec<String>,
  pub keys: Vec<NamedCell>,
}

#[derive(Debug, PartialEq)]
pub  struct  NamedCell {
  pub column: String,
  pub value: CellType,
}

impl NamedCell {
    pub fn new(column: String, value: CellType) -> Self {
        Self { column, value }
    }
}