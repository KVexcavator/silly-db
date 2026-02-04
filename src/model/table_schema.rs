use crate::model::data_types::CellType;
use crate::model::table_row::Row;

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub table: String,
    pub cols: Vec<Column>,
    pub pkey: Vec<usize>, // indexes of columns
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_types: CellType,
}

impl Schema {
    pub fn new_row(&self) -> Row {
        Row {
            cells: self.cols.iter().map(|col| match col.data_types {
                CellType::I64(_) => CellType::I64(0),
                CellType::Str(_) => CellType::Str(Vec::new()),
            }).collect(),
        }
    }
}


