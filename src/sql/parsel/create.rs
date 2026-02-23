use super::core::{Parser, ParserError};
use crate::sql::ast::StmtCreateTable;

impl<'a> Parser<'a> {
    pub fn parse_create_table(&mut self) -> Result<StmtCreateTable, ParserError> {
        Err(ParserError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

#[test]
    fn parse_stmt_create_table_recognized() {
        let sql = "create table t (a int64)";
        let mut p = Parser::new(sql);

        let err = p.parse_stmt().unwrap_err();

        assert_eq!(err, ParserError::NotImplemented);
    }  
}