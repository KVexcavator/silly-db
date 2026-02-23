use super::core::{Parser, ParserError};
use crate::sql::ast::StmtInsert;

impl<'a> Parser<'a> {
    pub fn parse_insert(&mut self) -> Result<StmtInsert, ParserError> {
        Err(ParserError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stmt_insert_recognized() {
        let sql = "insert into t values (1)";
        let mut p = Parser::new(sql);

        let err = p.parse_stmt().unwrap_err();

        assert_eq!(err, ParserError::NotImplemented);
    }

}