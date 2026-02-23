use super::core::{Parser, ParserError};
use crate::sql::ast::StmtSelect;

impl<'a> Parser<'a> {
    pub fn parse_select(&mut self) -> Result<StmtSelect, ParserError> {
        let mut cols = Vec::new();

        loop {
            if let Some(name) = self.try_name() {
                cols.push(name);
            } else {
                return Err(ParserError::ExpectValue);
            }

            if self.try_keyword("FROM") {
                break;
            }

            if !self.try_punctuation(",") {
                return Err(ParserError::ExpectValue);
            }
        }

        if cols.is_empty() {
            return Err(ParserError::ExpectValue);
        }

        let table = self.try_name().ok_or(ParserError::ExpectValue)?;

        let keys = self.parse_where()?;

        Ok(StmtSelect { table, cols, keys })
    }
}

#[cfg(test)]
mod tests {
    use crate::model::data_types::CellType;
    use crate::sql::ast::{NamedCell, Statement};
    use super::*;

    #[test]
    fn parse_select_simple() {
        let sql = "select a,b from t where c=1 and d='e'";
        let mut p = Parser::new(sql);

        let stmt = p.parse_stmt().unwrap();

        match stmt {
            Statement::Select(stmt) => {
                assert_eq!(
                    stmt,
                    StmtSelect {
                        table: "t".to_string(),
                        cols: vec!["a".to_string(), "b".to_string()],
                        keys: vec![
                            NamedCell {
                                column: "c".to_string(),
                                value: CellType::I64(1),
                            },
                            NamedCell {
                                column: "d".to_string(),
                                value: CellType::Str(b"e".to_vec()),
                            },
                        ],
                    }
                );
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_stmt_select() {
        let sql = "select a from t where id=1";
        let mut p = Parser::new(sql);

        let stmt = p.parse_stmt().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.table, "t");
                assert_eq!(s.cols, vec!["a"]);
            }
            _ => panic!("expected select"),
        }
    }

}