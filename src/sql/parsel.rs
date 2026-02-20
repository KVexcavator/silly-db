use crate::model::data_types::CellType;
use crate::sql::ast::{
    NamedCell, Statement, StmtCreateTable, StmtDelete, StmtInsert, StmtSelect, StmtUpdate,
};
use crate::sql::utils::{is_digit, is_name_continue, is_name_start, is_separator, is_space};

pub struct Parser<'a> {
    buffer: &'a [u8],
    position: usize,
}

#[derive(Debug, PartialEq)]
pub enum ParserError {
    ExpectValue,
    InvalidInt,
    UnterminatedString,
    InvalidEscape,
    UnknownStatement,
    NotImplemented,
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str) -> Self {
        Parser {
            buffer: s.as_bytes(),
            position: 0,
        }
    }

    pub fn try_name(&mut self) -> Option<String> {
        self.skip_spaces();

        let start = self.position;

        let Some(ch) = self.peek() else {
            return None;
        };

        if !is_name_start(ch) {
            return None;
        }

        self.position += 1;

        while let Some(ch) = self.peek() {
            if !is_name_continue(ch) {
                break;
            }
            self.position += 1;
        }

        let s = std::str::from_utf8(&self.buffer[start..self.position]).unwrap();
        Some(s.to_string())
    }

    pub fn try_keyword(&mut self, kw: &str) -> bool {
        self.skip_spaces();

        #[allow(unused)]
        let start = self.position;
        let kw_bytes = kw.as_bytes();

        if (self.buffer.len() - self.position) < kw_bytes.len() {
            return false;
        }

        // case-insensitive compare
        for (i, &k) in kw_bytes.iter().enumerate() {
            let b = self.buffer[self.position + i];
            if (b | 32) != (k | 32) {
                return false;
            }
        }

        let next_pos = self.position + kw_bytes.len();

        // must be separator or EOF
        if next_pos < self.buffer.len() {
            let ch = self.buffer[next_pos];
            if !is_separator(ch) {
                return false;
            }
        }

        self.position = next_pos;
        true
    }

    pub fn try_keywords(&mut self, kws: &[&str]) -> bool {
        let start = self.position;

        for kw in kws {
            if !self.try_keyword(kw) {
                self.position = start;
                return false;
            }
        }

        true
    }

    pub fn parse_value(&mut self) -> Result<CellType, ParserError> {
        self.skip_spaces();

        let ch = self.peek().ok_or(ParserError::ExpectValue)?;

        if ch == b'\'' || ch == b'"' {
            self.parse_string()
        } else if ch == b'+' || ch == b'-' || is_digit(ch) {
            self.parse_int()
        } else {
            Err(ParserError::ExpectValue)
        }
    }

    fn parse_int(&mut self) -> Result<CellType, ParserError> {
        self.skip_spaces();
        let start = self.position;

        if let Some(ch) = self.peek() {
            if ch == b'+' || ch == b'-' {
                self.position += 1;
            }
        }

        let digit_start = self.position;

        while let Some(ch) = self.peek() {
            if !is_digit(ch) {
                break;
            }
            self.position += 1;
        }

        if self.position == digit_start {
            self.position = start;
            return Err(ParserError::InvalidInt);
        }

        let s = std::str::from_utf8(&self.buffer[start..self.position]).unwrap();

        let value = s.parse::<i64>().map_err(|_| {
            self.position = start;
            ParserError::InvalidInt
        })?;

        Ok(CellType::I64(value))
    }

    fn parse_string(&mut self) -> Result<CellType, ParserError> {
        self.skip_spaces();
        let quote = self.peek().ok_or(ParserError::UnterminatedString)?;

        if quote != b'\'' && quote != b'"' {
            return Err(ParserError::UnterminatedString);
        }

        self.position += 1;

        let mut outer = Vec::new();

        while let Some(ch) = self.peek() {
            self.position += 1;

            if ch == quote {
                return Ok(CellType::Str(outer));
            }

            if ch == b'\\' {
                let escape = self.peek().ok_or(ParserError::InvalidEscape)?;
                self.position += 1;

                match escape {
                    b'\\' => outer.push(b'\\'),
                    b'\'' => outer.push(b'\''),
                    b'"' => outer.push(b'"'),
                    _ => return Err(ParserError::InvalidEscape),
                }
            } else {
                outer.push(ch);
            }
        }
        Err(ParserError::UnterminatedString)
    }

    pub fn try_punctuation(&mut self, pnt: &str) -> bool {
        self.skip_spaces();

        let bytes = pnt.as_bytes();

        if (self.buffer.len() - self.position) < bytes.len() {
            return false;
        }

        if &self.buffer[self.position..self.position + bytes.len()] != bytes {
            return false;
        }

        self.position += bytes.len();
        true
    }

    pub fn parse_equal(&mut self) -> Result<NamedCell, ParserError> {
        let column = self.try_name().ok_or(ParserError::ExpectValue)?;

        if !self.try_punctuation("=") {
            return Err(ParserError::ExpectValue);
        }

        let value = self.parse_value()?;

        Ok(NamedCell::new(column, value))
    }

    pub fn parse_where(&mut self) -> Result<Vec<NamedCell>, ParserError> {
        let mut keys = Vec::new();

        if !self.try_keyword("WHERE") {
            return Ok(keys);
        }

        loop {
            let name_cell = self.parse_equal()?;
            keys.push(name_cell);

            if !self.try_keyword("AND") {
                break;
            }
        }

        Ok(keys)
    }

    pub fn parse_stmt(&mut self) -> Result<Statement, ParserError> {
        self.skip_spaces();

        if self.try_keyword("SELECT") {
            let stmt = self.parse_select()?;
            return Ok(Statement::Select(stmt));
        }

        if self.try_keywords(&["CREATE", "TABLE"]) {
            let stmt = self.parse_create_table()?;
            return Ok(Statement::CreateTable(stmt));
        }

        if self.try_keywords(&["INSERT", "INTO"]) {
            let stmt = self.parse_insert()?;
            return Ok(Statement::Insert(stmt));
        }

        if self.try_keyword("UPDATE") {
            let stmt = self.parse_update()?;
            return Ok(Statement::Update(stmt));
        }

        if self.try_keywords(&["DELETE", "FROM"]) {
            let stmt = self.parse_delete()?;
            return Ok(Statement::Delete(stmt));
        }

        Err(ParserError::UnknownStatement)
    }

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

    fn parse_create_table(&mut self) -> Result<StmtCreateTable, ParserError> {
        Err(ParserError::NotImplemented)
    }

    fn parse_insert(&mut self) -> Result<StmtInsert, ParserError> {
        Err(ParserError::NotImplemented)
    }

    fn parse_update(&mut self) -> Result<StmtUpdate, ParserError> {
        Err(ParserError::NotImplemented)
    }

    fn parse_delete(&mut self) -> Result<StmtDelete, ParserError> {
        Err(ParserError::NotImplemented)
    }

    #[allow(unused)]
    fn eof(&self) -> bool {
        self.position >= self.buffer.len()
    }

    fn peek(&self) -> Option<u8> {
        self.buffer.get(self.position).copied()
    }

    fn skip_spaces(&mut self) {
        while let Some(ch) = self.peek() {
            if !is_space(ch) {
                break;
            }
            self.position += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_name_simple() {
        let mut p = Parser::new("abc");
        let name = p.try_name().unwrap();
        assert_eq!(name, "abc");
        assert_eq!(p.position, 3);
    }

    #[test]
    fn parse_name_with_spaces() {
        let mut p = Parser::new("   hello1 ");
        let name = p.try_name().unwrap();
        assert_eq!(name, "hello1");
    }

    #[test]
    fn parse_name_fail() {
        let mut p = Parser::new("123");
        assert!(p.try_name().is_none());
        assert_eq!(p.position, 0);
    }

    #[test]
    fn keyword_case_insensitive() {
        let mut p = Parser::new("   SeLeCt ");
        assert!(p.try_keyword("select"));
    }

    #[test]
    fn keyword_requires_separator() {
        let mut p = Parser::new("selectx");
        assert!(!p.try_keyword("select"));
    }

    #[test]
    fn keyword_moves_position() {
        let mut p = Parser::new("select a");
        assert!(p.try_keyword("select"));
        assert_eq!(p.position, 6);
    }

    #[test]
    fn parse_int_simple() {
        let mut p = Parser::new("123");
        let v = p.parse_value().unwrap();
        assert_eq!(v, CellType::I64(123));
    }

    #[test]
    fn parse_int_negative() {
        let mut p = Parser::new("-42");
        let v = p.parse_value().unwrap();
        assert_eq!(v, CellType::I64(-42));
    }

    #[test]
    fn parse_string_single_quotes() {
        let mut p = Parser::new("'abc'");
        let v = p.parse_value().unwrap();
        assert_eq!(v, CellType::Str(b"abc".to_vec()));
    }

    #[test]
    fn parse_string_double_quotes() {
        let mut p = Parser::new("\"abc\"");
        let v = p.parse_value().unwrap();
        assert_eq!(v, CellType::Str(b"abc".to_vec()));
    }

    #[test]
    fn parse_string_escape() {
        let mut p = Parser::new("'a\\'b'");
        let v = p.parse_value().unwrap();
        assert_eq!(v, CellType::Str(b"a'b".to_vec()));
    }

    #[test]
    fn parse_string_backslash() {
        let mut p = Parser::new("'a\\\\b'");
        let v = p.parse_value().unwrap();
        assert_eq!(v, CellType::Str(b"a\\b".to_vec()));
    }

    #[test]
    fn parse_value_fail() {
        let mut p = Parser::new("abc");
        assert!(p.parse_value().is_err());
    }

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

    #[test]
    fn parse_stmt_create_table_recognized() {
        let sql = "create table t (a int64)";
        let mut p = Parser::new(sql);

        let err = p.parse_stmt().unwrap_err();

        assert_eq!(err, ParserError::NotImplemented);
    }

    #[test]
    fn parse_stmt_insert_recognized() {
        let sql = "insert into t values (1)";
        let mut p = Parser::new(sql);

        let err = p.parse_stmt().unwrap_err();

        assert_eq!(err, ParserError::NotImplemented);
    }
}
