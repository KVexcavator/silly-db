use super::core::{Parser, ParserError};
use crate::model::data_types::CellType;
use crate::sql::utils::is_digit;

impl<'a> Parser<'a> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}