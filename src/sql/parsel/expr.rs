use super::core::{Parser, ParserError};
use crate::sql::ast::NamedCell;

impl<'a> Parser<'a> {
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

    pub fn parse_equal(&mut self) -> Result<NamedCell, ParserError> {
        let column = self.try_name().ok_or(ParserError::ExpectValue)?;

        if !self.try_punctuation("=") {
            return Err(ParserError::ExpectValue);
        }

        let value = self.parse_value()?;

        Ok(NamedCell::new(column, value))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

// }