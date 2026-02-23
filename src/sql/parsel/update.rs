use super::core::{Parser, ParserError};
use crate::sql::ast::StmtUpdate;

impl<'a> Parser<'a> {
    pub fn parse_update(&mut self) -> Result<StmtUpdate, ParserError> {
        Err(ParserError::NotImplemented)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

// }