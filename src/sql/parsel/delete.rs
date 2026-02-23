use super::core::{Parser, ParserError};
use crate::sql::ast::StmtDelete;

impl<'a> Parser<'a> {
    pub fn parse_delete(&mut self) -> Result<StmtDelete, ParserError> {
        Err(ParserError::NotImplemented)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

// }