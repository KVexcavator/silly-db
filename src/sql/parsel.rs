use crate::sql::utils::{
  is_name_continue, 
  is_name_start, 
  is_space,
  is_separator,
};

pub struct  Parser<'a> {
  buffer: &'a [u8],
  position: usize,
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
        return  None;
      };

      if !is_name_start(ch) {
        return  None;
      }

      self.position += 1;

      while  let Some(ch)  = self.peek() {
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

    #[allow(unused)]
    fn eof(&self) -> bool {
      self.position >= self.buffer.len()
    }

    fn peek(&self) -> Option<u8> {
      self.buffer.get(self.position).copied()
    }

    fn skip_spaces(&mut self) {
      while  let Some(ch) = self.peek() {
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
}
