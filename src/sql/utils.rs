
pub fn is_space(ch: u8) -> bool {
  matches!(ch, b'\t' | b'\n' | b'\x0B' | b'\x0C' | b'\r' | b' ')
}

pub fn is_alpha(ch: u8) -> bool {
    let c = ch | 32;
    b'a' <= c && c <= b'z'
}

pub fn is_digit(ch: u8) -> bool {
    b'0' <= ch && ch <= b'9'
}

pub fn is_name_start(ch: u8) -> bool {
    is_alpha(ch) || ch == b'_'
}

pub fn is_name_continue(ch: u8) -> bool {
    is_alpha(ch) || is_digit(ch) || ch == b'_'
}

pub fn is_separator(ch: u8) -> bool {
    ch < 128 && !is_name_continue(ch)
}
