//! Conversions of `string-or-bytes`, the type of the values Valkey keeps as given, UTF-8 or not.

use crate::types::StringOrBytes;

impl StringOrBytes {
    /// A value from bytes, a string when they are UTF-8, without copying them.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        match String::from_utf8(bytes) {
            Ok(text) => StringOrBytes::String(text),
            Err(error) => StringOrBytes::Bytes(error.into_bytes()),
        }
    }

    /// The bytes of the value, UTF-8 for a string.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            StringOrBytes::String(text) => text.as_bytes(),
            StringOrBytes::Bytes(bytes) => bytes,
        }
    }

    /// The bytes of the value, UTF-8 for a string, without copying them.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            StringOrBytes::String(text) => text.into_bytes(),
            StringOrBytes::Bytes(bytes) => bytes,
        }
    }

    /// The value with a prefix, e.g. `[` for an inclusive bound of ZRANGE BYLEX, of the same kind.
    pub fn prefixed(self, prefix: &str) -> Self {
        match self {
            StringOrBytes::String(text) => StringOrBytes::String(format!("{prefix}{text}")),
            StringOrBytes::Bytes(bytes) => {
                StringOrBytes::Bytes([prefix.as_bytes(), &bytes].concat())
            }
        }
    }
}

impl From<String> for StringOrBytes {
    fn from(text: String) -> Self {
        StringOrBytes::String(text)
    }
}

impl From<&str> for StringOrBytes {
    fn from(text: &str) -> Self {
        StringOrBytes::String(text.to_string())
    }
}

/// Equal to a string when it is the same text, e.g. a word of a command in a test.
impl PartialEq<str> for StringOrBytes {
    fn eq(&self, other: &str) -> bool {
        matches!(self, StringOrBytes::String(text) if text == other)
    }
}

impl PartialEq<&str> for StringOrBytes {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl PartialEq<String> for StringOrBytes {
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_bytes() {
        assert_eq!(
            StringOrBytes::from_bytes(b"text".to_vec()),
            StringOrBytes::String("text".to_string())
        );
        assert_eq!(
            StringOrBytes::from_bytes(vec![0x80]),
            StringOrBytes::Bytes(vec![0x80])
        );
    }

    #[test]
    fn prefixed() {
        assert_eq!(StringOrBytes::from("a").prefixed("["), "[a");
        assert_eq!(
            StringOrBytes::Bytes(vec![0x80]).prefixed("("),
            StringOrBytes::Bytes(vec![b'(', 0x80])
        );
    }

    #[test]
    fn bytes() {
        assert_eq!(StringOrBytes::from("a").as_bytes(), b"a");
        assert_eq!(StringOrBytes::Bytes(vec![0x80]).into_bytes(), vec![0x80]);
    }
}
