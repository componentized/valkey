//! RESP serialization, from <https://github.com/iorust/resp/tree/v1.0.3>.

use crate::types::{RespError as Error, RespNestedValue, RespValue};

/// up to 512 MB in length
const RESP_MAX_SIZE: i64 = 512 * 1024 * 1024;
const CRLF_BYTES: &'static [u8] = b"\r\n";

/// Encodes RESP value to RESP binary buffer.
/// # Examples
/// ```
/// # use componentized_valkey::resp::encode;
/// # use componentized_valkey::types::RespValue;
/// let val = RespValue::String("OK".to_string());
/// assert_eq!(encode(val), b"+OK\r\n");
/// ```
pub fn encode(value: RespValue) -> Vec<u8> {
    let mut res: Vec<u8> = Vec::new();
    buf_encode(value, &mut res);
    res
}

#[inline]
fn buf_encode(value: RespValue, buf: &mut Vec<u8>) {
    match value {
        RespValue::Null => {
            buf.push(b'_');
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::String(val) => {
            buf.push(b'+');
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::Error(val) => {
            buf.push(b'-');
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::Integer(val) => {
            buf.push(b':');
            buf.extend_from_slice(val.to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::BulkString(val) => {
            buf.push(b'$');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            buf.extend(val);
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::Array(val) => {
            buf.push(b'*');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for item in val {
                buf.extend_from_slice(&item);
            }
        }
        RespValue::Boolean(ref val) => {
            buf.push(b'#');
            match val {
                true => buf.push(b't'),
                false => buf.push(b'f'),
            }
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::Double(ref val) => {
            buf.push(b',');
            buf.extend_from_slice(val.to_string().to_lowercase().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::BigNumber(ref val) => {
            buf.push(b'(');
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::BulkError(ref val) => {
            buf.push(b'!');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::VerbatimString((ref encoding, ref value)) => {
            let val = format!("{encoding}:{value}");
            buf.push(b'=');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        RespValue::Map(val) => {
            buf.push(b'%');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for (key, value) in val {
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&value);
            }
        }
        RespValue::Set(val) => {
            buf.push(b'~');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for item in val {
                buf.extend_from_slice(&item);
            }
        }
        RespValue::Push(val) => {
            buf.push(b'>');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for item in val {
                buf.extend_from_slice(&item);
            }
        }
    }
}

/// Decodes the RESP value `value` holds, which must be exactly one whole value.
///
/// The elements of an aggregate, e.g. an array, are kept as the RESP they were received as, they
/// are only checked to be whole values.
///
/// # Examples
/// ```
/// # use componentized_valkey::resp::{decode, encode};
/// # use componentized_valkey::types::RespValue;
/// let value = RespValue::BulkString(b"Hello".to_vec());
/// assert_eq!(decode(&encode(value.clone())), Ok(value));
/// ```
pub fn decode(value: &[u8]) -> Result<RespValue, Error> {
    match frame_end(value, 0)? {
        Some(end) if end == value.len() => decode_frame(value),
        Some(end) => Err(format!(
            "unexpected bytes after the RESP value: {}",
            value.len() - end
        )),
        None => Err("incomplete RESP value".to_string()),
    }
}

/// The length of the first complete RESP value in `buf`, or `None` when the value is incomplete
/// and more bytes are needed. Only the framing is checked, the value is not decoded.
pub fn frame_len(buf: &[u8]) -> Result<Option<usize>, Error> {
    frame_end(buf, 0)
}

/// The end of the RESP value starting at `start`, or `None` when it is incomplete.
fn frame_end(buf: &[u8], start: usize) -> Result<Option<usize>, Error> {
    let Some(prefix) = buf.get(start) else {
        return Ok(None);
    };
    // the header line, after the type prefix up to the CRLF
    let Some(crlf) = buf[start..].windows(2).position(|w| w == CRLF_BYTES) else {
        return Ok(None);
    };
    let header = &buf[start + 1..start + crlf];
    let end = start + crlf + 2;

    let length = |min: i64| -> Result<i64, Error> {
        let int = parse_integer(header)?;
        if int < min || int >= RESP_MAX_SIZE {
            Err(format!("invalid length: {}", int))?
        }
        Ok(int)
    };
    let children = |end: usize, count: i64| -> Result<Option<usize>, Error> {
        let mut end = end;
        for _ in 0..count {
            match frame_end(buf, end)? {
                Some(child_end) => end = child_end,
                None => return Ok(None),
            }
        }
        Ok(Some(end))
    };

    match prefix {
        // simple values end with their header line
        b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => Ok(Some(end)),
        // blob values are followed by their length in bytes and a CRLF, a bulk string may be
        // null with a length of -1 in RESP2
        b'$' | b'!' | b'=' => {
            let int = length(if *prefix == b'$' { -1 } else { 0 })?;
            if int == -1 {
                return Ok(Some(end));
            }
            let end = end + int as usize + 2;
            match buf.get(end - 2..end) {
                Some(CRLF_BYTES) => Ok(Some(end)),
                Some(terminator) => Err(format!("invalid CRLF after blob: {terminator:?}")),
                None => Ok(None),
            }
        }
        // aggregates are followed by their elements, an array may be null with a length of -1 in
        // RESP2
        b'*' | b'~' | b'>' => {
            let int = length(if *prefix == b'*' { -1 } else { 0 })?;
            if int == -1 {
                return Ok(Some(end));
            }
            children(end, int)
        }
        // maps are followed by a key and a value for each entry
        b'%' => children(end, length(0)? * 2),
        prefix => Err(format!("invalid RESP type: {:?}", prefix)),
    }
}

/// Decodes the whole RESP value at the start of `buf`, its framing already checked by `frame_end`.
fn decode_frame(buf: &[u8]) -> Result<RespValue, Error> {
    let crlf = buf
        .windows(2)
        .position(|w| w == CRLF_BYTES)
        .ok_or("missing CRLF")?;
    let header = &buf[1..crlf];
    let body = crlf + 2;

    // the bytes of a blob value, after its length line
    let blob = || -> Result<&[u8], Error> {
        let len = parse_integer(header)? as usize;
        Ok(&buf[body..body + len])
    };
    // the elements of an aggregate value, each kept as its RESP
    let elements = |count: i64| -> Result<Vec<RespNestedValue>, Error> {
        let mut start = body;
        let mut elements = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let end = frame_end(buf, start)?.ok_or("incomplete RESP value")?;
            elements.push(buf[start..end].to_vec());
            start = end;
        }
        Ok(elements)
    };

    match buf[0] {
        b'+' => parse_string(header).map(RespValue::String),
        b'-' => parse_string(header).map(RespValue::Error),
        b':' => parse_integer(header).map(RespValue::Integer),
        b'_' => Ok(RespValue::Null),
        b'#' => match header {
            b"t" => Ok(RespValue::Boolean(true)),
            b"f" => Ok(RespValue::Boolean(false)),
            _ => Err(format!("invalid RESP boolean: {header:?}")),
        },
        b',' => parse_double(header).map(RespValue::Double),
        b'(' => parse_string(header).map(RespValue::BigNumber),
        b'$' => match parse_integer(header)? {
            // a null bulk string, RESP2's null
            -1 => Ok(RespValue::Null),
            _ => Ok(RespValue::BulkString(blob()?.to_vec())),
        },
        b'!' => parse_string(blob()?).map(RespValue::BulkError),
        b'=' => {
            let value = parse_string(blob()?)?;
            let Some((encoding, value)) = value.split_once(':') else {
                return Err("invalid verbatim string, missing encoding".to_string());
            };
            Ok(RespValue::VerbatimString((
                encoding.to_string(),
                value.to_string(),
            )))
        }
        b'*' => match parse_integer(header)? {
            // a null array, RESP2's null
            -1 => Ok(RespValue::Null),
            count => elements(count).map(RespValue::Array),
        },
        b'~' => elements(parse_integer(header)?).map(RespValue::Set),
        b'>' => elements(parse_integer(header)?).map(RespValue::Push),
        b'%' => {
            let mut entries = elements(parse_integer(header)? * 2)?.into_iter();
            let mut map = vec![];
            while let (Some(key), Some(value)) = (entries.next(), entries.next()) {
                map.push((key, value));
            }
            Ok(RespValue::Map(map))
        }
        prefix => Err(format!("invalid RESP type: {prefix:?}")),
    }
}

/// RESP values for people to read, e.g. in errors, each with its type, e.g. `integer<1>` and
/// `array<bulk-string<"a">, null>`. A bulk string that is not UTF-8 is shown escaped, e.g.
/// `bulk-string<b"\x80">`.
impl std::fmt::Display for RespValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "string<{value:?}>"),
            Self::Error(value) => write!(f, "error<{value:?}>"),
            Self::Integer(value) => write!(f, "integer<{value}>"),
            Self::BulkString(value) => match std::str::from_utf8(value) {
                Ok(text) => write!(f, "bulk-string<{text:?}>"),
                Err(_) => write!(f, "bulk-string<b\"{}\">", value.escape_ascii()),
            },
            Self::Array(items) => write_items(f, "array", items),
            Self::Null => f.write_str("null"),
            Self::Boolean(value) => write!(f, "boolean<{value}>"),
            Self::Double(value) => write!(f, "double<{value}>"),
            Self::BigNumber(value) => write!(f, "big-number<{value}>"),
            Self::BulkError(value) => write!(f, "bulk-error<{value:?}>"),
            Self::VerbatimString((format, value)) => {
                write!(f, "verbatim-string<{format}, {value:?}>")
            }
            Self::Map(entries) => {
                f.write_str("map<")?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write_nested(f, key)?;
                    f.write_str(": ")?;
                    write_nested(f, value)?;
                }
                f.write_str(">")
            }
            Self::Set(items) => write_items(f, "set", items),
            Self::Push(items) => write_items(f, "push", items),
        }
    }
}

/// The nested values of an aggregate, e.g. `array<integer<1>, null>`.
fn write_items(
    f: &mut std::fmt::Formatter<'_>,
    kind: &str,
    items: &[RespNestedValue],
) -> std::fmt::Result {
    write!(f, "{kind}<")?;
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        write_nested(f, item)?;
    }
    f.write_str(">")
}

fn write_nested(f: &mut std::fmt::Formatter<'_>, value: &[u8]) -> std::fmt::Result {
    match decode(value) {
        Ok(value) => write!(f, "{value}"),
        Err(_) => f.write_str("--invalid-resp--"),
    }
}

#[inline]
fn parse_string(bytes: &[u8]) -> Result<String, Error> {
    String::from_utf8(bytes.to_vec()).map_err(|err| err.to_string())
}

#[inline]
fn parse_integer(bytes: &[u8]) -> Result<i64, Error> {
    let str_integer = parse_string(bytes)?;
    (str_integer.parse::<i64>()).map_err(|err| err.to_string())
}

#[inline]
fn parse_double(bytes: &[u8]) -> Result<f64, Error> {
    let str_double = parse_string(bytes)?;
    (str_double.parse::<f64>()).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_len_complete_values() {
        let cases: &[&[u8]] = &[
            b"+OK\r\n",
            b"-ERR oops\r\n",
            b":42\r\n",
            b"_\r\n",
            b"#t\r\n",
            b",1.5\r\n",
            b"(123\r\n",
            b"$5\r\nhello\r\n",
            b"$0\r\n\r\n",
            b"$-1\r\n",
            b"!3\r\nerr\r\n",
            b"=7\r\ntxt:abc\r\n",
            b"*0\r\n",
            b"*-1\r\n",
            b"*2\r\n$1\r\na\r\n:1\r\n",
            b"%1\r\n+key\r\n*1\r\n$2\r\n\r\n\r\n",
            b"~2\r\n+a\r\n+b\r\n",
            b">1\r\n+msg\r\n",
        ];
        for value in cases {
            assert_eq!(frame_len(value).unwrap(), Some(value.len()), "{value:?}");
        }
    }

    #[test]
    fn frame_len_incomplete_values() {
        let value: &[u8] = b"*3\r\n$5\r\nhello\r\n%1\r\n+k\r\n$3\r\nval\r\n:7\r\n";
        for len in 0..value.len() {
            assert_eq!(frame_len(&value[..len]).unwrap(), None, "{len}");
        }
        assert_eq!(frame_len(value).unwrap(), Some(value.len()));
    }

    #[test]
    fn frame_len_ignores_following_bytes() {
        assert_eq!(frame_len(b"+OK\r\n:1\r\n").unwrap(), Some(5));
        // a bulk string's content may contain CRLF
        assert_eq!(frame_len(b"$4\r\n\r\n\r\n\r\n+next\r\n").unwrap(), Some(10));
    }

    #[test]
    fn frame_len_invalid_values() {
        assert!(frame_len(b"?nope\r\n").is_err());
        assert!(frame_len(b"$-2\r\n").is_err());
        assert!(frame_len(b"%-1\r\n").is_err());
        assert!(frame_len(b"*x\r\n").is_err());
    }

    #[test]
    fn decode_round_trips() {
        let values = vec![
            RespValue::String("OK".to_string()),
            RespValue::Error("ERR oops".to_string()),
            RespValue::Integer(-42),
            RespValue::BulkString(b"a\r\nb".to_vec()),
            RespValue::Null,
            RespValue::Boolean(false),
            RespValue::Double(1.5),
            RespValue::BigNumber("12345678901234567890".to_string()),
            RespValue::BulkError("SYNTAX invalid".to_string()),
            RespValue::VerbatimString(("txt".to_string(), "a:b".to_string())),
            RespValue::Array(vec![encode(RespValue::Integer(1)), encode(RespValue::Null)]),
            RespValue::Set(vec![encode(RespValue::String("a".to_string()))]),
            RespValue::Push(vec![encode(RespValue::String("message".to_string()))]),
            RespValue::Map(vec![(
                encode(RespValue::String("key".to_string())),
                encode(RespValue::Array(vec![encode(RespValue::Integer(1))])),
            )]),
        ];
        for value in values {
            assert_eq!(decode(&encode(value.clone())), Ok(value));
        }
    }

    #[test]
    fn decode_keeps_elements_as_received() {
        // a nested array, kept as its RESP rather than decoded and encoded again
        let decoded = decode(b"*2\r\n*1\r\n:1\r\n$1\r\na\r\n");
        assert_eq!(
            decoded,
            Ok(RespValue::Array(vec![
                b"*1\r\n:1\r\n".to_vec(),
                b"$1\r\na\r\n".to_vec()
            ]))
        );
    }

    #[test]
    fn decode_resp2_nulls() {
        assert_eq!(decode(b"$-1\r\n"), Ok(RespValue::Null));
        assert_eq!(decode(b"*-1\r\n"), Ok(RespValue::Null));
    }

    #[test]
    fn decode_needs_exactly_one_value() {
        assert!(decode(b"+OK\r\n:1\r\n").is_err());
        assert!(decode(b"*2\r\n:1\r\n").is_err());
        assert!(decode(&[]).is_err());
    }

    #[test]
    fn decode_invalid_values() {
        assert!(decode(b"$3\r\nabcde\r\n").is_err());
        assert!(decode(b"#x\r\n").is_err());
        assert!(decode(b"=3\r\ntxt\r\n").is_err());
        assert!(decode(b":one\r\n").is_err());
    }

    #[test]
    fn frame_len_matches_encode() {
        let value = RespValue::Array(vec![
            encode(RespValue::BulkString(b"a\r\nb".to_vec())),
            encode(RespValue::Integer(-3)),
        ]);
        let encoded = encode(value);
        assert_eq!(frame_len(&encoded).unwrap(), Some(encoded.len()));
    }

    #[test]
    fn decode_binary_bulk_strings() {
        let value = RespValue::BulkString(vec![0x80, 0x00, b'\r', b'\n', 0xff]);
        assert_eq!(decode(&encode(value.clone())), Ok(value));
    }

    #[test]
    fn display_scalars() {
        let display = |value: RespValue| value.to_string();
        assert_eq!(
            display(RespValue::String("OK".to_string())),
            r#"string<"OK">"#
        );
        assert_eq!(
            display(RespValue::Error("ERR x".to_string())),
            r#"error<"ERR x">"#
        );
        assert_eq!(display(RespValue::Integer(-1)), "integer<-1>");
        assert_eq!(
            display(RespValue::BulkString(b"a\"b".to_vec())),
            r#"bulk-string<"a\"b">"#
        );
        // bytes that are not UTF-8 are escaped
        assert_eq!(
            display(RespValue::BulkString(vec![0x80, b'a'])),
            r#"bulk-string<b"\x80a">"#
        );
        assert_eq!(display(RespValue::Null), "null");
        assert_eq!(display(RespValue::Boolean(true)), "boolean<true>");
        assert_eq!(display(RespValue::Double(1.5)), "double<1.5>");
        assert_eq!(
            display(RespValue::BigNumber("1".to_string())),
            "big-number<1>"
        );
        assert_eq!(
            display(RespValue::BulkError("SYNTAX".to_string())),
            r#"bulk-error<"SYNTAX">"#
        );
        assert_eq!(
            display(RespValue::VerbatimString((
                "txt".to_string(),
                "hi".to_string()
            ))),
            r#"verbatim-string<txt, "hi">"#
        );
    }

    #[test]
    fn display_aggregates() {
        let bulk = |text: &str| encode(RespValue::BulkString(text.as_bytes().to_vec()));
        let int = |value: i64| encode(RespValue::Integer(value));
        assert_eq!(RespValue::Array(vec![]).to_string(), "array<>");
        assert_eq!(
            RespValue::Array(vec![bulk("a"), int(1)]).to_string(),
            r#"array<bulk-string<"a">, integer<1>>"#
        );
        let nested = encode(RespValue::Array(vec![int(1), int(2)]));
        assert_eq!(
            RespValue::Array(vec![nested]).to_string(),
            "array<array<integer<1>, integer<2>>>"
        );
        assert_eq!(
            RespValue::Map(vec![(bulk("k"), int(1)), (bulk("j"), int(2))]).to_string(),
            r#"map<bulk-string<"k">: integer<1>, bulk-string<"j">: integer<2>>"#
        );
        assert_eq!(
            RespValue::Set(vec![bulk("a")]).to_string(),
            r#"set<bulk-string<"a">>"#
        );
        assert_eq!(
            RespValue::Push(vec![bulk("message"), bulk("news")]).to_string(),
            r#"push<bulk-string<"message">, bulk-string<"news">>"#
        );
        assert_eq!(
            RespValue::Array(vec![b"?".to_vec()]).to_string(),
            "array<--invalid-resp-->"
        );
    }
}
