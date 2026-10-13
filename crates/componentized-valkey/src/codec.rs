//! RESP serialization, from <https://github.com/iorust/resp/tree/v1.0.3>.

use crate::types::{NestedValue, RespError as Error, Value};

/// up to 512 MB in length
const RESP_MAX_SIZE: i64 = 512 * 1024 * 1024;
const CRLF_BYTES: &'static [u8] = b"\r\n";

/// Encodes RESP value to RESP binary buffer.
/// # Examples
/// ```
/// # use componentized_valkey::codec::encode;
/// # use componentized_valkey::types::Value;
/// let val = Value::String("OK".to_string());
/// assert_eq!(encode(val), b"+OK\r\n");
/// ```
pub fn encode(value: Value) -> Vec<u8> {
    let mut res: Vec<u8> = Vec::new();
    buf_encode(value, &mut res);
    res
}

#[inline]
fn buf_encode(value: Value, buf: &mut Vec<u8>) {
    match value {
        Value::Null => {
            buf.push(b'_');
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::String(val) => {
            buf.push(b'+');
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::Error(val) => {
            buf.push(b'-');
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::Integer(val) => {
            buf.push(b':');
            buf.extend_from_slice(val.to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::BulkString(val) => {
            buf.push(b'$');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::Array(val) => {
            buf.push(b'*');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for item in val {
                buf.extend_from_slice(&item);
            }
        }
        Value::Boolean(ref val) => {
            buf.push(b'#');
            match val {
                true => buf.push(b't'),
                false => buf.push(b'f'),
            }
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::Double(ref val) => {
            buf.push(b',');
            buf.extend_from_slice(val.to_string().to_lowercase().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::BigNumber(ref val) => {
            buf.push(b'(');
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::BulkError(ref val) => {
            buf.push(b'!');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::VerbatimString((ref encoding, ref value)) => {
            let val = format!("{encoding}:{value}");
            buf.push(b'=');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
        }
        Value::Map(val) => {
            buf.push(b'%');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for (key, value) in val {
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&value);
            }
        }
        Value::Set(val) => {
            buf.push(b'~');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            buf.extend_from_slice(CRLF_BYTES);
            for item in val {
                buf.extend_from_slice(&item);
            }
        }
        Value::Push(val) => {
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
/// # use componentized_valkey::codec::{decode, encode};
/// # use componentized_valkey::types::Value;
/// let value = Value::BulkString("Hello".to_string());
/// assert_eq!(decode(encode(value.clone())), Ok(value));
/// ```
pub fn decode(value: Vec<u8>) -> Result<Value, Error> {
    match frame_end(&value, 0)? {
        Some(end) if end == value.len() => decode_frame(&value),
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
fn decode_frame(buf: &[u8]) -> Result<Value, Error> {
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
    let elements = |count: i64| -> Result<Vec<NestedValue>, Error> {
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
        b'+' => parse_string(header).map(Value::String),
        b'-' => parse_string(header).map(Value::Error),
        b':' => parse_integer(header).map(Value::Integer),
        b'_' => Ok(Value::Null),
        b'#' => match header {
            b"t" => Ok(Value::Boolean(true)),
            b"f" => Ok(Value::Boolean(false)),
            _ => Err(format!("invalid RESP boolean: {header:?}")),
        },
        b',' => parse_double(header).map(Value::Double),
        b'(' => parse_string(header).map(Value::BigNumber),
        b'$' => match parse_integer(header)? {
            // a null bulk string, RESP2's null
            -1 => Ok(Value::Null),
            _ => parse_string(blob()?).map(Value::BulkString),
        },
        b'!' => parse_string(blob()?).map(Value::BulkError),
        b'=' => {
            let value = parse_string(blob()?)?;
            let Some((encoding, value)) = value.split_once(':') else {
                return Err("invalid verbatim string, missing encoding".to_string());
            };
            Ok(Value::VerbatimString((
                encoding.to_string(),
                value.to_string(),
            )))
        }
        b'*' => match parse_integer(header)? {
            // a null array, RESP2's null
            -1 => Ok(Value::Null),
            count => elements(count).map(Value::Array),
        },
        b'~' => elements(parse_integer(header)?).map(Value::Set),
        b'>' => elements(parse_integer(header)?).map(Value::Push),
        b'%' => {
            let mut entries = elements(parse_integer(header)? * 2)?.into_iter();
            let mut map = vec![];
            while let (Some(key), Some(value)) = (entries.next(), entries.next()) {
                map.push((key, value));
            }
            Ok(Value::Map(map))
        }
        prefix => Err(format!("invalid RESP type: {prefix:?}")),
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
            Value::String("OK".to_string()),
            Value::Error("ERR oops".to_string()),
            Value::Integer(-42),
            Value::BulkString("a\r\nb".to_string()),
            Value::Null,
            Value::Boolean(false),
            Value::Double(1.5),
            Value::BigNumber("12345678901234567890".to_string()),
            Value::BulkError("SYNTAX invalid".to_string()),
            Value::VerbatimString(("txt".to_string(), "a:b".to_string())),
            Value::Array(vec![encode(Value::Integer(1)), encode(Value::Null)]),
            Value::Set(vec![encode(Value::String("a".to_string()))]),
            Value::Push(vec![encode(Value::String("message".to_string()))]),
            Value::Map(vec![(
                encode(Value::String("key".to_string())),
                encode(Value::Array(vec![encode(Value::Integer(1))])),
            )]),
        ];
        for value in values {
            assert_eq!(decode(encode(value.clone())), Ok(value));
        }
    }

    #[test]
    fn decode_keeps_elements_as_received() {
        // a nested array, kept as its RESP rather than decoded and encoded again
        let decoded = decode(b"*2\r\n*1\r\n:1\r\n$1\r\na\r\n".to_vec());
        assert_eq!(
            decoded,
            Ok(Value::Array(vec![
                b"*1\r\n:1\r\n".to_vec(),
                b"$1\r\na\r\n".to_vec()
            ]))
        );
    }

    #[test]
    fn decode_resp2_nulls() {
        assert_eq!(decode(b"$-1\r\n".to_vec()), Ok(Value::Null));
        assert_eq!(decode(b"*-1\r\n".to_vec()), Ok(Value::Null));
    }

    #[test]
    fn decode_needs_exactly_one_value() {
        assert!(decode(b"+OK\r\n:1\r\n".to_vec()).is_err());
        assert!(decode(b"*2\r\n:1\r\n".to_vec()).is_err());
        assert!(decode(vec![]).is_err());
    }

    #[test]
    fn decode_invalid_values() {
        assert!(decode(b"$3\r\nabcde\r\n".to_vec()).is_err());
        assert!(decode(b"#x\r\n".to_vec()).is_err());
        assert!(decode(b"=3\r\ntxt\r\n".to_vec()).is_err());
        assert!(decode(b":one\r\n".to_vec()).is_err());
    }

    #[test]
    fn frame_len_matches_encode() {
        let value = Value::Array(vec![
            encode(Value::BulkString("a\r\nb".to_string())),
            encode(Value::Integer(-3)),
        ]);
        let encoded = encode(value);
        assert_eq!(frame_len(&encoded).unwrap(), Some(encoded.len()));
    }
}
