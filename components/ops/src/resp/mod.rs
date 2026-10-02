//! RESP serialize

use crate::exports::componentized::valkey::resp::{NestedValue, Value};
use crate::exports::componentized::valkey::store::Error;
use core::f64;
use std::io::{BufRead, BufReader, Read};
use std::string::String;
use std::vec::Vec;

/// up to 512 MB in length
const RESP_MAX_SIZE: i64 = 512 * 1024 * 1024;
const CRLF_BYTES: &'static [u8] = b"\r\n";

/// Encodes RESP value to RESP binary buffer.
/// # Examples
/// ```
/// # use self::resp::{Value, encode};
/// let val = Value::String("OK".to_string());
/// assert_eq!(encode(&val), vec![43, 79, 75, 13, 10]);
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
                buf_encode(item.into(), buf);
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
            for (key, value) in val {
                buf_encode(key.into(), buf);
                buf_encode(value.into(), buf);
            }
        }
        Value::Set(val) => {
            buf.push(b'~');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            for item in val {
                buf_encode(item.into(), buf);
            }
        }
        Value::Push(val) => {
            buf.push(b'>');
            buf.extend_from_slice(val.len().to_string().as_bytes());
            for item in val {
                buf_encode(item.into(), buf);
            }
        }
    }
}

pub fn decode(value: Vec<u8>) -> Result<Value, Error> {
    Decoder::new(BufReader::new(value.as_slice())).decode()
}

/// A streaming RESP Decoder.
#[derive(Debug)]
pub struct Decoder<R> {
    reader: BufReader<R>,
}

impl<R: Read> Decoder<R> {
    /// Creates a Decoder instance with given BufReader for decoding the RESP buffers.
    /// # Examples
    /// ```
    /// # use std::io::BufReader;
    /// # use self::resp::{Decoder, Value};
    ///
    /// let value = Value::BulkString("Hello".to_string());
    /// let buf = value.encode();
    /// let mut decoder = Decoder::new(BufReader::new(buf.as_slice()));
    /// assert_eq!(decoder.decode().unwrap(), Value::BulkString("Hello".to_string()));
    /// ```
    pub fn new(reader: BufReader<R>) -> Self {
        Decoder { reader: reader }
    }

    /// It will read buffers from the inner BufReader, decode it to a Value.
    pub fn decode(&mut self) -> Result<Value, Error> {
        let mut res: Vec<u8> = Vec::new();
        self.reader
            .read_until(b'\n', &mut res)
            .map_err(|e| Error::Resp(e.to_string()))?;

        let len = res.len();
        if len == 0 {
            Err(Error::Resp("unexpected EOF".to_string()))?
        }
        if len < 3 {
            Err(Error::Resp(format!("too short: {}", len)))?
        }
        if !is_crlf(res[len - 2], res[len - 1]) {
            Err(Error::Resp(format!("invalid CRLF: {:?}", res)))?
        }

        let bytes = res[1..len - 2].as_ref();
        match res[0] {
            // Value::String
            b'+' => parse_string(bytes).map(Value::String),
            // Value::Error
            b'-' => parse_string(bytes).map(Value::Error),
            // Value::Integer
            b':' => parse_integer(bytes).map(Value::Integer),
            // Value::BulkString
            b'$' => {
                let int = parse_integer(bytes)?;
                if int == -1 {
                    // Null bulk string, special case for RESP2
                    return Ok(Value::Null);
                }
                if int < -1 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!("invalid bulk string length: {}", int)))?
                }

                let mut buf: Vec<u8> = Vec::new();
                let int = int as usize;
                buf.resize(int + 2, 0);
                self.reader
                    .read_exact(buf.as_mut_slice())
                    .map_err(|e| Error::Resp(e.to_string()))?;
                if !is_crlf(buf[int], buf[int + 1]) {
                    Err(Error::Resp(format!("invalid CRLF: {:?}", buf)))?
                }
                buf.truncate(int);
                parse_string(buf.as_slice()).map(Value::BulkString)
            }
            // Value::Array
            b'*' => {
                let int = parse_integer(bytes)?;
                if int == -1 {
                    // Null array, special case for RESP2
                    return Ok(Value::Null);
                }
                if int < -1 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!("invalid array length: {}", int)))?
                }

                let mut array: Vec<NestedValue> = Vec::with_capacity(int as usize);
                for _ in 0..int {
                    // TODO avoid full decoding just to re-encode
                    let val = self.decode()?;
                    array.push(val.into());
                }
                Ok(Value::Array(array))
            }
            // Value::Null
            b'_' => Ok(Value::Null),
            // Value::Boolean
            b'#' => match bytes[0] {
                b'f' => Ok(Value::Boolean(false)),
                b't' => Ok(Value::Boolean(true)),
                _ => Err(Error::Resp(format!("invalid RESP boolean: {:?}", bytes))),
            },
            // Value::Double
            b',' => parse_double(bytes).map(Value::Double),
            // Value::BigNumber
            b'(' => parse_string(bytes).map(Value::BigNumber),
            // Value::BulkError
            b'!' => {
                let int = parse_integer(bytes)?;
                if int < 0 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!("invalid bulk error length: {}", int)))?
                }

                let mut buf: Vec<u8> = Vec::new();
                let int = int as usize;
                buf.resize(int + 2, 0);
                self.reader
                    .read_exact(buf.as_mut_slice())
                    .map_err(|e| Error::Resp(e.to_string()))?;
                if !is_crlf(buf[int], buf[int + 1]) {
                    Err(Error::Resp(format!("invalid CRLF: {:?}", buf)))?
                }
                buf.truncate(int);
                parse_string(buf.as_slice()).map(Value::BulkError)
            }
            // Value::VerbatimString
            b'=' => {
                let int = parse_integer(bytes)?;
                if int < 0 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!(
                        "invalid verbatim string length: {}",
                        int
                    )))?
                }

                let mut buf: Vec<u8> = Vec::new();
                let int = int as usize;
                buf.resize(int + 2, 0);
                self.reader
                    .read_exact(buf.as_mut_slice())
                    .map_err(|e| Error::Resp(e.to_string()))?;
                if !is_crlf(buf[int], buf[int + 1]) {
                    Err(Error::Resp(format!("invalid CRLF: {:?}", buf)))?
                }
                buf.truncate(int);
                match parse_string(buf.as_slice()) {
                    Err(err) => Err(err)?,
                    Ok(str) => {
                        let parts: Vec<&str> = str.splitn(2, ':').collect();
                        if parts.len() != 2 {
                            Err(Error::Resp(
                                "invalid verbatim string, missing encoding".to_string(),
                            ))?
                        }
                        Ok(Value::VerbatimString((
                            parts[0].to_string(),
                            parts[1].to_string(),
                        )))
                    }
                }
            }
            // Value::Map
            b'%' => {
                let int = parse_integer(bytes)?;
                if int < 0 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!("invalid array length: {}", int)))?
                }

                let mut map: Vec<(NestedValue, NestedValue)> = Vec::with_capacity(int as usize);
                for _ in 0..int {
                    // TODO avoid full decoding just to re-encode
                    let key = self.decode()?;
                    let val = self.decode()?;
                    map.push((key.into(), val.into()));
                }
                Ok(Value::Map(map))
            }
            // Value::Set
            b'~' => {
                let int = parse_integer(bytes)?;
                if int < 0 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!("invalid array length: {}", int)))?
                }

                let mut set: Vec<NestedValue> = Vec::with_capacity(int as usize);
                for _ in 0..int {
                    // TODO avoid full decoding just to re-encode
                    let val = self.decode()?;
                    set.push(val.into());
                }
                Ok(Value::Set(set))
            }
            // Value::Push
            b'>' => {
                let int = parse_integer(bytes)?;
                if int < 0 || int >= RESP_MAX_SIZE {
                    Err(Error::Resp(format!("invalid array length: {}", int)))?
                }

                let mut push: Vec<NestedValue> = Vec::with_capacity(int as usize);
                for _ in 0..int {
                    // TODO avoid full decoding just to re-encode
                    let val = self.decode()?;
                    push.push(val.into());
                }
                Ok(Value::Push(push))
            }

            prefix => Err(Error::Resp(format!("invalid RESP type: {:?}", prefix))),
        }
    }
}

#[inline]
fn is_crlf(a: u8, b: u8) -> bool {
    a == b'\r' && b == b'\n'
}

#[inline]
fn parse_string(bytes: &[u8]) -> Result<String, Error> {
    String::from_utf8(bytes.to_vec()).map_err(|err| Error::Resp(err.to_string()))
}

#[inline]
fn parse_integer(bytes: &[u8]) -> Result<i64, Error> {
    let str_integer = parse_string(bytes)?;
    (str_integer.parse::<i64>()).map_err(|err| Error::Resp(err.to_string()))
}

#[inline]
fn parse_double(bytes: &[u8]) -> Result<f64, Error> {
    let str_double = parse_string(bytes)?;
    (str_double.parse::<f64>()).map_err(|err| Error::Resp(err.to_string()))
}
