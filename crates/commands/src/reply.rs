//! Sending commands, and converting their replies.
//!
//! Connections speak RESP3. Where RESP3 replies with a richer type than RESP2, e.g. a map rather
//! than a flat array of keys and values, both are accepted.

use resp_codec::{NestedValue, Value};
use wit_bindgen::rt::Map;

use crate::componentized::valkey::connection::{Connection, Error};

/// Send a command, its words as bulk strings, and wait for the reply.
///
/// An error reply fails the command, see `connection.call`.
pub async fn call<I, W>(connection: &Connection, words: I) -> Result<Value, Error>
where
    I: IntoIterator<Item = W>,
    W: Into<String>,
{
    let command = words
        .into_iter()
        .map(|word| Value::BulkString(word.into()))
        .collect();
    connection.call(command).await
}

/// Send a command, its words as bulk strings, without waiting for a reply.
///
/// An error reply fails the command, see `connection.send`.
pub async fn send<I, W>(connection: &Connection, words: I) -> Result<(), Error>
where
    I: IntoIterator<Item = W>,
    W: Into<String>,
{
    let command = words
        .into_iter()
        .map(|word| Value::BulkString(word.into()))
        .collect();
    connection.send(command).await
}

pub fn unexpected(reply: &Value) -> Error {
    Error::Client(format!("unexpected reply: {reply:?}"))
}

pub fn decode(value: NestedValue) -> Result<Value, Error> {
    resp_codec::decode(value).map_err(Error::Resp)
}

/// A simple string `OK`.
pub fn ok(reply: Value) -> Result<(), Error> {
    match &reply {
        Value::String(value) if value == "OK" => Ok(()),
        _ => Err(unexpected(&reply)),
    }
}

pub fn integer(reply: Value) -> Result<i64, Error> {
    match reply {
        Value::Integer(value) => Ok(value),
        reply => Err(unexpected(&reply)),
    }
}

pub fn unsigned(reply: Value) -> Result<u64, Error> {
    let value = integer(reply)?;
    u64::try_from(value).map_err(|_| Error::Client(format!("unexpected negative reply: {value}")))
}

/// An integer `0` or `1`.
pub fn boolean(reply: Value) -> Result<bool, Error> {
    match reply {
        Value::Integer(0) => Ok(false),
        Value::Integer(1) => Ok(true),
        Value::Boolean(value) => Ok(value),
        reply => Err(unexpected(&reply)),
    }
}

/// A bulk or simple string.
pub fn string(reply: Value) -> Result<String, Error> {
    match reply {
        Value::BulkString(value) | Value::String(value) => Ok(value),
        Value::VerbatimString((_, value)) => Ok(value),
        reply => Err(unexpected(&reply)),
    }
}

/// A string, or null.
pub fn optional_string(reply: Value) -> Result<Option<String>, Error> {
    match reply {
        Value::Null => Ok(None),
        reply => string(reply).map(Some),
    }
}

/// The nested values of an array, or a set.
pub fn items(reply: Value) -> Result<Vec<Value>, Error> {
    match reply {
        Value::Array(items) | Value::Set(items) => items.into_iter().map(decode).collect(),
        reply => Err(unexpected(&reply)),
    }
}

pub fn strings(reply: Value) -> Result<Vec<String>, Error> {
    items(reply)?.into_iter().map(string).collect()
}

pub fn optional_strings(reply: Value) -> Result<Vec<Option<String>>, Error> {
    items(reply)?.into_iter().map(optional_string).collect()
}

/// The entries of a map, or of a flat array of keys and values.
pub fn entries(reply: Value) -> Result<Vec<(Value, Value)>, Error> {
    match reply {
        Value::Map(entries) => entries
            .into_iter()
            .map(|(key, value)| Ok((decode(key)?, decode(value)?)))
            .collect(),
        Value::Array(items) => {
            if items.len() % 2 != 0 {
                return Err(Error::Client(format!(
                    "unexpected odd number of keys and values: {}",
                    items.len()
                )));
            }
            let mut items = items.into_iter();
            let mut entries = vec![];
            while let (Some(key), Some(value)) = (items.next(), items.next()) {
                entries.push((decode(key)?, decode(value)?));
            }
            Ok(entries)
        }
        reply => Err(unexpected(&reply)),
    }
}

/// The entries of a map of strings to strings.
pub fn string_entries(reply: Value) -> Result<Vec<(String, String)>, Error> {
    entries(reply)?
        .into_iter()
        .map(|(key, value)| Ok((string(key)?, string(value)?)))
        .collect()
}

pub fn counts(reply: Value) -> Result<Map<String, i64>, Error> {
    let mut map = Map::new();
    for (key, value) in entries(reply)? {
        map.insert(string(key)?, integer(value)?);
    }
    Ok(map)
}

/// A duration, in nanoseconds, as whole seconds, the precision of EXPIRE and HEXPIRE.
pub fn whole_seconds(nanoseconds: u64) -> String {
    (nanoseconds / 1_000_000_000).to_string()
}
