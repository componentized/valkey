//! Helpers for the tests of the conversions, to build replies and check the outcomes.

use crate::resp::encode;
use crate::types::{Error, RespValue};
use crate::{FromValue, IntoCommand};

/// The words of a command that converts.
pub fn words(command: impl IntoCommand) -> crate::command::Words {
    command.into_command().expect("the words of the command")
}

/// The message of the client error a command fails to convert with.
pub fn words_error(command: impl IntoCommand) -> String {
    match command.into_command() {
        Err(Error::Client(message)) => message,
        result => panic!("expected a client error: {result:?}"),
    }
}

/// The value of a reply that converts.
pub fn from<T: FromValue>(value: RespValue) -> T {
    T::from_value(value).expect("the value of the reply")
}

/// The message of the client error a reply fails to convert with.
pub fn from_error<T: FromValue>(value: RespValue) -> String {
    match T::from_value(value) {
        Err(Error::Client(message)) => message,
        Err(error) => panic!("expected a client error: {error:?}"),
        Ok(_) => panic!("expected a client error"),
    }
}

/// The simple string `OK`.
pub fn ok() -> RespValue {
    simple("OK")
}

pub fn simple(text: &str) -> RespValue {
    RespValue::String(text.to_string())
}

pub fn bulk(text: &str) -> RespValue {
    RespValue::BulkString(text.as_bytes().to_vec())
}

pub fn int(value: i64) -> RespValue {
    RespValue::Integer(value)
}

pub fn double(value: f64) -> RespValue {
    RespValue::Double(value)
}

pub fn array(items: Vec<RespValue>) -> RespValue {
    RespValue::Array(items.into_iter().map(encode).collect())
}

pub fn map(entries: Vec<(RespValue, RespValue)>) -> RespValue {
    RespValue::Map(
        entries
            .into_iter()
            .map(|(key, value)| (encode(key), encode(value)))
            .collect(),
    )
}

/// A map of fields by name, e.g. a record in a RESP3 reply.
pub fn fields(entries: Vec<(&str, RespValue)>) -> RespValue {
    map(entries
        .into_iter()
        .map(|(name, value)| (bulk(name), value))
        .collect())
}

/// An array of bulk strings.
pub fn bulks(items: &[&str]) -> RespValue {
    array(items.iter().map(|item| bulk(item)).collect())
}
