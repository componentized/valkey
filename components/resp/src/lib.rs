//! Exports `componentized:valkey/resp` with the shared codec.
//!
//! An exported interface has types of its own, they are converted to and from the shared codec's
//! types. A value is not recursive, nested values are encoded RESP, so each conversion is one
//! match.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

use exports::componentized::valkey::resp::{Error, Guest, Value};

struct Component;

impl Guest for Component {
    fn decode(data: Vec<u8>) -> Result<Value, Error> {
        resp_codec::decode(data).map(from_codec)
    }

    fn encode(value: Value) -> Result<Vec<u8>, Error> {
        Ok(resp_codec::encode(to_codec(value)))
    }
}

fn to_codec(value: Value) -> resp_codec::Value {
    use resp_codec::Value as V;
    match value {
        Value::String(value) => V::String(value),
        Value::Error(value) => V::Error(value),
        Value::Integer(value) => V::Integer(value),
        Value::BulkString(value) => V::BulkString(value),
        Value::Array(value) => V::Array(value),
        Value::Null => V::Null,
        Value::Boolean(value) => V::Boolean(value),
        Value::Double(value) => V::Double(value),
        Value::BigNumber(value) => V::BigNumber(value),
        Value::BulkError(value) => V::BulkError(value),
        Value::VerbatimString(value) => V::VerbatimString(value),
        Value::Map(value) => V::Map(value),
        Value::Set(value) => V::Set(value),
        Value::Push(value) => V::Push(value),
    }
}

fn from_codec(value: resp_codec::Value) -> Value {
    use resp_codec::Value as V;
    match value {
        V::String(value) => Value::String(value),
        V::Error(value) => Value::Error(value),
        V::Integer(value) => Value::Integer(value),
        V::BulkString(value) => Value::BulkString(value),
        V::Array(value) => Value::Array(value),
        V::Null => Value::Null,
        V::Boolean(value) => Value::Boolean(value),
        V::Double(value) => Value::Double(value),
        V::BigNumber(value) => Value::BigNumber(value),
        V::BulkError(value) => Value::BulkError(value),
        V::VerbatimString(value) => Value::VerbatimString(value),
        V::Map(value) => Value::Map(value),
        V::Set(value) => Value::Set(value),
        V::Push(value) => Value::Push(value),
    }
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/resp/`
    path: "../wit",
    world: "resp",
    generate_all,
});

export!(Component);
