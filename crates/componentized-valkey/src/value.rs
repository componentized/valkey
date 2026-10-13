//! Converting a reply into Rust values, the impls for Rust's own types and the helpers the commands
//! share. The impls for the package's types are in [`crate::commands`].

use crate::types::{Double, Error, Integer, RespNestedValue, RespValue};

use wit_bindgen::rt::Map;

/// Converts a reply into a Rust type, e.g. `Option<String>` for a bulk string or null.
pub trait FromValue: Sized {
    fn from_value(value: RespValue) -> Result<Self, Error>;
}

/// The reply itself, e.g. for a field of mixed types.
impl FromValue for RespValue {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Ok(value)
    }
}

/// A simple string `OK`.
impl FromValue for () {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match &value {
            RespValue::String(reply) if reply == "OK" => Ok(()),
            _ => Err(unexpected(&value)),
        }
    }
}

impl FromValue for Integer {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Integer(value) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// A double, or a bulk string holding one, e.g. the reply of INCRBYFLOAT, even with RESP3.
impl FromValue for Double {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Double(value) => Ok(value),
            RespValue::BulkString(text) => text
                .parse()
                .map_err(|_| Error::Client(format!("unexpected reply, not a double: {text:?}"))),
            value => Err(unexpected(&value)),
        }
    }
}

/// A non-negative integer.
impl FromValue for u64 {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let value = Integer::from_value(value)?;
        u64::try_from(value)
            .map_err(|_| Error::Client(format!("unexpected negative reply: {value}")))
    }
}

/// An integer `0` or `1`, or a boolean.
impl FromValue for bool {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Integer(0) => Ok(false),
            RespValue::Integer(1) => Ok(true),
            RespValue::Boolean(value) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// A bulk, simple or verbatim string.
impl FromValue for String {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::BulkString(value) | RespValue::String(value) => Ok(value),
            RespValue::VerbatimString((_, value)) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// Null, or a value.
impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Null => Ok(None),
            value => T::from_value(value).map(Some),
        }
    }
}

/// An array, or a set.
impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        items(value)?.into_iter().map(T::from_value).collect()
    }
}

/// An array of exactly as many items as the tuple.
macro_rules! from_value_tuples {
    ($(($len:literal; $($item:ident),+)),*) => {
        $(
            impl<$($item: FromValue),+> FromValue for ($($item,)+) {
                #[allow(non_snake_case)]
                fn from_value(value: RespValue) -> Result<Self, Error> {
                    match <[RespValue; $len]>::try_from(items(value)?) {
                        Ok([$($item),+]) => Ok(($($item::from_value($item)?,)+)),
                        Err(items) => Err(Error::Client(format!(
                            "unexpected number of items, expected {}: {}",
                            $len,
                            items.len()
                        ))),
                    }
                }
            }
        )*
    };
}

from_value_tuples!((2; A, B), (3; A, B, C), (4; A, B, C, D));

/// A map, or a flat array of keys and values.
impl<K: FromValue + Ord, V: FromValue> FromValue for Map<K, V> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Ok(entries(value)?.into_iter().collect())
    }
}

/// The fields of a map, by name, e.g. to build a record.
///
/// Fields that are not taken are ignored, so newer servers may add fields.
pub struct Fields(Vec<(String, RespValue)>);

impl FromValue for Fields {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        entries(value).map(Fields)
    }
}

impl Fields {
    /// Take the named field, if present.
    pub fn optional<T: FromValue>(&mut self, name: &str) -> Result<Option<T>, Error> {
        match self.0.iter().position(|(key, _)| key == name) {
            Some(index) => T::from_value(self.0.swap_remove(index).1).map(Some),
            None => Ok(None),
        }
    }

    /// Take the named field, failing if it is missing.
    pub fn required<T: FromValue>(&mut self, name: &str) -> Result<T, Error> {
        self.optional(name)?
            .ok_or_else(|| Error::Client(format!("missing field: {name}")))
    }
}

/// A value, or `-1` for none, e.g. the `active-time` of a consumer that has never been active.
pub struct OrNever<T>(pub Option<T>);

impl<T: FromValue> FromValue for OrNever<T> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Integer(-1) => Ok(OrNever(None)),
            value => T::from_value(value).map(|value| OrNever(Some(value))),
        }
    }
}

/// Pairs in a flat array, `[key, value, key, value...]`, e.g. the members and scores of ZSCAN.
pub struct FlatPairs<K, V>(pub Vec<(K, V)>);

impl<K: FromValue, V: FromValue> FromValue for FlatPairs<K, V> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        entries(value).map(FlatPairs)
    }
}

/// A number sent as a string, e.g. the port of a replica in ROLE.
pub(crate) fn parse_string<T: std::str::FromStr>(value: RespValue) -> Result<T, Error> {
    let text = String::from_value(value)?;
    text.parse()
        .map_err(|_| Error::Client(format!("unexpected number: {text:?}")))
}

pub(crate) fn unexpected(reply: &RespValue) -> Error {
    Error::Client(format!("unexpected reply: {reply:?}"))
}

fn decode(value: RespNestedValue) -> Result<RespValue, Error> {
    crate::resp::decode(value).map_err(Error::Resp)
}

/// The nested values of an array, or a set.
pub(crate) fn items(reply: RespValue) -> Result<Vec<RespValue>, Error> {
    match reply {
        RespValue::Array(items) | RespValue::Set(items) => items.into_iter().map(decode).collect(),
        reply => Err(unexpected(&reply)),
    }
}

/// The entries of a map, or of a flat array of keys and values.
pub fn entries<K: FromValue, V: FromValue>(reply: RespValue) -> Result<Vec<(K, V)>, Error> {
    let entry = |key, value| Ok((K::from_value(decode(key)?)?, V::from_value(decode(value)?)?));
    match reply {
        RespValue::Map(entries) => entries
            .into_iter()
            .map(|(key, value)| entry(key, value))
            .collect(),
        RespValue::Array(items) => {
            if items.len() % 2 != 0 {
                return Err(Error::Client(format!(
                    "unexpected odd number of keys and values: {}",
                    items.len()
                )));
            }
            let mut items = items.into_iter();
            let mut entries = vec![];
            while let (Some(key), Some(value)) = (items.next(), items.next()) {
                entries.push(entry(key, value)?);
            }
            Ok(entries)
        }
        reply => Err(unexpected(&reply)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{array, bulk, from, from_error, int, map, ok, simple};

    #[test]
    fn value() {
        assert_eq!(from::<RespValue>(int(1)), int(1));
    }

    #[test]
    fn unit() {
        from::<()>(ok());
        assert_eq!(
            from_error::<()>(simple("QUEUED")),
            r#"unexpected reply: RespValue::String("QUEUED")"#
        );
    }

    #[test]
    fn integer() {
        assert_eq!(from::<Integer>(int(-5)), -5);
        assert_eq!(
            from_error::<Integer>(bulk("5")),
            r#"unexpected reply: RespValue::BulkString("5")"#
        );
    }

    #[test]
    fn double() {
        assert_eq!(from::<Double>(RespValue::Double(1.5)), 1.5);
        assert_eq!(from::<Double>(bulk("2.5")), 2.5);
        assert_eq!(
            from_error::<Double>(bulk("two")),
            r#"unexpected reply, not a double: "two""#
        );
    }

    #[test]
    fn unsigned() {
        assert_eq!(from::<u64>(int(3)), 3);
        assert_eq!(from_error::<u64>(int(-1)), "unexpected negative reply: -1");
    }

    #[test]
    fn boolean() {
        assert!(!from::<bool>(int(0)));
        assert!(from::<bool>(int(1)));
        assert!(from::<bool>(RespValue::Boolean(true)));
        assert_eq!(
            from_error::<bool>(int(2)),
            "unexpected reply: RespValue::Integer(2)"
        );
    }

    #[test]
    fn string() {
        assert_eq!(from::<String>(bulk("bulk")), "bulk");
        assert_eq!(from::<String>(simple("simple")), "simple");
        let verbatim = RespValue::VerbatimString(("txt".to_string(), "verbatim".to_string()));
        assert_eq!(from::<String>(verbatim), "verbatim");
        assert_eq!(
            from_error::<String>(int(1)),
            "unexpected reply: RespValue::Integer(1)"
        );
    }

    #[test]
    fn option() {
        assert_eq!(from::<Option<String>>(RespValue::Null), None);
        assert_eq!(from::<Option<String>>(bulk("a")), Some("a".to_string()));
    }

    #[test]
    fn list() {
        assert_eq!(
            from::<Vec<String>>(array(vec![bulk("a"), bulk("b")])),
            ["a", "b"]
        );
        let set = RespValue::Set(vec![crate::resp::encode(bulk("a"))]);
        assert_eq!(from::<Vec<String>>(set), ["a"]);
        assert_eq!(
            from_error::<Vec<String>>(bulk("a")),
            r#"unexpected reply: RespValue::BulkString("a")"#
        );
    }

    #[test]
    fn tuples() {
        assert_eq!(
            from::<(String, Integer)>(array(vec![bulk("a"), int(1)])),
            ("a".to_string(), 1)
        );
        assert_eq!(
            from::<(Integer, Integer, Integer, Integer)>(array(vec![
                int(1),
                int(2),
                int(3),
                int(4)
            ])),
            (1, 2, 3, 4)
        );
        assert_eq!(
            from_error::<(String, Integer)>(array(vec![bulk("a")])),
            "unexpected number of items, expected 2: 1"
        );
    }

    #[test]
    fn maps() {
        let expected: Map<String, Integer> = [("a".to_string(), 1)].into_iter().collect();
        // RESP3
        assert_eq!(
            from::<Map<String, Integer>>(map(vec![(bulk("a"), int(1))])),
            expected
        );
        // RESP2, a flat array
        assert_eq!(
            from::<Map<String, Integer>>(array(vec![bulk("a"), int(1)])),
            expected
        );
        assert_eq!(
            from_error::<Map<String, Integer>>(array(vec![bulk("a"), int(1), bulk("b")])),
            "unexpected odd number of keys and values: 3"
        );
    }

    #[test]
    fn fields() {
        let value = map(vec![
            (bulk("name"), bulk("a")),
            (bulk("count"), int(2)),
            (bulk("newer"), int(3)),
        ]);
        let mut fields = from::<Fields>(value);
        assert_eq!(fields.required::<String>("name").unwrap(), "a");
        assert_eq!(fields.optional::<Integer>("count").unwrap(), Some(2));
        assert_eq!(fields.optional::<Integer>("missing").unwrap(), None);
        match fields.required::<Integer>("missing") {
            Err(Error::Client(message)) => assert_eq!(message, "missing field: missing"),
            result => panic!("expected a client error: {result:?}"),
        }
    }

    #[test]
    fn or_never() {
        assert_eq!(from::<OrNever<Integer>>(int(-1)).0, None);
        assert_eq!(from::<OrNever<Integer>>(int(5)).0, Some(5));
    }

    #[test]
    fn flat_pairs() {
        let pairs =
            from::<FlatPairs<String, Integer>>(array(vec![bulk("b"), int(2), bulk("a"), int(1)]));
        // in the order of the reply
        assert_eq!(pairs.0, [("b".to_string(), 2), ("a".to_string(), 1)]);
    }

    #[test]
    fn number_in_a_string() {
        assert_eq!(parse_string::<u16>(bulk("6379")).unwrap(), 6379);
        match parse_string::<u16>(bulk("port")) {
            Err(Error::Client(message)) => assert_eq!(message, r#"unexpected number: "port""#),
            result => panic!("expected a client error: {result:?}"),
        }
    }
}
