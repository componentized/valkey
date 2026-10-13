use crate::exports::componentized::valkey::commands::{
    Double, DurationMilliseconds, DurationSeconds, Error, FieldExpiretime, FieldPersistResult,
    FieldPexpiretime, FieldPttl, FieldTtl, Integer, LcsIdxMatch, LcsIdxReply, StreamId, TtlResult,
    UnixTimeMilliseconds, UnixTimeSeconds, Value, XinfoConsumer, XinfoGroup, XinfoStreamReply,
    XinfoStreamReplyFull, XinfoStreamReplyFullConsumer, XinfoStreamReplyFullGroup,
    XpendingExtendedEntry, XpendingSummary,
};

use resp_codec::NestedValue;
use wit_bindgen::rt::Map;

/// Converts a reply into a Rust type, e.g. `Option<String>` for a bulk string or null.
pub trait FromValue: Sized {
    fn from_value(value: Value) -> Result<Self, Error>;
}

/// The reply itself, e.g. for a field of mixed types.
impl FromValue for Value {
    fn from_value(value: Value) -> Result<Self, Error> {
        Ok(value)
    }
}

/// A simple string `OK`.
impl FromValue for () {
    fn from_value(value: Value) -> Result<Self, Error> {
        match &value {
            Value::String(reply) if reply == "OK" => Ok(()),
            _ => Err(unexpected(&value)),
        }
    }
}

impl FromValue for Integer {
    fn from_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Integer(value) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// A double, or a bulk string holding one, e.g. the reply of INCRBYFLOAT, even with RESP3.
impl FromValue for Double {
    fn from_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Double(value) => Ok(value),
            Value::BulkString(text) => text
                .parse()
                .map_err(|_| Error::Client(format!("unexpected reply, not a double: {text:?}"))),
            value => Err(unexpected(&value)),
        }
    }
}

/// A non-negative integer.
impl FromValue for u64 {
    fn from_value(value: Value) -> Result<Self, Error> {
        let value = Integer::from_value(value)?;
        u64::try_from(value)
            .map_err(|_| Error::Client(format!("unexpected negative reply: {value}")))
    }
}

/// An integer `0` or `1`, or a boolean.
impl FromValue for bool {
    fn from_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            Value::Boolean(value) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// A bulk, simple or verbatim string.
impl FromValue for String {
    fn from_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::BulkString(value) | Value::String(value) => Ok(value),
            Value::VerbatimString((_, value)) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// Null, or a value.
impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            value => T::from_value(value).map(Some),
        }
    }
}

/// An array, or a set.
impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: Value) -> Result<Self, Error> {
        items(value)?.into_iter().map(T::from_value).collect()
    }
}

/// An array of exactly as many items as the tuple.
macro_rules! from_value_tuples {
    ($(($len:literal; $($item:ident),+)),*) => {
        $(
            impl<$($item: FromValue),+> FromValue for ($($item,)+) {
                #[allow(non_snake_case)]
                fn from_value(value: Value) -> Result<Self, Error> {
                    match <[Value; $len]>::try_from(items(value)?) {
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
    fn from_value(value: Value) -> Result<Self, Error> {
        Ok(entries(value)?.into_iter().collect())
    }
}

/// The fields of a map, by name, e.g. to build a record.
///
/// Fields that are not taken are ignored, so newer servers may add fields.
pub struct Fields(Vec<(String, Value)>);

impl FromValue for Fields {
    fn from_value(value: Value) -> Result<Self, Error> {
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

/// Whole seconds, never negative.
impl FromValue for DurationSeconds {
    fn from_value(value: Value) -> Result<Self, Error> {
        u64::from_value(value).map(|ex| DurationSeconds { ex })
    }
}

/// Whole milliseconds, never negative.
impl FromValue for DurationMilliseconds {
    fn from_value(value: Value) -> Result<Self, Error> {
        u64::from_value(value).map(|px| DurationMilliseconds { px })
    }
}

/// Whole seconds since the Unix epoch.
impl FromValue for UnixTimeSeconds {
    fn from_value(value: Value) -> Result<Self, Error> {
        Integer::from_value(value).map(|exat| UnixTimeSeconds { exat })
    }
}

/// Whole milliseconds since the Unix epoch.
impl FromValue for UnixTimeMilliseconds {
    fn from_value(value: Value) -> Result<Self, Error> {
        Integer::from_value(value).map(|pxat| UnixTimeMilliseconds { pxat })
    }
}

/// A value, or `-1` for none, e.g. the `active-time` of a consumer that has never been active.
pub struct OrNever<T>(pub Option<T>);

impl<T: FromValue> FromValue for OrNever<T> {
    fn from_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Integer(-1) => Ok(OrNever(None)),
            value => T::from_value(value).map(|value| OrNever(Some(value))),
        }
    }
}

/// A stream ID, or `0-0` for none, e.g. the start of the next XAUTOCLAIM, `0-0` once the scan is
/// complete.
pub struct OrZeroId(pub Option<StreamId>);

pub const ZERO_ID: &str = "0-0";

impl FromValue for OrZeroId {
    fn from_value(value: Value) -> Result<Self, Error> {
        let id = StreamId::from_value(value)?;
        Ok(OrZeroId((id != ZERO_ID).then_some(id)))
    }
}

/// `[count, lowest-id, highest-id, [[consumer, count]...]]`, the IDs and consumers are null when
/// nothing is pending, and each consumer's count is a bulk string.
impl FromValue for XpendingSummary {
    fn from_value(value: Value) -> Result<Self, Error> {
        let (count, lowest_id, highest_id, consumers): (_, _, _, Option<Vec<(String, String)>>) =
            FromValue::from_value(value)?;
        let consumers = consumers
            .unwrap_or_default()
            .into_iter()
            .map(|(consumer, count)| {
                let count = count.parse().map_err(|_| {
                    Error::Client(format!(
                        "unexpected pending count, not an integer: {count:?}"
                    ))
                })?;
                Ok((consumer, count))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self {
            count,
            lowest_id,
            highest_id,
            consumers,
        })
    }
}

/// `[id, consumer, milliseconds-since-delivery, delivery-count]`.
impl FromValue for XpendingExtendedEntry {
    fn from_value(value: Value) -> Result<Self, Error> {
        let (id, name, since_delivery, delivery_count) = FromValue::from_value(value)?;
        Ok(Self {
            id,
            name,
            since_delivery,
            delivery_count,
        })
    }
}

/// Members and their scores, e.g. from ZPOPMIN, which replies with one pair, or none, without a
/// count, and a list of pairs with one.
pub struct ScoredMembers(pub Vec<(String, Double)>);

impl FromValue for ScoredMembers {
    fn from_value(value: Value) -> Result<Self, Error> {
        let items = items(value)?;
        match items.first() {
            None => Ok(ScoredMembers(vec![])),
            Some(Value::Array(_)) => items
                .into_iter()
                .map(FromValue::from_value)
                .collect::<Result<_, _>>()
                .map(ScoredMembers),
            Some(_) => match <[Value; 2]>::try_from(items) {
                Ok([member, score]) => Ok(ScoredMembers(vec![(
                    String::from_value(member)?,
                    Double::from_value(score)?,
                )])),
                Err(items) => Err(Error::Client(format!(
                    "unexpected number of items, expected a member and its score: {}",
                    items.len()
                ))),
            },
        }
    }
}

/// Pairs in a flat array, `[key, value, key, value...]`, e.g. the members and scores of ZSCAN.
pub struct FlatPairs<K, V>(pub Vec<(K, V)>);

impl<K: FromValue, V: FromValue> FromValue for FlatPairs<K, V> {
    fn from_value(value: Value) -> Result<Self, Error> {
        entries(value).map(FlatPairs)
    }
}

/// The codes of HTTL and the like, `-2` when the field does not exist, `-1` when it has no
/// expiration, otherwise the time.
macro_rules! from_value_field_times {
    ($($variant:ident($time:ident) => $case:ident),*) => {
        $(
            impl FromValue for $variant {
                fn from_value(value: Value) -> Result<Self, Error> {
                    match value {
                        Value::Integer(-2) => Ok($variant::DoesNotExist),
                        Value::Integer(-1) => Ok($variant::NoExpiration),
                        value => $time::from_value(value).map($variant::$case),
                    }
                }
            }
        )*
    };
}

from_value_field_times!(
    FieldTtl(DurationSeconds) => ExpiresIn,
    FieldPttl(DurationMilliseconds) => ExpiresIn,
    FieldExpiretime(UnixTimeSeconds) => ExpiresAt,
    FieldPexpiretime(UnixTimeMilliseconds) => ExpiresAt
);

impl FromValue for FieldPersistResult {
    fn from_value(value: Value) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -2 => Ok(Self::DoesNotExist),
            -1 => Ok(Self::NoExpiration),
            1 => Ok(Self::Removed),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

impl FromValue for TtlResult {
    fn from_value(value: Value) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -2 => Ok(Self::DoesNotExist),
            0 => Ok(Self::ConditionNotMet),
            1 => Ok(Self::Applied),
            2 => Ok(Self::ZeroSeconds),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

/// A map of the matches and the length of the longest common subsequence.
impl FromValue for LcsIdxReply {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            matches: fields.required("matches")?,
            len: fields.required("len")?,
        })
    }
}

/// The inclusive ranges of a match in each key and, with `WITHMATCHLEN`, its length.
impl FromValue for LcsIdxMatch {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut items = items(value)?.into_iter();
        let (Some(key1_range), Some(key2_range), len, None) =
            (items.next(), items.next(), items.next(), items.next())
        else {
            return Err(Error::Client(
                "unexpected LCS match, expected 2 or 3 items".to_string(),
            ));
        };
        Ok(Self {
            key1_range: FromValue::from_value(key1_range)?,
            key2_range: FromValue::from_value(key2_range)?,
            len: len.map(FromValue::from_value).transpose()?,
        })
    }
}

impl FromValue for XinfoConsumer {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            pending: fields.required("pending")?,
            idle: fields.required("idle")?,
            inactive: fields
                .required::<OrNever<DurationMilliseconds>>("inactive")?
                .0,
        })
    }
}

impl FromValue for XinfoGroup {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            consumers: fields.required("consumers")?,
            pending: fields.required("pending")?,
            last_delivered_id: fields.required("last-delivered-id")?,
            entries_read: fields.required("entries-read")?,
            lag: fields.required("lag")?,
        })
    }
}

impl FromValue for XinfoStreamReply {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            length: fields.required("length")?,
            radix_tree_keys: fields.required("radix-tree-keys")?,
            radix_tree_nodes: fields.required("radix-tree-nodes")?,
            groups: fields.required("groups")?,
            last_generated_id: fields.required::<OrZeroId>("last-generated-id")?.0,
            max_deleted_entry_id: fields.required::<OrZeroId>("max-deleted-entry-id")?.0,
            entries_added: fields.required("entries-added")?,
            first_entry: fields.required("first-entry")?,
            last_entry: fields.required("last-entry")?,
        })
    }
}

impl FromValue for XinfoStreamReplyFull {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            length: fields.required("length")?,
            radix_tree_keys: fields.required("radix-tree-keys")?,
            radix_tree_nodes: fields.required("radix-tree-nodes")?,
            last_generated_id: fields.required::<OrZeroId>("last-generated-id")?.0,
            max_deleted_entry_id: fields.required::<OrZeroId>("max-deleted-entry-id")?.0,
            entries_added: fields.required("entries-added")?,
            entries: fields.required("entries")?,
            groups: fields.required("groups")?,
        })
    }
}

impl FromValue for XinfoStreamReplyFullGroup {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            last_delivered_id: fields.required("last-delivered-id")?,
            entries_read: fields.required("entries-read")?,
            lag: fields.required("lag")?,
            pel_count: fields.required("pel-count")?,
            pending: fields.required("pending")?,
            consumers: fields.required("consumers")?,
        })
    }
}

impl FromValue for XinfoStreamReplyFullConsumer {
    fn from_value(value: Value) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            seen_time: fields.required("seen-time")?,
            active_time: fields
                .required::<OrNever<UnixTimeMilliseconds>>("active-time")?
                .0,
            pel_count: fields.required("pel-count")?,
            pending: fields.required("pending")?,
        })
    }
}

fn unexpected(reply: &Value) -> Error {
    Error::Client(format!("unexpected reply: {reply:?}"))
}

fn decode(value: NestedValue) -> Result<Value, Error> {
    resp_codec::decode(value).map_err(Error::Resp)
}

/// The nested values of an array, or a set.
fn items(reply: Value) -> Result<Vec<Value>, Error> {
    match reply {
        Value::Array(items) | Value::Set(items) => items.into_iter().map(decode).collect(),
        reply => Err(unexpected(&reply)),
    }
}

/// The entries of a map, or of a flat array of keys and values.
fn entries<K: FromValue, V: FromValue>(reply: Value) -> Result<Vec<(K, V)>, Error> {
    let entry = |key, value| Ok((K::from_value(decode(key)?)?, V::from_value(decode(value)?)?));
    match reply {
        Value::Map(entries) => entries
            .into_iter()
            .map(|(key, value)| entry(key, value))
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
                entries.push(entry(key, value)?);
            }
            Ok(entries)
        }
        reply => Err(unexpected(&reply)),
    }
}
