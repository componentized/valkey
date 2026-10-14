//! The stream commands: XADD, XAUTOCLAIM, XCLAIM, XINFO, XPENDING, XTRIM and the like.

use crate::command::{Flag, IntoCommand, NamedOption, Words};
use crate::types::{
    DurationMilliseconds, Error, RespValue, StreamId, StreamTrim, StreamTrimEfficiency,
    UnixTimeMilliseconds, XclaimOpts, XinfoConsumersEntry, XinfoGroupsEntry, XinfoStreamReply,
    XinfoStreamReplyFull, XinfoStreamReplyFullConsumer, XinfoStreamReplyFullGroup,
    XpendingExtendedEntry, XpendingSummary,
};
use crate::value::{Fields, FromValue, OrNever};

impl IntoCommand for StreamTrim {
    fn into_command(self) -> Result<Words, Error> {
        //〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ]
        match self {
            StreamTrim::Maxlen(trim) => (
                "MAXLEN",
                trim.efficiency,
                trim.threshold,
                NamedOption("LIMIT", trim.limit),
            )
                .into_command(),
            StreamTrim::Minid(trim) => (
                "MINID",
                trim.efficiency,
                trim.threshold,
                NamedOption("LIMIT", trim.limit),
            )
                .into_command(),
        }
    }
}

impl IntoCommand for StreamTrimEfficiency {
    fn into_command(self) -> Result<Words, Error> {
        //  = | ~
        (match self {
            StreamTrimEfficiency::Exact => "=",
            StreamTrimEfficiency::MearlyExact => "~",
        })
        .into_command()
    }
}

impl IntoCommand for XclaimOpts {
    fn into_command(self) -> Result<Words, Error> {
        // [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] [ LASTID lastid ]
        (
            NamedOption("IDLE", self.idle),
            NamedOption("TIME", self.time),
            NamedOption("RETRYCOUNT", self.retrycount),
            Flag("FORCE", self.force),
            NamedOption("LASTID", self.lastid),
        )
            .into_command()
    }
}

/// A stream ID, or `0-0` for none, e.g. the start of the next XAUTOCLAIM, `0-0` once the scan is
/// complete.
pub struct OrZeroId(pub Option<StreamId>);

pub const ZERO_ID: &str = "0-0";

/// `0-0` for none, e.g. to start XAUTOCLAIM from the beginning.
impl IntoCommand for OrZeroId {
    fn into_command(self) -> Result<Words, Error> {
        self.0.unwrap_or_else(|| ZERO_ID.to_string()).into_command()
    }
}

impl FromValue for OrZeroId {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let id = StreamId::from_value(value)?;
        Ok(OrZeroId((id != ZERO_ID).then_some(id)))
    }
}

/// `[count, lowest-id, highest-id, [[consumer, count]...]]`, the IDs and consumers are null when
/// nothing is pending, and each consumer's count is a bulk string.
impl FromValue for XpendingSummary {
    fn from_value(value: RespValue) -> Result<Self, Error> {
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
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let (id, name, since_delivery, delivery_count) = FromValue::from_value(value)?;
        Ok(Self {
            id,
            name,
            since_delivery,
            delivery_count,
        })
    }
}

impl FromValue for XinfoConsumersEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
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

impl FromValue for XinfoGroupsEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
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
    fn from_value(value: RespValue) -> Result<Self, Error> {
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
    fn from_value(value: RespValue) -> Result<Self, Error> {
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
    fn from_value(value: RespValue) -> Result<Self, Error> {
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
    fn from_value(value: RespValue) -> Result<Self, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{array, bulk, bulks, fields, from, from_error, int, words};
    use crate::types::{StreamTrimMaxlen, StreamTrimMinid};

    use crate::types::StringOrBytes;
    use wit_bindgen::rt::Map;

    fn entry(id: &str, field: &str, value: &str) -> (StreamId, Map<String, StringOrBytes>) {
        (
            id.to_string(),
            [(field.to_string(), StringOrBytes::from(value))]
                .into_iter()
                .collect(),
        )
    }

    fn entry_value(id: &str, field: &str, value: &str) -> RespValue {
        array(vec![bulk(id), bulks(&[field, value])])
    }

    #[test]
    fn stream_trim() {
        let maxlen = StreamTrim::Maxlen(StreamTrimMaxlen {
            efficiency: Some(StreamTrimEfficiency::MearlyExact),
            threshold: 1000,
            limit: Some(10),
        });
        assert_eq!(words(maxlen), ["MAXLEN", "~", "1000", "LIMIT", "10"]);
        let minid = StreamTrim::Minid(StreamTrimMinid {
            efficiency: Some(StreamTrimEfficiency::Exact),
            threshold: "1-0".to_string(),
            limit: None,
        });
        assert_eq!(words(minid), ["MINID", "=", "1-0"]);
        let minid = StreamTrim::Minid(StreamTrimMinid {
            efficiency: None,
            threshold: "1-0".to_string(),
            limit: None,
        });
        assert_eq!(words(minid), ["MINID", "1-0"]);
    }

    #[test]
    fn xclaim_opts() {
        let opts = XclaimOpts {
            idle: Some(DurationMilliseconds { px: 5000 }),
            time: Some(UnixTimeMilliseconds {
                pxat: 1_700_000_000_000,
            }),
            retrycount: Some(2),
            force: true,
            lastid: Some("1-0".to_string()),
        };
        assert_eq!(
            words(opts),
            [
                "IDLE",
                "5000",
                "TIME",
                "1700000000000",
                "RETRYCOUNT",
                "2",
                "FORCE",
                "LASTID",
                "1-0"
            ]
        );
        let opts = XclaimOpts {
            idle: None,
            time: None,
            retrycount: None,
            force: false,
            lastid: None,
        };
        assert_eq!(words(opts), Vec::<String>::new());
    }

    #[test]
    fn or_zero_id() {
        assert_eq!(words(OrZeroId(None)), ["0-0"]);
        assert_eq!(words(OrZeroId(Some("1-1".to_string()))), ["1-1"]);
        assert_eq!(from::<OrZeroId>(bulk("0-0")).0, None);
        assert_eq!(from::<OrZeroId>(bulk("1-1")).0, Some("1-1".to_string()));
    }

    #[test]
    fn xpending_summary() {
        let value = array(vec![
            int(3),
            bulk("1-0"),
            bulk("3-0"),
            array(vec![bulks(&["alice", "2"]), bulks(&["bob", "1"])]),
        ]);
        assert_eq!(
            from::<XpendingSummary>(value),
            XpendingSummary {
                count: 3,
                lowest_id: Some("1-0".to_string()),
                highest_id: Some("3-0".to_string()),
                consumers: vec![("alice".to_string(), 2), ("bob".to_string(), 1)],
            }
        );
        // nothing pending
        let value = array(vec![
            int(0),
            RespValue::Null,
            RespValue::Null,
            RespValue::Null,
        ]);
        assert_eq!(
            from::<XpendingSummary>(value),
            XpendingSummary {
                count: 0,
                lowest_id: None,
                highest_id: None,
                consumers: vec![],
            }
        );
        let value = array(vec![
            int(1),
            bulk("1-0"),
            bulk("1-0"),
            array(vec![bulks(&["alice", "many"])]),
        ]);
        assert_eq!(
            from_error::<XpendingSummary>(value),
            r#"unexpected pending count, not an integer: "many""#
        );
    }

    #[test]
    fn xpending_extended_entry() {
        let value = array(vec![bulk("1-0"), bulk("alice"), int(5000), int(2)]);
        assert_eq!(
            from::<XpendingExtendedEntry>(value),
            XpendingExtendedEntry {
                id: "1-0".to_string(),
                name: "alice".to_string(),
                since_delivery: DurationMilliseconds { px: 5000 },
                delivery_count: 2,
            }
        );
    }

    #[test]
    fn xinfo_consumers_entry() {
        let consumer = |inactive| {
            fields(vec![
                ("name", bulk("alice")),
                ("pending", int(1)),
                ("idle", int(100)),
                ("inactive", inactive),
            ])
        };
        let expected = XinfoConsumersEntry {
            name: "alice".to_string(),
            pending: 1,
            idle: DurationMilliseconds { px: 100 },
            inactive: Some(DurationMilliseconds { px: 50 }),
        };
        assert_eq!(from::<XinfoConsumersEntry>(consumer(int(50))), expected);
        // never active
        assert_eq!(
            from::<XinfoConsumersEntry>(consumer(int(-1))),
            XinfoConsumersEntry {
                inactive: None,
                ..expected
            }
        );
    }

    #[test]
    fn xinfo_groups_entry() {
        let value = fields(vec![
            ("name", bulk("group")),
            ("consumers", int(2)),
            ("pending", int(3)),
            ("last-delivered-id", bulk("5-0")),
            ("entries-read", RespValue::Null),
            ("lag", int(1)),
        ]);
        assert_eq!(
            from::<XinfoGroupsEntry>(value),
            XinfoGroupsEntry {
                name: "group".to_string(),
                consumers: 2,
                pending: 3,
                last_delivered_id: "5-0".to_string(),
                entries_read: None,
                lag: Some(1),
            }
        );
    }

    #[test]
    fn xinfo_stream_reply() {
        let value = fields(vec![
            ("length", int(2)),
            ("radix-tree-keys", int(1)),
            ("radix-tree-nodes", int(2)),
            ("groups", int(1)),
            ("last-generated-id", bulk("2-0")),
            ("max-deleted-entry-id", bulk("0-0")),
            ("entries-added", int(2)),
            ("recorded-first-entry-id", bulk("1-0")),
            ("first-entry", entry_value("1-0", "a", "1")),
            ("last-entry", RespValue::Null),
        ]);
        assert_eq!(
            from::<XinfoStreamReply>(value),
            XinfoStreamReply {
                length: 2,
                radix_tree_keys: 1,
                radix_tree_nodes: 2,
                groups: 1,
                last_generated_id: Some("2-0".to_string()),
                max_deleted_entry_id: None,
                entries_added: 2,
                first_entry: Some(entry("1-0", "a", "1")),
                last_entry: None,
            }
        );
    }

    #[test]
    fn xinfo_stream_reply_full() {
        let consumer = fields(vec![
            ("name", bulk("alice")),
            ("seen-time", int(1_700_000_000_000)),
            ("active-time", int(-1)),
            ("pel-count", int(1)),
            (
                "pending",
                array(vec![array(vec![
                    bulk("1-0"),
                    int(1_700_000_000_000),
                    int(1),
                ])]),
            ),
        ]);
        let group = fields(vec![
            ("name", bulk("group")),
            ("last-delivered-id", bulk("1-0")),
            ("entries-read", int(1)),
            ("lag", int(0)),
            ("pel-count", int(1)),
            (
                "pending",
                array(vec![array(vec![
                    bulk("1-0"),
                    bulk("alice"),
                    int(1_700_000_000_000),
                    int(1),
                ])]),
            ),
            ("consumers", array(vec![consumer])),
        ]);
        let value = fields(vec![
            ("length", int(1)),
            ("radix-tree-keys", int(1)),
            ("radix-tree-nodes", int(2)),
            ("last-generated-id", bulk("1-0")),
            ("max-deleted-entry-id", bulk("0-0")),
            ("entries-added", int(1)),
            ("entries", array(vec![entry_value("1-0", "a", "1")])),
            ("groups", array(vec![group])),
        ]);
        let time = UnixTimeMilliseconds {
            pxat: 1_700_000_000_000,
        };
        assert_eq!(
            from::<XinfoStreamReplyFull>(value),
            XinfoStreamReplyFull {
                length: 1,
                radix_tree_keys: 1,
                radix_tree_nodes: 2,
                last_generated_id: Some("1-0".to_string()),
                max_deleted_entry_id: None,
                entries_added: 1,
                entries: vec![entry("1-0", "a", "1")],
                groups: vec![XinfoStreamReplyFullGroup {
                    name: "group".to_string(),
                    last_delivered_id: "1-0".to_string(),
                    entries_read: Some(1),
                    lag: Some(0),
                    pel_count: 1,
                    pending: vec![("1-0".to_string(), "alice".to_string(), time, 1)],
                    consumers: vec![XinfoStreamReplyFullConsumer {
                        name: "alice".to_string(),
                        seen_time: time,
                        active_time: None,
                        pel_count: 1,
                        pending: vec![("1-0".to_string(), time, 1)],
                    }],
                }],
            }
        );
    }
}
