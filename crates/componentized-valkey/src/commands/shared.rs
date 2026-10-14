//! Shared by the commands: times, cursors, the expiration of keys and fields, limits and sort order.

use crate::command::{IntoCommand, Words};
use crate::types::{
    Cursor, DurationMilliseconds, DurationSeconds, Error, ExpiretimeReply, Integer, Limit,
    PexpiretimeReply, PttlReply, RespValue, SortOrder, TtlMode, TtlReply, UnixTimeMilliseconds,
    UnixTimeSeconds,
};
use crate::value::FromValue;

/// Whole seconds, e.g. for EX.
impl IntoCommand for DurationSeconds {
    fn into_command(self) -> Result<Words, Error> {
        self.ex.into_command()
    }
}

/// Whole seconds, never negative.
impl FromValue for DurationSeconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        u64::from_value(value).map(|ex| DurationSeconds { ex })
    }
}

/// Whole milliseconds, e.g. for PX.
impl IntoCommand for DurationMilliseconds {
    fn into_command(self) -> Result<Words, Error> {
        self.px.into_command()
    }
}

/// Whole milliseconds, never negative.
impl FromValue for DurationMilliseconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        u64::from_value(value).map(|px| DurationMilliseconds { px })
    }
}

/// Whole seconds since the Unix epoch, e.g. for EXAT.
impl IntoCommand for UnixTimeSeconds {
    fn into_command(self) -> Result<Words, Error> {
        self.exat.into_command()
    }
}

/// Whole seconds since the Unix epoch.
impl FromValue for UnixTimeSeconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Integer::from_value(value).map(|exat| UnixTimeSeconds { exat })
    }
}

/// Whole milliseconds since the Unix epoch, e.g. for PXAT.
impl IntoCommand for UnixTimeMilliseconds {
    fn into_command(self) -> Result<Words, Error> {
        self.pxat.into_command()
    }
}

/// Whole milliseconds since the Unix epoch.
impl FromValue for UnixTimeMilliseconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Integer::from_value(value).map(|pxat| UnixTimeMilliseconds { pxat })
    }
}

/// Milliseconds as seconds with a fraction, e.g. the timeout of BZPOPMIN, `1500` as `1.500`.
pub struct DecimalSeconds(pub DurationMilliseconds);

impl IntoCommand for DecimalSeconds {
    fn into_command(self) -> Result<Words, Error> {
        let milliseconds = self.0.px;
        format!("{}.{:03}", milliseconds / 1_000, milliseconds % 1_000).into_command()
    }
}

/// `0` to start, failing to resume from `0`, which would restart the scan.
impl IntoCommand for Cursor {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            Cursor::Start => "0".into_command(),
            Cursor::Resume(cursor) if cursor == "0" => Err(Error::Client(
                "resume requires the cursor a scan returned, use start to begin a scan".to_string(),
            )),
            Cursor::Resume(cursor) => cursor.into_command(),
        }
    }
}

/// The cursor a scan returns to resume from, none once the scan is complete, `0`.
pub struct NextCursor(pub Option<Cursor>);

impl FromValue for NextCursor {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let cursor = String::from_value(value)?;
        Ok(NextCursor(
            (cursor != "0").then_some(Cursor::Resume(cursor)),
        ))
    }
}

impl IntoCommand for TtlMode {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            TtlMode::Nx => "NX",
            TtlMode::Xx => "XX",
            TtlMode::Gt => "GT",
            TtlMode::Lt => "LT",
        }
        .into_command()
    }
}

/// The codes of TTL, HTTL and the like, `-2` when the key or field does not exist, `-1` when it has
/// no expiration, otherwise the time.
macro_rules! from_value_ttl_replies {
    ($($variant:ident($time:ident) => $case:ident),*) => {
        $(
            impl FromValue for $variant {
                fn from_value(value: RespValue) -> Result<Self, Error> {
                    match value {
                        RespValue::Integer(-2) => Ok($variant::DoesNotExist),
                        RespValue::Integer(-1) => Ok($variant::NoExpiration),
                        value => $time::from_value(value).map($variant::$case),
                    }
                }
            }
        )*
    };
}

from_value_ttl_replies!(
    TtlReply(DurationSeconds) => ExpiresIn,
    PttlReply(DurationMilliseconds) => ExpiresIn,
    ExpiretimeReply(UnixTimeSeconds) => ExpiresAt,
    PexpiretimeReply(UnixTimeMilliseconds) => ExpiresAt
);

/// `LIMIT offset count`
impl IntoCommand for Limit {
    fn into_command(self) -> Result<Words, Error> {
        ("LIMIT", self.offset, self.count).into_command()
    }
}

impl IntoCommand for SortOrder {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        }
        .into_command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{bulk, from, from_error, int, words, words_error};

    #[test]
    fn durations() {
        assert_eq!(words(DurationSeconds { ex: 10 }), ["10"]);
        assert_eq!(words(DurationMilliseconds { px: 1500 }), ["1500"]);
        assert_eq!(from::<DurationSeconds>(int(10)), DurationSeconds { ex: 10 });
        assert_eq!(
            from::<DurationMilliseconds>(int(1500)),
            DurationMilliseconds { px: 1500 }
        );
        assert_eq!(
            from_error::<DurationSeconds>(int(-1)),
            "unexpected negative reply: -1"
        );
        assert_eq!(
            from_error::<DurationMilliseconds>(int(-1)),
            "unexpected negative reply: -1"
        );
    }

    #[test]
    fn unix_times() {
        assert_eq!(
            words(UnixTimeSeconds {
                exat: 1_700_000_000
            }),
            ["1700000000"]
        );
        assert_eq!(
            words(UnixTimeMilliseconds {
                pxat: 1_700_000_000_123
            }),
            ["1700000000123"]
        );
        assert_eq!(
            from::<UnixTimeSeconds>(int(1_700_000_000)),
            UnixTimeSeconds {
                exat: 1_700_000_000
            }
        );
        assert_eq!(
            from::<UnixTimeMilliseconds>(int(1_700_000_000_123)),
            UnixTimeMilliseconds {
                pxat: 1_700_000_000_123
            }
        );
    }

    #[test]
    fn decimal_seconds() {
        assert_eq!(
            words(DecimalSeconds(DurationMilliseconds { px: 1500 })),
            ["1.500"]
        );
        assert_eq!(
            words(DecimalSeconds(DurationMilliseconds { px: 5 })),
            ["0.005"]
        );
        assert_eq!(
            words(DecimalSeconds(DurationMilliseconds { px: 0 })),
            ["0.000"]
        );
        assert_eq!(
            words(DecimalSeconds(DurationMilliseconds { px: 61_010 })),
            ["61.010"]
        );
    }

    #[test]
    fn cursor() {
        assert_eq!(words(Cursor::Start), ["0"]);
        assert_eq!(words(Cursor::Resume("17".to_string())), ["17"]);
        assert_eq!(
            words_error(Cursor::Resume("0".to_string())),
            "resume requires the cursor a scan returned, use start to begin a scan"
        );
    }

    #[test]
    fn next_cursor() {
        assert_eq!(from::<NextCursor>(bulk("0")).0, None);
        assert_eq!(
            from::<NextCursor>(bulk("17")).0,
            Some(Cursor::Resume("17".to_string()))
        );
    }

    #[test]
    fn ttl_mode() {
        assert_eq!(
            words([TtlMode::Nx, TtlMode::Xx, TtlMode::Gt, TtlMode::Lt]),
            ["NX", "XX", "GT", "LT"]
        );
    }

    #[test]
    fn ttl_replies() {
        assert_eq!(from::<TtlReply>(int(-2)), TtlReply::DoesNotExist);
        assert_eq!(from::<TtlReply>(int(-1)), TtlReply::NoExpiration);
        assert_eq!(
            from::<TtlReply>(int(30)),
            TtlReply::ExpiresIn(DurationSeconds { ex: 30 })
        );
        assert_eq!(
            from_error::<TtlReply>(int(-3)),
            "unexpected negative reply: -3"
        );
        assert_eq!(from::<PttlReply>(int(-2)), PttlReply::DoesNotExist);
        assert_eq!(
            from::<PttlReply>(int(1500)),
            PttlReply::ExpiresIn(DurationMilliseconds { px: 1500 })
        );
        assert_eq!(
            from::<ExpiretimeReply>(int(-1)),
            ExpiretimeReply::NoExpiration
        );
        assert_eq!(
            from::<ExpiretimeReply>(int(1_700_000_000)),
            ExpiretimeReply::ExpiresAt(UnixTimeSeconds {
                exat: 1_700_000_000
            })
        );
        assert_eq!(
            from::<PexpiretimeReply>(int(1_700_000_000_123)),
            PexpiretimeReply::ExpiresAt(UnixTimeMilliseconds {
                pxat: 1_700_000_000_123
            })
        );
    }

    #[test]
    fn limit() {
        assert_eq!(
            words(Limit {
                offset: 0,
                count: 10
            }),
            ["LIMIT", "0", "10"]
        );
    }

    #[test]
    fn sort_order() {
        assert_eq!(words([SortOrder::Asc, SortOrder::Desc]), ["ASC", "DESC"]);
    }
}
