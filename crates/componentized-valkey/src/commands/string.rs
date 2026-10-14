//! The string commands: GETEX, LCS, MSETEX, SET and the like.

use crate::command::{IntoCommand, Words};
use crate::types::{
    Error, GetexTtl, LcsIdxMatch, LcsIdxReply, MsetexPrecondition, RespValue, SetPrecondition,
    SetTtl,
};
use crate::value::{Fields, FromValue, items};

/// Also `HgetexTtl`, the same type.
impl IntoCommand for GetexTtl {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            GetexTtl::Ex(duration) => ("EX", duration).into_command(),
            GetexTtl::Px(duration) => ("PX", duration).into_command(),
            GetexTtl::Exat(unix_time) => ("EXAT", unix_time).into_command(),
            GetexTtl::Pxat(unix_time) => ("PXAT", unix_time).into_command(),
            GetexTtl::Persist => "PERSIST".into_command(),
        }
    }
}

/// A map of the matches and the length of the longest common subsequence.
impl FromValue for LcsIdxReply {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            matches: fields.required("matches")?,
            len: fields.required("len")?,
        })
    }
}

/// The inclusive ranges of a match in each key and, with `WITHMATCHLEN`, its length.
impl FromValue for LcsIdxMatch {
    fn from_value(value: RespValue) -> Result<Self, Error> {
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

impl IntoCommand for MsetexPrecondition {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            MsetexPrecondition::Nx => "NX".into_command(),
            MsetexPrecondition::Xx => "XX".into_command(),
        }
    }
}

impl IntoCommand for SetPrecondition {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            SetPrecondition::Nx => "NX".into_command(),
            SetPrecondition::Xx => "XX".into_command(),
            SetPrecondition::Ifeq(value) => ("IFEQ", value).into_command(),
            SetPrecondition::Ifne(value) => ("IFNE", value).into_command(),
        }
    }
}

/// Also `MsetexTtl` and `HsetexTtl`, the same type.
impl IntoCommand for SetTtl {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            SetTtl::Ex(duration) => ("EX", duration).into_command(),
            SetTtl::Px(duration) => ("PX", duration).into_command(),
            SetTtl::Exat(unix_time) => ("EXAT", unix_time).into_command(),
            SetTtl::Pxat(unix_time) => ("PXAT", unix_time).into_command(),
            SetTtl::Keepttl => "KEEPTTL".into_command(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{array, fields, from, from_error, int, words};
    use crate::types::{
        DurationMilliseconds, DurationSeconds, UnixTimeMilliseconds, UnixTimeSeconds,
    };

    #[test]
    fn getex_ttl() {
        assert_eq!(
            words(vec![
                GetexTtl::Ex(DurationSeconds { ex: 10 }),
                GetexTtl::Px(DurationMilliseconds { px: 1500 }),
                GetexTtl::Exat(UnixTimeSeconds {
                    exat: 1_700_000_000
                }),
                GetexTtl::Pxat(UnixTimeMilliseconds {
                    pxat: 1_700_000_000_123
                }),
                GetexTtl::Persist,
            ]),
            [
                "EX",
                "10",
                "PX",
                "1500",
                "EXAT",
                "1700000000",
                "PXAT",
                "1700000000123",
                "PERSIST"
            ]
        );
    }

    #[test]
    fn set_ttl() {
        assert_eq!(
            words(vec![
                SetTtl::Ex(DurationSeconds { ex: 10 }),
                SetTtl::Px(DurationMilliseconds { px: 1500 }),
                SetTtl::Exat(UnixTimeSeconds {
                    exat: 1_700_000_000
                }),
                SetTtl::Pxat(UnixTimeMilliseconds {
                    pxat: 1_700_000_000_123
                }),
                SetTtl::Keepttl,
            ]),
            [
                "EX",
                "10",
                "PX",
                "1500",
                "EXAT",
                "1700000000",
                "PXAT",
                "1700000000123",
                "KEEPTTL"
            ]
        );
    }

    #[test]
    fn preconditions() {
        assert_eq!(
            words(vec![
                SetPrecondition::Nx,
                SetPrecondition::Xx,
                SetPrecondition::Ifeq("old".into()),
                SetPrecondition::Ifne("other".into()),
            ]),
            ["NX", "XX", "IFEQ", "old", "IFNE", "other"]
        );
        assert_eq!(
            words(vec![MsetexPrecondition::Nx, MsetexPrecondition::Xx]),
            ["NX", "XX"]
        );
    }

    fn range(start: i64, end: i64) -> RespValue {
        array(vec![int(start), int(end)])
    }

    #[test]
    fn lcs_idx_reply() {
        let value = fields(vec![
            (
                "matches",
                array(vec![
                    array(vec![range(4, 7), range(5, 8), int(4)]),
                    array(vec![range(2, 3), range(0, 1)]),
                ]),
            ),
            ("len", int(6)),
        ]);
        assert_eq!(
            from::<LcsIdxReply>(value),
            LcsIdxReply {
                matches: vec![
                    LcsIdxMatch {
                        key1_range: (4, 7),
                        key2_range: (5, 8),
                        len: Some(4),
                    },
                    LcsIdxMatch {
                        key1_range: (2, 3),
                        key2_range: (0, 1),
                        len: None,
                    },
                ],
                len: 6,
            }
        );
    }

    #[test]
    fn lcs_idx_match_invalid() {
        assert_eq!(
            from_error::<LcsIdxMatch>(array(vec![range(4, 7)])),
            "unexpected LCS match, expected 2 or 3 items"
        );
        assert_eq!(
            from_error::<LcsIdxMatch>(array(vec![range(4, 7), range(5, 8), int(4), int(1)])),
            "unexpected LCS match, expected 2 or 3 items"
        );
    }
}
