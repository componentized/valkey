//! The sorted set commands: ZADD, ZINTER, ZMPOP, ZRANGE, ZUNION and the like.

use crate::command::{Flag, IntoCommand, Words};
use crate::types::{
    Double, ErrorCode, LexBound, Limit, RespValue, ScoreBound, StringOrBytes, ZaddComparison,
    ZaddCondition, ZmpopEnd, ZrangeBy, ZsetAggregate,
};
use crate::value::{FromValue, items};

/// The start and stop of a range of scores, as given, the start is the maximum with REV.
pub struct ScoreRange {
    pub start: ScoreBound,
    pub stop: ScoreBound,
    pub rev: bool,
}

impl IntoCommand for ScoreRange {
    fn into_command(self) -> Result<Words, ErrorCode> {
        let bound = |bound, unbounded: &str| match bound {
            ScoreBound::Inclusive(score) => score.to_string().into(),
            ScoreBound::Exclusive(score) => format!("({score}").into(),
            ScoreBound::Unbounded => unbounded.into(),
        };
        let (low, high) = ("-inf", "+inf");
        let (start, stop) = if self.rev { (high, low) } else { (low, high) };
        Ok(vec![bound(self.start, start), bound(self.stop, stop)])
    }
}

/// The start and stop of a range of members, as given, the start is the maximum with REV.
pub struct LexRange {
    pub start: LexBound,
    pub stop: LexBound,
    pub rev: bool,
}

impl IntoCommand for LexRange {
    fn into_command(self) -> Result<Words, ErrorCode> {
        // a member may be bytes, the prefix keeps it so
        let bound = |bound, unbounded: &str| match bound {
            LexBound::Inclusive(member) => StringOrBytes::prefixed(member, "["),
            LexBound::Exclusive(member) => StringOrBytes::prefixed(member, "("),
            LexBound::Unbounded => unbounded.into(),
        };
        let (low, high) = ("-", "+");
        let (start, stop) = if self.rev { (high, low) } else { (low, high) };
        Ok(vec![bound(self.start, start), bound(self.stop, stop)])
    }
}

/// ZRANGE and ZRANGESTORE, `start stop [ BYSCORE | BYLEX ] [ REV ] [ LIMIT offset count ]`.
pub struct Zrange {
    pub range: ZrangeBy,
    pub rev: bool,
    pub limit: Option<Limit>,
}

impl IntoCommand for Zrange {
    fn into_command(self) -> Result<Words, ErrorCode> {
        let Zrange { range, rev, limit } = self;
        let (range, by) = match range {
            ZrangeBy::Rank((start, stop)) => ((start, stop).into_command()?, None),
            ZrangeBy::Score((start, stop)) => (
                ScoreRange { start, stop, rev }.into_command()?,
                Some("BYSCORE"),
            ),
            ZrangeBy::Lex((start, stop)) => {
                (LexRange { start, stop, rev }.into_command()?, Some("BYLEX"))
            }
        };
        (range, by, Flag("REV", rev), limit).into_command()
    }
}

impl IntoCommand for ZsetAggregate {
    fn into_command(self) -> Result<Words, ErrorCode> {
        let aggregate = match self {
            ZsetAggregate::Sum => "SUM",
            ZsetAggregate::Min => "MIN",
            ZsetAggregate::Max => "MAX",
        };
        ("AGGREGATE", aggregate).into_command()
    }
}

/// Also `BzmpopEnd`, the same type.
impl IntoCommand for ZmpopEnd {
    fn into_command(self) -> Result<Words, ErrorCode> {
        match self {
            ZmpopEnd::Min => "MIN",
            ZmpopEnd::Max => "MAX",
        }
        .into_command()
    }
}

impl IntoCommand for ZaddCondition {
    fn into_command(self) -> Result<Words, ErrorCode> {
        match self {
            ZaddCondition::Nx => "NX",
            ZaddCondition::Xx => "XX",
        }
        .into_command()
    }
}

impl IntoCommand for ZaddComparison {
    fn into_command(self) -> Result<Words, ErrorCode> {
        match self {
            ZaddComparison::Gt => "GT",
            ZaddComparison::Lt => "LT",
        }
        .into_command()
    }
}

/// Members and their scores, e.g. from ZPOPMIN, which replies with one pair, or none, without a
/// count, and a list of pairs with one.
pub struct ScoredMembers(pub Vec<(StringOrBytes, Double)>);

impl FromValue for ScoredMembers {
    fn from_value(value: RespValue) -> Result<Self, ErrorCode> {
        let items = items(value)?;
        match items.first() {
            None => Ok(ScoredMembers(vec![])),
            Some(RespValue::Array(_)) => items
                .into_iter()
                .map(FromValue::from_value)
                .collect::<Result<_, _>>()
                .map(ScoredMembers),
            Some(_) => match <[RespValue; 2]>::try_from(items) {
                Ok([member, score]) => Ok(ScoredMembers(vec![(
                    StringOrBytes::from_value(member)?,
                    Double::from_value(score)?,
                )])),
                Err(items) => Err(ErrorCode::Client(format!(
                    "unexpected number of items, expected a member and its score: {}",
                    items.len()
                ))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{array, bulk, bulks, double, from, from_error, words};

    #[test]
    fn score_range() {
        let range = |start, stop, rev| ScoreRange { start, stop, rev };
        assert_eq!(
            words(range(
                ScoreBound::Inclusive(1.0),
                ScoreBound::Exclusive(2.5),
                false
            )),
            ["1", "(2.5"]
        );
        assert_eq!(
            words(range(ScoreBound::Unbounded, ScoreBound::Unbounded, false)),
            ["-inf", "+inf"]
        );
        // with REV, the start is the maximum
        assert_eq!(
            words(range(ScoreBound::Unbounded, ScoreBound::Unbounded, true)),
            ["+inf", "-inf"]
        );
    }

    #[test]
    fn lex_range() {
        let range = |start, stop, rev| LexRange { start, stop, rev };
        assert_eq!(
            words(range(
                LexBound::Inclusive("a".into()),
                LexBound::Exclusive("c".into()),
                false
            )),
            ["[a", "(c"]
        );
        assert_eq!(
            words(range(LexBound::Unbounded, LexBound::Unbounded, false)),
            ["-", "+"]
        );
        assert_eq!(
            words(range(LexBound::Unbounded, LexBound::Unbounded, true)),
            ["+", "-"]
        );
    }

    #[test]
    fn zrange() {
        let by_rank = Zrange {
            range: ZrangeBy::Rank((0, -1)),
            rev: false,
            limit: None,
        };
        assert_eq!(words(by_rank), ["0", "-1"]);
        let by_score = Zrange {
            range: ZrangeBy::Score((ScoreBound::Unbounded, ScoreBound::Exclusive(1.0))),
            rev: true,
            limit: Some(Limit {
                offset: 0,
                count: 5,
            }),
        };
        assert_eq!(
            words(by_score),
            ["+inf", "(1", "BYSCORE", "REV", "LIMIT", "0", "5"]
        );
        let by_lex = Zrange {
            range: ZrangeBy::Lex((LexBound::Inclusive("a".into()), LexBound::Unbounded)),
            rev: false,
            limit: None,
        };
        assert_eq!(words(by_lex), ["[a", "+", "BYLEX"]);
    }

    #[test]
    fn keywords() {
        assert_eq!(
            words([ZsetAggregate::Sum, ZsetAggregate::Min, ZsetAggregate::Max]),
            ["AGGREGATE", "SUM", "AGGREGATE", "MIN", "AGGREGATE", "MAX"]
        );
        assert_eq!(words([ZmpopEnd::Min, ZmpopEnd::Max]), ["MIN", "MAX"]);
        assert_eq!(words([ZaddCondition::Nx, ZaddCondition::Xx]), ["NX", "XX"]);
        assert_eq!(
            words([ZaddComparison::Gt, ZaddComparison::Lt]),
            ["GT", "LT"]
        );
    }

    #[test]
    fn scored_members() {
        assert_eq!(from::<ScoredMembers>(array(vec![])).0, []);
        // one pair, flat
        assert_eq!(
            from::<ScoredMembers>(array(vec![bulk("a"), double(1.5)])).0,
            [(StringOrBytes::from("a"), 1.5)]
        );
        // a list of pairs, the scores as doubles or, with RESP2, bulk strings
        assert_eq!(
            from::<ScoredMembers>(array(vec![
                array(vec![bulk("a"), double(1.0)]),
                bulks(&["b", "2"]),
            ]))
            .0,
            [
                (StringOrBytes::from("a"), 1.0),
                (StringOrBytes::from("b"), 2.0)
            ]
        );
        assert_eq!(
            from_error::<ScoredMembers>(bulks(&["a", "1", "b"])),
            "unexpected number of items, expected a member and its score: 3"
        );
    }
}
