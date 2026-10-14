//! The hash commands: HEXPIRE, HPERSIST, HSETEX and the like.

use crate::command::{IntoCommand, NonEmpty, NumKeys, Words};
use crate::types::{
    Error, HexpireResult, HpersistResult, HsetexCondition, HsetexFieldCondition, Integer, RespValue,
};
use crate::value::FromValue;

/// `FIELDS numfields field [ field ... ]`, e.g. for HEXPIRE, a field and its value counts as one.
pub struct Fields<T>(pub NonEmpty<T>);

impl<T: IntoCommand> IntoCommand for Fields<T> {
    fn into_command(self) -> Result<Words, Error> {
        ("FIELDS", NumKeys(self.0)).into_command()
    }
}

/// Also `HexpireatResult`, `HpexpireResult` and `HpexpireatResult`, the same type.
impl FromValue for HexpireResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -2 => Ok(Self::DoesNotExist),
            0 => Ok(Self::ConditionNotMet),
            1 => Ok(Self::Applied),
            2 => Ok(Self::ZeroSeconds),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

impl FromValue for HpersistResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -2 => Ok(Self::DoesNotExist),
            -1 => Ok(Self::NoExpiration),
            1 => Ok(Self::Removed),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

impl IntoCommand for HsetexCondition {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            HsetexCondition::Nx => "NX",
            HsetexCondition::Xx => "XX",
        }
        .into_command()
    }
}

impl IntoCommand for HsetexFieldCondition {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            HsetexFieldCondition::Fnx => "FNX",
            HsetexFieldCondition::Fxx => "FXX",
        }
        .into_command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{from, from_error, int, words, words_error};

    #[test]
    fn fields() {
        assert_eq!(
            words(Fields(NonEmpty::new("fields", vec!["a", "b"]))),
            ["FIELDS", "2", "a", "b"]
        );
        // a field and its value counts as one
        assert_eq!(
            words(Fields(NonEmpty::new(
                "fields",
                vec![("f1", "v1"), ("f2", "v2")]
            ))),
            ["FIELDS", "2", "f1", "v1", "f2", "v2"]
        );
        assert_eq!(
            words_error(Fields(NonEmpty::new("fields", Vec::<String>::new()))),
            "fields requires at least one item"
        );
    }

    #[test]
    fn hexpire_result() {
        assert_eq!(from::<HexpireResult>(int(-2)), HexpireResult::DoesNotExist);
        assert_eq!(
            from::<HexpireResult>(int(0)),
            HexpireResult::ConditionNotMet
        );
        assert_eq!(from::<HexpireResult>(int(1)), HexpireResult::Applied);
        assert_eq!(from::<HexpireResult>(int(2)), HexpireResult::ZeroSeconds);
        assert_eq!(from_error::<HexpireResult>(int(-1)), "unexpected reply: -1");
    }

    #[test]
    fn hpersist_result() {
        assert_eq!(
            from::<HpersistResult>(int(-2)),
            HpersistResult::DoesNotExist
        );
        assert_eq!(
            from::<HpersistResult>(int(-1)),
            HpersistResult::NoExpiration
        );
        assert_eq!(from::<HpersistResult>(int(1)), HpersistResult::Removed);
        assert_eq!(from_error::<HpersistResult>(int(0)), "unexpected reply: 0");
    }

    #[test]
    fn conditions() {
        assert_eq!(
            words([HsetexCondition::Nx, HsetexCondition::Xx]),
            ["NX", "XX"]
        );
        assert_eq!(
            words([HsetexFieldCondition::Fnx, HsetexFieldCondition::Fxx]),
            ["FNX", "FXX"]
        );
    }
}
