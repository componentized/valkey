//! The list commands: LINSERT, LMOVE, LMPOP and the like.

use crate::command::IntoCommand;
use crate::types::{Error, Integer, LinsertPosition, LinsertResult, ListSide, RespValue};
use crate::value::FromValue;

impl IntoCommand for ListSide {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ListSide::Left => "LEFT",
            ListSide::Right => "RIGHT",
        }
        .into_command()
    }
}

impl IntoCommand for LinsertPosition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            LinsertPosition::Before => "BEFORE",
            LinsertPosition::After => "AFTER",
        }
        .into_command()
    }
}

/// The length of the list after the insert, `0` when the key does not exist, `-1` when the pivot
/// was not found.
impl FromValue for LinsertResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -1 => Ok(Self::PivotNotFound),
            0 => Ok(Self::NoSuchKey),
            length if length > 0 => Ok(Self::Inserted(length)),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{from, from_error, int, words};

    #[test]
    fn keywords() {
        assert_eq!(words([ListSide::Left, ListSide::Right]), ["LEFT", "RIGHT"]);
        assert_eq!(
            words([LinsertPosition::Before, LinsertPosition::After]),
            ["BEFORE", "AFTER"]
        );
    }

    #[test]
    fn linsert_result() {
        assert_eq!(from::<LinsertResult>(int(-1)), LinsertResult::PivotNotFound);
        assert_eq!(from::<LinsertResult>(int(0)), LinsertResult::NoSuchKey);
        assert_eq!(from::<LinsertResult>(int(4)), LinsertResult::Inserted(4));
        assert_eq!(from_error::<LinsertResult>(int(-2)), "unexpected reply: -2");
    }
}
