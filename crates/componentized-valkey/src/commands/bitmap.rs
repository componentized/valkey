//! The bitmap commands: BITCOUNT, BITFIELD, BITPOS, SETBIT and the like.

use crate::command::{IntoCommand, Words};
use crate::types::{
    BitRange, BitfieldEncoding, BitfieldGet, BitfieldIncrby, BitfieldOffset, BitfieldOperation,
    BitfieldOverflow, BitfieldSet, Error,
};

/// `start [ end [ BIT ] ]`, BYTE is the default, and left out for servers before it existed.
impl IntoCommand for BitRange {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            BitRange::From(start) => start.into_command(),
            BitRange::Bytes((start, end)) => (start, end).into_command(),
            BitRange::Bits((start, end)) => (start, end, "BIT").into_command(),
        }
    }
}

/// `i1` to `i64`, `u1` to `u63`, other widths fail.
impl IntoCommand for BitfieldEncoding {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            BitfieldEncoding::Signed(width @ 1..=64) => format!("i{width}").into_command(),
            BitfieldEncoding::Unsigned(width @ 1..=63) => format!("u{width}").into_command(),
            BitfieldEncoding::Signed(width) => Err(Error::Client(format!(
                "a signed bitfield is 1 to 64 bits wide, not {width}"
            ))),
            BitfieldEncoding::Unsigned(width) => Err(Error::Client(format!(
                "an unsigned bitfield is 1 to 63 bits wide, not {width}"
            ))),
        }
    }
}

/// The offset in bits, or `#` and the index of a field of the encoding's width.
impl IntoCommand for BitfieldOffset {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            BitfieldOffset::Bit(offset) => offset.into_command(),
            BitfieldOffset::Field(field) => format!("#{field}").into_command(),
        }
    }
}

impl IntoCommand for BitfieldOverflow {
    fn into_command(self) -> Result<Words, Error> {
        let overflow = match self {
            BitfieldOverflow::Wrap => "WRAP",
            BitfieldOverflow::Sat => "SAT",
            BitfieldOverflow::Fail => "FAIL",
        };
        ("OVERFLOW", overflow).into_command()
    }
}

/// Also `BitfieldRoGet`, the same type.
impl IntoCommand for BitfieldGet {
    fn into_command(self) -> Result<Words, Error> {
        ("GET", self.encoding, self.offset).into_command()
    }
}

impl IntoCommand for BitfieldSet {
    fn into_command(self) -> Result<Words, Error> {
        ("SET", self.encoding, self.offset, self.value).into_command()
    }
}

impl IntoCommand for BitfieldIncrby {
    fn into_command(self) -> Result<Words, Error> {
        ("INCRBY", self.encoding, self.offset, self.increment).into_command()
    }
}

/// A write is preceded by its overflow mode, when given.
impl IntoCommand for BitfieldOperation {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            BitfieldOperation::Get(get) => get.into_command(),
            BitfieldOperation::Set((overflow, set)) => (overflow, set).into_command(),
            BitfieldOperation::Incrby((overflow, incrby)) => (overflow, incrby).into_command(),
        }
    }
}

/// A bit, `1` or `0`, e.g. for SETBIT.
pub struct Bit(pub bool);

impl IntoCommand for Bit {
    fn into_command(self) -> Result<Words, Error> {
        if self.0 { "1" } else { "0" }.into_command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{words, words_error};

    #[test]
    fn bit_range() {
        assert_eq!(words(BitRange::From(5)), ["5"]);
        assert_eq!(words(BitRange::Bytes((0, -1))), ["0", "-1"]);
        assert_eq!(words(BitRange::Bits((1, 8))), ["1", "8", "BIT"]);
    }

    #[test]
    fn bitfield_encoding() {
        assert_eq!(
            words([
                BitfieldEncoding::Signed(1),
                BitfieldEncoding::Signed(64),
                BitfieldEncoding::Unsigned(1),
                BitfieldEncoding::Unsigned(63),
            ]),
            ["i1", "i64", "u1", "u63"]
        );
        assert_eq!(
            words_error(BitfieldEncoding::Signed(0)),
            "a signed bitfield is 1 to 64 bits wide, not 0"
        );
        assert_eq!(
            words_error(BitfieldEncoding::Signed(65)),
            "a signed bitfield is 1 to 64 bits wide, not 65"
        );
        assert_eq!(
            words_error(BitfieldEncoding::Unsigned(64)),
            "an unsigned bitfield is 1 to 63 bits wide, not 64"
        );
    }

    #[test]
    fn bitfield_offset() {
        assert_eq!(words(BitfieldOffset::Bit(100)), ["100"]);
        assert_eq!(words(BitfieldOffset::Field(2)), ["#2"]);
    }

    #[test]
    fn bitfield_overflow() {
        assert_eq!(
            words([
                BitfieldOverflow::Wrap,
                BitfieldOverflow::Sat,
                BitfieldOverflow::Fail
            ]),
            ["OVERFLOW", "WRAP", "OVERFLOW", "SAT", "OVERFLOW", "FAIL"]
        );
    }

    #[test]
    fn bitfield_operations() {
        let get = BitfieldGet {
            encoding: BitfieldEncoding::Unsigned(8),
            offset: BitfieldOffset::Bit(0),
        };
        let set = BitfieldSet {
            encoding: BitfieldEncoding::Signed(8),
            offset: BitfieldOffset::Field(1),
            value: -5,
        };
        let incrby = BitfieldIncrby {
            encoding: BitfieldEncoding::Unsigned(4),
            offset: BitfieldOffset::Bit(16),
            increment: 3,
        };
        assert_eq!(words(get.clone()), ["GET", "u8", "0"]);
        assert_eq!(
            words(vec![
                BitfieldOperation::Get(get),
                BitfieldOperation::Set((Some(BitfieldOverflow::Sat), set)),
                BitfieldOperation::Incrby((None, incrby)),
            ]),
            [
                "GET", "u8", "0", "OVERFLOW", "SAT", "SET", "i8", "#1", "-5", "INCRBY", "u4", "16",
                "3"
            ]
        );
    }

    #[test]
    fn bitfield_invalid_encoding() {
        let get = BitfieldGet {
            encoding: BitfieldEncoding::Unsigned(64),
            offset: BitfieldOffset::Bit(0),
        };
        assert_eq!(
            words_error(BitfieldOperation::Get(get)),
            "an unsigned bitfield is 1 to 63 bits wide, not 64"
        );
    }

    #[test]
    fn bit() {
        assert_eq!(words([Bit(true), Bit(false)]), ["1", "0"]);
    }
}
