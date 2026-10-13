//! Converting Rust values into the words of a command, the impls for Rust's own types and the
//! helpers the commands share. The impls for the package's types are in [`crate::commands`].

use crate::types::Error;

/// Converts a Rust type into the words of a command, e.g. `Option<String>` into no word, or one.
///
/// The impls compose, a tuple is the words of each of its items in order, so a command is written
/// as one value, e.g. `("SET", key, value, precondition, ttl)`.
///
/// A value the command does not accept, e.g. an empty list where it requires an item, fails with a
/// client error, and the command is not sent.
pub trait IntoCommand {
    fn into_command(self) -> Result<Vec<String>, Error>;
}

impl IntoCommand for &str {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self.to_string()])
    }
}

impl IntoCommand for String {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self])
    }
}

impl IntoCommand for &String {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self.clone()])
    }
}

/// Numbers, without a unit, see `DurationSeconds` and the other records for times.
macro_rules! into_command_numbers {
    ($($number:ty),*) => {
        $(
            impl IntoCommand for $number {
                fn into_command(self) -> Result<Vec<String>, Error> {
                    Ok(vec![self.to_string()])
                }
            }
        )*
    };
}

into_command_numbers!(i32, i64, u8, u16, u32, u64, usize, f64);

/// No word for none.
impl<T: IntoCommand> IntoCommand for Option<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        self.map_or(Ok(vec![]), IntoCommand::into_command)
    }
}

impl<T: IntoCommand> IntoCommand for Vec<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let mut command = vec![];
        for item in self {
            command.extend(item.into_command()?);
        }
        Ok(command)
    }
}

impl<T: IntoCommand, const N: usize> IntoCommand for [T; N] {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let mut command = vec![];
        for item in self {
            command.extend(item.into_command()?);
        }
        Ok(command)
    }
}

/// The words of each item, in order.
macro_rules! into_command_tuples {
    ($(($($item:ident),+)),*) => {
        $(
            impl<$($item: IntoCommand),+> IntoCommand for ($($item,)+) {
                #[allow(non_snake_case)]
                fn into_command(self) -> Result<Vec<String>, Error> {
                    let ($($item,)+) = self;
                    let mut command = vec![];
                    $(command.extend($item.into_command()?);)+
                    Ok(command)
                }
            }
        )*
    };
}

into_command_tuples!(
    (A),
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
    (A, B, C, D, E, F, G),
    (A, B, C, D, E, F, G, H),
    (A, B, C, D, E, F, G, H, I),
    (A, B, C, D, E, F, G, H, I, J)
);

/// A word that is present when true, e.g. `Flag("WITHMATCHLEN", with_match_len)`.
///
/// A `bool` has no impl of its own, the word it stands for depends on the command.
pub struct Flag(pub &'static str, pub bool);

impl IntoCommand for Flag {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let Flag(word, present) = self;
        Ok(if present {
            vec![word.to_string()]
        } else {
            vec![]
        })
    }
}

/// A value preceded by its name, e.g. `COUNT count`, no words for none.
pub struct NamedOption<T>(pub &'static str, pub Option<T>);

impl<T: IntoCommand> IntoCommand for NamedOption<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let NamedOption(name, value) = self;
        match value {
            Some(_) => (name, value).into_command(),
            None => Ok(vec![]),
        }
    }
}

/// A list the command requires at least one item of, e.g. the keys of DEL, named for the error when
/// it is empty.
pub struct NonEmpty<T>(&'static str, Vec<T>);

impl<T> NonEmpty<T> {
    pub fn new(name: &'static str, items: Vec<T>) -> Self {
        NonEmpty(name, items)
    }

    /// A list the command allows to be left out, none rather than empty.
    pub fn optional(name: &'static str, items: Option<Vec<T>>) -> Option<Self> {
        items.map(|items| NonEmpty(name, items))
    }

    pub fn len(&self) -> usize {
        self.1.len()
    }
}

impl<T: IntoCommand> IntoCommand for NonEmpty<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let NonEmpty(name, items) = self;
        if items.is_empty() {
            return Err(Error::Client(format!("{name} requires at least one item")));
        }
        items.into_command()
    }
}

/// The items preceded by how many there are, e.g. the keys of ZINTER, or the keys and values of
/// MSETEX, a pair counts as one.
pub struct NumKeys<T>(pub NonEmpty<T>);

impl<T: IntoCommand> IntoCommand for NumKeys<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        (self.0.len(), self.0).into_command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{words, words_error};

    #[test]
    fn strings() {
        assert_eq!(words("GET"), ["GET"]);
        assert_eq!(words("key".to_string()), ["key"]);
        assert_eq!(words(&"key".to_string()), ["key"]);
    }

    #[test]
    fn numbers() {
        assert_eq!(
            words((-1i32, -2i64, 3u8, 4u16, 5u32, 6u64, 7usize)),
            ["-1", "-2", "3", "4", "5", "6", "7"]
        );
        assert_eq!(words((1.5f64, 2f64, -0.25f64)), ["1.5", "2", "-0.25"]);
    }

    #[test]
    fn option() {
        assert_eq!(words(None::<&str>), Vec::<String>::new());
        assert_eq!(words(Some("value")), ["value"]);
    }

    #[test]
    fn lists() {
        assert_eq!(words(vec!["a", "b"]), ["a", "b"]);
        assert_eq!(words(Vec::<&str>::new()), Vec::<String>::new());
        assert_eq!(words(["a", "b"]), ["a", "b"]);
    }

    #[test]
    fn tuples() {
        assert_eq!(
            words(("SET", "key", "value", None::<&str>, Some(("EX", 10u64)))),
            ["SET", "key", "value", "EX", "10"]
        );
        assert_eq!(words(vec![("field", "value")]), ["field", "value"]);
    }

    #[test]
    fn an_error_fails_the_whole_command() {
        let command = ("DEL", NonEmpty::new("keys", Vec::<String>::new()));
        assert_eq!(words_error(command), "keys requires at least one item");
    }

    #[test]
    fn flag() {
        assert_eq!(words(Flag("FORCE", true)), ["FORCE"]);
        assert_eq!(words(Flag("FORCE", false)), Vec::<String>::new());
    }

    #[test]
    fn named_option() {
        assert_eq!(words(NamedOption("COUNT", Some(5i64))), ["COUNT", "5"]);
        assert_eq!(
            words(NamedOption("COUNT", None::<i64>)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn non_empty() {
        assert_eq!(words(NonEmpty::new("keys", vec!["a", "b"])), ["a", "b"]);
        assert_eq!(
            words_error(NonEmpty::new("keys", Vec::<String>::new())),
            "keys requires at least one item"
        );
    }

    #[test]
    fn non_empty_optional() {
        assert_eq!(
            words(NonEmpty::optional("args", None::<Vec<String>>)),
            Vec::<String>::new()
        );
        assert_eq!(words(NonEmpty::optional("args", Some(vec!["a"]))), ["a"]);
        assert_eq!(
            words_error(NonEmpty::optional("args", Some(Vec::<String>::new()))),
            "args requires at least one item"
        );
    }

    #[test]
    fn num_keys() {
        assert_eq!(
            words(NumKeys(NonEmpty::new("keys", vec!["a", "b"]))),
            ["2", "a", "b"]
        );
        // a pair counts as one
        assert_eq!(
            words(NumKeys(NonEmpty::new(
                "keys",
                vec![("k1", "v1"), ("k2", "v2")]
            ))),
            ["2", "k1", "v1", "k2", "v2"]
        );
        assert_eq!(
            words_error(NumKeys(NonEmpty::new("keys", Vec::<String>::new()))),
            "keys requires at least one item"
        );
    }
}
