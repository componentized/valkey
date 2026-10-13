//! The keyspace commands: MIGRATE, SCAN, SORT, TYPE and the like.

use crate::command::{Flag, IntoCommand, NamedOption, NonEmpty};
use crate::types::{Error, KeyType, MigrateAuth, RespValue, SortOpts};
use crate::value::{FromValue, unexpected};

/// The name of the type, e.g. for SCAN's TYPE.
impl IntoCommand for KeyType {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            KeyType::String => "string".to_string(),
            KeyType::List => "list".to_string(),
            KeyType::Set => "set".to_string(),
            KeyType::Zset => "zset".to_string(),
            KeyType::Hash => "hash".to_string(),
            KeyType::Stream => "stream".to_string(),
            KeyType::Other(name) => name,
        }
        .into_command()
    }
}

/// The type of a key, by name, none for `none`, a key that does not exist.
pub struct OptionalKeyType(pub Option<KeyType>);

impl FromValue for OptionalKeyType {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        // `none` rather than null, though the reply schema has null
        let name: Option<String> = FromValue::from_value(value)?;
        Ok(OptionalKeyType(match name.as_deref() {
            None | Some("none") => None,
            Some("string") => Some(KeyType::String),
            Some("list") => Some(KeyType::List),
            Some("set") => Some(KeyType::Set),
            Some("zset") => Some(KeyType::Zset),
            Some("hash") => Some(KeyType::Hash),
            Some("stream") => Some(KeyType::Stream),
            Some(_) => name.map(KeyType::Other),
        }))
    }
}

impl IntoCommand for MigrateAuth {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            MigrateAuth::Password(password) => ("AUTH", password).into_command(),
            MigrateAuth::UsernamePassword((username, password)) => {
                ("AUTH2", username, password).into_command()
            }
        }
    }
}

/// `OK` when keys were moved, `NOKEY` when none of them exist.
pub struct Migrated(pub bool);

impl FromValue for Migrated {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match &value {
            RespValue::String(reply) if reply == "OK" => Ok(Migrated(true)),
            RespValue::String(reply) if reply == "NOKEY" => Ok(Migrated(false)),
            _ => Err(unexpected(&value)),
        }
    }
}

/// `[ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]`, also
/// `SortRoOpts`, the same type.
impl IntoCommand for SortOpts {
    fn into_command(self) -> Result<Vec<String>, Error> {
        // GET before each pattern
        let get = self.get.map(|patterns| {
            let patterns = patterns.into_iter().map(|pattern| ("GET", pattern));
            patterns.collect::<Vec<_>>()
        });
        (
            NamedOption("BY", self.by),
            self.limit,
            NonEmpty::optional("get", get),
            self.order,
            Flag("ALPHA", self.alpha),
        )
            .into_command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{bulk, from, from_error, ok, simple, words, words_error};
    use crate::types::{Limit, SortOrder};

    #[test]
    fn key_type() {
        assert_eq!(
            words(vec![
                KeyType::String,
                KeyType::List,
                KeyType::Set,
                KeyType::Zset,
                KeyType::Hash,
                KeyType::Stream,
                KeyType::Other("ReJSON-RL".to_string()),
            ]),
            [
                "string",
                "list",
                "set",
                "zset",
                "hash",
                "stream",
                "ReJSON-RL"
            ]
        );
    }

    #[test]
    fn optional_key_type() {
        let names = [
            ("string", KeyType::String),
            ("list", KeyType::List),
            ("set", KeyType::Set),
            ("zset", KeyType::Zset),
            ("hash", KeyType::Hash),
            ("stream", KeyType::Stream),
            ("ReJSON-RL", KeyType::Other("ReJSON-RL".to_string())),
        ];
        for (name, expected) in names {
            assert_eq!(from::<OptionalKeyType>(simple(name)).0, Some(expected));
        }
        assert_eq!(from::<OptionalKeyType>(simple("none")).0, None);
        assert_eq!(from::<OptionalKeyType>(RespValue::Null).0, None);
    }

    #[test]
    fn migrate_auth() {
        assert_eq!(
            words(MigrateAuth::Password("secret".to_string())),
            ["AUTH", "secret"]
        );
        assert_eq!(
            words(MigrateAuth::UsernamePassword((
                "user".to_string(),
                "secret".to_string()
            ))),
            ["AUTH2", "user", "secret"]
        );
    }

    #[test]
    fn migrated() {
        assert!(from::<Migrated>(ok()).0);
        assert!(!from::<Migrated>(simple("NOKEY")).0);
        assert_eq!(
            from_error::<Migrated>(bulk("OK")),
            r#"unexpected reply: RespValue::BulkString("OK")"#
        );
    }

    #[test]
    fn sort_opts() {
        let opts = SortOpts {
            by: Some("weight_*".to_string()),
            limit: Some(Limit {
                offset: 0,
                count: 10,
            }),
            get: Some(vec!["#".to_string(), "object_*".to_string()]),
            order: Some(SortOrder::Desc),
            alpha: true,
        };
        assert_eq!(
            words(opts),
            [
                "BY", "weight_*", "LIMIT", "0", "10", "GET", "#", "GET", "object_*", "DESC",
                "ALPHA"
            ]
        );
        let opts = SortOpts {
            by: None,
            limit: None,
            get: None,
            order: None,
            alpha: false,
        };
        assert_eq!(words(opts), Vec::<String>::new());
        let opts = SortOpts {
            by: None,
            limit: None,
            get: Some(vec![]),
            order: None,
            alpha: false,
        };
        assert_eq!(words_error(opts), "get requires at least one item");
    }
}
