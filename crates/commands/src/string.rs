//! `string-commands`, Valkey's @string ACL category.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::string_commands::Guest;
use crate::reply::{call, integer, ok, optional_string};

impl Guest for crate::Commands {
    async fn get(connection: &Connection, key: String) -> Result<Option<String>, Error> {
        // https://valkey.io/commands/get/
        // GET key
        optional_string(call(connection, ["GET".to_string(), key]).await?)
    }

    async fn incr(connection: &Connection, key: String) -> Result<i64, Error> {
        // https://valkey.io/commands/incr/
        // INCR key
        integer(call(connection, ["INCR".to_string(), key]).await?)
    }

    async fn incrby(connection: &Connection, key: String, increment: i64) -> Result<i64, Error> {
        // https://valkey.io/commands/incrby/
        // INCRBY key increment
        integer(
            call(
                connection,
                ["INCRBY".to_string(), key, increment.to_string()],
            )
            .await?,
        )
    }

    async fn set(connection: &Connection, key: String, value: String) -> Result<(), Error> {
        // https://valkey.io/commands/set/
        // SET key value
        ok(call(connection, ["SET".to_string(), key, value]).await?)
    }
}
