//! `pubsub-commands`, Valkey's @pubsub ACL category.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::pubsub_commands::Guest;
use crate::reply::{call, integer};

impl Guest for crate::Commands {
    async fn publish(
        connection: &Connection,
        channel: String,
        message: String,
    ) -> Result<i64, Error> {
        // https://valkey.io/commands/publish/
        // PUBLISH channel message
        integer(call(connection, ["PUBLISH".to_string(), channel, message]).await?)
    }
}
