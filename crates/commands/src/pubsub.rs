//! `pubsub-commands`, Valkey's @pubsub ACL category.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::pubsub_commands::Guest;
use crate::reply::{call, counts, integer, send, strings};
use wit_bindgen::rt::Map;

impl Guest for crate::Commands {
    #[doc = " Subscribes the client to the given patterns."]
    #[doc = ""]
    #[doc = " PSUBSCRIBE pattern [ pattern ... ]"]
    #[doc = " https://valkey.io/commands/psubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn psubscribe(connection: &Connection, patterns: Vec<String>) -> Result<(), Error> {
        let mut command = vec!["PSUBSCRIBE".to_string()];
        command.extend(patterns);
        send(connection, command).await
    }

    #[doc = " Posts a message to the given channel."]
    #[doc = ""]
    #[doc = " PUBLISH channel message"]
    #[doc = " https://valkey.io/commands/publish/"]
    #[allow(async_fn_in_trait)]
    async fn publish(
        connection: &Connection,
        channel: String,
        message: String,
    ) -> Result<i64, Error> {
        let command = ["PUBLISH".to_string(), channel, message];
        let response = call(connection, command).await?;
        integer(response)
    }

    #[doc = " Lists the currently active channels."]
    #[doc = ""]
    #[doc = " PUBSUB CHANNELS [ pattern ]"]
    #[doc = " https://valkey.io/commands/pubsub-channels/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_channels(
        connection: &Connection,
        pattern: Option<String>,
    ) -> Result<Vec<String>, Error> {
        let mut command = vec!["PUBSUB".to_string(), "CHANNELS".to_string()];
        command.extend(pattern);
        let response = call(connection, command).await?;
        strings(response)
    }

    #[doc = " Returns the number of unique patterns that are subscribed to by clients."]
    #[doc = ""]
    #[doc = " PUBSUB NUMPAT"]
    #[doc = " https://valkey.io/commands/pubsub-numpat/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numpat(connection: &Connection) -> Result<i64, Error> {
        let command = ["PUBSUB".to_string(), "NUMPAT".to_string()];
        let response = call(connection, command).await?;
        integer(response)
    }

    #[doc = " Returns the number of subscribers (exclusive of clients subscribed to patterns) for the specified channels."]
    #[doc = ""]
    #[doc = " PUBSUB NUMSUB [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/pubsub-numsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numsub(
        connection: &Connection,
        channels: Vec<String>,
    ) -> Result<Map<String, i64>, Error> {
        let mut command = vec!["PUBSUB".to_string(), "NUMSUB".to_string()];
        command.extend(channels);
        let response = call(connection, command).await?;
        counts(response)
    }

    #[doc = " Lists the currently active shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDCHANNELS [ pattern ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardchannels/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardchannels(
        connection: &Connection,
        pattern: Option<String>,
    ) -> Result<Vec<String>, Error> {
        let mut command = vec!["PUBSUB".to_string(), "SHARDCHANNELS".to_string()];
        command.extend(pattern);
        let response = call(connection, command).await?;
        strings(response)
    }

    #[doc = " Returns the number of subscribers for the specified shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDNUMSUB [ shardchannel [ shardchannel ... ] ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardnumsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardnumsub(
        connection: &Connection,
        shardchannels: Vec<String>,
    ) -> Result<Map<String, i64>, Error> {
        let mut command = vec!["PUBSUB".to_string(), "SHARDNUMSUB".to_string()];
        command.extend(shardchannels);
        let response = call(connection, command).await?;
        counts(response)
    }

    #[doc = " Unsubscribes the client from the given patterns, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " PUNSUBSCRIBE [ pattern [ pattern ... ] ]"]
    #[doc = " https://valkey.io/commands/punsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn punsubscribe(connection: &Connection, patterns: Vec<String>) -> Result<(), Error> {
        let mut command = vec!["PUNSUBSCRIBE".to_string()];
        command.extend(patterns);
        send(connection, command).await
    }

    #[doc = " Posts a message to the given shard channel."]
    #[doc = ""]
    #[doc = " SPUBLISH shardchannel message"]
    #[doc = " https://valkey.io/commands/spublish/"]
    #[allow(async_fn_in_trait)]
    async fn spublish(
        connection: &Connection,
        shardchannel: String,
        message: String,
    ) -> Result<i64, Error> {
        let command = vec!["SSUBSCRIBE".to_string(), shardchannel, message];
        let response = call(connection, command).await?;
        integer(response)
    }

    #[doc = " Subscribes the client to the specified shard channels."]
    #[doc = ""]
    #[doc = " SSUBSCRIBE shardchannel [ shardchannel ... ]"]
    #[doc = " https://valkey.io/commands/ssubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn ssubscribe(connection: &Connection, shardchannels: Vec<String>) -> Result<(), Error> {
        let mut command = vec!["SSUBSCRIBE".to_string()];
        command.extend(shardchannels);
        send(connection, command).await
    }

    #[doc = " Subscribes the client to the specified channels."]
    #[doc = ""]
    #[doc = " SUBSCRIBE channel [ channel ... ]"]
    #[doc = " https://valkey.io/commands/subscribe/"]
    #[allow(async_fn_in_trait)]
    async fn subscribe(connection: &Connection, channels: Vec<String>) -> Result<(), Error> {
        let mut command = vec!["SUBSCRIBE".to_string()];
        command.extend(channels);
        send(connection, command).await
    }

    #[doc = " Unsubscribes the client from the given shard channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " SUNSUBSCRIBE [ shardchannel ] [ [ shardchannel ] ... ]"]
    #[doc = " https://valkey.io/commands/sunsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn sunsubscribe(
        connection: &Connection,
        shardchannels: Vec<String>,
    ) -> Result<(), Error> {
        let mut command = vec!["SUNSUBSCRIBE".to_string()];
        command.extend(shardchannels);
        send(connection, command).await
    }

    #[doc = " Unsubscribes the client from the given channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " UNSUBSCRIBE [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/unsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn unsubscribe(connection: &Connection, channels: Vec<String>) -> Result<(), Error> {
        let mut command = vec!["UNSUBSCRIBE".to_string()];
        command.extend(channels);
        send(connection, command).await
    }
}
