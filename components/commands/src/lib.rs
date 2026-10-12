use crate::command::{Flag, Milliseconds, NamedOption, Seconds, emit, query};
use crate::exports::componentized::valkey::commands::{
    Connection, Cursor, Double, Duration, Error, GetexTtl, Guest, Instant, Integer, LcsIdxReply,
    MsetexPrecondition, MsetexTtl, Rule, SetPrecondition, SetTtl, StreamId, StreamTrim, TtlMode,
    TtlResult, Value, XclaimOpts, XinfoConsumer, XinfoGroup, XinfoStreamReply,
    XinfoStreamReplyFull, XpendingExtendedEntry, XpendingSummary,
};
use crate::value::OrZeroId;

use wit_bindgen::rt::Map;

mod command;
mod value;

struct Commands;

impl Guest for Commands {
    #[doc = " admin commands"]
    #[doc = " Delete all the specified ACL users and terminate all the connections that are authenticated with such users."]
    #[doc = ""]
    #[doc = " ACL DELUSER username [ username ... ]"]
    #[doc = " https://valkey.io/commands/acl-deluser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_deluser(
        connection: &Connection,
        usernames: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("ACL", "DELUSER", usernames)).await
    }

    #[doc = " Generates a random password."]
    #[doc = ""]
    #[doc = " ACL GENPASS [ bits ]"]
    #[doc = " https://valkey.io/commands/acl-genpass/"]
    #[allow(async_fn_in_trait)]
    async fn acl_genpass(connection: &Connection, bits: Option<Integer>) -> Result<String, Error> {
        query(connection, ("ACL", "GENPASS", bits)).await
    }

    #[doc = " Create an ACL user with the specified rules or modify the rules of an existing user."]
    #[doc = ""]
    #[doc = " ACL SETUSER username [ rule ] [ [ rule ] ... ]"]
    #[doc = " https://valkey.io/commands/acl-setuser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_setuser(
        connection: &Connection,
        username: String,
        rules: Vec<Rule>,
    ) -> Result<(), Error> {
        query(connection, ("ACL", "SETUSER", username, rules)).await
    }

    #[doc = " Authenticates the current connection."]
    #[doc = ""]
    #[doc = " AUTH [ username ] password"]
    #[doc = " https://valkey.io/commands/auth/"]
    #[allow(async_fn_in_trait)]
    async fn auth(
        connection: &Connection,
        username: Option<String>,
        password: String,
    ) -> Result<(), Error> {
        query(connection, ("AUTH", username, password)).await
    }

    #[doc = " Switch to a different protocol, optionally authenticating and setting the connection\'s name, or provide a contextual client report."]
    #[doc = ""]
    #[doc = " RESP2 is not fully supported by this client."]
    #[doc = ""]
    #[doc = " HELLO 3 [ AUTH username password ] [ SETNAME clientname ]"]
    #[doc = " https://valkey.io/commands/hello/"]
    #[allow(async_fn_in_trait)]
    async fn hello(
        connection: &Connection,
        auth: Option<(String, String)>,
        client_name: Option<String>,
    ) -> Result<Map<String, Value>, Error> {
        query(
            connection,
            (
                "HELLO",
                "3",
                auth.map(|auth| ("AUTH", auth)),
                client_name.map(|name| ("SETNAME", name)),
            ),
        )
        .await
    }

    #[doc = " Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk."]
    #[doc = ""]
    #[doc = " PING [ message ]"]
    #[doc = " https://valkey.io/commands/ping/"]
    #[allow(async_fn_in_trait)]
    async fn ping(connection: &Connection, message: Option<String>) -> Result<(), Error> {
        let reply: String = query(connection, ("PING", message.clone())).await?;
        if reply == message.unwrap_or("PONG".to_string()) {
            Ok(())
        } else {
            Err(Error::Client(format!("unexpected reply: {reply:?}")))
        }
    }

    #[doc = " Removes the specified fields from the hash stored at key."]
    #[doc = ""]
    #[doc = " HDEL key field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hdel/"]
    #[allow(async_fn_in_trait)]
    async fn hdel(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("HDEL", key, fields)).await
    }

    #[doc = " Returns if field is an existing field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HEXISTS key field"]
    #[doc = " https://valkey.io/commands/hexists/"]
    #[allow(async_fn_in_trait)]
    async fn hexists(connection: &Connection, key: String, field: String) -> Result<bool, Error> {
        query(connection, ("HEXISTS", key, field)).await
    }

    #[doc = " Manipulate the expiration time for existing hash fields."]
    #[doc = ""]
    #[doc = " HEXPIRE key seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpire/"]
    #[allow(async_fn_in_trait)]
    async fn hexpire(
        connection: &Connection,
        key: String,
        seconds: Duration,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<TtlResult>, Error> {
        query(
            connection,
            (
                "HEXPIRE",
                key,
                Seconds(seconds),
                mode,
                "FIELDS",
                fields.len(),
                fields,
            ),
        )
        .await
    }

    #[doc = " Manipulate the expiration time for existing hash fields."]
    #[doc = ""]
    #[doc = " HEXPIREAT key unix-time-seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpireat/"]
    #[allow(async_fn_in_trait)]
    async fn hexpireat(
        connection: &Connection,
        key: String,
        seconds: Instant,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<TtlResult>, Error> {
        query(
            connection,
            (
                "HEXPIREAT",
                key,
                Seconds(seconds),
                mode,
                "FIELDS",
                fields.len(),
                fields,
            ),
        )
        .await
    }

    #[doc = " Returns the value associated with field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HGET key field"]
    #[doc = " https://valkey.io/commands/hget/"]
    #[allow(async_fn_in_trait)]
    async fn hget(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Option<String>, Error> {
        query(connection, ("HGET", key, field)).await
    }

    #[doc = " Returns all fields and values of the hash stored at key."]
    #[doc = ""]
    #[doc = " HGETALL key"]
    #[doc = " https://valkey.io/commands/hgetall/"]
    #[allow(async_fn_in_trait)]
    async fn hgetall(connection: &Connection, key: String) -> Result<Map<String, String>, Error> {
        query(connection, ("HGETALL", key)).await
    }

    #[doc = " Increments the number stored at field in the hash stored at key by increment."]
    #[doc = ""]
    #[doc = " HINCRBY key field increment"]
    #[doc = " https://valkey.io/commands/hincrby/"]
    #[allow(async_fn_in_trait)]
    async fn hincrby(
        connection: &Connection,
        key: String,
        field: String,
        increment: Integer,
    ) -> Result<Integer, Error> {
        query(connection, ("HINCRBY", key, field, increment)).await
    }

    #[doc = " Increment the specified field of a hash stored at key, and representing a floating point number, by the specified increment."]
    #[doc = ""]
    #[doc = " HINCRBYFLOAT key field increment"]
    #[doc = " https://valkey.io/commands/hincrbyfloat/"]
    #[allow(async_fn_in_trait)]
    async fn hincrbyfloat(
        connection: &Connection,
        key: String,
        field: String,
        increment: Double,
    ) -> Result<Double, Error> {
        query(connection, ("HINCRBYFLOAT", key, field, increment)).await
    }

    #[doc = " Returns all field names in the hash stored at key."]
    #[doc = ""]
    #[doc = " HKEYS key"]
    #[doc = " https://valkey.io/commands/hkeys/"]
    #[allow(async_fn_in_trait)]
    async fn hkeys(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        query(connection, ("HKEYS", key)).await
    }

    #[doc = " Returns the number of fields contained in the hash stored at key."]
    #[doc = ""]
    #[doc = " HLEN key"]
    #[doc = " https://valkey.io/commands/hlen/"]
    #[allow(async_fn_in_trait)]
    async fn hlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("HLEN", key)).await
    }

    #[doc = " Returns the values associated with the specified fields in the hash stored at key."]
    #[doc = ""]
    #[doc = " HMGET key field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hmget/"]
    #[allow(async_fn_in_trait)]
    async fn hmget(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(connection, ("HMGET", key, fields)).await
    }

    #[doc = " Sets the specified fields to their respective values in the hash stored at key."]
    #[doc = ""]
    #[doc = " HMSET key field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hmset/"]
    #[allow(async_fn_in_trait)]
    async fn hmset(
        connection: &Connection,
        key: String,
        field_and_values: Vec<(String, String)>,
    ) -> Result<(), Error> {
        query(connection, ("HMSET", key, field_and_values)).await
    }

    #[doc = " Random fields from the hash value stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one field, or none when the key does not exist. A negative count may return"]
    #[doc = " the same field more than once, its absolute value is the number of fields returned."]
    #[doc = ""]
    #[doc = " Also see `hrandfield-withvalues`."]
    #[doc = ""]
    #[doc = " HRANDFIELD key [ count ]"]
    #[doc = " https://valkey.io/commands/hrandfield/"]
    #[allow(async_fn_in_trait)]
    async fn hrandfield(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("HRANDFIELD", key, count)).await;
        }
        // without a count the reply is the field itself, or null
        let field: Option<String> = query(connection, ("HRANDFIELD", key)).await?;
        Ok(field.into_iter().collect())
    }

    #[doc = " Random fields and their values from the hash value stored at key, up to the count."]
    #[doc = ""]
    #[doc = " A negative count may return the same field more than once, its absolute value is the number of"]
    #[doc = " fields returned."]
    #[doc = ""]
    #[doc = " Also see `hrandfield`."]
    #[doc = ""]
    #[doc = " HRANDFIELD key count WITHVALUES"]
    #[doc = " https://valkey.io/commands/hrandfield/"]
    #[allow(async_fn_in_trait)]
    async fn hrandfield_withvalues(
        connection: &Connection,
        key: String,
        count: Integer,
    ) -> Result<Vec<(String, String)>, Error> {
        query(connection, ("HRANDFIELD", key, count, "WITHVALUES")).await
    }

    #[doc = " Incrementally iterate over a collection of elements."]
    #[doc = ""]
    #[doc = " See also `hscan-novalues`."]
    #[doc = ""]
    #[doc = " HSCAN key cursor [ MATCH pattern ] [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/hscan/"]
    #[allow(async_fn_in_trait)]
    async fn hscan(
        connection: &Connection,
        key: String,
        cursor: Option<Cursor>,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Cursor, Map<String, String>), Error> {
        query(
            connection,
            (
                "HSCAN",
                key,
                cursor.unwrap_or(Cursor::from("0")),
                match_.map(|m| ("MATCH", m)),
                count.map(|c| ("COUNT", c)),
            ),
        )
        .await
    }

    #[doc = " Incrementally iterate over a collection of keys."]
    #[doc = ""]
    #[doc = " See also `hscan`."]
    #[doc = ""]
    #[doc = " HSCAN key cursor [ MATCH pattern ] [ COUNT count ] NOVALUES"]
    #[doc = " https://valkey.io/commands/hscan/"]
    #[allow(async_fn_in_trait)]
    async fn hscan_novalues(
        connection: &Connection,
        key: String,
        cursor: Option<Cursor>,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Cursor, Vec<String>), Error> {
        query(
            connection,
            (
                "HSCAN",
                key,
                cursor.unwrap_or(Cursor::from("0")),
                match_.map(|m| ("MATCH", m)),
                count.map(|c| ("COUNT", c)),
                "NOVALUES",
            ),
        )
        .await
    }

    #[doc = " Sets the specified fields to their respective values in the hash stored at key."]
    #[doc = ""]
    #[doc = " HSET key field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hset/"]
    #[allow(async_fn_in_trait)]
    async fn hset(
        connection: &Connection,
        key: String,
        field_and_values: Vec<(String, String)>,
    ) -> Result<Integer, Error> {
        query(connection, ("HSET", key, field_and_values)).await
    }

    #[doc = " Sets field in the hash stored at key to value, only if field does not yet exist."]
    #[doc = ""]
    #[doc = " HSETNX key field value"]
    #[doc = " https://valkey.io/commands/hsetnx/"]
    #[allow(async_fn_in_trait)]
    async fn hsetnx(
        connection: &Connection,
        key: String,
        field: String,
        value: String,
    ) -> Result<bool, Error> {
        query(connection, ("HSETNX", key, field, value)).await
    }

    #[doc = " Returns the string length of the value associated with field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HSTRLEN key field"]
    #[doc = " https://valkey.io/commands/hstrlen/"]
    #[allow(async_fn_in_trait)]
    async fn hstrlen(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Integer, Error> {
        query(connection, ("HSTRLEN", key, field)).await
    }

    #[doc = " All values in the hash stored at key."]
    #[doc = ""]
    #[doc = " HVALS key"]
    #[doc = " https://valkey.io/commands/hvals/"]
    #[allow(async_fn_in_trait)]
    async fn hvals(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        query(connection, ("HVALS", key)).await
    }

    #[doc = " Removes the specified keys."]
    #[doc = ""]
    #[doc = " DEL key [ key ... ]"]
    #[doc = " https://valkey.io/commands/del/"]
    #[allow(async_fn_in_trait)]
    async fn del(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("DEL", keys)).await
    }

    #[doc = " If key exists."]
    #[doc = ""]
    #[doc = " EXISTS key [ key ... ]"]
    #[doc = " https://valkey.io/commands/exists/"]
    #[allow(async_fn_in_trait)]
    async fn exists(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("EXISTS", keys)).await
    }

    #[doc = " Set a timeout on key."]
    #[doc = ""]
    #[doc = " EXPIRE key seconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/expire/"]
    #[allow(async_fn_in_trait)]
    async fn expire(
        connection: &Connection,
        key: String,
        seconds: Duration,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        query(connection, ("EXPIRE", key, seconds, mode)).await
    }

    #[doc = " Set a timeout on key."]
    #[doc = ""]
    #[doc = " EXPIREAT key unix-time-seconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/expireat/"]
    #[allow(async_fn_in_trait)]
    async fn expireat(
        connection: &Connection,
        key: String,
        seconds: Instant,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        query(connection, ("EXPIREAT", key, Seconds(seconds), mode)).await
    }

    #[doc = " Returns all keys matching pattern."]
    #[doc = ""]
    #[doc = " KEYS pattern"]
    #[doc = " https://valkey.io/commands/keys/"]
    #[allow(async_fn_in_trait)]
    async fn keys(connection: &Connection, pattern: String) -> Result<Vec<String>, Error> {
        query(connection, ("KEYS", pattern)).await
    }

    #[doc = " Subscribes the client to the given patterns."]
    #[doc = ""]
    #[doc = " PSUBSCRIBE pattern [ pattern ... ]"]
    #[doc = " https://valkey.io/commands/psubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn psubscribe(connection: &Connection, patterns: Vec<String>) -> Result<(), Error> {
        emit(connection, ("PSUBSCRIBE", patterns)).await
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
    ) -> Result<Integer, Error> {
        query(connection, ("PUBLISH", channel, message)).await
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
        query(connection, ("PUBSUB", "CHANNELS", pattern)).await
    }

    #[doc = " Returns the number of unique patterns that are subscribed to by clients."]
    #[doc = ""]
    #[doc = " PUBSUB NUMPAT"]
    #[doc = " https://valkey.io/commands/pubsub-numpat/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numpat(connection: &Connection) -> Result<Integer, Error> {
        query(connection, ("PUBSUB", "NUMPAT")).await
    }

    #[doc = " Returns the number of subscribers (exclusive of clients subscribed to patterns) for the specified channels."]
    #[doc = ""]
    #[doc = " PUBSUB NUMSUB [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/pubsub-numsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numsub(
        connection: &Connection,
        channels: Vec<String>,
    ) -> Result<Map<String, Integer>, Error> {
        query(connection, ("PUBSUB", "NUMSUB", channels)).await
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
        query(connection, ("PUBSUB", "SHARDCHANNELS", pattern)).await
    }

    #[doc = " Returns the number of subscribers for the specified shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDNUMSUB [ shardchannel [ shardchannel ... ] ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardnumsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardnumsub(
        connection: &Connection,
        shardchannels: Vec<String>,
    ) -> Result<Map<String, Integer>, Error> {
        query(connection, ("PUBSUB", "SHARDNUMSUB", shardchannels)).await
    }

    #[doc = " Unsubscribes the client from the given patterns, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " PUNSUBSCRIBE [ pattern [ pattern ... ] ]"]
    #[doc = " https://valkey.io/commands/punsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn punsubscribe(connection: &Connection, patterns: Vec<String>) -> Result<(), Error> {
        emit(connection, ("PUNSUBSCRIBE", patterns)).await
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
    ) -> Result<Integer, Error> {
        query(connection, ("SPUBLISH", shardchannel, message)).await
    }

    #[doc = " Subscribes the client to the specified shard channels."]
    #[doc = ""]
    #[doc = " SSUBSCRIBE shardchannel [ shardchannel ... ]"]
    #[doc = " https://valkey.io/commands/ssubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn ssubscribe(connection: &Connection, shardchannels: Vec<String>) -> Result<(), Error> {
        emit(connection, ("SSUBSCRIBE", shardchannels)).await
    }

    #[doc = " Subscribes the client to the specified channels."]
    #[doc = ""]
    #[doc = " SUBSCRIBE channel [ channel ... ]"]
    #[doc = " https://valkey.io/commands/subscribe/"]
    #[allow(async_fn_in_trait)]
    async fn subscribe(connection: &Connection, channels: Vec<String>) -> Result<(), Error> {
        emit(connection, ("SUBSCRIBE", channels)).await
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
        emit(connection, ("SUNSUBSCRIBE", shardchannels)).await
    }

    #[doc = " Unsubscribes the client from the given channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " UNSUBSCRIBE [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/unsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn unsubscribe(connection: &Connection, channels: Vec<String>) -> Result<(), Error> {
        emit(connection, ("UNSUBSCRIBE", channels)).await
    }

    #[doc = " Appends the value at the end of the string."]
    #[doc = ""]
    #[doc = " APPEND key value"]
    #[doc = " https://valkey.io/commands/append/"]
    #[allow(async_fn_in_trait)]
    async fn append(connection: &Connection, key: String, value: String) -> Result<Integer, Error> {
        query(connection, ("APPEND", key, value)).await
    }

    #[doc = " Decrements the number stored at key by one."]
    #[doc = ""]
    #[doc = " DECR key"]
    #[doc = " https://valkey.io/commands/decr/"]
    #[allow(async_fn_in_trait)]
    async fn decr(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("DECR", key)).await
    }

    #[doc = " Decrements the number stored at key by decrement."]
    #[doc = ""]
    #[doc = " DECRBY key decrement"]
    #[doc = " https://valkey.io/commands/decrby/"]
    #[allow(async_fn_in_trait)]
    async fn decrby(
        connection: &Connection,
        key: String,
        decrement: Integer,
    ) -> Result<Integer, Error> {
        query(connection, ("DECRBY", key, decrement)).await
    }

    #[doc = " Deletes the key if its value matches the given string."]
    #[doc = ""]
    #[doc = " DELIFEQ key value"]
    #[doc = " https://valkey.io/commands/delifeq/"]
    #[allow(async_fn_in_trait)]
    async fn delifeq(connection: &Connection, key: String, value: String) -> Result<bool, Error> {
        query(connection, ("DELIFEQ", key, value)).await
    }

    #[doc = " Get the value of key."]
    #[doc = ""]
    #[doc = " GET key"]
    #[doc = " https://valkey.io/commands/get/"]
    #[allow(async_fn_in_trait)]
    async fn get(connection: &Connection, key: String) -> Result<Option<String>, Error> {
        query(connection, ("GET", key)).await
    }

    #[doc = " Get the value of key and delete the key."]
    #[doc = ""]
    #[doc = " GETDEL key"]
    #[doc = " https://valkey.io/commands/getdel/"]
    #[allow(async_fn_in_trait)]
    async fn getdel(connection: &Connection, key: String) -> Result<Option<String>, Error> {
        query(connection, ("GETDEL", key)).await
    }

    #[doc = " Get the value of key and optionally set its expiration."]
    #[doc = ""]
    #[doc = " GETEX key [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST ]"]
    #[doc = " https://valkey.io/commands/getex/"]
    #[allow(async_fn_in_trait)]
    async fn getex(
        connection: &Connection,
        key: String,
        ttl: Option<GetexTtl>,
    ) -> Result<Option<String>, Error> {
        query(connection, ("GETEX", key, ttl)).await
    }

    #[doc = " Returns the substring of the string value stored at key, determined by the offsets start and end."]
    #[doc = ""]
    #[doc = " GETRANGE key start end"]
    #[doc = " https://valkey.io/commands/getrange/"]
    #[allow(async_fn_in_trait)]
    async fn getrange(
        connection: &Connection,
        key: String,
        start: Integer,
        end: Integer,
    ) -> Result<String, Error> {
        query(connection, ("GETRANGE", key, start, end)).await
    }

    #[doc = " Atomically sets key to value and returns the old value stored at key."]
    #[doc = ""]
    #[doc = " GETSET key value"]
    #[doc = " https://valkey.io/commands/getset/"]
    #[allow(async_fn_in_trait)]
    async fn getset(
        connection: &Connection,
        key: String,
        value: String,
    ) -> Result<Option<String>, Error> {
        query(connection, ("GETSET", key, value)).await
    }

    #[doc = " Increments the number stored at key by one."]
    #[doc = ""]
    #[doc = " INCR key"]
    #[doc = " https://valkey.io/commands/incr/"]
    #[allow(async_fn_in_trait)]
    async fn incr(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("INCR", key)).await
    }

    #[doc = " Increments the number stored at key by increment."]
    #[doc = ""]
    #[doc = " INCRBY key increment"]
    #[doc = " https://valkey.io/commands/incrby/"]
    #[allow(async_fn_in_trait)]
    async fn incrby(
        connection: &Connection,
        key: String,
        increment: Integer,
    ) -> Result<Integer, Error> {
        query(connection, ("INCRBY", key, increment)).await
    }

    #[doc = " Increment the string representing a floating point number stored at key by the specified increment."]
    #[doc = ""]
    #[doc = " INCRBYFLOAT key increment"]
    #[doc = " https://valkey.io/commands/incrbyfloat/"]
    #[allow(async_fn_in_trait)]
    async fn incrbyfloat(
        connection: &Connection,
        key: String,
        increment: Double,
    ) -> Result<Double, Error> {
        query(connection, ("INCRBYFLOAT", key, increment)).await
    }

    #[doc = " The LCS command implements the longest common subsequence algorithm."]
    #[doc = ""]
    #[doc = " See also `lcs-len` and `lcs-idx`"]
    #[doc = ""]
    #[doc = " LCS key1 key2"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs(connection: &Connection, key1: String, key2: String) -> Result<String, Error> {
        query(connection, ("LCS", key1, key2)).await
    }

    #[doc = " The LCS command implements the longest common subsequence algorithm."]
    #[doc = ""]
    #[doc = " See also `lcs` and `lcs-idx`"]
    #[doc = ""]
    #[doc = " LCS key1 key2 LEN"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs_len(
        connection: &Connection,
        key1: String,
        key2: String,
    ) -> Result<Integer, Error> {
        query(connection, ("LCS", key1, key2, "LEN")).await
    }

    #[doc = " The LCS command implements the longest common subsequence algorithm."]
    #[doc = ""]
    #[doc = " See also `lcs` and `lcs-len`"]
    #[doc = ""]
    #[doc = " LCS key1 key2 IDX [ MINMATCHLEN min-match-len ] [ WITHMATCHLEN ]"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs_idx(
        connection: &Connection,
        key1: String,
        key2: String,
        min_match_len: Option<Integer>,
        with_match_len: bool,
    ) -> Result<LcsIdxReply, Error> {
        query(
            connection,
            (
                "LCS",
                key1,
                key2,
                "IDX",
                min_match_len.map(|len| ("MINMATCHLEN", len)),
                Flag("WITHMATCHLEN", with_match_len),
            ),
        )
        .await
    }

    #[doc = " Returns the values of all specified keys."]
    #[doc = ""]
    #[doc = " MGET key [ key ... ]"]
    #[doc = " https://valkey.io/commands/mget/"]
    #[allow(async_fn_in_trait)]
    async fn mget(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(connection, ("MGET", keys)).await
    }

    #[doc = " Sets the given keys to their respective values."]
    #[doc = ""]
    #[doc = " MSET key value [ key value ... ]"]
    #[doc = " https://valkey.io/commands/mset/"]
    #[allow(async_fn_in_trait)]
    async fn mset(
        connection: &Connection,
        keys_and_values: Vec<(String, String)>,
    ) -> Result<(), Error> {
        query(connection, ("MSET", keys_and_values)).await
    }

    #[doc = " Sets multiple key-value pairs while also assigning a shared expiration time to all the keys atomically."]
    #[doc = ""]
    #[doc = " MSETEX numkeys key value [ key value ... ] [ NX | XX ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/msetex/"]
    #[allow(async_fn_in_trait)]
    async fn msetex(
        connection: &Connection,
        keys_and_values: Vec<(String, String)>,
        precondition: Option<MsetexPrecondition>,
        ttl: Option<MsetexTtl>,
    ) -> Result<bool, Error> {
        query(
            connection,
            (
                "MSETEX",
                keys_and_values.len(),
                keys_and_values,
                precondition,
                ttl,
            ),
        )
        .await
    }

    #[doc = " Sets the given keys to their respective values."]
    #[doc = ""]
    #[doc = " MSETNX key value [ key value ... ]"]
    #[doc = " https://valkey.io/commands/msetnx/"]
    #[allow(async_fn_in_trait)]
    async fn msetnx(
        connection: &Connection,
        keys_and_values: Vec<(String, String)>,
    ) -> Result<bool, Error> {
        query(connection, ("MSETNX", keys_and_values)).await
    }

    #[doc = " Set key to hold the string value with expiration in milliseconds."]
    #[doc = ""]
    #[doc = " PSETEX key milliseconds value"]
    #[doc = " https://valkey.io/commands/psetex/"]
    #[allow(async_fn_in_trait)]
    async fn psetex(
        connection: &Connection,
        key: String,
        milliseconds: Duration,
        value: String,
    ) -> Result<(), Error> {
        query(
            connection,
            ("PSETEX", key, Milliseconds(milliseconds), value),
        )
        .await
    }

    #[doc = " Set key to hold the string value."]
    #[doc = ""]
    #[doc = " Returns false when the precondition is not met, the key is unchanged."]
    #[doc = ""]
    #[doc = " SET key value [ NX | XX | IFEQ comparison-value | IFNE comparison-not-equal ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/set/"]
    #[allow(async_fn_in_trait)]
    async fn set(
        connection: &Connection,
        key: String,
        value: String,
        precondition: Option<SetPrecondition>,
        ttl: Option<SetTtl>,
    ) -> Result<bool, Error> {
        // OK once set, null when the precondition is not met
        let reply: Option<()> = query(connection, ("SET", key, value, precondition, ttl)).await?;
        Ok(reply.is_some())
    }

    #[doc = " Set key to hold the string value. Returning the previous value."]
    #[doc = ""]
    #[doc = " SET key value [ NX | XX | IFEQ comparison-value | IFNE comparison-not-equal ] GET [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/set/"]
    #[allow(async_fn_in_trait)]
    async fn set_get(
        connection: &Connection,
        key: String,
        value: String,
        precondition: Option<SetPrecondition>,
        ttl: Option<SetTtl>,
    ) -> Result<Option<String>, Error> {
        query(connection, ("SET", key, value, precondition, "GET", ttl)).await
    }

    #[doc = " Set key to hold the string value with expiration in seconds."]
    #[doc = ""]
    #[doc = " SETEX key seconds value"]
    #[doc = " https://valkey.io/commands/setex/"]
    #[allow(async_fn_in_trait)]
    async fn setex(
        connection: &Connection,
        key: String,
        seconds: Duration,
        value: String,
    ) -> Result<(), Error> {
        query(connection, ("SETEX", key, seconds, value)).await
    }

    #[doc = " Set key to hold string value if key does not exist."]
    #[doc = ""]
    #[doc = " SETNX key value"]
    #[doc = " https://valkey.io/commands/setnx/"]
    #[allow(async_fn_in_trait)]
    async fn setnx(connection: &Connection, key: String, value: String) -> Result<bool, Error> {
        query(connection, ("SETNX", key, value)).await
    }

    #[doc = " Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value."]
    #[doc = ""]
    #[doc = " SETRANGE key offset value"]
    #[doc = " https://valkey.io/commands/setrange/"]
    #[allow(async_fn_in_trait)]
    async fn setrange(
        connection: &Connection,
        key: String,
        offset: u32,
        value: String,
    ) -> Result<Integer, Error> {
        query(connection, ("SETRANGE", key, offset, value)).await
    }

    #[doc = " Returns the length of the string value stored at key."]
    #[doc = ""]
    #[doc = " STRLEN key"]
    #[doc = " hthttps://valkey.io/commands/strlen/"]
    #[allow(async_fn_in_trait)]
    async fn strlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("STRLEN", key)).await
    }

    #[doc = " Returns the substring of the string value stored at key, determined by the offsets start and end."]
    #[doc = ""]
    #[doc = " SUBSTR key start end"]
    #[doc = " https://valkey.io/commands/substr/"]
    #[allow(async_fn_in_trait)]
    async fn substr(
        connection: &Connection,
        key: String,
        start: Integer,
        end: Integer,
    ) -> Result<String, Error> {
        query(connection, ("SUBSTR", key, start, end)).await
    }

    #[doc = " Removes one or multiple messages from the Pending Entries List (PEL) of a stream consumer group."]
    #[doc = ""]
    #[doc = " XACK key group ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xack/"]
    #[allow(async_fn_in_trait)]
    async fn xack(
        connection: &Connection,
        key: String,
        group: String,
        ids: Vec<StreamId>,
    ) -> Result<Integer, Error> {
        query(connection, ("XACK", key, group, ids)).await
    }

    #[doc = " Appends the specified stream entry to the stream at the specified key."]
    #[doc = ""]
    #[doc = " XADD key [ NOMKSTREAM ] [ 〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ] ] 〈 * | id 〉 field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/xadd/"]
    #[allow(async_fn_in_trait)]
    async fn xadd(
        connection: &Connection,
        key: String,
        nomkstream: bool,
        trim: Option<StreamTrim>,
        id: StreamId,
        field_and_values: Vec<(String, String)>,
    ) -> Result<Option<StreamId>, Error> {
        query(
            connection,
            (
                "XADD",
                key,
                Flag("NOMKSTREAM", nomkstream),
                trim,
                id,
                field_and_values,
            ),
        )
        .await
    }

    #[doc = " Transfers ownership of pending stream entries that match the specified criteria."]
    #[doc = ""]
    #[doc = " See also `xautoclaim-justid`"]
    #[doc = ""]
    #[doc = " XAUTOCLAIM key group consumer min-idle-time start [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xautoclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xautoclaim(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: Duration,
        start: Option<StreamId>,
        count: Option<Integer>,
    ) -> Result<
        (
            Option<StreamId>,
            Vec<(StreamId, Map<String, String>)>,
            Vec<StreamId>,
        ),
        Error,
    > {
        let (next, claimed, deleted): (OrZeroId, _, _) = query(
            connection,
            (
                "XAUTOCLAIM",
                key,
                group,
                consumer,
                Milliseconds(min_idle_time),
                OrZeroId(start),
                NamedOption("COUNT", count),
            ),
        )
        .await?;
        Ok((next.0, claimed, deleted))
    }

    #[doc = " Transfers ownership of pending stream entries that match the specified criteria."]
    #[doc = ""]
    #[doc = " See also `xautoclaim`"]
    #[doc = ""]
    #[doc = " XAUTOCLAIM key group consumer min-idle-time start [ COUNT count ] JUSTID"]
    #[doc = " https://valkey.io/commands/xautoclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xautoclaim_justid(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: Duration,
        start: Option<StreamId>,
        count: Option<Integer>,
    ) -> Result<(Option<StreamId>, Vec<StreamId>, Vec<StreamId>), Error> {
        let (next, claimed, deleted): (OrZeroId, _, _) = query(
            connection,
            (
                "XAUTOCLAIM",
                key,
                group,
                consumer,
                Milliseconds(min_idle_time),
                OrZeroId(start),
                NamedOption("COUNT", count),
                "JUSTID",
            ),
        )
        .await?;
        Ok((next.0, claimed, deleted))
    }

    #[doc = " Changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument."]
    #[doc = ""]
    #[doc = " Also see `xclaim-justid`"]
    #[doc = ""]
    #[doc = " XCLAIM key group consumer min-idle-time ID [ ID ... ] [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] [ LASTID lastid ]"]
    #[doc = " https://valkey.io/commands/xclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xclaim(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: Duration,
        ids: Vec<StreamId>,
        opts: Option<XclaimOpts>,
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
            connection,
            ("XCLAIM", key, group, consumer, min_idle_time, ids, opts),
        )
        .await
    }

    #[doc = " Changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument."]
    #[doc = ""]
    #[doc = " Also see `xclaim`"]
    #[doc = ""]
    #[doc = " XCLAIM key group consumer min-idle-time ID [ ID ... ] [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] JUSTID [ LASTID lastid ]"]
    #[doc = " https://valkey.io/commands/xclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xclaim_justid(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: Duration,
        ids: Vec<StreamId>,
        opts: Option<XclaimOpts>,
    ) -> Result<Vec<StreamId>, Error> {
        query(
            connection,
            ("XCLAIM", key, group, consumer, min_idle_time, ids, opts),
        )
        .await
    }

    #[doc = " Removes the specified entries from a stream, and returns the number of entries deleted."]
    #[doc = ""]
    #[doc = " XDEL key ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xdel/"]
    #[allow(async_fn_in_trait)]
    async fn xdel(
        connection: &Connection,
        key: String,
        ids: Vec<StreamId>,
    ) -> Result<Integer, Error> {
        query(connection, ("XDEL", key, ids)).await
    }

    #[doc = " Create a new consumer group uniquely identified by <groupname> for the stream stored at <key>."]
    #[doc = ""]
    #[doc = " XGROUP CREATE key group 〈 id | $ 〉 [ MKSTREAM ] [ ENTRIESREAD entriesread ]"]
    #[doc = " https://valkey.io/commands/xgroup-create/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_create(
        connection: &Connection,
        key: String,
        group: String,
        id: StreamId,
        mkstream: bool,
        entriesread: Option<Integer>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "XGROUP",
                "CREATE",
                key,
                group,
                id,
                Flag("MKSTREAM", mkstream),
                NamedOption("ENTRIESREAD", entriesread),
            ),
        )
        .await
    }

    #[doc = " Create a consumer named <consumername> in the consumer group <groupname> of the stream that\'s stored at <key>."]
    #[doc = ""]
    #[doc = " XGROUP CREATECONSUMER key group consumer"]
    #[doc = " https://valkey.io/commands/xgroup-createconsumer/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_createconsumer(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
    ) -> Result<bool, Error> {
        query(
            connection,
            ("XGROUP", "CREATECONSUMER", key, group, consumer),
        )
        .await
    }

    #[doc = " The XGROUP DELCONSUMER command deletes a consumer from the consumer group."]
    #[doc = ""]
    #[doc = " XGROUP DELCONSUMER key group consumer"]
    #[doc = " https://valkey.io/commands/xgroup-delconsumer/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_delconsumer(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
    ) -> Result<Integer, Error> {
        query(connection, ("XGROUP", "DELCONSUMER", key, group, consumer)).await
    }

    #[doc = " Completely destroys the consumer group."]
    #[doc = ""]
    #[doc = " XGROUP DESTROY key group"]
    #[doc = " https://valkey.io/commands/xgroup-destroy/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_destroy(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<bool, Error> {
        query(connection, ("XGROUP", "DESTROY", key, group)).await
    }

    #[doc = " Set the last delivered ID for a consumer group."]
    #[doc = ""]
    #[doc = " XGROUP SETID key group 〈 id | $ 〉 [ ENTRIESREAD entriesread ]"]
    #[doc = " https://valkey.io/commands/xgroup-setid/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_setid(
        connection: &Connection,
        key: String,
        group: String,
        id: StreamId,
        entriesread: Option<Integer>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "XGROUP",
                "SETID",
                key,
                group,
                id,
                NamedOption("ENTRIESREAD", entriesread),
            ),
        )
        .await
    }

    #[doc = " Consumers that belong to the <groupname> consumer group of the stream stored at <key>."]
    #[doc = ""]
    #[doc = " XINFO CONSUMERS key group"]
    #[doc = " https://valkey.io/commands/xinfo-consumers/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_consumers(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<Vec<XinfoConsumer>, Error> {
        query(connection, ("XINFO", "CONSUMERS", key, group)).await
    }

    #[doc = " Consumer groups of the stream stored at <key>."]
    #[doc = ""]
    #[doc = " XINFO GROUPS key"]
    #[doc = " https://valkey.io/commands/xinfo-groups/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_groups(connection: &Connection, key: String) -> Result<Vec<XinfoGroup>, Error> {
        query(connection, ("XINFO", "GROUPS", key)).await
    }

    #[doc = " Information about the stream stored at <key>."]
    #[doc = ""]
    #[doc = " See also `xinfo-stream-full`"]
    #[doc = ""]
    #[doc = " XINFO STREAM key"]
    #[doc = " https://valkey.io/commands/xinfo-stream/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_stream(connection: &Connection, key: String) -> Result<XinfoStreamReply, Error> {
        query(connection, ("XINFO", "STREAM", key)).await
    }

    #[doc = " Information about the stream stored at <key>."]
    #[doc = ""]
    #[doc = " See also `xinfo-stream`"]
    #[doc = ""]
    #[doc = " XINFO STREAM key FULL [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xinfo-stream/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_stream_full(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<XinfoStreamReplyFull, Error> {
        query(
            connection,
            ("XINFO", "STREAM", key, "FULL", NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Number of entries inside a stream."]
    #[doc = ""]
    #[doc = " XLEN key"]
    #[doc = " https://valkey.io/commands/xlen/"]
    #[allow(async_fn_in_trait)]
    async fn xlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("XLEN", key)).await
    }

    #[doc = " List of pending messages."]
    #[doc = ""]
    #[doc = " See also `xpending-extended`."]
    #[doc = ""]
    #[doc = " XPENDING key group"]
    #[doc = " https://valkey.io/commands/xpending/"]
    #[allow(async_fn_in_trait)]
    async fn xpending(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<XpendingSummary, Error> {
        query(connection, ("XPENDING", key, group)).await
    }

    #[doc = " List of pending messages."]
    #[doc = ""]
    #[doc = " See also `xpending`."]
    #[doc = ""]
    #[doc = " XPENDING key group [ IDLE min-idle-time ] start end count [ consumer ]"]
    #[doc = " https://valkey.io/commands/xpending/"]
    #[allow(async_fn_in_trait)]
    async fn xpending_extended(
        connection: &Connection,
        key: String,
        group: String,
        min_idle_time: Option<Duration>,
        start: StreamId,
        end: StreamId,
        count: Integer,
        consumer: Option<String>,
    ) -> Result<Vec<XpendingExtendedEntry>, Error> {
        query(
            connection,
            (
                "XPENDING",
                key,
                group,
                NamedOption("IDLE", min_idle_time.map(Milliseconds)),
                start,
                end,
                count,
                consumer,
            ),
        )
        .await
    }

    #[doc = " Stream entries matching a given range of IDs."]
    #[doc = ""]
    #[doc = " XRANGE key start end [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xrange/"]
    #[allow(async_fn_in_trait)]
    async fn xrange(
        connection: &Connection,
        key: String,
        start: StreamId,
        end: StreamId,
        count: Option<Integer>,
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
            connection,
            ("XRANGE", key, start, end, NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Read data from one or multiple streams, only returning entries with an ID greater than the last received ID reported by the caller."]
    #[doc = ""]
    #[doc = " XREAD [ COUNT count ] [ BLOCK milliseconds ] STREAMS key [ key ... ] ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xread/"]
    #[allow(async_fn_in_trait)]
    async fn xread(
        connection: &Connection,
        count: Option<Integer>,
        block: Option<Duration>,
        keys: Vec<String>,
        ids: Vec<StreamId>,
    ) -> Result<Option<Map<String, Vec<(StreamId, Map<String, String>)>>>, Error> {
        query(
            connection,
            (
                "XREAD",
                NamedOption("COUNT", count),
                NamedOption("BLOCK", block.map(Milliseconds)),
                "STREAMS",
                keys,
                ids,
            ),
        )
        .await
    }

    #[doc = " Special version of the XREAD command with support for consumer groups."]
    #[doc = ""]
    #[doc = " The fields of an entry are none when it was deleted after it was delivered, e.g. when reading"]
    #[doc = " the pending entries with the ID `0`."]
    #[doc = ""]
    #[doc = " XREADGROUP GROUP group consumer [ COUNT count ] [ BLOCK milliseconds ] [ NOACK ] STREAMS key [ key ... ] ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xreadgroup/"]
    #[allow(async_fn_in_trait)]
    async fn xreadgroup(
        connection: &Connection,
        group: String,
        consumer: String,
        count: Option<Integer>,
        block: Option<Duration>,
        noack: bool,
        keys: Vec<String>,
        ids: Vec<StreamId>,
    ) -> Result<Option<Map<String, Vec<(StreamId, Option<Map<String, String>>)>>>, Error> {
        query(
            connection,
            (
                "XREADGROUP",
                "GROUP",
                group,
                consumer,
                NamedOption("COUNT", count),
                NamedOption("BLOCK", block.map(Milliseconds)),
                Flag("NOACK", noack),
                "STREAMS",
                keys,
                ids,
            ),
        )
        .await
    }

    #[doc = " XRANGE, but returning the entries in reverse order."]
    #[doc = ""]
    #[doc = " XREVRANGE key end start [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xrevrange/"]
    #[allow(async_fn_in_trait)]
    async fn xrevrange(
        connection: &Connection,
        key: String,
        end: StreamId,
        start: StreamId,
        count: Option<Integer>,
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
            connection,
            ("XREVRANGE", key, end, start, NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Internal command. It is used by a Valkey primary to replicate the last delivered ID of streams."]
    #[doc = ""]
    #[doc = " XSETID key last-id [ ENTRIESADDED entries-added ] [ MAXDELETEDID max-deleted-id ]"]
    #[doc = " https://valkey.io/commands/xsetid/"]
    #[allow(async_fn_in_trait)]
    async fn xsetid(
        connection: &Connection,
        key: String,
        last_id: StreamId,
        entries_added: Option<Integer>,
        max_deleted_id: Option<StreamId>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "XSETID",
                key,
                last_id,
                NamedOption("ENTRIESADDED", entries_added),
                NamedOption("MAXDELETEDID", max_deleted_id),
            ),
        )
        .await
    }

    #[doc = " Trims the stream by evicting older entries."]
    #[doc = ""]
    #[doc = " XTRIM key 〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ]"]
    #[doc = " https://valkey.io/commands/xtrim/"]
    #[allow(async_fn_in_trait)]
    async fn xtrim(
        connection: &Connection,
        key: String,
        trim: StreamTrim,
    ) -> Result<Integer, Error> {
        query(connection, ("XTRIM", key, trim)).await
    }
}

wit_bindgen::generate!({
    path: "../wit",
    world: "commands",
    // the resp types shared by every component
    with: {
        "componentized:valkey/resp@0.3.0-dev": resp_codec::bindings::componentized::valkey::resp,
    },
    generate_all,
});

export!(Commands);
