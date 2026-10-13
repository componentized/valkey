use crate::command::{
    DecimalSeconds, Fields, Flag, LexRange, NamedOption, NonEmpty, NumKeys, ScoreRange, Zrange,
    emit, query,
};
use crate::exports::componentized::valkey::commands::{
    Aggregate, Connection, Cursor, Double, DurationMilliseconds, DurationSeconds, Error,
    FieldExpiretime, FieldPersistResult, FieldPexpiretime, FieldPttl, FieldTtl, GetexTtl, Guest,
    HsetexCondition, HsetexFieldCondition, Integer, LcsIdxReply, LexBound, Limit,
    MsetexPrecondition, MsetexTtl, PopEnd, Rule, ScoreBound, SetPrecondition, SetTtl, SortOpts,
    StreamId, StreamTrim, TtlMode, TtlResult, UnixTimeMilliseconds, UnixTimeSeconds, Value,
    XclaimOpts, XinfoConsumer, XinfoGroup, XinfoStreamReply, XinfoStreamReplyFull,
    XpendingExtendedEntry, XpendingSummary, ZaddComparison, ZaddCondition, ZrangeBy,
    ZrangeWithscoresBy,
};
use crate::value::{FlatPairs, NextCursor, OrNever, OrZeroId, ScoredMembers};

use wit_bindgen::rt::Map;

mod command;
mod value;

struct Commands;

/// The number of values SORT returns for each element, one for each GET pattern, or the element.
fn sort_row_len(opts: &Option<SortOpts>) -> usize {
    opts.as_ref()
        .and_then(|opts| opts.get.as_ref())
        .map_or(1, Vec::len)
}

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
        query(
            connection,
            ("ACL", "DELUSER", NonEmpty::new("usernames", usernames)),
        )
        .await
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
        rules: Option<Vec<Rule>>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "ACL",
                "SETUSER",
                username,
                NonEmpty::optional("rules", rules),
            ),
        )
        .await
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
        query(connection, ("HDEL", key, NonEmpty::new("fields", fields))).await
    }

    #[doc = " Returns if field is an existing field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HEXISTS key field"]
    #[doc = " https://valkey.io/commands/hexists/"]
    #[allow(async_fn_in_trait)]
    async fn hexists(connection: &Connection, key: String, field: String) -> Result<bool, Error> {
        query(connection, ("HEXISTS", key, field)).await
    }

    #[doc = " When each of the hash fields expires, to the second."]
    #[doc = ""]
    #[doc = " See also `hpexpiretime`."]
    #[doc = ""]
    #[doc = " HEXPIRETIME key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn hexpiretime(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<FieldExpiretime>, Error> {
        query(
            connection,
            ("HEXPIRETIME", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " The values of the hash fields, none for a field that does not exist, deleting the fields."]
    #[doc = ""]
    #[doc = " HGETDEL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hgetdel/"]
    #[allow(async_fn_in_trait)]
    async fn hgetdel(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(
            connection,
            ("HGETDEL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " The values of the hash fields, none for a field that does not exist, optionally setting or"]
    #[doc = " removing their expiration."]
    #[doc = ""]
    #[doc = " HGETEX key [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hgetex/"]
    #[allow(async_fn_in_trait)]
    async fn hgetex(
        connection: &Connection,
        key: String,
        ttl: Option<GetexTtl>,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(
            connection,
            ("HGETEX", key, ttl, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Removes the expiration of the hash fields."]
    #[doc = ""]
    #[doc = " HPERSIST key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpersist/"]
    #[allow(async_fn_in_trait)]
    async fn hpersist(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<FieldPersistResult>, Error> {
        query(
            connection,
            ("HPERSIST", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Manipulate the expiration time for existing hash fields, in milliseconds."]
    #[doc = ""]
    #[doc = " HPEXPIRE key milliseconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpire/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpire(
        connection: &Connection,
        key: String,
        milliseconds: DurationMilliseconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<TtlResult>, Error> {
        let command = (
            "HPEXPIRE",
            key,
            milliseconds,
            mode,
            Fields(NonEmpty::new("fields", fields)),
        );
        query(connection, command).await
    }

    #[doc = " Manipulate the expiration time for existing hash fields, as a Unix time in milliseconds."]
    #[doc = ""]
    #[doc = " HPEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpireat/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpireat(
        connection: &Connection,
        key: String,
        milliseconds: UnixTimeMilliseconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<TtlResult>, Error> {
        let command = (
            "HPEXPIREAT",
            key,
            milliseconds,
            mode,
            Fields(NonEmpty::new("fields", fields)),
        );
        query(connection, command).await
    }

    #[doc = " When each of the hash fields expires, to the millisecond."]
    #[doc = ""]
    #[doc = " See also `hexpiretime`."]
    #[doc = ""]
    #[doc = " HPEXPIRETIME key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpiretime(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<FieldPexpiretime>, Error> {
        query(
            connection,
            ("HPEXPIRETIME", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " The remaining time to live of each of the hash fields, to the millisecond."]
    #[doc = ""]
    #[doc = " See also `httl`."]
    #[doc = ""]
    #[doc = " HPTTL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpttl/"]
    #[allow(async_fn_in_trait)]
    async fn hpttl(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<FieldPttl>, Error> {
        query(
            connection,
            ("HPTTL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Sets the fields to their values in the hash stored at key, optionally setting their"]
    #[doc = " expiration, returning whether they were set, false when a condition prevented it."]
    #[doc = ""]
    #[doc = " HSETEX key [ NX | XX ] [ FNX | FXX ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ] FIELDS numfields field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hsetex/"]
    #[allow(async_fn_in_trait)]
    async fn hsetex(
        connection: &Connection,
        key: String,
        condition: Option<HsetexCondition>,
        field_condition: Option<HsetexFieldCondition>,
        ttl: Option<SetTtl>,
        field_and_values: Vec<(String, String)>,
    ) -> Result<bool, Error> {
        let command = (
            "HSETEX",
            key,
            condition,
            field_condition,
            ttl,
            Fields(NonEmpty::new("field-and-values", field_and_values)),
        );
        query(connection, command).await
    }

    #[doc = " The remaining time to live of each of the hash fields, to the second."]
    #[doc = ""]
    #[doc = " See also `hpttl`."]
    #[doc = ""]
    #[doc = " HTTL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/httl/"]
    #[allow(async_fn_in_trait)]
    async fn httl(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<FieldTtl>, Error> {
        query(
            connection,
            ("HTTL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Manipulate the expiration time for existing hash fields."]
    #[doc = ""]
    #[doc = " HEXPIRE key seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpire/"]
    #[allow(async_fn_in_trait)]
    async fn hexpire(
        connection: &Connection,
        key: String,
        seconds: DurationSeconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<TtlResult>, Error> {
        query(
            connection,
            (
                "HEXPIRE",
                key,
                seconds,
                mode,
                Fields(NonEmpty::new("fields", fields)),
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
        seconds: UnixTimeSeconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<TtlResult>, Error> {
        query(
            connection,
            (
                "HEXPIREAT",
                key,
                seconds,
                mode,
                Fields(NonEmpty::new("fields", fields)),
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
        query(connection, ("HMGET", key, NonEmpty::new("fields", fields))).await
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
        query(
            connection,
            (
                "HMSET",
                key,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
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
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Map<String, String>), Error> {
        let (cursor, items): (NextCursor, Map<String, String>) = query(
            connection,
            (
                "HSCAN",
                key,
                cursor,
                match_.map(|m| ("MATCH", m)),
                count.map(|c| ("COUNT", c)),
            ),
        )
        .await?;
        Ok((cursor.0, items))
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
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let (cursor, items): (NextCursor, Vec<String>) = query(
            connection,
            (
                "HSCAN",
                key,
                cursor,
                match_.map(|m| ("MATCH", m)),
                count.map(|c| ("COUNT", c)),
                "NOVALUES",
            ),
        )
        .await?;
        Ok((cursor.0, items))
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
        query(
            connection,
            (
                "HSET",
                key,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
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
        query(connection, ("DEL", NonEmpty::new("keys", keys))).await
    }

    #[doc = " If key exists."]
    #[doc = ""]
    #[doc = " EXISTS key [ key ... ]"]
    #[doc = " https://valkey.io/commands/exists/"]
    #[allow(async_fn_in_trait)]
    async fn exists(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("EXISTS", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Set a timeout on key."]
    #[doc = ""]
    #[doc = " EXPIRE key seconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/expire/"]
    #[allow(async_fn_in_trait)]
    async fn expire(
        connection: &Connection,
        key: String,
        seconds: DurationSeconds,
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
        seconds: UnixTimeSeconds,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        query(connection, ("EXPIREAT", key, seconds, mode)).await
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
        emit(
            connection,
            ("PSUBSCRIBE", NonEmpty::new("patterns", patterns)),
        )
        .await
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
        channels: Option<Vec<String>>,
    ) -> Result<Map<String, Integer>, Error> {
        query(
            connection,
            ("PUBSUB", "NUMSUB", NonEmpty::optional("channels", channels)),
        )
        .await
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
        shardchannels: Option<Vec<String>>,
    ) -> Result<Map<String, Integer>, Error> {
        query(
            connection,
            (
                "PUBSUB",
                "SHARDNUMSUB",
                NonEmpty::optional("shardchannels", shardchannels),
            ),
        )
        .await
    }

    #[doc = " Unsubscribes the client from the given patterns, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " PUNSUBSCRIBE [ pattern [ pattern ... ] ]"]
    #[doc = " https://valkey.io/commands/punsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn punsubscribe(
        connection: &Connection,
        patterns: Option<Vec<String>>,
    ) -> Result<(), Error> {
        emit(
            connection,
            ("PUNSUBSCRIBE", NonEmpty::optional("patterns", patterns)),
        )
        .await
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
        emit(
            connection,
            ("SSUBSCRIBE", NonEmpty::new("shardchannels", shardchannels)),
        )
        .await
    }

    #[doc = " Subscribes the client to the specified channels."]
    #[doc = ""]
    #[doc = " SUBSCRIBE channel [ channel ... ]"]
    #[doc = " https://valkey.io/commands/subscribe/"]
    #[allow(async_fn_in_trait)]
    async fn subscribe(connection: &Connection, channels: Vec<String>) -> Result<(), Error> {
        emit(
            connection,
            ("SUBSCRIBE", NonEmpty::new("channels", channels)),
        )
        .await
    }

    #[doc = " Unsubscribes the client from the given shard channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " SUNSUBSCRIBE [ shardchannel ] [ [ shardchannel ] ... ]"]
    #[doc = " https://valkey.io/commands/sunsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn sunsubscribe(
        connection: &Connection,
        shardchannels: Option<Vec<String>>,
    ) -> Result<(), Error> {
        emit(
            connection,
            (
                "SUNSUBSCRIBE",
                NonEmpty::optional("shardchannels", shardchannels),
            ),
        )
        .await
    }

    #[doc = " Unsubscribes the client from the given channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " UNSUBSCRIBE [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/unsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn unsubscribe(
        connection: &Connection,
        channels: Option<Vec<String>>,
    ) -> Result<(), Error> {
        emit(
            connection,
            ("UNSUBSCRIBE", NonEmpty::optional("channels", channels)),
        )
        .await
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
        query(connection, ("MGET", NonEmpty::new("keys", keys))).await
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
        query(
            connection,
            ("MSET", NonEmpty::new("keys-and-values", keys_and_values)),
        )
        .await
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
                NumKeys(NonEmpty::new("keys-and-values", keys_and_values)),
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
        query(
            connection,
            ("MSETNX", NonEmpty::new("keys-and-values", keys_and_values)),
        )
        .await
    }

    #[doc = " Set key to hold the string value with expiration in milliseconds."]
    #[doc = ""]
    #[doc = " PSETEX key milliseconds value"]
    #[doc = " https://valkey.io/commands/psetex/"]
    #[allow(async_fn_in_trait)]
    async fn psetex(
        connection: &Connection,
        key: String,
        milliseconds: DurationMilliseconds,
        value: String,
    ) -> Result<(), Error> {
        query(connection, ("PSETEX", key, milliseconds, value)).await
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
        seconds: DurationSeconds,
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
        query(connection, ("XACK", key, group, NonEmpty::new("ids", ids))).await
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
                NonEmpty::new("field-and-values", field_and_values),
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
        min_idle_time: DurationMilliseconds,
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
                min_idle_time,
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
        min_idle_time: DurationMilliseconds,
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
                min_idle_time,
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
        min_idle_time: DurationMilliseconds,
        ids: Vec<StreamId>,
        opts: Option<XclaimOpts>,
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
            connection,
            (
                "XCLAIM",
                key,
                group,
                consumer,
                min_idle_time,
                NonEmpty::new("ids", ids),
                opts,
            ),
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
        min_idle_time: DurationMilliseconds,
        ids: Vec<StreamId>,
        opts: Option<XclaimOpts>,
    ) -> Result<Vec<StreamId>, Error> {
        query(
            connection,
            (
                "XCLAIM",
                key,
                group,
                consumer,
                min_idle_time,
                NonEmpty::new("ids", ids),
                opts,
            ),
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
        query(connection, ("XDEL", key, NonEmpty::new("ids", ids))).await
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
        min_idle_time: Option<DurationMilliseconds>,
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
                NamedOption("IDLE", min_idle_time),
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
        block: Option<DurationMilliseconds>,
        keys: Vec<String>,
        ids: Vec<StreamId>,
    ) -> Result<Option<Map<String, Vec<(StreamId, Map<String, String>)>>>, Error> {
        query(
            connection,
            (
                "XREAD",
                NamedOption("COUNT", count),
                NamedOption("BLOCK", block),
                "STREAMS",
                NonEmpty::new("keys", keys),
                NonEmpty::new("ids", ids),
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
        block: Option<DurationMilliseconds>,
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
                NamedOption("BLOCK", block),
                Flag("NOACK", noack),
                "STREAMS",
                NonEmpty::new("keys", keys),
                NonEmpty::new("ids", ids),
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

    #[doc = " Adds the members to the set stored at key, returning how many were not already members."]
    #[doc = ""]
    #[doc = " SADD key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/sadd/"]
    #[allow(async_fn_in_trait)]
    async fn sadd(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("SADD", key, NonEmpty::new("members", members))).await
    }

    #[doc = " The number of members in the set stored at key."]
    #[doc = ""]
    #[doc = " SCARD key"]
    #[doc = " https://valkey.io/commands/scard/"]
    #[allow(async_fn_in_trait)]
    async fn scard(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("SCARD", key)).await
    }

    #[doc = " The members of the first set that are not in any of the others."]
    #[doc = ""]
    #[doc = " SDIFF key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sdiff/"]
    #[allow(async_fn_in_trait)]
    async fn sdiff(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("SDIFF", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Stores the members of the first set that are not in any of the others, returning how many."]
    #[doc = ""]
    #[doc = " SDIFFSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sdiffstore/"]
    #[allow(async_fn_in_trait)]
    async fn sdiffstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("SDIFFSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " The members in all of the sets."]
    #[doc = ""]
    #[doc = " SINTER key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sinter/"]
    #[allow(async_fn_in_trait)]
    async fn sinter(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("SINTER", NonEmpty::new("keys", keys))).await
    }

    #[doc = " The number of members in all of the sets, counting stops at the limit."]
    #[doc = ""]
    #[doc = " SINTERCARD numkeys key [ key ... ] [ LIMIT limit ]"]
    #[doc = " https://valkey.io/commands/sintercard/"]
    #[allow(async_fn_in_trait)]
    async fn sintercard(
        connection: &Connection,
        keys: Vec<String>,
        limit: Option<Integer>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "SINTERCARD",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("LIMIT", limit),
            ),
        )
        .await
    }

    #[doc = " Stores the members in all of the sets, returning how many."]
    #[doc = ""]
    #[doc = " SINTERSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sinterstore/"]
    #[allow(async_fn_in_trait)]
    async fn sinterstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("SINTERSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Whether the member is in the set stored at key, false when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `sismember-xx`."]
    #[doc = ""]
    #[doc = " SISMEMBER key member"]
    #[doc = " https://valkey.io/commands/sismember/"]
    #[allow(async_fn_in_trait)]
    async fn sismember(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<bool, Error> {
        query(connection, ("SISMEMBER", key, member)).await
    }

    #[doc = " Whether the member is in the set stored at key, none when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `sismember`."]
    #[doc = ""]
    #[doc = " SISMEMBER key member XX"]
    #[doc = " https://valkey.io/commands/sismember/"]
    #[allow(async_fn_in_trait)]
    async fn sismember_xx(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<bool>, Error> {
        // -1 when the key does not exist
        let member: OrNever<bool> = query(connection, ("SISMEMBER", key, member, "XX")).await?;
        Ok(member.0)
    }

    #[doc = " The members of the set stored at key."]
    #[doc = ""]
    #[doc = " SMEMBERS key"]
    #[doc = " https://valkey.io/commands/smembers/"]
    #[allow(async_fn_in_trait)]
    async fn smembers(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        query(connection, ("SMEMBERS", key)).await
    }

    #[doc = " Whether each of the members is in the set stored at key."]
    #[doc = ""]
    #[doc = " SMISMEMBER key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/smismember/"]
    #[allow(async_fn_in_trait)]
    async fn smismember(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Vec<bool>, Error> {
        query(
            connection,
            ("SMISMEMBER", key, NonEmpty::new("members", members)),
        )
        .await
    }

    #[doc = " Moves the member from one set to another, false when it was not in the source set."]
    #[doc = ""]
    #[doc = " SMOVE source destination member"]
    #[doc = " https://valkey.io/commands/smove/"]
    #[allow(async_fn_in_trait)]
    async fn smove(
        connection: &Connection,
        source: String,
        destination: String,
        member: String,
    ) -> Result<bool, Error> {
        query(connection, ("SMOVE", source, destination, member)).await
    }

    #[doc = " Removes and returns random members of the set stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one member, or none when the key does not exist."]
    #[doc = ""]
    #[doc = " SPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/spop/"]
    #[allow(async_fn_in_trait)]
    async fn spop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("SPOP", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<String> = query(connection, ("SPOP", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Random members of the set stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one member, or none when the key does not exist. A negative count may return"]
    #[doc = " the same member more than once, its absolute value is the number of members returned."]
    #[doc = ""]
    #[doc = " SRANDMEMBER key [ count ]"]
    #[doc = " https://valkey.io/commands/srandmember/"]
    #[allow(async_fn_in_trait)]
    async fn srandmember(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("SRANDMEMBER", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<String> = query(connection, ("SRANDMEMBER", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Removes the members from the set stored at key, returning how many were members."]
    #[doc = ""]
    #[doc = " SREM key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/srem/"]
    #[allow(async_fn_in_trait)]
    async fn srem(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("SREM", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Incrementally iterate over the members of the set stored at key."]
    #[doc = ""]
    #[doc = " SSCAN key cursor [ MATCH pattern ] [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/sscan/"]
    #[allow(async_fn_in_trait)]
    async fn sscan(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let (cursor, items): (NextCursor, Vec<String>) = query(
            connection,
            (
                "SSCAN",
                key,
                cursor,
                NamedOption("MATCH", match_),
                NamedOption("COUNT", count),
            ),
        )
        .await?;
        Ok((cursor.0, items))
    }

    #[doc = " The members in any of the sets."]
    #[doc = ""]
    #[doc = " SUNION key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sunion/"]
    #[allow(async_fn_in_trait)]
    async fn sunion(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("SUNION", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Stores the members in any of the sets, returning how many."]
    #[doc = ""]
    #[doc = " SUNIONSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sunionstore/"]
    #[allow(async_fn_in_trait)]
    async fn sunionstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("SUNIONSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Sorts the elements of the list, set or sorted set stored at key."]
    #[doc = ""]
    #[doc = " Each row is the element, or with `get`, the value of each pattern, none when its key does not"]
    #[doc = " exist."]
    #[doc = ""]
    #[doc = " SORT key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]"]
    #[doc = " https://valkey.io/commands/sort/"]
    #[allow(async_fn_in_trait)]
    async fn sort(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        let row = sort_row_len(&opts);
        let values: Vec<Option<String>> = query(connection, ("SORT", key, opts)).await?;
        Ok(values.chunks(row).map(<[_]>::to_vec).collect())
    }

    #[doc = " Sorts the elements of the list, set or sorted set stored at key, storing them as a list,"]
    #[doc = " returning how many."]
    #[doc = ""]
    #[doc = " SORT key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ] STORE destination"]
    #[doc = " https://valkey.io/commands/sort/"]
    #[allow(async_fn_in_trait)]
    async fn sort_store(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
        destination: String,
    ) -> Result<Integer, Error> {
        query(connection, ("SORT", key, opts, "STORE", destination)).await
    }

    #[doc = " The read-only variant of `sort`, e.g. for replicas."]
    #[doc = ""]
    #[doc = " SORT_RO key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]"]
    #[doc = " https://valkey.io/commands/sort_ro/"]
    #[allow(async_fn_in_trait)]
    async fn sort_ro(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        let row = sort_row_len(&opts);
        let values: Vec<Option<String>> = query(connection, ("SORT_RO", key, opts)).await?;
        Ok(values.chunks(row).map(<[_]>::to_vec).collect())
    }

    #[doc = " Removes the members with the lowest or highest scores from the first non-empty sorted set,"]
    #[doc = " waiting up to the timeout for one, returning its key and the members and their scores."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZMPOP timeout numkeys key [ key ... ] 〈 MIN | MAX 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/bzmpop/"]
    #[allow(async_fn_in_trait)]
    async fn bzmpop(
        connection: &Connection,
        timeout: DurationMilliseconds,
        keys: Vec<String>,
        end: PopEnd,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<(String, Double)>)>, Error> {
        let command = (
            "BZMPOP",
            DecimalSeconds(timeout),
            NumKeys(NonEmpty::new("keys", keys)),
            end,
            NamedOption("COUNT", count),
        );
        query(connection, command).await
    }

    #[doc = " Removes the member with the highest score from the first non-empty sorted set, waiting up to"]
    #[doc = " the timeout for one, returning its key, the member and its score."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZPOPMAX key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/bzpopmax/"]
    #[allow(async_fn_in_trait)]
    async fn bzpopmax(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, String, Double)>, Error> {
        query(
            connection,
            (
                "BZPOPMAX",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Removes the member with the lowest score from the first non-empty sorted set, waiting up to"]
    #[doc = " the timeout for one, returning its key, the member and its score."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZPOPMIN key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/bzpopmin/"]
    #[allow(async_fn_in_trait)]
    async fn bzpopmin(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, String, Double)>, Error> {
        query(
            connection,
            (
                "BZPOPMIN",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Adds the members with their scores to the sorted set stored at key, or updates their scores,"]
    #[doc = " returning how many were added, or with `changed`, added or updated."]
    #[doc = ""]
    #[doc = " See also `zadd-incr`."]
    #[doc = ""]
    #[doc = " ZADD key [ NX | XX ] [ GT | LT ] [ CH ] score member [ score member ... ]"]
    #[doc = " https://valkey.io/commands/zadd/"]
    #[allow(async_fn_in_trait)]
    async fn zadd(
        connection: &Connection,
        key: String,
        condition: Option<ZaddCondition>,
        comparison: Option<ZaddComparison>,
        changed: bool,
        score_and_members: Vec<(Double, String)>,
    ) -> Result<Option<Integer>, Error> {
        let command = (
            "ZADD",
            key,
            condition,
            comparison,
            Flag("CH", changed),
            NonEmpty::new("score-and-members", score_and_members),
        );
        query(connection, command).await
    }

    #[doc = " Increments the score of the member in the sorted set stored at key, returning the new score,"]
    #[doc = " none when the condition or comparison prevented it."]
    #[doc = ""]
    #[doc = " See also `zadd` and `zincrby`."]
    #[doc = ""]
    #[doc = " ZADD key [ NX | XX ] [ GT | LT ] INCR score member"]
    #[doc = " https://valkey.io/commands/zadd/"]
    #[allow(async_fn_in_trait)]
    async fn zadd_incr(
        connection: &Connection,
        key: String,
        condition: Option<ZaddCondition>,
        comparison: Option<ZaddComparison>,
        increment: Double,
        member: String,
    ) -> Result<Option<Double>, Error> {
        let command = (
            "ZADD", key, condition, comparison, "INCR", increment, member,
        );
        query(connection, command).await
    }

    #[doc = " The number of members in the sorted set stored at key."]
    #[doc = ""]
    #[doc = " ZCARD key"]
    #[doc = " https://valkey.io/commands/zcard/"]
    #[allow(async_fn_in_trait)]
    async fn zcard(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("ZCARD", key)).await
    }

    #[doc = " The number of members of the sorted set stored at key with scores in the range."]
    #[doc = ""]
    #[doc = " ZCOUNT key min max"]
    #[doc = " https://valkey.io/commands/zcount/"]
    #[allow(async_fn_in_trait)]
    async fn zcount(
        connection: &Connection,
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZCOUNT",
                key,
                ScoreRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " The members of the first sorted set that are not in any of the others."]
    #[doc = ""]
    #[doc = " See also `zdiff-withscores`."]
    #[doc = ""]
    #[doc = " ZDIFF numkeys key [ key ... ]"]
    #[doc = " https://valkey.io/commands/zdiff/"]
    #[allow(async_fn_in_trait)]
    async fn zdiff(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("ZDIFF", NumKeys(NonEmpty::new("keys", keys)))).await
    }

    #[doc = " The members, and their scores, of the first sorted set that are not in any of the others."]
    #[doc = ""]
    #[doc = " See also `zdiff`."]
    #[doc = ""]
    #[doc = " ZDIFF numkeys key [ key ... ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zdiff/"]
    #[allow(async_fn_in_trait)]
    async fn zdiff_withscores(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<(String, Double)>, Error> {
        query(
            connection,
            ("ZDIFF", NumKeys(NonEmpty::new("keys", keys)), "WITHSCORES"),
        )
        .await
    }

    #[doc = " Stores the members of the first sorted set that are not in any of the others, returning how"]
    #[doc = " many."]
    #[doc = ""]
    #[doc = " ZDIFFSTORE destination numkeys key [ key ... ]"]
    #[doc = " https://valkey.io/commands/zdiffstore/"]
    #[allow(async_fn_in_trait)]
    async fn zdiffstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZDIFFSTORE",
                destination,
                NumKeys(NonEmpty::new("keys", keys)),
            ),
        )
        .await
    }

    #[doc = " Increments the score of the member in the sorted set stored at key, returning the new score."]
    #[doc = ""]
    #[doc = " ZINCRBY key increment member"]
    #[doc = " https://valkey.io/commands/zincrby/"]
    #[allow(async_fn_in_trait)]
    async fn zincrby(
        connection: &Connection,
        key: String,
        increment: Double,
        member: String,
    ) -> Result<Double, Error> {
        query(connection, ("ZINCRBY", key, increment, member)).await
    }

    #[doc = " The members in all of the sorted sets."]
    #[doc = ""]
    #[doc = " The weights, when given, multiply the scores of each sorted set, one for each key."]
    #[doc = ""]
    #[doc = " See also `zinter-withscores`."]
    #[doc = ""]
    #[doc = " ZINTER numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zinter/"]
    #[allow(async_fn_in_trait)]
    async fn zinter(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<String>, Error> {
        query(
            connection,
            (
                "ZINTER",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
                aggregate,
            ),
        )
        .await
    }

    #[doc = " The members, and their combined scores, in all of the sorted sets."]
    #[doc = ""]
    #[doc = " See also `zinter`."]
    #[doc = ""]
    #[doc = " ZINTER numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zinter/"]
    #[allow(async_fn_in_trait)]
    async fn zinter_withscores(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let command = (
            "ZINTER",
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
            "WITHSCORES",
        );
        query(connection, command).await
    }

    #[doc = " The number of members in all of the sorted sets, counting stops at the limit."]
    #[doc = ""]
    #[doc = " ZINTERCARD numkeys key [ key ... ] [ LIMIT limit ]"]
    #[doc = " https://valkey.io/commands/zintercard/"]
    #[allow(async_fn_in_trait)]
    async fn zintercard(
        connection: &Connection,
        keys: Vec<String>,
        limit: Option<Integer>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZINTERCARD",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("LIMIT", limit),
            ),
        )
        .await
    }

    #[doc = " Stores the members in all of the sorted sets, with their combined scores, returning how many."]
    #[doc = ""]
    #[doc = " ZINTERSTORE destination numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zinterstore/"]
    #[allow(async_fn_in_trait)]
    async fn zinterstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Integer, Error> {
        let command = (
            "ZINTERSTORE",
            destination,
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
        );
        query(connection, command).await
    }

    #[doc = " The number of members of the sorted set stored at key in the range, for members of equal"]
    #[doc = " score."]
    #[doc = ""]
    #[doc = " ZLEXCOUNT key min max"]
    #[doc = " https://valkey.io/commands/zlexcount/"]
    #[allow(async_fn_in_trait)]
    async fn zlexcount(
        connection: &Connection,
        key: String,
        min: LexBound,
        max: LexBound,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZLEXCOUNT",
                key,
                LexRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " Removes the members with the lowest or highest scores from the first non-empty sorted set,"]
    #[doc = " returning its key and the members and their scores, none when every sorted set is empty."]
    #[doc = ""]
    #[doc = " ZMPOP numkeys key [ key ... ] 〈 MIN | MAX 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/zmpop/"]
    #[allow(async_fn_in_trait)]
    async fn zmpop(
        connection: &Connection,
        keys: Vec<String>,
        end: PopEnd,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<(String, Double)>)>, Error> {
        query(
            connection,
            (
                "ZMPOP",
                NumKeys(NonEmpty::new("keys", keys)),
                end,
                NamedOption("COUNT", count),
            ),
        )
        .await
    }

    #[doc = " The score of each of the members of the sorted set stored at key, none for a member that is"]
    #[doc = " not in it."]
    #[doc = ""]
    #[doc = " ZMSCORE key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/zmscore/"]
    #[allow(async_fn_in_trait)]
    async fn zmscore(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Vec<Option<Double>>, Error> {
        query(
            connection,
            ("ZMSCORE", key, NonEmpty::new("members", members)),
        )
        .await
    }

    #[doc = " Removes and returns the members with the highest scores, and their scores, up to the count,"]
    #[doc = " one without a count."]
    #[doc = ""]
    #[doc = " ZPOPMAX key [ count ]"]
    #[doc = " https://valkey.io/commands/zpopmax/"]
    #[allow(async_fn_in_trait)]
    async fn zpopmax(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let members: ScoredMembers = query(connection, ("ZPOPMAX", key, count)).await?;
        Ok(members.0)
    }

    #[doc = " Removes and returns the members with the lowest scores, and their scores, up to the count,"]
    #[doc = " one without a count."]
    #[doc = ""]
    #[doc = " ZPOPMIN key [ count ]"]
    #[doc = " https://valkey.io/commands/zpopmin/"]
    #[allow(async_fn_in_trait)]
    async fn zpopmin(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let members: ScoredMembers = query(connection, ("ZPOPMIN", key, count)).await?;
        Ok(members.0)
    }

    #[doc = " Random members of the sorted set stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one member, or none when the key does not exist. A negative count may return"]
    #[doc = " the same member more than once, its absolute value is the number of members returned."]
    #[doc = ""]
    #[doc = " See also `zrandmember-withscores`."]
    #[doc = ""]
    #[doc = " ZRANDMEMBER key [ count ]"]
    #[doc = " https://valkey.io/commands/zrandmember/"]
    #[allow(async_fn_in_trait)]
    async fn zrandmember(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("ZRANDMEMBER", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<String> = query(connection, ("ZRANDMEMBER", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Random members of the sorted set stored at key, and their scores, up to the count."]
    #[doc = ""]
    #[doc = " A negative count may return the same member more than once, its absolute value is the number"]
    #[doc = " of members returned."]
    #[doc = ""]
    #[doc = " See also `zrandmember`."]
    #[doc = ""]
    #[doc = " ZRANDMEMBER key count WITHSCORES"]
    #[doc = " https://valkey.io/commands/zrandmember/"]
    #[allow(async_fn_in_trait)]
    async fn zrandmember_withscores(
        connection: &Connection,
        key: String,
        count: Integer,
    ) -> Result<Vec<(String, Double)>, Error> {
        query(connection, ("ZRANDMEMBER", key, count, "WITHSCORES")).await
    }

    #[doc = " The members of the sorted set stored at key in the range, from the lowest score, or the"]
    #[doc = " highest with `rev`."]
    #[doc = ""]
    #[doc = " The limit is a range of the members in the range, only with BYSCORE or BYLEX. With `xx`,"]
    #[doc = " returns none when the key does not exist."]
    #[doc = ""]
    #[doc = " Also covers the deprecated ZRANGEBYLEX, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX and"]
    #[doc = " ZREVRANGEBYSCORE. See also `zrange-withscores`."]
    #[doc = ""]
    #[doc = " ZRANGE key start stop [ BYSCORE | BYLEX ] [ REV ] [ LIMIT offset count ] [ XX ]"]
    #[doc = " https://valkey.io/commands/zrange/"]
    #[allow(async_fn_in_trait)]
    async fn zrange(
        connection: &Connection,
        key: String,
        range: ZrangeBy,
        rev: bool,
        limit: Option<Limit>,
        xx: bool,
    ) -> Result<Option<Vec<String>>, Error> {
        let range = Zrange { range, rev, limit };
        query(connection, ("ZRANGE", key, range, Flag("XX", xx))).await
    }

    #[doc = " The members of the sorted set stored at key in the range, and their scores, from the lowest"]
    #[doc = " score, or the highest with `rev`."]
    #[doc = ""]
    #[doc = " The limit is a range of the members in the range, only with BYSCORE. With `xx`, returns none"]
    #[doc = " when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `zrange`."]
    #[doc = ""]
    #[doc = " ZRANGE key start stop [ BYSCORE ] [ REV ] [ LIMIT offset count ] WITHSCORES [ XX ]"]
    #[doc = " https://valkey.io/commands/zrange/"]
    #[allow(async_fn_in_trait)]
    async fn zrange_withscores(
        connection: &Connection,
        key: String,
        range: ZrangeWithscoresBy,
        rev: bool,
        limit: Option<Limit>,
        xx: bool,
    ) -> Result<Option<Vec<(String, Double)>>, Error> {
        let range = match range {
            ZrangeWithscoresBy::Rank(rank) => ZrangeBy::Rank(rank),
            ZrangeWithscoresBy::Score(score) => ZrangeBy::Score(score),
        };
        let range = Zrange { range, rev, limit };
        let command = ("ZRANGE", key, range, "WITHSCORES", Flag("XX", xx));
        query(connection, command).await
    }

    #[doc = " Stores the members of the sorted set stored at source in the range, returning how many."]
    #[doc = ""]
    #[doc = " The limit is a range of the members in the range, only with BYSCORE or BYLEX."]
    #[doc = ""]
    #[doc = " ZRANGESTORE dst src min max [ BYSCORE | BYLEX ] [ REV ] [ LIMIT offset count ]"]
    #[doc = " https://valkey.io/commands/zrangestore/"]
    #[allow(async_fn_in_trait)]
    async fn zrangestore(
        connection: &Connection,
        destination: String,
        source: String,
        range: ZrangeBy,
        rev: bool,
        limit: Option<Limit>,
    ) -> Result<Integer, Error> {
        let range = Zrange { range, rev, limit };
        query(connection, ("ZRANGESTORE", destination, source, range)).await
    }

    #[doc = " The position of the member in the sorted set stored at key, from the lowest score, none when"]
    #[doc = " it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrank-withscore` and `zrevrank`."]
    #[doc = ""]
    #[doc = " ZRANK key member"]
    #[doc = " https://valkey.io/commands/zrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrank(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<Integer>, Error> {
        query(connection, ("ZRANK", key, member)).await
    }

    #[doc = " The position, and the score, of the member in the sorted set stored at key, from the lowest"]
    #[doc = " score, none when it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrank`."]
    #[doc = ""]
    #[doc = " ZRANK key member WITHSCORE"]
    #[doc = " https://valkey.io/commands/zrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrank_withscore(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<(Integer, Double)>, Error> {
        query(connection, ("ZRANK", key, member, "WITHSCORE")).await
    }

    #[doc = " Removes the members from the sorted set stored at key, returning how many were members."]
    #[doc = ""]
    #[doc = " ZREM key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/zrem/"]
    #[allow(async_fn_in_trait)]
    async fn zrem(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("ZREM", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Removes the members of the sorted set stored at key in the range, for members of equal score,"]
    #[doc = " returning how many."]
    #[doc = ""]
    #[doc = " ZREMRANGEBYLEX key min max"]
    #[doc = " https://valkey.io/commands/zremrangebylex/"]
    #[allow(async_fn_in_trait)]
    async fn zremrangebylex(
        connection: &Connection,
        key: String,
        min: LexBound,
        max: LexBound,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZREMRANGEBYLEX",
                key,
                LexRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " Removes the members of the sorted set stored at key at the positions, both included,"]
    #[doc = " negative from the end, returning how many."]
    #[doc = ""]
    #[doc = " ZREMRANGEBYRANK key start stop"]
    #[doc = " https://valkey.io/commands/zremrangebyrank/"]
    #[allow(async_fn_in_trait)]
    async fn zremrangebyrank(
        connection: &Connection,
        key: String,
        start: Integer,
        stop: Integer,
    ) -> Result<Integer, Error> {
        query(connection, ("ZREMRANGEBYRANK", key, start, stop)).await
    }

    #[doc = " Removes the members of the sorted set stored at key with scores in the range, returning how"]
    #[doc = " many."]
    #[doc = ""]
    #[doc = " ZREMRANGEBYSCORE key min max"]
    #[doc = " https://valkey.io/commands/zremrangebyscore/"]
    #[allow(async_fn_in_trait)]
    async fn zremrangebyscore(
        connection: &Connection,
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZREMRANGEBYSCORE",
                key,
                ScoreRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " The position of the member in the sorted set stored at key, from the highest score, none"]
    #[doc = " when it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrevrank-withscore` and `zrank`."]
    #[doc = ""]
    #[doc = " ZREVRANK key member"]
    #[doc = " https://valkey.io/commands/zrevrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrevrank(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<Integer>, Error> {
        query(connection, ("ZREVRANK", key, member)).await
    }

    #[doc = " The position, and the score, of the member in the sorted set stored at key, from the highest"]
    #[doc = " score, none when it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrevrank`."]
    #[doc = ""]
    #[doc = " ZREVRANK key member WITHSCORE"]
    #[doc = " https://valkey.io/commands/zrevrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrevrank_withscore(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<(Integer, Double)>, Error> {
        query(connection, ("ZREVRANK", key, member, "WITHSCORE")).await
    }

    #[doc = " Incrementally iterate over the members of the sorted set stored at key, and their scores."]
    #[doc = ""]
    #[doc = " See also `zscan-noscores`."]
    #[doc = ""]
    #[doc = " ZSCAN key cursor [ MATCH pattern ] [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/zscan/"]
    #[allow(async_fn_in_trait)]
    async fn zscan(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<(String, Double)>), Error> {
        // the members and scores are a flat array, the scores bulk strings
        let (cursor, members): (NextCursor, FlatPairs<String, Double>) = query(
            connection,
            (
                "ZSCAN",
                key,
                cursor,
                NamedOption("MATCH", match_),
                NamedOption("COUNT", count),
            ),
        )
        .await?;
        Ok((cursor.0, members.0))
    }

    #[doc = " Incrementally iterate over the members of the sorted set stored at key."]
    #[doc = ""]
    #[doc = " See also `zscan`."]
    #[doc = ""]
    #[doc = " ZSCAN key cursor [ MATCH pattern ] [ COUNT count ] NOSCORES"]
    #[doc = " https://valkey.io/commands/zscan/"]
    #[allow(async_fn_in_trait)]
    async fn zscan_noscores(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let (cursor, items): (NextCursor, Vec<String>) = query(
            connection,
            (
                "ZSCAN",
                key,
                cursor,
                NamedOption("MATCH", match_),
                NamedOption("COUNT", count),
                "NOSCORES",
            ),
        )
        .await?;
        Ok((cursor.0, items))
    }

    #[doc = " The score of the member in the sorted set stored at key, none when it is not a member."]
    #[doc = ""]
    #[doc = " ZSCORE key member"]
    #[doc = " https://valkey.io/commands/zscore/"]
    #[allow(async_fn_in_trait)]
    async fn zscore(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<Double>, Error> {
        query(connection, ("ZSCORE", key, member)).await
    }

    #[doc = " The members in any of the sorted sets."]
    #[doc = ""]
    #[doc = " The weights, when given, multiply the scores of each sorted set, one for each key."]
    #[doc = ""]
    #[doc = " See also `zunion-withscores`."]
    #[doc = ""]
    #[doc = " ZUNION numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zunion/"]
    #[allow(async_fn_in_trait)]
    async fn zunion(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<String>, Error> {
        query(
            connection,
            (
                "ZUNION",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
                aggregate,
            ),
        )
        .await
    }

    #[doc = " The members, and their combined scores, in any of the sorted sets."]
    #[doc = ""]
    #[doc = " See also `zunion`."]
    #[doc = ""]
    #[doc = " ZUNION numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zunion/"]
    #[allow(async_fn_in_trait)]
    async fn zunion_withscores(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let command = (
            "ZUNION",
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
            "WITHSCORES",
        );
        query(connection, command).await
    }

    #[doc = " Stores the members in any of the sorted sets, with their combined scores, returning how many."]
    #[doc = ""]
    #[doc = " ZUNIONSTORE destination numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zunionstore/"]
    #[allow(async_fn_in_trait)]
    async fn zunionstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Integer, Error> {
        let command = (
            "ZUNIONSTORE",
            destination,
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
        );
        query(connection, command).await
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
