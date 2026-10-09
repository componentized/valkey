//! `hash-commands`, Valkey's @hash ACL category.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::hash_commands::{
    Duration, ExpireMode, ExpireResult, Guest, HrandfieldOpts, HscanOpts, Instant,
};
use crate::reply::{
    boolean, call, integer, items, ok, optional_string, optional_strings, string, string_entries,
    strings, unexpected, unsigned, whole_seconds,
};
use resp_codec::Value;

impl Guest for crate::Commands {
    async fn hdel(connection: &Connection, key: String, field: String) -> Result<(), Error> {
        // https://valkey.io/commands/hdel/
        // HDEL key field [ field ... ]
        integer(call(connection, ["HDEL".to_string(), key, field]).await?)?;
        Ok(())
    }

    async fn hexists(connection: &Connection, key: String, field: String) -> Result<bool, Error> {
        // https://valkey.io/commands/hexists/
        // HEXISTS key field
        boolean(call(connection, ["HEXISTS".to_string(), key, field]).await?)
    }

    async fn hexpire(
        connection: &Connection,
        key: String,
        seconds: Duration,
        mode: Option<ExpireMode>,
        fields: Vec<String>,
    ) -> Result<Vec<ExpireResult>, Error> {
        // https://valkey.io/commands/hexpire/
        // HEXPIRE key seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]
        let mut command = vec!["HEXPIRE".to_string(), key, whole_seconds(seconds)];
        command.extend(mode.map(expire_mode));
        command.extend(expire_fields(fields));
        expire_results(call(connection, command).await?)
    }

    async fn hexpireat(
        connection: &Connection,
        key: String,
        seconds: Instant,
        mode: Option<ExpireMode>,
        fields: Vec<String>,
    ) -> Result<Vec<ExpireResult>, Error> {
        // https://valkey.io/commands/hexpireat/
        // HEXPIREAT key unix-time-seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]
        let mut command = vec!["HEXPIREAT".to_string(), key, seconds.seconds.to_string()];
        command.extend(mode.map(expire_mode));
        command.extend(expire_fields(fields));
        expire_results(call(connection, command).await?)
    }

    async fn hget(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Option<String>, Error> {
        // https://valkey.io/commands/hget/
        // HGET key field
        optional_string(call(connection, ["HGET".to_string(), key, field]).await?)
    }

    async fn hgetall(connection: &Connection, key: String) -> Result<Vec<(String, String)>, Error> {
        // https://valkey.io/commands/hgetall/
        // HGETALL key
        string_entries(call(connection, ["HGETALL".to_string(), key]).await?)
    }

    async fn hincrby(
        connection: &Connection,
        key: String,
        field: String,
        increment: i64,
    ) -> Result<i64, Error> {
        // https://valkey.io/commands/hincrby/
        // HINCRBY key field increment
        integer(
            call(
                connection,
                ["HINCRBY".to_string(), key, field, increment.to_string()],
            )
            .await?,
        )
    }

    async fn hincrbyfloat(
        connection: &Connection,
        key: String,
        field: String,
        increment: f64,
    ) -> Result<String, Error> {
        // https://valkey.io/commands/hincrbyfloat/
        // HINCRBYFLOAT key field increment
        let command = [
            "HINCRBYFLOAT".to_string(),
            key,
            field,
            increment.to_string(),
        ];
        match call(connection, command).await? {
            Value::Double(value) => Ok(value.to_string()),
            reply => string(reply),
        }
    }

    async fn hkeys(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        // https://valkey.io/commands/hkeys/
        // HKEYS key
        strings(call(connection, ["HKEYS".to_string(), key]).await?)
    }

    async fn hlen(connection: &Connection, key: String) -> Result<u64, Error> {
        // https://valkey.io/commands/hlen/
        // HLEN key
        unsigned(call(connection, ["HLEN".to_string(), key]).await?)
    }

    async fn hmget(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        // https://valkey.io/commands/hmget/
        // HMGET key field [ field ... ]
        let command = ["HMGET".to_string(), key].into_iter().chain(fields);
        optional_strings(call(connection, command).await?)
    }

    async fn hmset(
        connection: &Connection,
        key: String,
        fields: Vec<(String, String)>,
    ) -> Result<(), Error> {
        // https://valkey.io/commands/hmset/
        // HMSET key field value [ field value ... ]
        let command = ["HMSET".to_string(), key]
            .into_iter()
            .chain(fields.into_iter().flat_map(|(field, value)| [field, value]));
        ok(call(connection, command).await?)
    }

    async fn hrandfield(
        connection: &Connection,
        key: String,
        opts: Option<HrandfieldOpts>,
    ) -> Result<Option<Vec<(String, Option<String>)>>, Error> {
        // https://valkey.io/commands/hrandfield/
        // HRANDFIELD key [ count [ WITHVALUES ] ]
        let (count, with_values) = match opts {
            Some(opts) => (opts.count, opts.with_values.unwrap_or(false)),
            None => (None, false),
        };
        let mut command = vec!["HRANDFIELD".to_string(), key];
        match (count, with_values) {
            (None, true) => {
                return Err(Error::Client(
                    "count must be specified to use with-values".to_string(),
                ));
            }
            (None, false) => {}
            (Some(count), with_values) => {
                command.push(count.to_string());
                if with_values {
                    command.push("WITHVALUES".to_string());
                }
            }
        }

        let reply = call(connection, command).await?;
        if count.is_none() {
            // a single field, or null when the hash does not exist
            return Ok(optional_string(reply)?.map(|field| vec![(field, None)]));
        }
        let items = items(reply)?;
        if items.is_empty() {
            return Ok(None);
        }
        let fields = if with_values {
            field_value_pairs(items)?
        } else {
            items
                .into_iter()
                .map(|field| Ok((string(field)?, None)))
                .collect::<Result<_, Error>>()?
        };
        Ok(Some(fields))
    }

    async fn hscan(
        connection: &Connection,
        key: String,
        cursor: Option<String>,
        opts: Option<HscanOpts>,
    ) -> Result<(Option<String>, Vec<(String, Option<String>)>), Error> {
        // https://valkey.io/commands/hscan/
        // HSCAN key cursor [ MATCH pattern ] [ COUNT count ] [ NOVALUES ]
        let mut command = vec![
            "HSCAN".to_string(),
            key,
            cursor.unwrap_or_else(|| "0".to_string()),
        ];
        let mut no_values = false;
        if let Some(opts) = opts {
            if let Some(pattern) = opts.match_ {
                command.extend(["MATCH".to_string(), pattern]);
            }
            if let Some(count) = opts.count {
                command.extend(["COUNT".to_string(), count.to_string()]);
            }
            if opts.no_values == Some(true) {
                command.push("NOVALUES".to_string());
                no_values = true;
            }
        }

        let reply = call(connection, command).await?;
        let mut parts = items(reply.clone())?.into_iter();
        let (Some(cursor), Some(elements), None) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(unexpected(&reply));
        };
        let cursor = string(cursor)?;
        let elements = items(elements)?;
        let fields = if no_values {
            elements
                .into_iter()
                .map(|field| Ok((string(field)?, None)))
                .collect::<Result<_, Error>>()?
        } else {
            field_value_pairs(elements)?
        };
        // a cursor of 0 ends the scan
        let cursor = (cursor != "0").then_some(cursor);
        Ok((cursor, fields))
    }

    async fn hset(
        connection: &Connection,
        key: String,
        field: String,
        value: String,
    ) -> Result<(), Error> {
        // https://valkey.io/commands/hset/
        // HSET key field value [ field value ... ]
        integer(call(connection, ["HSET".to_string(), key, field, value]).await?)?;
        Ok(())
    }

    async fn hsetnx(
        connection: &Connection,
        key: String,
        field: String,
        value: String,
    ) -> Result<bool, Error> {
        // https://valkey.io/commands/hsetnx/
        // HSETNX key field value
        boolean(call(connection, ["HSETNX".to_string(), key, field, value]).await?)
    }

    async fn hstrlen(connection: &Connection, key: String, field: String) -> Result<u64, Error> {
        // https://valkey.io/commands/hstrlen/
        // HSTRLEN key field
        unsigned(call(connection, ["HSTRLEN".to_string(), key, field]).await?)
    }

    async fn hvals(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        // https://valkey.io/commands/hvals/
        // HVALS key
        strings(call(connection, ["HVALS".to_string(), key]).await?)
    }
}

/// Fields and their values, as `[field, value]` pairs (RESP3) or a flat array (RESP2).
fn field_value_pairs(items: Vec<Value>) -> Result<Vec<(String, Option<String>)>, Error> {
    if matches!(items.first(), Some(Value::Array(_))) {
        return items
            .into_iter()
            .map(|pair| {
                let mut pair = crate::reply::items(pair)?.into_iter();
                match (pair.next(), pair.next(), pair.next()) {
                    (Some(field), Some(value), None) => Ok((string(field)?, Some(string(value)?))),
                    _ => Err(Error::Client("unexpected field and value pair".to_string())),
                }
            })
            .collect();
    }
    if items.len() % 2 != 0 {
        return Err(Error::Client(format!(
            "unexpected odd number of fields and values: {}",
            items.len()
        )));
    }
    let mut items = items.into_iter();
    let mut fields = vec![];
    while let (Some(field), Some(value)) = (items.next(), items.next()) {
        fields.push((string(field)?, Some(string(value)?)));
    }
    Ok(fields)
}

fn expire_mode(mode: ExpireMode) -> String {
    match mode {
        ExpireMode::Nx => "NX",
        ExpireMode::Xx => "XX",
        ExpireMode::Gt => "GT",
        ExpireMode::Lt => "LT",
    }
    .to_string()
}

fn expire_fields(fields: Vec<String>) -> impl Iterator<Item = String> {
    ["FIELDS".to_string(), fields.len().to_string()]
        .into_iter()
        .chain(fields)
}

fn expire_results(reply: Value) -> Result<Vec<ExpireResult>, Error> {
    items(reply)?
        .into_iter()
        .map(|code| match integer(code)? {
            -2 => Ok(ExpireResult::DoesNotExist),
            0 => Ok(ExpireResult::ConditionNotMet),
            1 => Ok(ExpireResult::Applied),
            2 => Ok(ExpireResult::ZeroSeconds),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        })
        .collect()
}
