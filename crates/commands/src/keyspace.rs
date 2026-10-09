//! `keyspace-commands`, Valkey's @keyspace ACL category.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::keyspace_commands::{
    Duration, ExpireMode, ExpireResult, Guest, Instant,
};
use crate::reply::{boolean, call, integer, strings, whole_seconds};

impl Guest for crate::Commands {
    async fn del(connection: &Connection, key: String) -> Result<(), Error> {
        // https://valkey.io/commands/del/
        // DEL key [ key ... ]
        integer(call(connection, ["DEL".to_string(), key]).await?)?;
        Ok(())
    }

    async fn exists(connection: &Connection, key: String) -> Result<bool, Error> {
        // https://valkey.io/commands/exists/
        // EXISTS key [ key ... ]
        boolean(call(connection, ["EXISTS".to_string(), key]).await?)
    }

    async fn expire(
        connection: &Connection,
        key: String,
        seconds: Duration,
        mode: Option<ExpireMode>,
    ) -> Result<ExpireResult, Error> {
        // https://valkey.io/commands/expire/
        // EXPIRE key seconds [ NX | XX | GT | LT ]
        let mut command = vec!["EXPIRE".to_string(), key, whole_seconds(seconds)];
        command.extend(mode.map(expire_mode));
        expire_result(integer(call(connection, command).await?)?)
    }

    async fn expireat(
        connection: &Connection,
        key: String,
        seconds: Instant,
        mode: Option<ExpireMode>,
    ) -> Result<ExpireResult, Error> {
        // https://valkey.io/commands/expireat/
        // EXPIREAT key unix-time-seconds [ NX | XX | GT | LT ]
        let mut command = vec!["EXPIREAT".to_string(), key, seconds.seconds.to_string()];
        command.extend(mode.map(expire_mode));
        expire_result(integer(call(connection, command).await?)?)
    }

    async fn keys(connection: &Connection, pattern: String) -> Result<Vec<String>, Error> {
        // https://valkey.io/commands/keys/
        // KEYS pattern
        strings(call(connection, ["KEYS".to_string(), pattern]).await?)
    }
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

fn expire_result(code: i64) -> Result<ExpireResult, Error> {
    match code {
        // EXPIRE replies 0 both for a key that does not exist and for a condition not met
        0 => Ok(ExpireResult::ConditionNotMet),
        1 => Ok(ExpireResult::Applied),
        code => Err(Error::Client(format!("unexpected reply: {code}"))),
    }
}
