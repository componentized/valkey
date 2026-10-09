//! `connection-commands`, Valkey's @connection ACL category.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::connection_commands::{Guest, HelloOpts};
use crate::reply::{call, entries, ok, string, unexpected};
use resp_codec::Value;

impl Guest for crate::Commands {
    async fn auth(
        connection: &Connection,
        username: String,
        password: String,
    ) -> Result<(), Error> {
        // https://valkey.io/commands/auth/
        // AUTH [ username ] password
        ok(call(connection, ["AUTH".to_string(), username, password]).await?)
    }

    async fn hello(
        connection: &Connection,
        opts: Option<HelloOpts>,
    ) -> Result<Vec<(String, Value)>, Error> {
        // https://valkey.io/commands/hello/
        // HELLO [ protover [ AUTH username password ] [ SETNAME clientname ] ]
        let mut command = vec!["HELLO".to_string(), "3".to_string()];
        if let Some(opts) = opts {
            if let Some((username, password)) = opts.auth {
                command.extend(["AUTH".to_string(), username, password]);
            }
            if let Some(client_name) = opts.client_name {
                command.extend(["SETNAME".to_string(), client_name]);
            }
        }
        entries(call(connection, command).await?)?
            .into_iter()
            .map(|(name, value)| Ok((string(name)?, value)))
            .collect()
    }

    async fn ping(connection: &Connection) -> Result<(), Error> {
        // https://valkey.io/commands/ping/
        // PING [ message ]
        match call(connection, ["PING"]).await? {
            Value::String(reply) if reply == "PONG" => Ok(()),
            reply => Err(unexpected(&reply)),
        }
    }
}
