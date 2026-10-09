//! `admin-commands`, Valkey's @admin ACL category, and the rest of the ACL family.

use crate::componentized::valkey::connection::{Connection, Error};
use crate::exports::componentized::valkey::admin_commands::Guest;
use crate::reply::{call, integer, ok, string};

impl Guest for crate::Commands {
    async fn acl_deluser(connection: &Connection, username: String) -> Result<(), Error> {
        // https://valkey.io/commands/acl-deluser/
        // ACL DELUSER username [ username ... ]
        integer(
            call(
                connection,
                ["ACL".to_string(), "DELUSER".to_string(), username],
            )
            .await?,
        )?;
        Ok(())
    }

    async fn acl_genpass(connection: &Connection) -> Result<String, Error> {
        // https://valkey.io/commands/acl-genpass/
        // ACL GENPASS [ bits ]
        string(call(connection, ["ACL", "GENPASS"]).await?)
    }

    async fn acl_setuser(
        connection: &Connection,
        username: String,
        rules: Vec<String>,
    ) -> Result<(), Error> {
        // https://valkey.io/commands/acl-setuser/
        // ACL SETUSER username [ rule ] [ [ rule ] ... ]
        let command = ["ACL".to_string(), "SETUSER".to_string(), username]
            .into_iter()
            .chain(rules);
        ok(call(connection, command).await?)
    }
}
