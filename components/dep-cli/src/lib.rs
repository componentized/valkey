use clap::{ArgAction, Args, Parser, Subcommand};
use componentized::valkey::{
    admin_commands,
    connection::{ConnectOpts, Error},
    connection_commands, hash_commands,
    hash_commands::{HrandfieldOpts, HscanOpts},
    keyspace_commands, pubsub_commands,
    resp::{self, Value},
    string_commands, tcp,
};
use exports::wasi::cli::run::Guest;
use std::fmt;
use wasi::cli::{environment::get_arguments, stderr, stdout, types::ErrorCode};
use wit_bindgen::{FutureReader, StreamReader};

/// Prints a line to stdout, like `std::println!`, which has no stdout on wasm32-unknown-unknown.
/// The write is awaited, so it is only usable in an async function.
macro_rules! println {
    () => {
        $crate::write_to(stdout::write_via_stream, String::from("\n")).await
    };
    ($($arg:tt)*) => {
        $crate::write_to(stdout::write_via_stream, format!("{}\n", format_args!($($arg)*))).await
    };
}

/// Prints a line to stderr, like `std::eprintln!`, which has no stderr on wasm32-unknown-unknown.
/// The write is awaited, so it is only usable in an async function.
macro_rules! eprintln {
    () => {
        $crate::write_to(stderr::write_via_stream, String::from("\n")).await
    };
    ($($arg:tt)*) => {
        $crate::write_to(stderr::write_via_stream, format!("{}\n", format_args!($($arg)*))).await
    };
}

/// Writes the value to the target, panics when it can't be written, like `std::println!`.
async fn write_to(
    write_via_stream: fn(StreamReader<u8>) -> FutureReader<Result<(), ErrorCode>>,
    value: String,
) {
    let (mut writer, reader) = wit_stream::new();
    let result = write_via_stream(reader);
    writer.write_all(value.into_bytes()).await;
    // the host finishes writing once the writable end is dropped
    drop(writer);
    if let Err(e) = result.await {
        panic!("failed printing: {e:?}");
    }
}

impl Guest for Cli {
    async fn run() -> Result<(), ()> {
        // clap prints help and errors itself and exits the process, neither works on this target,
        // they are printed here instead
        let cli = match Cli::try_parse_from(get_arguments()) {
            Ok(cli) => cli,
            Err(error) => {
                let message = error.render().to_string();
                if error.use_stderr() {
                    eprintln!("{}", message.trim_end());
                } else {
                    println!("{}", message.trim_end());
                }
                // help and version succeed, invalid arguments fail
                return if error.exit_code() == 0 {
                    Ok(())
                } else {
                    Err(())
                };
            }
        };
        match exec(cli).await {
            Err(e) => {
                eprintln!("Error: {e}");
                Err(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Parser)]
#[command(name = "cli", version, about, long_about = None)]
struct Cli {
    /// Host or ip address hosting the Valkey service
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port hosting the Valkey service
    #[arg(long, default_value = "6379")]
    port: u16,

    /// Authentication username
    #[arg(short, long, default_value = "default")]
    username: String,

    /// Authentication password
    #[arg(short, long)]
    password: Option<String>,

    /// Client name
    #[arg(long)]
    client_name: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a raw command
    SEND {
        #[arg()]
        cmd: Vec<String>,
    },

    /// Access Control List commands
    ACL(ACLArgs),
    /// Removes the specified keys
    DEL {
        /// Key to delete
        #[arg()]
        key: String,
    },
    /// Returns if key exists
    EXISTS {
        /// Key to check
        #[arg()]
        key: String,
    },
    /// Get the value of key
    GET {
        /// Key to get
        #[arg()]
        key: String,
    },
    /// Removes the specified fields from the hash stored at key
    HDEL {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to delete
        #[arg()]
        field: String,
    },
    /// Current server and connection properties
    HELLO,
    /// Returns if the field exists in the hash stored at key
    HEXISTS {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to check
        #[arg()]
        field: String,
    },
    /// Returns the value associated with field in the hash stored at key
    HGET {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to get
        #[arg()]
        field: String,
    },
    /// Returns all fields and values of the hash stored at key
    HGETALL {
        /// Key for hash
        #[arg()]
        key: String,
    },
    /// Increments the number stored at field in the hash stored at key by increment
    HINCRBY {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to increment
        #[arg()]
        field: String,

        /// Amount to increment by
        #[arg(allow_hyphen_values = true)]
        increment: i64,
    },
    /// Increments the number stored at field in the hash stored at key by increment
    HINCRBYFLOAT {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to increment
        #[arg()]
        field: String,

        /// Amount to increment by
        #[arg(allow_hyphen_values = true)]
        increment: f64,
    },
    /// Returns all field names in the hash stored at key
    HKEYS {
        /// Key for hash
        #[arg()]
        key: String,
    },
    /// Returns the number of fields contained in the hash stored at key
    HLEN {
        /// Key for hash
        #[arg()]
        key: String,
    },
    /// Returns the values associated with the specified fields in the hash stored at key
    HMGET {
        /// Key for hash
        #[arg()]
        key: String,

        /// Fields to get
        #[arg()]
        fields: Vec<String>,
    },
    /// Sets the specified fields to their respective values in the hash stored at key
    HMSET {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field-value pairs to set
        #[arg(value_name = "FIELD VALUE", required = true)]
        fields: Vec<String>,
    },
    /// Random field from the hash value stored at key
    HRANDFIELD {
        /// Key for hash
        #[arg()]
        key: String,

        /// Number of fields to return
        #[arg(short, long, default_value = "1")]
        count: Option<i64>,

        /// Return the corresponding values along with the keys in the hash table
        #[arg(long, action = ArgAction::SetTrue)]
        with_values: Option<bool>,
    },
    /// Incrementally iterate over a collection of fields in a hash stored at key
    HSCAN {
        /// Key for hash
        #[arg()]
        key: String,

        /// Cursor to resume a previous scan
        #[arg()]
        cursor: Option<String>,

        /// Only iterate elements matching a given glob-style pattern
        #[arg(short, long)]
        match_: Option<String>,

        /// Amount of work that should be done at every call in order to retrieve elements from the collection
        #[arg(short, long)]
        count: Option<i64>,

        /// Return only the keys in the hash table without their corresponding values
        #[arg(long, action = ArgAction::SetTrue)]
        no_values: Option<bool>,
    },
    /// Sets the specified field to a value in the hash stored at key
    HSET {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to set
        #[arg()]
        field: String,

        /// Value to set
        #[arg()]
        value: String,
    },
    /// Sets field in the hash stored at key to value, only if field does not yet exist
    HSETNX {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to set
        #[arg()]
        field: String,

        /// Value to set
        #[arg()]
        value: String,
    },
    /// Returns the string length of the value associated with field in the hash stored at key
    HSTRLEN {
        /// Key for hash
        #[arg()]
        key: String,

        /// Field to check
        #[arg()]
        field: String,
    },
    /// Returns all values in the hash stored at key
    HVALS {
        /// Key for hash
        #[arg()]
        key: String,
    },
    /// Increments the number stored at key by one
    INCR {
        /// Key to increment
        #[arg()]
        key: String,
    },
    /// Increments the number stored at key by increment
    INCRBY {
        /// Key to increment
        #[arg()]
        key: String,

        /// Amount to increment by
        #[arg(allow_hyphen_values = true)]
        increment: i64,
    },
    /// Returns all keys matching pattern
    KEYS {
        /// Pattern for matching keys
        #[arg()]
        pattern: String,
    },
    /// Posts a message to the given channel
    PUBLISH {
        /// Channel where message should be sent
        #[arg()]
        channel: String,

        /// Message to send
        #[arg()]
        message: String,
    },
    /// Set key to hold the string value
    SET {
        /// Key to set
        #[arg()]
        key: String,

        /// Value to set
        #[arg()]
        value: String,
    },
}

#[derive(Args)]
struct ACLArgs {
    #[command(subcommand)]
    command: ACLCommands,
}

#[derive(Subcommand)]
enum ACLCommands {
    /// Deletes the specified ACL user
    DELUSER {
        /// User to delete
        #[arg()]
        username: String,
    },
    /// Generates a long and strong password
    GENPASS,
    /// Create an ACL user with the specified rules or modify the rules of an existing user
    SETUSER {
        /// User to set
        #[arg()]
        username: String,

        /// ACL rules to set
        #[arg()]
        rules: Vec<String>,
    },
}

async fn exec(cli: Cli) -> Result<(), Error> {
    let opts = ConnectOpts {
        auth: cli.password.map(|password| (cli.username, password)),
        client_name: cli.client_name,
        keep_alive_interval: None,
    };
    let connection = tcp::connect(cli.host, cli.port, Some(opts)).await?;

    match &cli.command {
        Commands::SEND { cmd } => {
            let cmd: Vec<Value> = cmd
                .iter()
                .map(|c| Value::BulkString(c.to_string()))
                .collect();
            let response = connection.call(cmd).await?;
            println!("{response}");
        }
        Commands::ACL(aclargs) => match &aclargs.command {
            ACLCommands::DELUSER { username } => {
                admin_commands::acl_deluser(&connection, username.to_string()).await?;
                println!("Deleted user {username}");
            }
            ACLCommands::GENPASS => {
                let pass = admin_commands::acl_genpass(&connection).await?;
                println!("{pass}");
            }
            ACLCommands::SETUSER { username, rules } => {
                admin_commands::acl_setuser(&connection, username.to_string(), rules.to_vec())
                    .await?;
                println!("Set user {username}");
            }
        },
        Commands::DEL { key } => {
            keyspace_commands::del(&connection, key.to_string()).await?;
            println!("Deleted {key}");
        }
        Commands::EXISTS { key } => {
            match keyspace_commands::exists(&connection, key.to_string()).await? {
                true => println!("true"),
                false => println!("false"),
            }
        }
        Commands::GET { key } => match string_commands::get(&connection, key.to_string()).await? {
            Some(value) => println!("{}", value),
            None => println!("<empty>"),
        },
        Commands::HDEL { key, field } => {
            hash_commands::hdel(&connection, key.to_string(), field.to_string()).await?;
            println!("Deleted {field}");
        }
        Commands::HELLO => {
            for (key, value) in connection_commands::hello(&connection, None).await? {
                match value {
                    Value::Null => println!("{key}: <null>"),
                    Value::String(value) => println!("{key}: {value}"),
                    Value::Integer(value) => println!("{key}: {value}"),
                    Value::BulkString(value) => println!("{key}: {value}"),
                    Value::Array(items) => {
                        if items.is_empty() {
                            println!("{key}: <empty>");
                            continue;
                        }
                        println!("{key}:");
                        for item in items {
                            match resp::decode(&item).map_err(|e| Error::Resp(e))? {
                                Value::Null => println!("- <null>"),
                                Value::String(item) => println!("- {item}"),
                                Value::Integer(item) => println!("- {item}"),
                                Value::BulkString(item) => println!("- {item}"),
                                item => {
                                    println!("- {}", indent(&item.to_string(), "  "))
                                }
                            }
                        }
                    }
                    value => println!("{key}:\n  {}", indent(&value.to_string(), "  ")),
                }
            }
        }
        Commands::HEXISTS { key, field } => {
            match hash_commands::hexists(&connection, key.to_string(), field.to_string()).await? {
                true => println!("true"),
                false => println!("false"),
            }
        }
        Commands::HGET { key, field } => {
            match hash_commands::hget(&connection, key.to_string(), field.to_string()).await? {
                Some(value) => println!("{}", value),
                None => println!("<empty>"),
            }
        }
        Commands::HGETALL { key } => {
            let fields = hash_commands::hgetall(&connection, key.to_string()).await?;
            if fields.len() == 0 {
                println!("<empty>");
            }
            for (key, value) in fields {
                println!("- {key}: {value}");
            }
        }
        Commands::HINCRBY {
            key,
            field,
            increment,
        } => {
            let value =
                hash_commands::hincrby(&connection, key.to_string(), field.to_string(), *increment)
                    .await?;
            println!("{}", value);
        }
        Commands::HINCRBYFLOAT {
            key,
            field,
            increment,
        } => {
            let value = hash_commands::hincrbyfloat(
                &connection,
                key.to_string(),
                field.to_string(),
                *increment,
            )
            .await?;
            println!("{}", value);
        }
        Commands::HKEYS { key } => {
            for key in hash_commands::hkeys(&connection, key.to_string()).await? {
                println!("- {key}");
            }
        }
        Commands::HLEN { key } => {
            let len = hash_commands::hlen(&connection, key.to_string()).await?;
            println!("{len}");
        }
        Commands::HMGET { key, fields } => {
            let fields =
                hash_commands::hmget(&connection, key.to_string().to_string(), fields.to_vec())
                    .await?;
            for field in fields {
                match field {
                    None => println!("- <empty>"),
                    Some(value) => println!("- {value}"),
                }
            }
        }
        Commands::HMSET { key, fields } => {
            if fields.len() % 2 != 0 {
                Err(Error::Client(
                    "An equal number of FIELDs and VALUEs are required".to_string(),
                ))?;
            }
            let fields: Vec<(String, String)> = fields
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            hash_commands::hmset(&connection, key.to_string(), fields.clone()).await?;
            println!("Set {} field(s)", fields.len());
        }
        Commands::HRANDFIELD {
            key,
            count,
            with_values,
        } => {
            let opts = HrandfieldOpts {
                count: *count,
                with_values: *with_values,
            };
            let fields =
                hash_commands::hrandfield(&connection, key.to_string(), Some(opts)).await?;
            match fields {
                None => println!("<empty>"),
                Some(fields) => {
                    for (field, value) in fields {
                        match value {
                            None => println!("- {field}"),
                            Some(value) => println!("- {field}: {value}"),
                        }
                    }
                }
            }
        }
        Commands::HSCAN {
            key,
            cursor,
            match_,
            count,
            no_values,
        } => {
            let opts = &HscanOpts {
                match_: match_.clone(),
                count: *count,
                no_values: *no_values,
            };
            let (cursor, fields) = hash_commands::hscan(
                &connection,
                key.to_string(),
                cursor.clone(),
                Some(opts.clone()),
            )
            .await?;
            if let Some(cursor) = cursor {
                println!("(cursor) {cursor}");
            }
            if fields.len() == 0 {
                println!("<empty>");
            }
            for (field, value) in fields {
                match value {
                    None => println!("- {field}"),
                    Some(value) => println!("- {field}: {value}"),
                }
            }
        }
        Commands::HSET { key, field, value } => {
            hash_commands::hset(
                &connection,
                key.to_string(),
                field.to_string(),
                value.to_string(),
            )
            .await?;
            println!("Set {field}");
        }
        Commands::HSETNX { key, field, value } => {
            match hash_commands::hsetnx(
                &connection,
                key.to_string(),
                field.to_string(),
                value.to_string(),
            )
            .await?
            {
                false => println!("Field already set"),
                true => println!("Set {field}"),
            };
        }
        Commands::HSTRLEN { key, field } => {
            let len =
                hash_commands::hstrlen(&connection, key.to_string(), field.to_string()).await?;
            println!("{len}");
        }
        Commands::HVALS { key } => {
            let values = hash_commands::hvals(&connection, key.to_string()).await?;
            if values.len() == 0 {
                println!("<empty>");
            }
            for value in values {
                println!("- {value}");
            }
        }
        Commands::INCR { key } => {
            let value = string_commands::incr(&connection, key.to_string()).await?;
            println!("{}", value);
        }
        Commands::INCRBY { key, increment } => {
            let value =
                string_commands::incrby(&connection, key.to_string(), increment.clone()).await?;
            println!("{}", value);
        }
        Commands::KEYS { pattern } => {
            for key in keyspace_commands::keys(&connection, pattern.to_string()).await? {
                println!("{key}");
            }
        }
        Commands::PUBLISH { channel, message } => {
            let value =
                pubsub_commands::publish(&connection, channel.to_string(), message.to_string())
                    .await?;
            println!("{}", value);
        }
        Commands::SET { key, value } => {
            string_commands::set(&connection, key.to_string(), value.to_string()).await?;
            println!("Set {key}");
        }
    }

    Ok(())
}

/// Indents each line after the first, for multi-line values printed after a prefix.
fn indent(value: &str, padding: &str) -> String {
    value.replace('\n', &format!("\n{padding}"))
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(val) => write!(f, "{}", val),
            Value::Error(val) => write!(f, "(error) {val}"),
            Value::Integer(val) => write!(f, "(integer) {}", val),
            Value::BulkString(val) => write!(f, "\"{}\"", val),
            Value::Array(val) => {
                if val.len() == 0 {
                    write!(f, "(empty array)")?;
                }
                let i_max_width = val.len().to_string().len();
                for (i, val) in val.iter().enumerate() {
                    let val = resp::decode(val).map_err(|_| fmt::Error)?.to_string();
                    let prefix = format!("{:i_max_width$}) ", i + 1);
                    let mut padding = String::new();
                    for _ in 0..prefix.len() {
                        padding.push(' ')
                    }
                    if i != 0 {
                        write!(f, "\n")?;
                    }
                    for (j, line) in val.lines().enumerate() {
                        match j {
                            0 => write!(f, "{prefix}{line}")?,
                            _ => write!(f, "\n{padding}{line}")?,
                        }
                    }
                }
                Ok(())
            }
            Value::Null => write!(f, "(nil)"),
            Value::Boolean(val) => match val {
                false => write!(f, "(false)"),
                true => write!(f, "(true)"),
            },
            Value::Double(val) => write!(f, "(double) {val}"),
            Value::BigNumber(val) => write!(f, "(number) {val}"),
            Value::BulkError(val) => write!(f, "(error) {val}"),
            // text to show as is, e.g. from INFO, the encoding is `txt` or `mkd`
            Value::VerbatimString((_encoding, val)) => write!(f, "{val}"),
            Value::Map(val) => {
                if val.len() == 0 {
                    write!(f, "(empty map)")?;
                }
                let i_max_width = val.len().to_string().len();
                for (i, (key, val)) in val.iter().enumerate() {
                    let key = resp::decode(key).map_err(|_| fmt::Error)?.to_string();
                    let val = resp::decode(val).map_err(|_| fmt::Error)?.to_string();
                    if i != 0 {
                        write!(f, "\n")?;
                    }
                    // a key over several lines is indented under its number, the value follows its
                    // last line
                    let number = format!("{:i_max_width$}# ", i + 1);
                    let key_lines = lines(&key);
                    let (last, first) = key_lines.split_last().expect("at least one line");
                    for (j, line) in first.iter().enumerate() {
                        let indent = if j == 0 {
                            number.clone()
                        } else {
                            " ".repeat(number.len())
                        };
                        write!(f, "{indent}{line}\n")?;
                    }
                    let indent = if first.is_empty() {
                        number.clone()
                    } else {
                        " ".repeat(number.len())
                    };
                    let prefix = format!("{indent}{last} => ");
                    let padding = " ".repeat(prefix.len());
                    for (j, line) in lines(&val).into_iter().enumerate() {
                        match j {
                            0 => write!(f, "{prefix}{line}")?,
                            _ => write!(f, "\n{padding}{line}")?,
                        }
                    }
                }
                Ok(())
            }
            Value::Set(val) => {
                if val.len() == 0 {
                    write!(f, "(empty set)")?;
                }
                let i_max_width = val.len().to_string().len();
                for (i, val) in val.iter().enumerate() {
                    let val = resp::decode(val).map_err(|_| fmt::Error)?.to_string();
                    let prefix = format!("{:i_max_width$}~ ", i + 1);
                    let mut padding = String::new();
                    for _ in 0..prefix.len() {
                        padding.push(' ')
                    }
                    if i != 0 {
                        write!(f, "\n")?;
                    }
                    for (j, line) in val.lines().enumerate() {
                        match j {
                            0 => write!(f, "{prefix}{line}")?,
                            _ => write!(f, "\n{padding}{line}")?,
                        }
                    }
                }
                Ok(())
            }
            Value::Push(val) => {
                if val.len() == 0 {
                    write!(f, "(empty push)")?;
                }
                let i_max_width = val.len().to_string().len();
                for (i, val) in val.iter().enumerate() {
                    let val = resp::decode(val).map_err(|_| fmt::Error)?.to_string();
                    let prefix = format!("{:i_max_width$}) ", i + 1);
                    let mut padding = String::new();
                    for _ in 0..prefix.len() {
                        padding.push(' ')
                    }
                    if i != 0 {
                        write!(f, "\n")?;
                    }
                    for (j, line) in val.lines().enumerate() {
                        match j {
                            0 => write!(f, "{prefix}{line}")?,
                            _ => write!(f, "\n{padding}{line}")?,
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

/// The lines of a value, an empty value is one empty line.
fn lines(value: &str) -> Vec<&str> {
    let lines: Vec<&str> = value.lines().collect();
    if lines.is_empty() { vec![""] } else { lines }
}

wit_bindgen::generate!({
    world: "cli",
    path: "../wit",
    generate_all
});

export!(Cli);
