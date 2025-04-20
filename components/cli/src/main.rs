use clap::{arg, ArgAction, Args, Parser, Subcommand};
use componentized::valkey::{
    resp::{self, Value},
    store::{connect, Error, HelloOpts, HrandfieldOpts, HscanOpts},
};
use std::{fmt, process};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Host or ip address hosting the Valkey service
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port hosting the Valkey service
    #[arg(long, default_value = "6379")]
    port: u16,

    /// RESP Protocol version
    #[arg(
        long,
        // TODO change default to 3
        default_value = "2",
        value_parser = clap::builder::PossibleValuesParser::new(["2", "3"]),
    )]
    proto_ver: String,

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

fn main() {
    match exec() {
        Err(e) => {
            println!("Error: {e}");
            process::exit(1);
        }
        _ => {}
    }
}

fn exec() -> Result<(), Error> {
    let cli = Cli::parse();

    let opts = HelloOpts {
        proto_ver: Some(cli.proto_ver.to_string()),
        auth: match cli.password {
            Some(password) => Some((cli.username, password)),
            None => None,
        },
        client_name: cli.client_name,
    };
    let connection = connect(&cli.host, cli.port, Some(&opts))?;

    match &cli.command {
        Commands::SEND { cmd } => {
            let cmd: Vec<Value> = cmd
                .iter()
                .map(|c| Value::BulkString(c.to_string()))
                .collect();
            let response = connection.send(&cmd)?;
            println!("{response}");
        }
        Commands::ACL(aclargs) => match &aclargs.command {
            ACLCommands::DELUSER { username } => {
                connection.acl_deluser(&username)?;
                println!("Deleted user {username}");
            }
            ACLCommands::GENPASS => {
                let pass = connection.acl_genpass()?;
                println!("{pass}");
            }
            ACLCommands::SETUSER { username, rules } => {
                connection.acl_setuser(username, rules.as_slice())?;
                println!("Set user {username}");
            }
        },
        Commands::DEL { key } => {
            connection.del(key)?;
            println!("Deleted {key}");
        }
        Commands::EXISTS { key } => match connection.exists(key)? {
            true => println!("true"),
            false => println!("false"),
        },
        Commands::GET { key } => match connection.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("<empty>"),
        },
        Commands::HDEL { key, field } => {
            connection.hdel(key, field)?;
            println!("Deleted {field}");
        }
        Commands::HELLO => {
            for (key, value) in connection.hello(None)? {
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
                                _ => todo!(),
                            }
                        }
                    }
                    _ => todo!(),
                }
            }
        }
        Commands::HEXISTS { key, field } => match connection.hexists(key, field)? {
            true => println!("true"),
            false => println!("false"),
        },
        Commands::HGET { key, field } => match connection.hget(key, field)? {
            Some(value) => println!("{}", value),
            None => println!("<empty>"),
        },
        Commands::HGETALL { key } => {
            let fields = connection.hgetall(key)?;
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
            let value = connection.hincrby(key, field, *increment)?;
            println!("{}", value);
        }
        Commands::HINCRBYFLOAT {
            key,
            field,
            increment,
        } => {
            let value = connection.hincrbyfloat(key, field, *increment)?;
            println!("{}", value);
        }
        Commands::HKEYS { key } => {
            for key in connection.hkeys(key)? {
                println!("- {key}");
            }
        }
        Commands::HLEN { key } => {
            let len = connection.hlen(key)?;
            println!("{len}");
        }
        Commands::HMGET { key, fields } => {
            let fields = connection.hmget(key, fields)?;
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
            connection.hmset(key, &fields)?;
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
            let fields = connection.hrandfield(key, Some(opts))?;
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
            let (cursor, fields) = connection.hscan(key, cursor.as_deref(), Some(opts))?;
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
            connection.hset(key, field, value)?;
            println!("Set {field}");
        }
        Commands::HSETNX { key, field, value } => {
            match connection.hsetnx(key, field, value)? {
                false => println!("Field already set"),
                true => println!("Set {field}"),
            };
        }
        Commands::HSTRLEN { key, field } => {
            let len = connection.hstrlen(key, field)?;
            println!("{len}");
        }
        Commands::HVALS { key } => {
            let values = connection.hvals(key)?;
            if values.len() == 0 {
                println!("<empty>");
            }
            for value in values {
                println!("- {value}");
            }
        }
        Commands::INCR { key } => {
            let value = connection.incr(key)?;
            println!("{}", value);
        }
        Commands::INCRBY { key, increment } => {
            let value = connection.incrby(key, increment.clone())?;
            println!("{}", value);
        }
        Commands::KEYS { pattern } => {
            for key in connection.keys(pattern)? {
                println!("{key}");
            }
        }
        Commands::PUBLISH { channel, message } => {
            let value = connection.publish(channel, message)?;
            println!("{}", value);
        }
        Commands::SET { key, value } => {
            connection.set(key, value)?;
            println!("Set {key}");
        }
    }

    Ok(())
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
            Value::VerbatimString((_encoding, _val)) => {
                // TODO
                write!(f, "{self:?}")
            }
            Value::Map(val) => {
                if val.len() == 0 {
                    write!(f, "(empty map)")?;
                }
                let i_max_width = val.len().to_string().len();
                for (i, (key, val)) in val.iter().enumerate() {
                    let key = resp::decode(key).map_err(|_| fmt::Error)?.to_string();
                    let val = resp::decode(val).map_err(|_| fmt::Error)?.to_string();
                    // TODO handle multi-line keys
                    let prefix = format!("{:i_max_width$}# {key} => ", i + 1);
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

wit_bindgen::generate!({
    world: "cli",
    path: "../wit",
    generate_all
});
