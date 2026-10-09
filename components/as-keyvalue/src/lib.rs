#![no_main]

use componentized::valkey::store::{self as valkey, Connection, Duration, HelloOpts, HscanOpts};
use exports::wasmcloud::keyvalue::atomics::Guest as AtomicsGuest;
use exports::wasmcloud::keyvalue::batch::Guest as BatchGuest;
use exports::wasmcloud::keyvalue::store::Guest as StoreGuest;
use exports::wasmcloud::keyvalue::types::{
    Bucket, BucketBorrow, Error, Guest as TypesGuest, GuestBucket, KeyResponse, SetOptions,
};
use wasi::config::store::{self as config};

const HOSTNAME_KEY: &str = "hostname";
const HOSTNAME_DEFAULT: &str = "127.0.0.1";
const PORT_KEY: &str = "port";
const PORT_DEFAULT: &str = "6379";
const USERNAME_KEY: &str = "username";
const USERNAME_DEFAULT: &str = "default";
const PASSWORD_KEY: &str = "password";
const KEY_PREFIX_KEY: &str = "key-prefix";
const KEY_PREFIX_DEFAULT: &str = "";

#[derive(Debug, Clone)]
struct AsKeyvalue;

impl TypesGuest for AsKeyvalue {
    type Bucket = KeyvalueToValkeyBucket;
}

impl StoreGuest for AsKeyvalue {
    async fn open(identifier: String) -> Result<Bucket, Error> {
        let hostname: String = config::get(HOSTNAME_KEY)?.unwrap_or(HOSTNAME_DEFAULT.to_string());
        let port = config::get(PORT_KEY)?.unwrap_or(PORT_DEFAULT.to_string());
        let port: u16 = port
            .parse()
            .map_err(|_| Error::Other(String::from("port must be an integer")))?;

        let opts = HelloOpts {
            proto_ver: Some("3".to_string()),
            auth: match config::get(PASSWORD_KEY)? {
                Some(password) => {
                    let username: String =
                        config::get(USERNAME_KEY)?.unwrap_or(USERNAME_DEFAULT.to_string());
                    Some((username, password))
                }
                None => None,
            },
            client_name: None,
        };
        let connection = valkey::connect(hostname, port, Some(opts))
            .await
            .map_err(|_| Error::StoreUnavailable)?;

        let key_prefix = config::get(KEY_PREFIX_KEY)?.unwrap_or(KEY_PREFIX_DEFAULT.to_string());
        let hash_key = format!("{key_prefix}{identifier}");

        Ok(Bucket::new(KeyvalueToValkeyBucket {
            hash_key,
            connection,
        }))
    }
}

struct KeyvalueToValkeyBucket {
    hash_key: String,
    connection: Connection,
}

impl GuestBucket for KeyvalueToValkeyBucket {
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Error> {
        match self.connection.hget(self.hash_key.clone(), key).await? {
            Some(value) => Ok(Some(value.into_bytes())),
            None => Ok(None),
        }
    }

    async fn set(
        &self,
        key: String,
        value: Vec<u8>,
        options: Option<SetOptions>,
    ) -> Result<(), Error> {
        let value = String::from_utf8(value).map_err(|e| Error::Other(e.to_string()))?;
        let options = options.unwrap_or(SetOptions {
            ttl_ms: None,
            if_not_exists: false,
        });

        if options.if_not_exists {
            match self
                .connection
                .hsetnx(self.hash_key.clone(), key.clone(), value)
                .await?
            {
                true => (),
                false => Err(Error::PreconditionFailed)?,
            }
        } else {
            self.connection
                .hset(self.hash_key.clone(), key.clone(), value)
                .await?
        }
        if let Some(ttl_ms) = options.ttl_ms {
            let ttl: Duration = ttl_ms / 1000;
            self.connection
                .hexpire(self.hash_key.clone(), ttl, None, vec![key])
                .await?;
        }
        Ok(())
    }

    async fn delete(&self, key: String) -> Result<(), Error> {
        Ok(self.connection.hdel(self.hash_key.clone(), key).await?)
    }

    async fn exists(&self, key: String) -> Result<bool, Error> {
        Ok(self.connection.hexists(self.hash_key.clone(), key).await?)
    }

    async fn list_keys(
        &self,
        prefix: Option<String>,
        cursor: Option<String>,
    ) -> Result<KeyResponse, Error> {
        let opts = HscanOpts {
            match_: prefix.map(|prefix| format!("{}*", escape_glob(&prefix))),
            count: None,
            no_values: Some(true),
        };
        let (cursor, fields) = self
            .connection
            .hscan(self.hash_key.clone(), cursor, Some(opts))
            .await?;

        Ok(KeyResponse {
            keys: fields.into_iter().map(|(key, _)| key).collect(),
            cursor,
        })
    }
}

impl AtomicsGuest for AsKeyvalue {
    async fn increment(bucket: BucketBorrow<'_>, key: String, delta: i64) -> Result<i64, Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        Ok(bucket
            .connection
            .hincrby(bucket.hash_key.clone(), key, delta)
            .await?)
    }
}

impl BatchGuest for AsKeyvalue {
    async fn get_many(
        bucket: BucketBorrow<'_>,
        keys: Vec<String>,
    ) -> Result<Vec<Option<(String, Vec<u8>)>>, Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        let mut values: Vec<Option<(String, Vec<u8>)>> = vec![];
        for key in keys {
            let value = match bucket.get(key.clone()).await? {
                Some(value) => Some((key, value)),
                None => None,
            };
            values.push(value);
        }

        Ok(values)
    }

    async fn set_many(
        bucket: BucketBorrow<'_>,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> Result<(), Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        for (key, value) in key_values {
            bucket.set(key, value, None).await?;
        }

        Ok(())
    }

    async fn delete_many(bucket: BucketBorrow<'_>, keys: Vec<String>) -> Result<(), Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        for key in keys {
            bucket.delete(key).await?;
        }

        Ok(())
    }
}

/// Escapes the glob-style pattern characters Valkey's MATCH interprets, so a prefix matches
/// literally.
fn escape_glob(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

impl From<config::Error> for Error {
    fn from(e: config::Error) -> Self {
        match e {
            config::Error::Upstream(msg) => Self::Other(format!("Config store Upstream: {msg}")),
            config::Error::Io(msg) => Self::Other(format!("Config store IO: {msg}")),
        }
    }
}

impl From<valkey::Error> for Error {
    fn from(e: valkey::Error) -> Self {
        // TODO distinguish between IO, auth and other types of errors
        Self::Other(format!("Valkey: {e}"))
    }
}

wit_bindgen::generate!({
    world: "as-keyvalue",
    path: "../wit",
    generate_all
});

export!(AsKeyvalue);
