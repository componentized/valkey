use componentized::valkey::commands::{self, DurationMilliseconds};
use componentized::valkey::connection::{Connection, Error as ValkeyError};
use componentized::valkey::connector;
use exports::wasmcloud::keyvalue::atomics::Guest as AtomicsGuest;
use exports::wasmcloud::keyvalue::batch::Guest as BatchGuest;
use exports::wasmcloud::keyvalue::store::Guest as StoreGuest;
use exports::wasmcloud::keyvalue::types::{
    Bucket, BucketBorrow, Error, Guest as TypesGuest, GuestBucket, KeyResponse, SetOptions,
};
use wasi::config::store::{self as config};

const KEY_PREFIX_KEY: &str = "key-prefix";
const KEY_PREFIX_DEFAULT: &str = "";

#[derive(Debug, Clone)]
struct AsKeyvalue;

impl TypesGuest for AsKeyvalue {
    type Bucket = KeyvalueToValkeyBucket;
}

impl StoreGuest for AsKeyvalue {
    async fn open(identifier: String) -> Result<Bucket, Error> {
        // the connector decides the transport and the server, e.g. from its own configuration
        let connection = connector::connect()
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
        match commands::hget(&self.connection, self.hash_key.clone(), key).await? {
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
            match commands::hsetnx(&self.connection, self.hash_key.clone(), key.clone(), value)
                .await?
            {
                true => (),
                false => Err(Error::PreconditionFailed)?,
            }
        } else {
            commands::hset(
                &self.connection,
                self.hash_key.clone(),
                vec![(key.clone(), value)],
            )
            .await?;
        }
        if let Some(ttl_ms) = options.ttl_ms {
            let ttl = DurationMilliseconds { px: ttl_ms };
            commands::hpexpire(
                &self.connection,
                self.hash_key.clone(),
                ttl,
                None,
                vec![key],
            )
            .await?;
        }
        Ok(())
    }

    async fn delete(&self, key: String) -> Result<(), Error> {
        commands::hdel(&self.connection, self.hash_key.clone(), vec![key]).await?;
        Ok(())
    }

    async fn exists(&self, key: String) -> Result<bool, Error> {
        Ok(commands::hexists(&self.connection, self.hash_key.clone(), key).await?)
    }

    async fn list_keys(
        &self,
        prefix: Option<String>,
        cursor: Option<String>,
    ) -> Result<KeyResponse, Error> {
        let match_ = prefix.map(|prefix| format!("{}*", escape_glob(&prefix)));
        let count = None;

        let (cursor, keys) = commands::hscan_novalues(
            &self.connection,
            self.hash_key.clone(),
            cursor,
            match_,
            count,
        )
        .await?;

        Ok(KeyResponse {
            keys,
            cursor: Some(cursor.to_string()),
        })
    }
}

impl AtomicsGuest for AsKeyvalue {
    async fn increment(bucket: BucketBorrow<'_>, key: String, delta: i64) -> Result<i64, Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        Ok(commands::hincrby(&bucket.connection, bucket.hash_key.clone(), key, delta).await?)
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

impl From<ValkeyError> for Error {
    fn from(e: ValkeyError) -> Self {
        match e {
            // the transport closed, retrying with a new bucket may succeed
            ValkeyError::Closed(_) => Self::StoreUnavailable,
            e => Self::Other(format!("Valkey: {e}")),
        }
    }
}

wit_bindgen::generate!({
    world: "as-keyvalue",
    path: "../wit",
    generate_all
});

export!(AsKeyvalue);
