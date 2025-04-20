#![no_main]

use componentized::valkey::store::{self as valkey, Connection, HelloOpts};
use exports::wasi::keyvalue::atomics::{Cas, CasError, Guest as AtomicsGuest, GuestCas};
use exports::wasi::keyvalue::batch::Guest as BatchGuest;
use exports::wasi::keyvalue::store::{
    Bucket, BucketBorrow, Error, Guest as StoreGuest, GuestBucket, KeyResponse,
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
struct KeyvalueToValkey;

impl StoreGuest for KeyvalueToValkey {
    type Bucket = KeyvalueToValkeyBucket;

    fn open(identifier: String) -> Result<Bucket, Error> {
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
        let connection = valkey::connect(&hostname, port, Some(&opts))?;

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
    fn get(&self, key: String) -> Result<Option<Vec<u8>>, Error> {
        match self.connection.hget(&self.hash_key, &key)? {
            Some(value) => Ok(Some(value.as_bytes().to_vec())),
            None => Ok(None),
        }
    }

    fn set(&self, key: String, value: Vec<u8>) -> Result<(), Error> {
        let value = String::from_utf8(value).map_err(|e| Error::Other(e.to_string()))?;
        Ok(self.connection.hset(&self.hash_key, &key, &value)?)
    }

    fn delete(&self, key: String) -> Result<(), Error> {
        Ok(self.connection.hdel(&self.hash_key, &key)?)
    }

    fn exists(&self, key: String) -> Result<bool, Error> {
        Ok(self.connection.hexists(&self.hash_key, &key)?)
    }

    fn list_keys(&self, cursor: Option<String>) -> Result<KeyResponse, Error> {
        if cursor.is_some() {
            Err(Error::Other("cursor is not supported".to_string()))?;
        }

        Ok(KeyResponse {
            cursor: None,
            keys: self.connection.hkeys(&self.hash_key)?,
        })
    }
}

impl AtomicsGuest for KeyvalueToValkey {
    type Cas = KeyvalueToValkeyCas;

    fn increment(bucket: BucketBorrow<'_>, key: String, delta: i64) -> Result<i64, Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        Ok(bucket.connection.hincrby(&bucket.hash_key, &key, delta)?)
    }

    fn swap(_cas: Cas, _value: Vec<u8>) -> Result<(), CasError> {
        todo!()
    }
}

struct KeyvalueToValkeyCas;

impl GuestCas for KeyvalueToValkeyCas {
    fn new(_bucket: BucketBorrow<'_>, _key: String) -> Result<Cas, Error> {
        todo!()
    }

    fn current(&self) -> Result<Option<Vec<u8>>, Error> {
        todo!()
    }
}

impl BatchGuest for KeyvalueToValkey {
    fn get_many(
        bucket: BucketBorrow<'_>,
        keys: Vec<String>,
    ) -> Result<Vec<Option<(String, Vec<u8>)>>, Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        let mut values: Vec<Option<(String, Vec<u8>)>> = vec![];
        for key in keys {
            let value = match bucket.get(key.clone())? {
                Some(value) => Some((key, value)),
                None => None,
            };
            values.push(value);
        }

        Ok(values)
    }

    fn set_many(bucket: BucketBorrow<'_>, key_values: Vec<(String, Vec<u8>)>) -> Result<(), Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        for (key, value) in key_values {
            bucket.set(key, value)?;
        }

        Ok(())
    }

    fn delete_many(bucket: BucketBorrow<'_>, keys: Vec<String>) -> Result<(), Error> {
        let bucket: &KeyvalueToValkeyBucket = bucket.get();

        for key in keys {
            bucket.delete(key)?;
        }

        Ok(())
    }
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
    world: "keyvalue-to-valkey",
    path: "../wit",
    generate_all
});

export!(KeyvalueToValkey);
