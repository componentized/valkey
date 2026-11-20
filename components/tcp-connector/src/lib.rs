//! A connector for connections over TCP, to the server configured with componentized:secrets.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

use componentized::valkey::connection::{ConnectOpts, Connection, DurationMilliseconds, ErrorCode};
use componentized::valkey::tcp;
// the keys of the secrets and their defaults
use componentized::secrets::store as secrets;
use componentized::valkey::tcp_connector_constants as keys;
use exports::componentized::valkey::connector::Guest;

struct Component;

impl Guest for Component {
    async fn connect() -> Result<Connection, ErrorCode> {
        secrets::init().await.map_err(secrets_error)?;

        let hostname = get(&keys::hostname_key())?.unwrap_or_else(keys::hostname_default);
        let port = match get(&keys::port_key())? {
            Some(port) => port.parse().map_err(|_| {
                ErrorCode::Client(format!(
                    "{} must be a port number: {port}",
                    keys::port_key()
                ))
            })?,
            None => keys::port_default(),
        };
        let auth = match get(&keys::password_key())? {
            Some(password) => {
                let username = get(&keys::username_key())?.unwrap_or_else(keys::username_default);
                Some((username, password))
            }
            None => None,
        };
        let keep_alive_interval = match get(&keys::keep_alive_interval_ms_key())? {
            Some(interval) => {
                let px = interval.parse().map_err(|_| {
                    ErrorCode::Client(format!(
                        "{} must be a number of milliseconds: {interval}",
                        keys::keep_alive_interval_ms_key()
                    ))
                })?;
                Some(DurationMilliseconds { px })
            }
            None => None,
        };
        let opts = ConnectOpts {
            auth,
            client_name: get(&keys::client_name_key())?,
            keep_alive_interval,
        };
        tcp::connect(hostname, port, Some(opts)).await
    }
}

/// The value of the secret, none when it is not found.
fn get(key: &str) -> Result<Option<String>, ErrorCode> {
    match secrets::get(key) {
        Ok(secrets::Secret::String(secret)) => Ok(Some(secret)),
        Ok(secrets::Secret::Bytes(_)) => {
            Err(ErrorCode::Client(format!("secrets: value for {key} must be a string ")))
        }
        Err(secrets::ErrorCode::NotFound) => Ok(None),
        Err(error) => Err(secrets_error(error)),
    }
}

fn secrets_error(error: secrets::ErrorCode) -> ErrorCode {
    match error {
        secrets::ErrorCode::NotFound => ErrorCode::Client("secrets: not found".to_string()),
        secrets::ErrorCode::NotInitialized => {
            ErrorCode::Client("secrets: not initialized".to_string())
        }
        secrets::ErrorCode::Other(None) => ErrorCode::Client("secrets: other".to_string()),
        secrets::ErrorCode::Other(Some(message)) => {
            ErrorCode::Client(format!("secrets: {message}"))
        }
    }
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/tcp-connector/`
    path: "../wit",
    world: "tcp-connector",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.1-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Component);
