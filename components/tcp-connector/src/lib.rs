//! A connector for connections over TCP, to the server configured with wasi:config.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

use componentized::valkey::connection::{ConnectOpts, Connection, DurationMilliseconds, Error};
use componentized::valkey::tcp;
use exports::componentized::valkey::connector::Guest;
use wasi::config::store as config;

const HOSTNAME_KEY: &str = "hostname";
const HOSTNAME_DEFAULT: &str = "127.0.0.1";
const PORT_KEY: &str = "port";
const PORT_DEFAULT: &str = "6379";
const USERNAME_KEY: &str = "username";
const USERNAME_DEFAULT: &str = "default";
const PASSWORD_KEY: &str = "password";
const CLIENT_NAME_KEY: &str = "client-name";
const KEEP_ALIVE_INTERVAL_MS_KEY: &str = "keep-alive-interval-ms";

struct Component;

impl Guest for Component {
    async fn connect() -> Result<Connection, Error> {
        let hostname = get(HOSTNAME_KEY)?.unwrap_or(HOSTNAME_DEFAULT.to_string());
        let port = get(PORT_KEY)?.unwrap_or(PORT_DEFAULT.to_string());
        let port: u16 = port
            .parse()
            .map_err(|_| Error::Client(format!("{PORT_KEY} must be a port number: {port}")))?;
        let auth = match get(PASSWORD_KEY)? {
            Some(password) => {
                let username = get(USERNAME_KEY)?.unwrap_or(USERNAME_DEFAULT.to_string());
                Some((username, password))
            }
            None => None,
        };
        let keep_alive_interval = match get(KEEP_ALIVE_INTERVAL_MS_KEY)? {
            Some(interval) => {
                let px = interval.parse().map_err(|_| {
                    Error::Client(format!(
                        "{KEEP_ALIVE_INTERVAL_MS_KEY} must be a number of milliseconds: {interval}"
                    ))
                })?;
                Some(DurationMilliseconds { px })
            }
            None => None,
        };
        let opts = ConnectOpts {
            auth,
            client_name: get(CLIENT_NAME_KEY)?,
            keep_alive_interval,
        };
        tcp::connect(hostname, port, Some(opts)).await
    }
}

fn get(key: &str) -> Result<Option<String>, Error> {
    config::get(key).map_err(|error| match error {
        config::Error::Upstream(message) => Error::Client(format!("config upstream: {message}")),
        config::Error::Io(message) => Error::Client(format!("config io: {message}")),
    })
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/tcp-connector/`
    path: "../wit",
    world: "tcp-connector",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": resp_codec::bindings::componentized::valkey::types,
    },
    generate_all,
});

export!(Component);
