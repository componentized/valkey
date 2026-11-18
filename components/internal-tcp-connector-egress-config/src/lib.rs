//! The config of a `latch-cidr-egress`, from the secrets of a `tcp-connector`: outbound traffic is
//! denied, except to the server's port.

use componentized::secrets::store as secrets;
use componentized::valkey::tcp_connector_constants as keys;
use exports::wasi::config::store::{Error, Guest};

struct Component;

impl Guest for Component {
    fn get(key: String) -> Result<Option<String>, Error> {
        Ok(config()?
            .into_iter()
            .find_map(|(name, value)| (name == key).then_some(value)))
    }

    fn get_all() -> Result<Vec<(String, String)>, Error> {
        config()
    }
}

fn config() -> Result<Vec<(String, String)>, Error> {
    let port = secret(&keys::port_key())?;
    let port = match port {
        Some(port) => port.parse::<u16>().ok(),
        None => Some(keys::port_default()),
    };
    Ok(cidr_egress_config(port))
}

/// The value of the connector's secret, none when it is not found.
///
/// The store is initialized by the `tcp-connector` before it opens a socket, before the latch
/// asks for this config, an uninitialized store is an error, the latch denies.
fn secret(key: &str) -> Result<Option<String>, Error> {
    match secrets::get(key) {
        Ok(secrets::Secret::String(secret)) => Ok(Some(secret)),
        Ok(secrets::Secret::Bytes(_)) => {
            Err(Error::Upstream(format!("secrets: value for {key} must be a string ")))
        }
        Err(secrets::ErrorCode::NotFound) => Ok(None),
        Err(secrets::ErrorCode::NotInitialized) => {
            Err(Error::Upstream("secrets: not initialized".to_string()))
        }
        Err(secrets::ErrorCode::Other(None)) => Err(Error::Upstream("secrets: other".to_string())),
        Err(secrets::ErrorCode::Other(Some(message))) => Err(Error::Upstream(message)),
    }
}

/// Deny every port, but the server's, on any address. Without a port, one that is not a port
/// number and fails the connector, every port is denied.
fn cidr_egress_config(port: Option<u16>) -> Vec<(String, String)> {
    let mut config = vec![("default".to_string(), "deny".to_string())];
    if let Some(port) = port {
        // a port without an address matches every address
        // addresses are guarded by latch-deny-connect-unless-lookup-address
        config.push(("defer".to_string(), format!(":{port}")));
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn defers_the_port() {
        assert_eq!(
            cidr_egress_config(Some(6380)),
            config(&[("default", "deny"), ("defer", ":6380")])
        );
    }

    #[test]
    fn denies_everything_without_a_port() {
        assert_eq!(cidr_egress_config(None), config(&[("default", "deny")]));
    }
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/internal-tcp-connector-egress-config/`
    path: "../wit",
    world: "tcp-connector-config",
    generate_all,
});

export!(Component);
