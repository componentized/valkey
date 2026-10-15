//! The config of a `latch-cidr-egress`, from the config of a `tcp-connector`: outbound traffic is
//! denied, except to the server's port.

use componentized::valkey::tcp_connector_constants as keys;
use exports::wasi::config::store::{Error, Guest};
use wasi::config::store;

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
    let port = store::get(&keys::port_key()).map_err(|error| match error {
        store::Error::Upstream(message) => Error::Upstream(message),
        store::Error::Io(message) => Error::Io(message),
    })?;
    let port = match port {
        Some(port) => port.parse::<u16>().ok(),
        None => Some(keys::port_default()),
    };
    Ok(cidr_egress_config(port))
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
