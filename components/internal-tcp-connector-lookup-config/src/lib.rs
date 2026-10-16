//! The config of a `latch-ip-name-lookup-glob`, from the config of a `tcp-connector`: name lookups
//! are denied, except for the server's hostname.

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
    let hostname = store::get(&keys::hostname_key()).map_err(|error| match error {
        store::Error::Upstream(message) => Error::Upstream(message),
        store::Error::Io(message) => Error::Io(message),
    })?;
    Ok(lookup_glob_config(
        hostname.unwrap_or_else(keys::hostname_default),
    ))
}

/// Deny every name, but the hostname, an IP address too, which the `tcp` component looks up as
/// well. A hostname with glob characters would match other names, it is no hostname, so every
/// name is denied.
fn lookup_glob_config(hostname: String) -> Vec<(String, String)> {
    let mut config = vec![("default".to_string(), "deny".to_string())];
    if !hostname.contains(['*', '?', '[', ']']) {
        config.push(("defer".to_string(), hostname));
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
    fn defers_the_hostname() {
        assert_eq!(
            lookup_glob_config("valkey.example.com".to_string()),
            config(&[("default", "deny"), ("defer", "valkey.example.com")])
        );
    }

    #[test]
    fn defers_an_ip_address() {
        assert_eq!(
            lookup_glob_config("127.0.0.1".to_string()),
            config(&[("default", "deny"), ("defer", "127.0.0.1")])
        );
        assert_eq!(
            lookup_glob_config("::1".to_string()),
            config(&[("default", "deny"), ("defer", "::1")])
        );
    }

    #[test]
    fn denies_everything_for_a_pattern() {
        let deny = config(&[("default", "deny")]);
        assert_eq!(lookup_glob_config("*.example.com".to_string()), deny);
        assert_eq!(lookup_glob_config("valkey?.example.com".to_string()), deny);
        assert_eq!(lookup_glob_config("[a-z].example.com".to_string()), deny);
    }
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/internal-tcp-connector-lookup-config/`
    path: "../wit",
    world: "tcp-connector-config",
    generate_all,
});

export!(Component);
