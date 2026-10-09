//! Valkey's commands, grouped by ACL category.
//!
//! These sources are shared by the command components. Each component compiles them with the
//! features for the interfaces it exports, and exports them on the connection it imports:
//!
//! - `admin`: exports `admin-commands`
//! - `connection`: exports `connection-commands`
//! - `hash`: exports `hash-commands`
//! - `keyspace`: exports `keyspace-commands`
//! - `pubsub`: exports `pubsub-commands`
//! - `string`: exports `string-commands`
//!
//! A component compiles one feature, e.g. `hash-commands`, or every feature, `commands`.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

// each component uses the conversions its commands need
#[allow(dead_code)]
mod reply;

#[cfg(feature = "admin")]
mod admin;
#[cfg(feature = "connection")]
mod connection;
#[cfg(feature = "hash")]
mod hash;
#[cfg(feature = "keyspace")]
mod keyspace;
#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(feature = "string")]
mod string;

struct Commands;

/// Generate the bindings for the world of the component compiling these sources.
macro_rules! generate {
    ($world:literal) => {
        wit_bindgen::generate!({
            // `path` is relative to the manifest of the component compiling these sources,
            // `components/<name>/`
            path: "../wit",
            world: $world,
            // the resp types shared by every component
            with: {
                "componentized:valkey/resp@0.3.0-dev": resp_codec::bindings::componentized::valkey::resp,
            },
            generate_all,
        });
    };
}

#[cfg(all(
    feature = "admin",
    feature = "connection",
    feature = "hash",
    feature = "keyspace",
    feature = "pubsub",
    feature = "string"
))]
generate!("commands");

#[cfg(all(
    feature = "admin",
    not(any(
        feature = "connection",
        feature = "hash",
        feature = "keyspace",
        feature = "pubsub",
        feature = "string"
    ))
))]
generate!("admin-commands");

#[cfg(all(
    feature = "connection",
    not(any(
        feature = "admin",
        feature = "hash",
        feature = "keyspace",
        feature = "pubsub",
        feature = "string"
    ))
))]
generate!("connection-commands");

#[cfg(all(
    feature = "hash",
    not(any(
        feature = "admin",
        feature = "connection",
        feature = "keyspace",
        feature = "pubsub",
        feature = "string"
    ))
))]
generate!("hash-commands");

#[cfg(all(
    feature = "keyspace",
    not(any(
        feature = "admin",
        feature = "connection",
        feature = "hash",
        feature = "pubsub",
        feature = "string"
    ))
))]
generate!("keyspace-commands");

#[cfg(all(
    feature = "pubsub",
    not(any(
        feature = "admin",
        feature = "connection",
        feature = "hash",
        feature = "keyspace",
        feature = "string"
    ))
))]
generate!("pubsub-commands");

#[cfg(all(
    feature = "string",
    not(any(
        feature = "admin",
        feature = "connection",
        feature = "hash",
        feature = "keyspace",
        feature = "pubsub"
    ))
))]
generate!("string-commands");

// a feature count other than one or all six has no world
#[cfg(not(any(
    all(
        feature = "admin",
        feature = "connection",
        feature = "hash",
        feature = "keyspace",
        feature = "pubsub",
        feature = "string"
    ),
    all(
        feature = "admin",
        not(any(
            feature = "connection",
            feature = "hash",
            feature = "keyspace",
            feature = "pubsub",
            feature = "string"
        ))
    ),
    all(
        feature = "connection",
        not(any(
            feature = "admin",
            feature = "hash",
            feature = "keyspace",
            feature = "pubsub",
            feature = "string"
        ))
    ),
    all(
        feature = "hash",
        not(any(
            feature = "admin",
            feature = "connection",
            feature = "keyspace",
            feature = "pubsub",
            feature = "string"
        ))
    ),
    all(
        feature = "keyspace",
        not(any(
            feature = "admin",
            feature = "connection",
            feature = "hash",
            feature = "pubsub",
            feature = "string"
        ))
    ),
    all(
        feature = "pubsub",
        not(any(
            feature = "admin",
            feature = "connection",
            feature = "hash",
            feature = "keyspace",
            feature = "string"
        ))
    ),
    all(
        feature = "string",
        not(any(
            feature = "admin",
            feature = "connection",
            feature = "hash",
            feature = "keyspace",
            feature = "pubsub"
        ))
    ),
)))]
compile_error!("enable one command feature, or all of them");

export!(Commands);
