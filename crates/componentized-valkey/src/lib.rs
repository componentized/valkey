//! Rust support for the `componentized:valkey` WIT package, for components that use or implement
//! its interfaces.
//!
//! - [`types`]: the package's `types` interface, shared by every component's bindings.
//! - [`codec`]: RESP serialization of a [`types::Value`], to and from bytes.
//! - [`command`]: [`IntoCommand`], converting Rust values into the words of a command.
//! - [`value`]: [`FromValue`], converting a reply into Rust values.
//!
//! A component's bindings use these types for the package's types interface, so every component
//! shares them, e.g.
//!
//! ```text
//! wit_bindgen::generate!({
//!     with: { "componentized:valkey/types@0.3.0-dev": componentized_valkey::types },
//!     ...
//! });
//! ```

mod bindings {
    wit_bindgen::generate!({
        // `path` is relative to the manifest, `crates/componentized-valkey/`
        path: "../../wit",
        inline: "
            package componentized:valkey-crate;

            world types {
                import componentized:valkey/types@0.3.0-dev;
            }
        ",
        world: "componentized:valkey-crate/types",
        additional_derives: [PartialEq],
        generate_all,
    });
}

/// The `componentized:valkey/types` interface.
pub use bindings::componentized::valkey::types;

pub mod codec;
pub mod command;
pub mod value;

pub use command::IntoCommand;
pub use value::FromValue;
