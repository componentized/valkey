//! The RESP codec, the `componentized:valkey/types` it works with, and the conversions between
//! those types and commands: `IntoCommand` for a command's words, `FromValue` for its reply.
//!
//! A component that imports an interface of the package uses these types for it, so every
//! component shares them, e.g.
//!
//! ```text
//! wit_bindgen::generate!({
//!     with: { "componentized:valkey/types@0.3.0-dev": resp_codec::bindings::componentized::valkey::types },
//!     ...
//! });
//! ```

pub mod bindings {
    wit_bindgen::generate!({
        // `path` is relative to the manifest, `crates/resp-codec/`
        path: "../../wit",
        inline: "
            package componentized:resp-codec;

            world types {
                import componentized:valkey/types@0.3.0-dev;
            }
        ",
        world: "componentized:resp-codec/types",
        additional_derives: [PartialEq],
        generate_all,
    });
}

mod codec;
pub mod command;
pub mod value;

pub use bindings::componentized::valkey::types::{NestedValue, RespError as Error, Value};
pub use codec::{decode, encode, frame_len};
pub use command::IntoCommand;
pub use value::FromValue;
