//! The conversions of the package's types, [`IntoCommand`](crate::IntoCommand) for those in a
//! command and [`FromValue`](crate::FromValue) for those in a reply, by the commands they are for,
//! as the sections of the `types` interface.

pub mod admin;
pub mod bitmap;
pub mod connection;
pub mod geo;
pub mod hash;
pub mod keyspace;
pub mod list;
pub mod scripting;
pub mod shared;
pub mod sortedset;
pub mod stream;
pub mod string;
