//! The tests of the commands, run inside `dep-test-commands`, composed with `commands` and
//! `connection` as `test-commands`. Each test plays the server over the streams of a connection,
//! checking the command a function sends and how it converts the reply.
//!
//! Wasmtime's bindings for the host cannot call `commands` itself, which has functions named for
//! Rust keywords, `move` and `type`, so the tests run in a component.

use test_harness::{Harness, run_suite};

/// The tests that do not need a Valkey server.
#[tokio::test(flavor = "multi_thread")]
async fn commands() -> wasmtime::Result<()> {
    let harness = Harness::new("test-commands").await?;
    run_suite(harness, |test| !test.starts_with("with-valkey ")).await
}

/// The tests against a Valkey server, ignored unless run with `make test-valkey`, which runs one in
/// a container and only runs the ignored tests in `with_valkey` modules.
mod with_valkey {
    use super::*;

    /// Against a Valkey server, see [`test_harness::valkey_server`].
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn against_valkey() -> wasmtime::Result<()> {
        let harness = Harness::with_valkey("test-commands", &[]).await?;
        run_suite(harness, |test| test.starts_with("with-valkey ")).await
    }
}
