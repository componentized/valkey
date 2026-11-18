//! The tests of the `tcp-connector-latch`, run inside `test-tcp-connector-latch-suite`, behind a
//! gate with the latch, over `test-stub-sockets`, composed as `test-tcp-connector-latch`. Each
//! config the suite's tests expect runs in a new instance, with a latch that has seen no lookups.
//! The connector initializes the secrets before the latch reads them, without a connector the
//! tests initialize them.

use test_harness::{Harness, run_suite};

const COMPONENT: &str = "test-tcp-connector-latch";

#[tokio::test(flavor = "multi_thread")]
async fn by_hostname() -> wasmtime::Result<()> {
    let config = [("hostname", "valkey.test"), ("port", "6380")];
    let mut harness = Harness::with_secrets(COMPONENT, &config).await?;
    harness.init_secrets();
    run_suite(harness, |test| test.starts_with("by hostname ")).await
}

#[tokio::test(flavor = "multi_thread")]
async fn by_address() -> wasmtime::Result<()> {
    let config = [("hostname", "127.0.0.1"), ("port", "6380")];
    let mut harness = Harness::with_secrets(COMPONENT, &config).await?;
    harness.init_secrets();
    run_suite(harness, |test| test.starts_with("by address ")).await
}

#[tokio::test(flavor = "multi_thread")]
async fn by_default() -> wasmtime::Result<()> {
    let mut harness = Harness::new(COMPONENT).await?;
    harness.init_secrets();
    run_suite(harness, |test| test.starts_with("by default ")).await
}
