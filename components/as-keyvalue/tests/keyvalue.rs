//! Tests for `as-keyvalue`, run against `client`, which composes it with `tcp-connector` and the
//! rest of the client components.

use std::time::Duration;

use test_harness::Harness;
use test_harness::keyvalue::types::{Error, KeyResponse, SetOptions};

/// Each bucket is a hash, named for the test, with a prefix to keep them apart from other data.
const KEY_PREFIX: &str = "as-keyvalue-test:";

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_servers_are_unavailable() -> wasmtime::Result<()> {
    // a port nothing listens on
    let port = test_harness::Server::bind().await?.port().to_string();
    let mut harness = Harness::with_config("client", &[("port", &port)]).await?;
    harness
        .run(async |accessor, exports| {
            let bucket = exports
                .keyvalue_store()
                .call_open(accessor, "bucket".to_string())
                .await?;
            assert_eq!(bucket.err(), Some(Error::StoreUnavailable));
            Ok(())
        })
        .await
}

/// The tests against a Valkey server on localhost:6379, ignored unless run with
/// `make test-valkey`, which only runs the ignored tests in `with_valkey` modules.
mod with_valkey {
    use super::*;

    /// Against a Valkey server on localhost:6379, run with `cargo test -p as-keyvalue -- --ignored with_valkey::`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server on localhost:6379"]
    async fn buckets_against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::with_config("client", &[("key-prefix", KEY_PREFIX)]).await?;
        harness
            .run(async |accessor, exports| {
                let s = |value: &str| value.to_string();
                let b = |value: &str| value.as_bytes().to_vec();
                let bucket = exports
                    .keyvalue_store()
                    .call_open(accessor, s("buckets"))
                    .await?
                    .map_err(|error| wasmtime::format_err!("{error:?}"))?;
                let buckets = exports.keyvalue_types().bucket();

                buckets
                    .call_set(accessor, bucket, s("a"), b("1"), None)
                    .await??;
                assert_eq!(
                    buckets.call_get(accessor, bucket, s("a")).await??,
                    Some(b("1"))
                );
                assert!(buckets.call_exists(accessor, bucket, s("a")).await??);
                assert_eq!(
                    buckets.call_get(accessor, bucket, s("missing")).await??,
                    None
                );

                // if-not-exists only writes a new key
                let if_not_exists = Some(SetOptions {
                    ttl_ms: None,
                    if_not_exists: true,
                });
                assert_eq!(
                    buckets
                        .call_set(accessor, bucket, s("a"), b("2"), if_not_exists.clone())
                        .await?,
                    Err(Error::PreconditionFailed)
                );
                buckets
                    .call_set(accessor, bucket, s("b"), b("2"), if_not_exists)
                    .await??;

                // keys are listed by prefix, glob characters in it match literally
                buckets
                    .call_set(accessor, bucket, s("a*"), b("3"), None)
                    .await??;
                let KeyResponse { mut keys, cursor } = buckets
                    .call_list_keys(accessor, bucket, Some(s("a")), None)
                    .await??;
                keys.sort();
                assert_eq!((keys, cursor), (vec![s("a"), s("a*")], None));
                let KeyResponse { keys, .. } = buckets
                    .call_list_keys(accessor, bucket, Some(s("a*")), None)
                    .await??;
                assert_eq!(keys, vec![s("a*")]);

                assert_eq!(
                    exports
                        .keyvalue_atomics()
                        .call_increment(accessor, bucket, s("count"), 5)
                        .await??,
                    5
                );

                let batch = exports.keyvalue_batch();
                batch
                    .call_set_many(accessor, bucket, vec![(s("x"), b("1")), (s("y"), b("2"))])
                    .await??;
                assert_eq!(
                    batch
                        .call_get_many(accessor, bucket, vec![s("x"), s("z")])
                        .await??,
                    vec![Some((s("x"), b("1"))), None]
                );

                // a ttl under a second is rounded up to one, the precision of HEXPIRE
                buckets
                    .call_set(
                        accessor,
                        bucket,
                        s("expiring"),
                        b("1"),
                        Some(SetOptions {
                            ttl_ms: Some(200),
                            if_not_exists: false,
                        }),
                    )
                    .await??;
                assert!(
                    buckets
                        .call_exists(accessor, bucket, s("expiring"))
                        .await??
                );
                tokio::time::sleep(Duration::from_millis(1500)).await;
                assert!(
                    !buckets
                        .call_exists(accessor, bucket, s("expiring"))
                        .await??
                );

                let keys = vec![s("a"), s("b"), s("a*"), s("count"), s("x"), s("y")];
                batch.call_delete_many(accessor, bucket, keys).await??;
                let KeyResponse { keys, .. } = buckets
                    .call_list_keys(accessor, bucket, None, None)
                    .await??;
                assert!(keys.is_empty(), "left over {keys:?}");
                Ok(())
            })
            .await
    }
}
