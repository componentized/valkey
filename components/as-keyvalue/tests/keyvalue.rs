//! Tests for `as-keyvalue`, run against `client`, which composes it with `tcp-connector` and the
//! rest of the client components.

use std::time::Duration;

use test_harness::keyvalue::types::{Error, KeyResponse, SetOptions};
use test_harness::{Client, Harness, RespValue, Server, bulk, command, hello_reply, ok, setinfo};
use wasmtime::component::ResourceAny;

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

/// Answer the handshake of a connection.
async fn handshake(client: &mut Client) {
    assert_eq!(client.request().await, Some(command(&["HELLO", "3"])));
    for setinfo in setinfo() {
        assert_eq!(client.request().await, Some(setinfo));
    }
    client.reply(hello_reply()).await;
    for _ in setinfo() {
        client.reply(ok()).await;
    }
}

fn array(items: Vec<RespValue>) -> RespValue {
    RespValue::Array(items.into_iter().map(test_harness::encode).collect())
}

/// Answer each command the call sends with the next reply, returning the call's result and the
/// commands, as words.
async fn served<T>(
    client: &mut Client,
    replies: Vec<RespValue>,
    call: impl Future<Output = wasmtime::Result<T>>,
) -> (T, Vec<Vec<RespValue>>) {
    let serve = async {
        let mut requests = vec![];
        for reply in replies {
            requests.push(client.request().await.expect("a command"));
            client.reply(reply).await;
        }
        requests
    };
    let (result, requests) = tokio::join!(call, serve);
    (result.expect("call"), requests)
}

#[tokio::test(flavor = "multi_thread")]
async fn buckets_are_hashes() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let port = server.port().to_string();
    let config = [("port", port.as_str()), ("key-prefix", "app:")];
    let mut harness = Harness::with_config("client", &config).await?;
    harness
        .run(async |accessor, exports| {
            let s = |value: &str| value.to_string();
            let b = |value: &str| value.as_bytes().to_vec();
            let open = exports.keyvalue_store().call_open(accessor, s("bucket"));
            let serve = async {
                let mut client = server.accept().await.expect("accept");
                handshake(&mut client).await;
                client
            };
            let (bucket, mut client) = tokio::join!(open, serve);
            let bucket: ResourceAny =
                bucket?.map_err(|error| wasmtime::format_err!("{error:?}"))?;
            let buckets = exports.keyvalue_types().bucket();

            let (result, sent) = served(
                &mut client,
                vec![bulk("1")],
                buckets.call_get(accessor, bucket, s("a")),
            )
            .await;
            assert_eq!(sent, vec![command(&["HGET", "app:bucket", "a"])]);
            assert_eq!(result, Ok(Some(b("1"))));

            let (result, _) = served(
                &mut client,
                vec![RespValue::Null],
                buckets.call_get(accessor, bucket, s("missing")),
            )
            .await;
            assert_eq!(result, Ok(None));

            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(1)],
                buckets.call_set(accessor, bucket, s("a"), b("2"), None),
            )
            .await;
            assert_eq!(sent, vec![command(&["HSET", "app:bucket", "a", "2"])]);
            assert_eq!(result, Ok(()));

            // if-not-exists only writes a new field
            let if_not_exists = Some(SetOptions {
                ttl_ms: None,
                if_not_exists: true,
            });
            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(0)],
                buckets.call_set(accessor, bucket, s("a"), b("3"), if_not_exists),
            )
            .await;
            assert_eq!(sent, vec![command(&["HSETNX", "app:bucket", "a", "3"])]);
            assert_eq!(result, Err(Error::PreconditionFailed));

            // a ttl to the millisecond, on the field
            let ttl = Some(SetOptions {
                ttl_ms: Some(1500),
                if_not_exists: false,
            });
            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(1), array(vec![RespValue::Integer(1)])],
                buckets.call_set(accessor, bucket, s("t"), b("1"), ttl),
            )
            .await;
            assert_eq!(
                sent,
                vec![
                    command(&["HSET", "app:bucket", "t", "1"]),
                    command(&["HPEXPIRE", "app:bucket", "1500", "FIELDS", "1", "t"]),
                ]
            );
            assert_eq!(result, Ok(()));

            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(1)],
                buckets.call_exists(accessor, bucket, s("a")),
            )
            .await;
            assert_eq!(sent, vec![command(&["HEXISTS", "app:bucket", "a"])]);
            assert_eq!(result, Ok(true));

            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(1)],
                buckets.call_delete(accessor, bucket, s("a")),
            )
            .await;
            assert_eq!(sent, vec![command(&["HDEL", "app:bucket", "a"])]);
            assert_eq!(result, Ok(()));

            // keys by prefix, glob characters in it match literally, resumed with the cursor
            let (result, sent) = served(
                &mut client,
                vec![array(vec![bulk("17"), array(vec![bulk("a*1")])])],
                buckets.call_list_keys(accessor, bucket, Some(s("a*")), None),
            )
            .await;
            assert_eq!(
                sent,
                vec![command(&[
                    "HSCAN",
                    "app:bucket",
                    "0",
                    "MATCH",
                    "a\\**",
                    "NOVALUES"
                ])]
            );
            assert_eq!(
                result,
                Ok(KeyResponse {
                    keys: vec![s("a*1")],
                    cursor: Some(s("17")),
                })
            );
            let (result, sent) = served(
                &mut client,
                vec![array(vec![bulk("0"), array(vec![])])],
                buckets.call_list_keys(accessor, bucket, None, Some(s("17"))),
            )
            .await;
            assert_eq!(
                sent,
                vec![command(&["HSCAN", "app:bucket", "17", "NOVALUES"])]
            );
            assert_eq!(
                result,
                Ok(KeyResponse {
                    keys: vec![],
                    cursor: None,
                })
            );

            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(5)],
                exports
                    .keyvalue_atomics()
                    .call_increment(accessor, bucket, s("count"), 5),
            )
            .await;
            assert_eq!(
                sent,
                vec![command(&["HINCRBY", "app:bucket", "count", "5"])]
            );
            assert_eq!(result, Ok(5));

            let batch = exports.keyvalue_batch();
            let (result, sent) = served(
                &mut client,
                vec![bulk("1"), RespValue::Null],
                batch.call_get_many(accessor, bucket, vec![s("x"), s("z")]),
            )
            .await;
            assert_eq!(
                sent,
                vec![
                    command(&["HGET", "app:bucket", "x"]),
                    command(&["HGET", "app:bucket", "z"]),
                ]
            );
            assert_eq!(result, Ok(vec![Some((s("x"), b("1"))), None]));

            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(1), RespValue::Integer(1)],
                batch.call_set_many(accessor, bucket, vec![(s("x"), b("1")), (s("y"), b("2"))]),
            )
            .await;
            assert_eq!(
                sent,
                vec![
                    command(&["HSET", "app:bucket", "x", "1"]),
                    command(&["HSET", "app:bucket", "y", "2"]),
                ]
            );
            assert_eq!(result, Ok(()));

            let (result, sent) = served(
                &mut client,
                vec![RespValue::Integer(1), RespValue::Integer(1)],
                batch.call_delete_many(accessor, bucket, vec![s("x"), s("y")]),
            )
            .await;
            assert_eq!(
                sent,
                vec![
                    command(&["HDEL", "app:bucket", "x"]),
                    command(&["HDEL", "app:bucket", "y"]),
                ]
            );
            assert_eq!(result, Ok(()));

            // a closed connection is unavailable, a new bucket may connect again
            client.close().await;
            let result = buckets.call_get(accessor, bucket, s("a")).await?;
            assert_eq!(result, Err(Error::StoreUnavailable));
            Ok(())
        })
        .await
}

/// The tests against a Valkey server, ignored unless run with `make test-valkey`, which runs one in
/// a container and only runs the ignored tests in `with_valkey` modules.
mod with_valkey {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn buckets_against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::with_valkey("client", &[("key-prefix", KEY_PREFIX)]).await?;
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

                // a ttl to the millisecond, with HPEXPIRE
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
                tokio::time::sleep(Duration::from_millis(500)).await;
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
