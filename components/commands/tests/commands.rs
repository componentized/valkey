//! Tests for the commands, run against `test-commands`, `commands` composed with `connection`,
//! `resp` and `tcp`. The test plays the server over a stub transport, checking the command sent
//! and the conversion of its reply.

use std::{collections::HashMap, future::Future};

use test_harness::{
    Harness, Transport, Value, bulk, command, connection_commands, establish, hash_commands,
    keyspace_commands, map,
};

/// Answer the next command with `reply` while `call` waits for it, returning the call's result
/// and the command sent.
async fn round_trip<T>(
    transport: &mut Transport,
    reply: Value,
    call: impl Future<Output = wasmtime::Result<T>>,
) -> (T, Vec<Value>) {
    let serve = async {
        let request = transport.request().await.expect("a command");
        transport.reply(reply);
        request
    };
    let (result, request) = tokio::join!(call, serve);
    (result.expect("call"), request)
}

fn ok() -> Value {
    Value::String("OK".to_string())
}

fn array(items: Vec<Value>) -> Value {
    Value::Array(items.into_iter().map(test_harness::encode).collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn string_commands() -> wasmtime::Result<()> {
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, conn) = establish(accessor, exports).await?;
            let strings = exports.string_commands();

            let (result, sent) = round_trip(
                &mut transport,
                bulk("value"),
                strings.call_get(accessor, conn, "key".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["GET", "key"]));
            assert_eq!(result, Ok(Some("value".to_string())));

            let (result, _) = round_trip(
                &mut transport,
                Value::Null,
                strings.call_get(accessor, conn, "missing".to_string()),
            )
            .await;
            assert_eq!(result, Ok(None));

            let (result, sent) = round_trip(
                &mut transport,
                ok(),
                strings.call_set(
                    accessor,
                    conn,
                    "key".to_string(),
                    "value".to_string(),
                    None,
                    None,
                ),
            )
            .await;
            assert_eq!(sent, command(&["SET", "key", "value"]));
            assert_eq!(result, Ok(()));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(1),
                strings.call_incr(accessor, conn, "count".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["INCR", "count"]));
            assert_eq!(result, Ok(1));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(-4),
                strings.call_incrby(accessor, conn, "count".to_string(), -5),
            )
            .await;
            assert_eq!(sent, command(&["INCRBY", "count", "-5"]));
            assert_eq!(result, Ok(-4));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn keyspace_commands() -> wasmtime::Result<()> {
    use keyspace_commands::{ExpireMode, ExpireResult};
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, conn) = establish(accessor, exports).await?;
            let keyspace = exports.keyspace_commands();

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(1),
                keyspace.call_del(accessor, conn, "key".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["DEL", "key"]));
            assert_eq!(result, Ok(1));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(0),
                keyspace.call_exists(accessor, conn, "key".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["EXISTS", "key"]));
            assert_eq!(result, Ok(false));

            // durations are nanoseconds, EXPIRE takes whole seconds
            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(1),
                keyspace.call_expire(
                    accessor,
                    conn,
                    "key".to_string(),
                    90_500_000_000,
                    Some(ExpireMode::Nx),
                ),
            )
            .await;
            assert_eq!(sent, command(&["EXPIRE", "key", "91", "NX"]));
            assert_eq!(result, Ok(ExpireResult::Applied));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(0),
                keyspace.call_expireat(
                    accessor,
                    conn,
                    "key".to_string(),
                    keyspace_commands::Instant {
                        seconds: 1_900_000_000,
                        nanoseconds: 0,
                    },
                    None,
                ),
            )
            .await;
            assert_eq!(sent, command(&["EXPIREAT", "key", "1900000000"]));
            assert_eq!(result, Ok(ExpireResult::ConditionNotMet));

            let (result, sent) = round_trip(
                &mut transport,
                array(vec![bulk("a"), bulk("b")]),
                keyspace.call_keys(accessor, conn, "*".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["KEYS", "*"]));
            assert_eq!(result, Ok(vec!["a".to_string(), "b".to_string()]));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_commands() -> wasmtime::Result<()> {
    use hash_commands::{ExpireResult, HrandfieldOpts, HscanOpts};
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, conn) = establish(accessor, exports).await?;
            let hashes = exports.hash_commands();
            let key = || "hash".to_string();

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(1),
                hashes.call_hset(accessor, conn, key(), "f".to_string(), "v".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["HSET", "hash", "f", "v"]));
            assert_eq!(result, Ok(1));

            let (result, sent) = round_trip(
                &mut transport,
                bulk("v"),
                hashes.call_hget(accessor, conn, key(), "f".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["HGET", "hash", "f"]));
            assert_eq!(result, Ok(Some("v".to_string())));

            // RESP3 replies to HGETALL with a map
            let (result, sent) = round_trip(
                &mut transport,
                map(vec![(bulk("f"), bulk("v")), (bulk("g"), bulk("w"))]),
                hashes.call_hgetall(accessor, conn, key()),
            )
            .await;
            assert_eq!(sent, command(&["HGETALL", "hash"]));
            assert_eq!(
                result,
                Ok(HashMap::from([
                    ("f".to_string(), "v".to_string()),
                    ("g".to_string(), "w".to_string())
                ]))
            );

            let (result, _) = round_trip(
                &mut transport,
                Value::Integer(1),
                hashes.call_hsetnx(accessor, conn, key(), "f".to_string(), "v".to_string()),
            )
            .await;
            assert_eq!(result, Ok(true));

            let (result, _) = round_trip(
                &mut transport,
                Value::Integer(1),
                hashes.call_hexists(accessor, conn, key(), "f".to_string()),
            )
            .await;
            assert_eq!(result, Ok(true));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(2),
                hashes.call_hlen(accessor, conn, key()),
            )
            .await;
            assert_eq!(sent, command(&["HLEN", "hash"]));
            assert_eq!(result, Ok(2));

            let (result, sent) = round_trip(
                &mut transport,
                array(vec![bulk("v"), Value::Null]),
                hashes.call_hmget(
                    accessor,
                    conn,
                    key(),
                    vec!["f".to_string(), "x".to_string()],
                ),
            )
            .await;
            assert_eq!(sent, command(&["HMGET", "hash", "f", "x"]));
            assert_eq!(result, Ok(vec![Some("v".to_string()), None]));

            let (result, sent) = round_trip(
                &mut transport,
                ok(),
                hashes.call_hmset(
                    accessor,
                    conn,
                    key(),
                    vec![
                        ("f".to_string(), "v".to_string()),
                        ("g".to_string(), "w".to_string()),
                    ],
                ),
            )
            .await;
            assert_eq!(sent, command(&["HMSET", "hash", "f", "v", "g", "w"]));
            assert_eq!(result, Ok(()));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(6),
                hashes.call_hincrby(accessor, conn, key(), "n".to_string(), 5),
            )
            .await;
            assert_eq!(sent, command(&["HINCRBY", "hash", "n", "5"]));
            assert_eq!(result, Ok(6));

            let (result, sent) = round_trip(
                &mut transport,
                bulk("2.5"),
                hashes.call_hincrbyfloat(accessor, conn, key(), "n".to_string(), 1.5),
            )
            .await;
            assert_eq!(sent, command(&["HINCRBYFLOAT", "hash", "n", "1.5"]));
            assert_eq!(result, Ok("2.5".to_string()));

            let (result, _) = round_trip(
                &mut transport,
                array(vec![bulk("f"), bulk("g")]),
                hashes.call_hkeys(accessor, conn, key()),
            )
            .await;
            assert_eq!(result, Ok(vec!["f".to_string(), "g".to_string()]));

            let (result, _) = round_trip(
                &mut transport,
                array(vec![bulk("v"), bulk("w")]),
                hashes.call_hvals(accessor, conn, key()),
            )
            .await;
            assert_eq!(result, Ok(vec!["v".to_string(), "w".to_string()]));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(1),
                hashes.call_hstrlen(accessor, conn, key(), "f".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["HSTRLEN", "hash", "f"]));
            assert_eq!(result, Ok(1));

            let (result, sent) = round_trip(
                &mut transport,
                array(vec![Value::Integer(1), Value::Integer(-2)]),
                hashes.call_hexpire(
                    accessor,
                    conn,
                    key(),
                    60_000_000_000,
                    None,
                    vec!["f".to_string(), "x".to_string()],
                ),
            )
            .await;
            assert_eq!(
                sent,
                command(&["HEXPIRE", "hash", "60", "FIELDS", "2", "f", "x"])
            );
            assert_eq!(
                result,
                Ok(vec![ExpireResult::Applied, ExpireResult::DoesNotExist])
            );

            // without a count, one field or null
            let (result, sent) = round_trip(
                &mut transport,
                bulk("f"),
                hashes.call_hrandfield(accessor, conn, key(), None),
            )
            .await;
            assert_eq!(sent, command(&["HRANDFIELD", "hash"]));
            assert_eq!(result, Ok(Some(vec![("f".to_string(), None)])));

            // RESP3 replies with values as [field, value] pairs
            let (result, sent) = round_trip(
                &mut transport,
                array(vec![
                    array(vec![bulk("f"), bulk("v")]),
                    array(vec![bulk("g"), bulk("w")]),
                ]),
                hashes.call_hrandfield(
                    accessor,
                    conn,
                    key(),
                    Some(HrandfieldOpts {
                        count: Some(2),
                        with_values: Some(true),
                    }),
                ),
            )
            .await;
            assert_eq!(sent, command(&["HRANDFIELD", "hash", "2", "WITHVALUES"]));
            assert_eq!(
                result,
                Ok(Some(vec![
                    ("f".to_string(), Some("v".to_string())),
                    ("g".to_string(), Some("w".to_string()))
                ]))
            );

            let (result, sent) = round_trip(
                &mut transport,
                array(vec![bulk("17"), array(vec![bulk("f"), bulk("v")])]),
                hashes.call_hscan(
                    accessor,
                    conn,
                    key(),
                    None,
                    Some(HscanOpts {
                        match_: Some("f*".to_string()),
                        count: Some(10),
                        no_values: None,
                    }),
                ),
            )
            .await;
            assert_eq!(
                sent,
                command(&["HSCAN", "hash", "0", "MATCH", "f*", "COUNT", "10"])
            );
            assert_eq!(
                result,
                Ok((
                    Some("17".to_string()),
                    vec![("f".to_string(), Some("v".to_string()))]
                ))
            );

            // the last page has a cursor of 0
            let (result, sent) = round_trip(
                &mut transport,
                array(vec![bulk("0"), array(vec![bulk("g")])]),
                hashes.call_hscan(
                    accessor,
                    conn,
                    key(),
                    Some("17".to_string()),
                    Some(HscanOpts {
                        match_: None,
                        count: None,
                        no_values: Some(true),
                    }),
                ),
            )
            .await;
            assert_eq!(sent, command(&["HSCAN", "hash", "17", "NOVALUES"]));
            assert_eq!(result, Ok((None, vec![("g".to_string(), None)])));

            let (result, _) = round_trip(
                &mut transport,
                Value::Integer(1),
                hashes.call_hdel(accessor, conn, key(), "f".to_string()),
            )
            .await;
            assert_eq!(result, Ok(1));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn hrandfield_with_values_needs_a_count() -> wasmtime::Result<()> {
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (_transport, conn) = establish(accessor, exports).await?;
            let result = exports
                .hash_commands()
                .call_hrandfield(
                    accessor,
                    conn,
                    "hash".to_string(),
                    Some(hash_commands::HrandfieldOpts {
                        count: None,
                        with_values: Some(true),
                    }),
                )
                .await?;
            assert!(matches!(result, Err(test_harness::Error::Client(_))));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_commands() -> wasmtime::Result<()> {
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, conn) = establish(accessor, exports).await?;
            let connections = exports.connection_commands();

            let (result, sent) = round_trip(
                &mut transport,
                Value::String("PONG".to_string()),
                connections.call_ping(accessor, conn),
            )
            .await;
            assert_eq!(sent, command(&["PING"]));
            assert_eq!(result, Ok(()));

            let (result, sent) = round_trip(
                &mut transport,
                ok(),
                connections.call_auth(accessor, conn, "user".to_string(), "secret".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["AUTH", "user", "secret"]));
            assert_eq!(result, Ok(()));

            let (result, sent) = round_trip(
                &mut transport,
                map(vec![(bulk("server"), bulk("valkey"))]),
                connections.call_hello(
                    accessor,
                    conn,
                    Some(connection_commands::HelloOpts {
                        auth: None,
                        client_name: Some("tests".to_string()),
                    }),
                ),
            )
            .await;
            assert_eq!(sent, command(&["HELLO", "3", "SETNAME", "tests"]));
            assert_eq!(result, Ok(vec![("server".to_string(), bulk("valkey"))]));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_and_pubsub_commands() -> wasmtime::Result<()> {
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, conn) = establish(accessor, exports).await?;
            let admin = exports.admin_commands();

            let (result, sent) = round_trip(
                &mut transport,
                ok(),
                admin.call_acl_setuser(
                    accessor,
                    conn,
                    "user".to_string(),
                    vec!["on".to_string(), ">secret".to_string()],
                ),
            )
            .await;
            assert_eq!(sent, command(&["ACL", "SETUSER", "user", "on", ">secret"]));
            assert_eq!(result, Ok(()));

            let (result, sent) = round_trip(
                &mut transport,
                bulk("dd721260bfe1b3d9601e7fbab36de6d04e2e67b0ef1c53de59d45950db0dd3cc"),
                admin.call_acl_genpass(accessor, conn),
            )
            .await;
            assert_eq!(sent, command(&["ACL", "GENPASS"]));
            assert_eq!(result.map(|pass| pass.len()), Ok(64));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(1),
                admin.call_acl_deluser(accessor, conn, "user".to_string()),
            )
            .await;
            assert_eq!(sent, command(&["ACL", "DELUSER", "user"]));
            assert_eq!(result, Ok(1));

            let (result, sent) = round_trip(
                &mut transport,
                Value::Integer(2),
                exports.pubsub_commands().call_publish(
                    accessor,
                    conn,
                    "news".to_string(),
                    "hello".to_string(),
                ),
            )
            .await;
            assert_eq!(sent, command(&["PUBLISH", "news", "hello"]));
            assert_eq!(result, Ok(2));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn unexpected_replies_fail_the_command() -> wasmtime::Result<()> {
    let mut harness = Harness::new("test-commands").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, conn) = establish(accessor, exports).await?;
            let (result, _) = round_trip(
                &mut transport,
                Value::Integer(7),
                exports
                    .string_commands()
                    .call_get(accessor, conn, "key".to_string()),
            )
            .await;
            assert!(matches!(result, Err(test_harness::Error::Client(_))));
            Ok(())
        })
        .await
}

/// The tests against a Valkey server on localhost:6379, ignored unless run with
/// `make test-valkey`, which only runs the ignored tests in `with_valkey` modules.
mod with_valkey {
    use std::collections::HashMap;

    use super::*;

    /// Every command against a Valkey server on localhost:6379, run with
    /// `cargo test -p commands -- --ignored with_valkey::`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server on localhost:6379"]
    async fn against_valkey() -> wasmtime::Result<()> {
        use hash_commands::{HrandfieldOpts, HscanOpts};
        let mut harness = Harness::new("test-commands").await?;
        harness
            .run(async |accessor, exports| {
                let conn = exports
                    .tcp()
                    .call_connect(accessor, "localhost".to_string(), 6379, None)
                    .await?
                    .map_err(|error| wasmtime::format_err!("{error:?}"))?;
                let strings = exports.string_commands();
                let keyspace = exports.keyspace_commands();
                let hashes = exports.hash_commands();
                let s = |value: &str| value.to_string();

                exports
                    .connection_commands()
                    .call_ping(accessor, conn)
                    .await??;
                let hello = exports
                    .connection_commands()
                    .call_hello(accessor, conn, None)
                    .await??;
                assert!(hello.contains(&(s("proto"), Value::Integer(3))));

                strings
                    .call_set(
                        accessor,
                        conn,
                        s("commands-test:string"),
                        s("1"),
                        None,
                        None,
                    )
                    .await??;
                assert_eq!(
                    strings
                        .call_get(accessor, conn, s("commands-test:string"))
                        .await??,
                    Some(s("1"))
                );
                assert_eq!(
                    strings
                        .call_incr(accessor, conn, s("commands-test:string"))
                        .await??,
                    2
                );
                assert_eq!(
                    strings
                        .call_incrby(accessor, conn, s("commands-test:string"), 3)
                        .await??,
                    5
                );
                assert!(
                    keyspace
                        .call_exists(accessor, conn, s("commands-test:string"))
                        .await??
                );
                assert_eq!(
                    keyspace
                        .call_expire(
                            accessor,
                            conn,
                            s("commands-test:string"),
                            60_000_000_000,
                            None
                        )
                        .await??,
                    keyspace_commands::ExpireResult::Applied
                );
                assert_eq!(
                    keyspace
                        .call_keys(accessor, conn, s("commands-test:str*"))
                        .await??,
                    vec![s("commands-test:string")]
                );
                keyspace
                    .call_del(accessor, conn, s("commands-test:string"))
                    .await??;
                assert_eq!(
                    strings
                        .call_get(accessor, conn, s("commands-test:string"))
                        .await??,
                    None
                );

                let key = || s("commands-test:hash");
                hashes
                    .call_hmset(
                        accessor,
                        conn,
                        key(),
                        vec![(s("f"), s("v")), (s("g"), s("w"))],
                    )
                    .await??;
                let all = hashes.call_hgetall(accessor, conn, key()).await??;
                assert_eq!(all, HashMap::from([(s("f"), s("v")), (s("g"), s("w"))]));
                assert_eq!(hashes.call_hlen(accessor, conn, key()).await??, 2);
                assert_eq!(
                    hashes
                        .call_hmget(accessor, conn, key(), vec![s("f"), s("x")])
                        .await??,
                    vec![Some(s("v")), None]
                );
                let fields = hashes
                    .call_hrandfield(
                        accessor,
                        conn,
                        key(),
                        Some(HrandfieldOpts {
                            count: Some(2),
                            with_values: Some(true),
                        }),
                    )
                    .await??
                    .expect("fields");
                assert_eq!(fields.len(), 2);
                assert!(fields.iter().all(|(_, value)| value.is_some()));
                let (cursor, mut scanned) = hashes
                    .call_hscan(
                        accessor,
                        conn,
                        key(),
                        None,
                        Some(HscanOpts {
                            match_: None,
                            count: None,
                            no_values: None,
                        }),
                    )
                    .await??;
                scanned.sort();
                assert_eq!(cursor, None);
                assert_eq!(
                    scanned,
                    vec![(s("f"), Some(s("v"))), (s("g"), Some(s("w")))]
                );
                assert_eq!(
                    hashes
                        .call_hincrbyfloat(accessor, conn, key(), s("n"), 1.5)
                        .await??,
                    s("1.5")
                );
                assert_eq!(
                    hashes
                        .call_hexpire(
                            accessor,
                            conn,
                            key(),
                            60_000_000_000,
                            None,
                            vec![s("f"), s("x")]
                        )
                        .await??,
                    vec![
                        hash_commands::ExpireResult::Applied,
                        hash_commands::ExpireResult::DoesNotExist
                    ]
                );
                keyspace.call_del(accessor, conn, key()).await??;

                assert_eq!(
                    exports
                        .pubsub_commands()
                        .call_publish(accessor, conn, s("commands-test"), s("hello"))
                        .await??,
                    0
                );
                let pass = exports
                    .admin_commands()
                    .call_acl_genpass(accessor, conn)
                    .await??;
                assert_eq!(pass.len(), 64);
                Ok(())
            })
            .await
    }
}
