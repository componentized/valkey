//! Tests for the `connection` component, over a stub transport playing the server.

use std::time::Duration;

use test_harness::{
    ConnectOpts, Error, Harness, Value, bulk, command, connect, establish, hello_reply, push,
    receive, resolve,
};

/// Long enough for the connection to act on what it was given, short enough to keep tests quick.
const SETTLE: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn establishes_with_hello() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (_transport, connection) = establish(accessor, exports).await?;
            let properties = guest.call_server_properties(accessor, connection).await?;
            assert_eq!(
                properties,
                vec![
                    ("server".to_string(), bulk("valkey")),
                    ("version".to_string(), bulk("9.1.2")),
                    ("proto".to_string(), Value::Integer(3)),
                ]
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_carries_auth_and_client_name() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let opts = ConnectOpts {
                auth: Some(("user".to_string(), "secret".to_string())),
                client_name: Some("tests".to_string()),
                keep_alive_interval: None,
            };
            let (mut transport, connection) = connect(accessor, exports, Some(opts)).await?;
            assert_eq!(
                transport.request().await,
                Some(command(&[
                    "HELLO", "3", "AUTH", "user", "secret", "SETNAME", "tests"
                ]))
            );
            transport.reply(hello_reply());
            assert!(resolve(accessor, connection).await?.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_error_fails_to_establish() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, connection) = connect(accessor, exports, None).await?;
            transport.request().await;
            transport.reply(Value::Error("WRONGPASS invalid password".to_string()));
            let Err(error) = resolve(accessor, connection).await? else {
                panic!("established despite the error reply");
            };
            assert_eq!(
                error,
                Error::Valkey("WRONGPASS invalid password".to_string())
            );
            // the connection is dropped, which closes the requests stream
            assert_eq!(transport.request().await, None);
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_before_hello_fails_to_establish() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let (mut transport, connection) = connect(accessor, exports, None).await?;
            transport.request().await;
            transport.close(Err(Error::Closed(Some("connection refused".to_string()))));
            assert_eq!(
                resolve(accessor, connection).await?.err(),
                Some(Error::Closed(Some("connection refused".to_string())))
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_are_paired_with_replies_in_order() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let first = guest.call_call(accessor, connection, command(&["GET", "a"]));
            let second = guest.call_call(accessor, connection, command(&["GET", "b"]));
            let server = async {
                // both are in flight before either is answered
                assert_eq!(transport.request().await, Some(command(&["GET", "a"])));
                assert_eq!(transport.request().await, Some(command(&["GET", "b"])));
                transport.reply(bulk("first"));
                transport.reply(bulk("second"));
            };
            let (first, second, ()) = tokio::join!(first, second, server);
            assert_eq!(first?, Ok(bulk("first")));
            assert_eq!(second?, Ok(bulk("second")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn error_replies_fail_the_call() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let call = guest.call_call(accessor, connection, command(&["INCR", "text"]));
            let server = async {
                transport.request().await;
                transport.reply(Value::Error("ERR value is not an integer".to_string()));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(
                result?,
                Err(Error::Valkey("ERR value is not an integer".to_string()))
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn send_discards_its_reply() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            guest
                .call_send(accessor, connection, command(&["SET", "a", "1"]))
                .await??;
            let call = guest.call_call(accessor, connection, command(&["GET", "a"]));
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["SET", "a", "1"])));
                assert_eq!(transport.request().await, Some(command(&["GET", "a"])));
                // the reply to SET is discarded, even an error
                transport.reply(Value::Error("ERR discarded".to_string()));
                transport.reply(bulk("1"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(bulk("1")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn push_messages_go_to_every_reader() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let first = guest.call_pushes(accessor, connection).await?;
            let second = guest.call_pushes(accessor, connection).await?;
            let mut first = receive(accessor, first)?;
            let mut second = receive(accessor, second)?;

            let message = push(vec![bulk("message"), bulk("news"), bulk("hello")]);
            let call = guest.call_call(accessor, connection, command(&["PING"]));
            let server = async {
                transport.request().await;
                // a push message arriving ahead of a reply is not mistaken for it
                transport.reply(message.clone());
                transport.reply(Value::String("PONG".to_string()));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(Value::String("PONG".to_string())));
            assert_eq!(first.recv().await, Some(message.clone()));
            assert_eq!(second.recv().await, Some(message));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_push_readers_are_closed() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            // a reader that never reads
            let _slow = guest.call_pushes(accessor, connection).await?;
            for n in 0..1000 {
                transport.reply(push(vec![
                    bulk("message"),
                    bulk("news"),
                    bulk(&n.to_string()),
                ]));
            }
            // replies are not held up behind the reader
            let call = guest.call_call(accessor, connection, command(&["PING"]));
            let server = async {
                transport.request().await;
                transport.reply(Value::String("PONG".to_string()));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(Value::String("PONG".to_string())));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_fails_waiting_and_later_calls() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let reset = Error::Closed(Some("connection reset".to_string()));
            let call = guest.call_call(accessor, connection, command(&["GET", "a"]));
            let server = async {
                transport.request().await;
                transport.close(Err(reset.clone()));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Err(reset.clone()));
            let later = guest
                .call_call(accessor, connection, command(&["GET", "a"]))
                .await?;
            assert_eq!(later, Err(reset));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_alive_pings_an_idle_connection() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let interval = Duration::from_millis(20).as_nanos() as u64;
            guest
                .call_set_keep_alive(accessor, connection, Some(interval))
                .await?;
            assert_eq!(transport.request().await, Some(command(&["PING"])));
            transport.reply(Value::String("PONG".to_string()));
            assert_eq!(transport.request().await, Some(command(&["PING"])));
            transport.reply(Value::String("PONG".to_string()));

            // disabled again, no more pings
            guest
                .call_set_keep_alive(accessor, connection, None)
                .await?;
            let quiet = tokio::time::timeout(SETTLE, transport.request()).await;
            assert!(quiet.is_err(), "pinged after keep-alive was disabled");
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_connection_closes_the_requests() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    let (mut transport, connection) = harness
        .run(async |accessor, exports| establish(accessor, exports).await)
        .await?;
    harness.drop_resource(connection).await?;
    // the guest closes the requests stream once its tasks run again
    harness
        .run(async |_accessor, _exports| {
            assert_eq!(transport.request().await, None);
            Ok(())
        })
        .await
}
