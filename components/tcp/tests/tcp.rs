//! Tests for the `tcp` component, composed with `connection` and `resp` as `test-tcp`, against a
//! server played by the test over TCP.

use test_harness::{
    Client, Ctx, Error, Exports, Harness, Server, Value, bulk, command, hello_reply, resolve, words,
};
use wasmtime::component::{Accessor, ResourceAny};

/// Connect to the server and answer HELLO, returning the server's side and the connection.
async fn establish(
    accessor: &Accessor<Ctx>,
    exports: &Exports,
    server: &Server,
    host: &str,
) -> wasmtime::Result<(Client, ResourceAny)> {
    let connect = exports
        .tcp()
        .call_connect(accessor, host.to_string(), server.port(), None);
    let serve = async {
        let mut client = server.accept().await.expect("accept");
        assert_eq!(client.request().await, Some(command(&["HELLO", "3"])));
        client.reply(hello_reply()).await;
        client
    };
    let (connection, client) = tokio::join!(connect, serve);
    let connection = connection?.map_err(|error| wasmtime::format_err!("{error:?}"))?;
    Ok((client, connection))
}

#[tokio::test(flavor = "multi_thread")]
async fn connects_and_exchanges() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            let (mut client, connection) =
                establish(accessor, exports, &server, "127.0.0.1").await?;
            let guest = exports.connection();
            let call = guest.call_exchange(accessor, connection, words(&["PING"]));
            let serve = async {
                assert_eq!(client.request().await, Some(command(&["PING"])));
                client.reply(Value::String("PONG".to_string())).await;
            };
            let (result, ()) = tokio::join!(call, serve);
            assert_eq!(result?, Ok(Value::String("PONG".to_string())));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn resolves_hostnames() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            // localhost may resolve to ::1 first, which nothing listens on, the next address is tried
            establish(accessor, exports, &server, "localhost").await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn replies_may_span_many_reads() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            let (mut client, connection) =
                establish(accessor, exports, &server, "127.0.0.1").await?;
            let value: String = (0..200_000)
                .map(|n| char::from(b'a' + (n % 26) as u8))
                .collect();
            let guest = exports.connection();
            let call = guest.call_exchange(accessor, connection, words(&["GET", "big"]));
            let serve = async {
                client.request().await;
                let reply = test_harness::encode(bulk(&value));
                for chunk in reply.chunks(1000) {
                    client.write(chunk).await;
                    tokio::task::yield_now().await;
                }
            };
            let (result, ()) = tokio::join!(call, serve);
            assert_eq!(result?, Ok(bulk(&value)));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn replies_may_share_a_read() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            let (mut client, connection) =
                establish(accessor, exports, &server, "127.0.0.1").await?;
            let guest = exports.connection();
            let first = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let second = guest.call_exchange(accessor, connection, words(&["GET", "b"]));
            let serve = async {
                client.request().await;
                client.request().await;
                let mut replies = test_harness::encode(bulk("first"));
                replies.extend(test_harness::encode(bulk("second")));
                client.write(&replies).await;
            };
            let (first, second, ()) = tokio::join!(first, second, serve);
            assert_eq!(first?, Ok(bulk("first")));
            assert_eq!(second?, Ok(bulk("second")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn server_closing_fails_exchanges() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            let (client, connection) = establish(accessor, exports, &server, "127.0.0.1").await?;
            client.close().await;
            let result = exports
                .connection()
                .call_exchange(accessor, connection, words(&["PING"]))
                .await?;
            assert!(
                matches!(result, Err(Error::Closed(_))),
                "expected a closed error, got {result:?}"
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_connections_fail_to_connect() -> wasmtime::Result<()> {
    // a port nothing listens on
    let port = Server::bind().await?.port();
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            let result = exports
                .tcp()
                .call_connect(accessor, "127.0.0.1".to_string(), port, None)
                .await?;
            let Err(Error::Closed(Some(reason))) = result else {
                panic!("expected a closed error, got {result:?}");
            };
            assert!(
                reason.starts_with("unable to connect to 127.0.0.1"),
                "{reason}"
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn transports_wire_to_connections() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    harness
        .run(async |accessor, exports| {
            let transport = exports.tcp().transport();
            let open = transport.call_open(accessor, "127.0.0.1".to_string(), server.port());
            let (transport_handle, client) =
                tokio::join!(open, async { server.accept().await.expect("accept") });
            let transport_handle =
                transport_handle?.map_err(|error| wasmtime::format_err!("{error:?}"))?;
            let mut client = client;

            let (replies, closed) = transport.call_receive(accessor, transport_handle).await?;
            let (requests, connection) = exports
                .connection()
                .call_new(accessor, replies, closed, None)
                .await?;
            let sent = transport
                .call_send(accessor, transport_handle, requests)
                .await?;
            drop(sent);

            let serve = async {
                assert_eq!(client.request().await, Some(command(&["HELLO", "3"])));
                client.reply(hello_reply()).await;
            };
            let (connection, ()) = tokio::join!(resolve(accessor, connection), serve);
            assert!(connection?.is_ok());

            // each stream is handed over once
            let (_, again) = transport.call_receive(accessor, transport_handle).await?;
            assert!(matches!(
                resolve(accessor, again).await?,
                Err(Error::Client(_))
            ));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_connection_closes_the_socket() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let mut harness = Harness::new("test-tcp").await?;
    let (mut client, connection) = harness
        .run(async |accessor, exports| establish(accessor, exports, &server, "127.0.0.1").await)
        .await?;
    harness.drop_resource(connection).await?;
    harness
        .run(async |_accessor, _exports| {
            assert_eq!(client.request().await, None);
            Ok(())
        })
        .await
}

/// The tests against a Valkey server on localhost:6379, ignored unless run with
/// `make test-valkey`, which only runs the ignored tests in `with_valkey` modules.
mod with_valkey {
    use super::*;

    /// Against a Valkey server on localhost:6379, run with `cargo test -p tcp -- --ignored with_valkey::`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server on localhost:6379"]
    async fn against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::new("test-tcp").await?;
        harness
            .run(async |accessor, exports| {
                let connection = exports
                    .tcp()
                    .call_connect(accessor, "localhost".to_string(), 6379, None)
                    .await?
                    .map_err(|error| wasmtime::format_err!("{error:?}"))?;
                let guest = exports.connection();
                let properties = guest.call_server_properties(accessor, connection).await?;
                assert!(properties.contains(&("proto".to_string(), Value::Integer(3))));

                let value: String = (0..100_000)
                    .map(|n| char::from(b'a' + (n % 26) as u8))
                    .collect();
                let set = guest
                    .call_exchange(accessor, connection, words(&["SET", "tcp-test", &value]))
                    .await?;
                assert_eq!(set, Ok(Value::String("OK".to_string())));
                let get = guest
                    .call_exchange(accessor, connection, words(&["GET", "tcp-test"]))
                    .await?;
                assert_eq!(get, Ok(bulk(&value)));
                let del = guest
                    .call_exchange(accessor, connection, words(&["DEL", "tcp-test"]))
                    .await?;
                assert_eq!(del, Ok(Value::Integer(1)));
                Ok(())
            })
            .await
    }
}
