//! Tests for the `tcp-connector` component, composed with `tcp` and `connection` as `test-tcp`,
//! configured with `wasi:config`, against a server played by the test over TCP.

use test_harness::{Client, Error, Harness, Server, command, hello_reply, ok, setinfo};

/// Answer the handshake of a connection, expecting `hello`.
async fn handshake(client: &mut Client, hello: &[&str]) {
    assert_eq!(client.request().await, Some(command(hello)));
    for setinfo in setinfo() {
        assert_eq!(client.request().await, Some(setinfo));
    }
    client.reply(hello_reply()).await;
    for _ in setinfo() {
        client.reply(ok()).await;
    }
}

/// Connect with the configuration, answering the handshake with `hello` expected.
async fn connects_with(config: &[(&str, &str)], hello: &[&str]) -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let port = server.port().to_string();
    let mut config = config.to_vec();
    config.push(("port", &port));
    let mut harness = Harness::with_config("test-tcp", &config).await?;
    harness
        .run(async |accessor, exports| {
            let connect = exports.connector().call_connect(accessor);
            let serve = async {
                let mut client = server.accept().await.expect("accept");
                handshake(&mut client, hello).await;
                client
            };
            let (connection, _client) = tokio::join!(connect, serve);
            assert!(connection?.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn connects_to_the_configured_server() -> wasmtime::Result<()> {
    connects_with(&[("hostname", "localhost")], &["HELLO", "3"]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticates_and_names_the_connection() -> wasmtime::Result<()> {
    let config = [
        ("username", "user"),
        ("password", "secret"),
        ("client-name", "tests"),
    ];
    let hello = ["HELLO", "3", "AUTH", "user", "secret", "SETNAME", "tests"];
    connects_with(&config, &hello).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_password_alone_is_for_the_default_user() -> wasmtime::Result<()> {
    let hello = ["HELLO", "3", "AUTH", "default", "secret"];
    connects_with(&[("password", "secret")], &hello).await
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_alive_pings_an_idle_connection() -> wasmtime::Result<()> {
    let server = Server::bind().await?;
    let port = server.port().to_string();
    let config = [("port", port.as_str()), ("keep-alive-interval-ms", "20")];
    let mut harness = Harness::with_config("test-tcp", &config).await?;
    harness
        .run(async |accessor, exports| {
            let connect = exports.connector().call_connect(accessor);
            let serve = async {
                let mut client = server.accept().await.expect("accept");
                handshake(&mut client, &["HELLO", "3"]).await;
                client
            };
            let (connection, mut client) = tokio::join!(connect, serve);
            assert!(connection?.is_ok());
            assert_eq!(client.request().await, Some(command(&["PING"])));
            Ok(())
        })
        .await
}

/// The error connecting with an invalid configuration, nothing is sent to a server.
async fn fails_with(config: &[(&str, &str)]) -> wasmtime::Result<Error> {
    let mut harness = Harness::with_config("test-tcp", config).await?;
    harness
        .run(async |accessor, exports| {
            let result = exports.connector().call_connect(accessor).await?;
            Ok(result.err().expect("an error"))
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_configuration_fails_to_connect() -> wasmtime::Result<()> {
    assert_eq!(
        fails_with(&[("port", "http")]).await?,
        Error::Client("port must be a port number: http".to_string())
    );
    assert_eq!(
        fails_with(&[("keep-alive-interval-ms", "soon")]).await?,
        Error::Client("keep-alive-interval-ms must be a number of milliseconds: soon".to_string())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_servers_fail_to_connect() -> wasmtime::Result<()> {
    // a port nothing listens on
    let port = Server::bind().await?.port().to_string();
    let error = fails_with(&[("port", &port)]).await?;
    assert!(matches!(error, Error::Closed(Some(_))), "{error:?}");
    Ok(())
}
