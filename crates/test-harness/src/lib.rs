//! Test harness for the valkey components.
//!
//! A [`Harness`] instantiates a component (`target/components/*/*.wasm`) with wasmtime. No Valkey
//! server is needed, the test plays the server:
//!
//! - over a stub [`Transport`], reading the commands a connection writes, and deciding when each
//!   reply, push message or close arrives
//! - over TCP with a [`Server`], for components that open sockets, e.g. `tcp`

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::sync::Mutex;
use std::task::{Context, Poll};

use tokio::sync::{mpsc, oneshot};
use wasmtime::component::{
    Accessor, Component, Destination, FutureConsumer, FutureReader, HasSelf, Lift, Linker,
    ResourceAny, ResourceTable, Source, StreamConsumer, StreamProducer, StreamReader, StreamResult,
};
use wasmtime::error::Context as _;
use wasmtime::{Config, Engine, Result, Store, StoreContextMut, bail, format_err};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

pub use bindings::componentized::valkey::types::{
    self, ConnectOpts, Error, RespNestedValue, RespValue, StringOrBytes,
};

mod server;

pub use server::{Client, Server};

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "../../components/wit",
        inline: "
            package componentized:test-harness;

            world harness {
                import componentized:valkey/types@0.3.0-dev;
                import wasi:config/store@0.2.0-rc.1;
                export componentized:valkey/connection@0.3.0-dev;
                export componentized:valkey/tcp@0.3.0-dev;
                export componentized:valkey/connector@0.3.0-dev;
                export componentized:valkey-components/test-suite;
                export wasmcloud:keyvalue/types@0.2.0;
                export wasmcloud:keyvalue/store@0.2.0;
                export wasmcloud:keyvalue/atomics@0.2.0;
                export wasmcloud:keyvalue/batch@0.2.0;
            }
        ",
        world: "componentized:test-harness/harness",
        exports: { default: async | store },
        additional_derives: [PartialEq],
    });
}

/// Encode a value as RESP, e.g. to nest it in an aggregate.
pub fn encode(value: RespValue) -> RespNestedValue {
    componentized_valkey::resp::encode(to_codec(value))
}

/// Decode a nested value.
pub fn decode(value: RespNestedValue) -> Result<RespValue, String> {
    componentized_valkey::resp::decode(&value).map(from_codec)
}

// a value is not recursive, nested values are encoded RESP, so each conversion is one match
fn to_codec(value: RespValue) -> componentized_valkey::types::RespValue {
    use componentized_valkey::types::RespValue as V;
    match value {
        RespValue::String(value) => V::String(value),
        RespValue::Error(value) => V::Error(value),
        RespValue::Integer(value) => V::Integer(value),
        RespValue::BulkString(value) => V::BulkString(value),
        RespValue::Array(value) => V::Array(value),
        RespValue::Null => V::Null,
        RespValue::Boolean(value) => V::Boolean(value),
        RespValue::Double(value) => V::Double(value),
        RespValue::BigNumber(value) => V::BigNumber(value),
        RespValue::BulkError(value) => V::BulkError(value),
        RespValue::VerbatimString(value) => V::VerbatimString(value),
        RespValue::Map(value) => V::Map(value),
        RespValue::Set(value) => V::Set(value),
        RespValue::Push(value) => V::Push(value),
    }
}

fn from_codec(value: componentized_valkey::types::RespValue) -> RespValue {
    use componentized_valkey::types::RespValue as V;
    match value {
        V::String(value) => RespValue::String(value),
        V::Error(value) => RespValue::Error(value),
        V::Integer(value) => RespValue::Integer(value),
        V::BulkString(value) => RespValue::BulkString(value),
        V::Array(value) => RespValue::Array(value),
        V::Null => RespValue::Null,
        V::Boolean(value) => RespValue::Boolean(value),
        V::Double(value) => RespValue::Double(value),
        V::BigNumber(value) => RespValue::BigNumber(value),
        V::BulkError(value) => RespValue::BulkError(value),
        V::VerbatimString(value) => RespValue::VerbatimString(value),
        V::Map(value) => RespValue::Map(value),
        V::Set(value) => RespValue::Set(value),
        V::Push(value) => RespValue::Push(value),
    }
}

pub type Connection<'a> = bindings::exports::componentized::valkey::connection::GuestConnection<'a>;
pub type Subscription<'a> =
    bindings::exports::componentized::valkey::connection::GuestSubscription<'a>;
pub type Tcp = bindings::exports::componentized::valkey::tcp::Guest;
pub type Connector = bindings::exports::componentized::valkey::connector::Guest;
/// A suite of tests run inside a component, e.g. those of the commands, see [`run_suite`].
pub type TestSuite = bindings::exports::componentized::valkey_components::test_suite::Guest;
pub use bindings::exports::wasmcloud::keyvalue;

/// A bulk string, the type commands are sent as.
pub fn bulk(value: &str) -> RespValue {
    RespValue::BulkString(value.as_bytes().to_vec())
}

/// The words of a command to send, e.g. `words(&["GET", "key"])`, see `connection.exchange`.
pub fn words(words: &[&str]) -> Vec<StringOrBytes> {
    words
        .iter()
        .map(|word| StringOrBytes::String(word.to_string()))
        .collect()
}

/// Strings, e.g. the keys of `connection.watch`, or the words of a command of a transaction.
pub fn strings(words: &[&str]) -> Vec<String> {
    words.iter().map(|word| word.to_string()).collect()
}

/// A command as the server receives it, e.g. `command(&["GET", "key"])`.
pub fn command(words: &[&str]) -> Vec<RespValue> {
    words.iter().map(|word| bulk(word)).collect()
}

/// A map reply, e.g. to HELLO.
pub fn map(entries: Vec<(RespValue, RespValue)>) -> RespValue {
    RespValue::Map(
        entries
            .into_iter()
            .map(|(key, value)| (encode(key), encode(value)))
            .collect(),
    )
}

/// A push message, e.g. a pub/sub message.
pub fn push(items: Vec<RespValue>) -> RespValue {
    RespValue::Push(items.into_iter().map(encode).collect())
}

/// A simple string `OK`.
pub fn ok() -> RespValue {
    RespValue::String("OK".to_string())
}

/// The server's properties in the canned reply to HELLO.
pub fn hello_reply() -> RespValue {
    map(vec![
        (bulk("server"), bulk("valkey")),
        (bulk("version"), bulk("9.1.2")),
        (bulk("proto"), RespValue::Integer(3)),
    ])
}

/// The stub transport and server of a connection.
pub struct Transport {
    requests: mpsc::UnboundedReceiver<RespValue>,
    replies: Option<mpsc::UnboundedSender<RespValue>>,
    closed: Option<oneshot::Sender<Result<(), Error>>>,
}

impl Transport {
    /// The next command the connection wrote, as its words, `None` once it closed the requests
    /// stream.
    pub async fn request(&mut self) -> Option<Vec<RespValue>> {
        let request = self.requests.recv().await?;
        let RespValue::Array(words) = request else {
            panic!("a command is an array, got {request:?}");
        };
        Some(
            words
                .into_iter()
                .map(|word| decode(word).expect("encoded word"))
                .collect(),
        )
    }

    /// Send a value to the connection, a reply or a push message.
    pub fn reply(&self, value: RespValue) {
        let replies = self.replies.as_ref().expect("transport is open");
        replies.send(value).expect("connection reads replies");
    }

    /// Close the transport, gracefully with `Ok(())`, or with the error.
    pub fn close(&mut self, result: Result<(), Error>) {
        self.replies.take();
        if let Some(closed) = self.closed.take() {
            let _ = closed.send(result);
        }
    }
}

/// Start a connection over a stub transport, returning the transport and the connection the
/// guest resolves once it is established.
pub async fn connect(
    accessor: &Accessor<Ctx>,
    exports: &Exports,
    opts: Option<ConnectOpts>,
) -> Result<(Transport, FutureReader<Result<ResourceAny, Error>>)> {
    let (replies_tx, replies_rx) = mpsc::unbounded_channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let replies = accessor.with(|store| StreamReader::new(store, ChannelProducer(replies_rx)))?;
    let closed = accessor.with(|store| {
        FutureReader::new(store, async move {
            // a transport dropped without closing it closed gracefully
            Ok::<_, wasmtime::Error>(closed_rx.await.unwrap_or(Ok(())))
        })
    })?;
    let (requests, connection) = exports
        .connection()
        .call_new(accessor, replies, closed, opts)
        .await?;
    let (requests_tx, requests_rx) = mpsc::unbounded_channel();
    accessor.with(|store| requests.pipe(store, ChannelConsumer(requests_tx)))?;
    Ok((
        Transport {
            requests: requests_rx,
            replies: Some(replies_tx),
            closed: Some(closed_tx),
        },
        connection,
    ))
}

/// The commands the connection sends when it is established, after HELLO, to report the library.
pub fn setinfo() -> [Vec<RespValue>; 2] {
    [
        command(&["CLIENT", "SETINFO", "LIB-NAME", "componentized:valkey"]),
        command(&["CLIENT", "SETINFO", "LIB-VER", env!("CARGO_PKG_VERSION")]),
    ]
}

/// Answer the handshake of a connection: HELLO with [`hello_reply`], then the CLIENT SETINFO
/// commands with OK.
pub async fn handshake(transport: &mut Transport, hello: &[&str]) {
    assert_eq!(transport.request().await, Some(command(hello)));
    for setinfo in setinfo() {
        assert_eq!(transport.request().await, Some(setinfo));
    }
    transport.reply(hello_reply());
    for _ in setinfo() {
        transport.reply(ok());
    }
}

/// Start a connection and answer its handshake, see [`handshake`], returning the transport and the
/// established connection.
pub async fn establish(
    accessor: &Accessor<Ctx>,
    exports: &Exports,
) -> Result<(Transport, ResourceAny)> {
    let (mut transport, connection) = connect(accessor, exports, None).await?;
    handshake(&mut transport, &["HELLO", "3"]).await;
    let connection = resolve(accessor, connection)
        .await?
        .map_err(|error| format_err!("failed to establish the connection: {error:?}"))?;
    Ok((transport, connection))
}

/// Wait for the value of a guest future.
pub async fn resolve<T: Lift + Send + Sync + 'static>(
    accessor: &Accessor<Ctx>,
    future: FutureReader<T>,
) -> Result<T> {
    let (tx, rx) = oneshot::channel();
    accessor.with(|store| future.pipe(store, OneshotConsumer(Some(tx))))?;
    rx.await
        .map_err(|_| format_err!("future closed without a value"))
}

/// Read every item of a guest stream into a channel, until it closes.
pub fn receive<T: Lift + Send + Sync + 'static>(
    accessor: &Accessor<Ctx>,
    stream: StreamReader<T>,
) -> Result<mpsc::UnboundedReceiver<T>> {
    let (tx, rx) = mpsc::unbounded_channel();
    accessor.with(|store| stream.pipe(store, ChannelConsumer(tx)))?;
    Ok(rx)
}

/// Produces the items sent to the channel, until every sender is dropped.
struct ChannelProducer<T>(mpsc::UnboundedReceiver<T>);

impl<D, T: Send + Sync + 'static> StreamProducer<D> for ChannelProducer<T> {
    type Item = T;
    type Buffer = Option<T>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _store: StoreContextMut<'a, D>,
        mut destination: Destination<'a, T, Option<T>>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        match self.get_mut().0.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                destination.set_buffer(Some(item));
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending if finish => Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct OneshotConsumer<T>(Option<oneshot::Sender<T>>);

impl<D, T: Lift + Send + Sync + 'static> FutureConsumer<D> for OneshotConsumer<T> {
    type Item = T;

    fn poll_consume(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        mut source: Source<'_, T>,
        _finish: bool,
    ) -> Poll<Result<()>> {
        let mut item = None;
        source.read(store, &mut item)?;
        if let (Some(item), Some(tx)) = (item, self.get_mut().0.take()) {
            // the receiver is only dropped when the test stopped waiting
            let _ = tx.send(item);
        }
        Poll::Ready(Ok(()))
    }
}

struct ChannelConsumer<T>(mpsc::UnboundedSender<T>);

impl<D, T: Lift + Send + Sync + 'static> StreamConsumer<D> for ChannelConsumer<T> {
    type Item = T;

    fn poll_consume(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        mut source: Source<'_, T>,
        _finish: bool,
    ) -> Poll<Result<StreamResult>> {
        let mut item = None;
        source.read(store, &mut item)?;
        if let Some(item) = item {
            if self.0.send(item).is_err() {
                return Poll::Ready(Ok(StreamResult::Dropped));
            }
        }
        Poll::Ready(Ok(StreamResult::Completed))
    }
}

/// Root of the workspace, where the Makefile lives.
fn workspace_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    dir.canonicalize().unwrap_or(dir)
}

/// Path containing the built component.
pub fn component_path(name: &str) -> PathBuf {
    workspace_dir().join(format!("target/components/{name}/{name}.wasm"))
}

/// Rebuild the named component in `target/components/` with make, unless it was already built
/// by this process.
fn ensure_built(name: &str) -> Result<()> {
    static BUILT: Mutex<Option<HashSet<String>>> = Mutex::new(None);

    // hold the lock while building so concurrent tests don't run make over each other
    let mut built = BUILT.lock().unwrap_or_else(|err| err.into_inner());
    let built = built.get_or_insert_with(HashSet::new);
    if built.contains(name) {
        return Ok(());
    }

    // the Makefile's targets are relative to the workspace, make matches them textually
    let target = format!("target/components/{name}/{name}.wasm");
    let output = Command::new("make")
        .arg("-C")
        .arg(workspace_dir())
        .arg(&target)
        .output()
        .context("failed to run make")?;
    if !output.status.success() {
        bail!(
            "failed to build {target}:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    built.insert(name.to_string());
    Ok(())
}

pub struct Ctx {
    wasi: WasiCtx,
    table: ResourceTable,
    /// The values of `wasi:config/store`.
    config: Vec<(String, String)>,
}

impl bindings::wasi::config::store::Host for Ctx {
    fn get(&mut self, key: String) -> Result<Option<String>, bindings::wasi::config::store::Error> {
        Ok(self
            .config
            .iter()
            .rev()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone()))
    }

    fn get_all(&mut self) -> Result<Vec<(String, String)>, bindings::wasi::config::store::Error> {
        Ok(self.config.clone())
    }
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// The interfaces a component exports.
pub struct Exports {
    connection: Option<bindings::exports::componentized::valkey::connection::Guest>,
    tcp: Option<Tcp>,
    connector: Option<Connector>,
    test_suite: Option<TestSuite>,
    keyvalue_store: Option<keyvalue::store::Guest>,
    keyvalue_types: Option<keyvalue::types::Guest>,
    keyvalue_atomics: Option<keyvalue::atomics::Guest>,
    keyvalue_batch: Option<keyvalue::batch::Guest>,
}

impl Exports {
    pub fn keyvalue_store(&self) -> &keyvalue::store::Guest {
        self.keyvalue_store
            .as_ref()
            .expect("component exports wasmcloud:keyvalue/store")
    }

    pub fn keyvalue_types(&self) -> &keyvalue::types::Guest {
        self.keyvalue_types
            .as_ref()
            .expect("component exports wasmcloud:keyvalue/types")
    }

    pub fn keyvalue_atomics(&self) -> &keyvalue::atomics::Guest {
        self.keyvalue_atomics
            .as_ref()
            .expect("component exports wasmcloud:keyvalue/atomics")
    }

    pub fn keyvalue_batch(&self) -> &keyvalue::batch::Guest {
        self.keyvalue_batch
            .as_ref()
            .expect("component exports wasmcloud:keyvalue/batch")
    }

    pub fn test_suite(&self) -> &TestSuite {
        self.test_suite
            .as_ref()
            .expect("component exports componentized:valkey-components/test-suite")
    }

    pub fn connector(&self) -> &Connector {
        self.connector
            .as_ref()
            .expect("component exports componentized:valkey/connector")
    }

    pub fn tcp(&self) -> &Tcp {
        self.tcp
            .as_ref()
            .expect("component exports componentized:valkey/tcp")
    }

    pub fn connection(&self) -> Connection<'_> {
        self.connection
            .as_ref()
            .expect("component exports componentized:valkey/connection")
            .connection()
    }

    pub fn subscription(&self) -> Subscription<'_> {
        self.connection
            .as_ref()
            .expect("component exports componentized:valkey/connection")
            .subscription()
    }
}

/// An instantiated component, see [`Harness::run`].
pub struct Harness {
    store: Store<Ctx>,
    exports: Exports,
}

impl Harness {
    /// Build and instantiate the named component, e.g. `connection`.
    pub async fn new(name: &str) -> Result<Self> {
        Self::with_config(name, &[]).await
    }

    /// Build and instantiate the named component, with the values of `wasi:config/store`.
    pub async fn with_config(name: &str, config_values: &[(&str, &str)]) -> Result<Self> {
        ensure_built(name)?;

        let mut config = Config::new();
        config.wasm_component_model_map(true);
        let engine = Engine::new(&config)?;
        let component = Component::from_file(&engine, component_path(name))
            .with_context(|| format!("failed to load {name}"))?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p3::add_to_linker(&mut linker)?;
        bindings::wasi::config::store::add_to_linker::<_, HasSelf<Ctx>>(&mut linker, |ctx| ctx)?;

        let mut store = Store::new(
            &engine,
            Ctx {
                wasi: WasiCtxBuilder::new()
                    .inherit_stdio()
                    // for components that open sockets, the tests' servers listen on localhost
                    .inherit_network()
                    .allow_tcp(true)
                    .allow_ip_name_lookup(true)
                    .build(),
                table: ResourceTable::new(),
                config: config_values
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            },
        );
        let instance_pre = linker.instantiate_pre(&component)?;
        let instance = instance_pre
            .instantiate_async(&mut store)
            .await
            .with_context(|| format!("failed to instantiate {name}"))?;
        let exports = Exports {
            connection: bindings::exports::componentized::valkey::connection::GuestIndices::new(
                &instance_pre,
            )
            .ok()
            .and_then(|indices| indices.load(&mut store, &instance).ok()),
            tcp: bindings::exports::componentized::valkey::tcp::GuestIndices::new(&instance_pre)
                .ok()
                .and_then(|indices| indices.load(&mut store, &instance).ok()),
            connector: bindings::exports::componentized::valkey::connector::GuestIndices::new(
                &instance_pre,
            )
            .ok()
            .and_then(|indices| indices.load(&mut store, &instance).ok()),
            test_suite:
                bindings::exports::componentized::valkey_components::test_suite::GuestIndices::new(
                    &instance_pre,
                )
                .ok()
                .and_then(|indices| indices.load(&mut store, &instance).ok()),
            keyvalue_store: keyvalue::store::GuestIndices::new(&instance_pre)
                .ok()
                .and_then(|indices| indices.load(&mut store, &instance).ok()),
            keyvalue_types: keyvalue::types::GuestIndices::new(&instance_pre)
                .ok()
                .and_then(|indices| indices.load(&mut store, &instance).ok()),
            keyvalue_atomics: keyvalue::atomics::GuestIndices::new(&instance_pre)
                .ok()
                .and_then(|indices| indices.load(&mut store, &instance).ok()),
            keyvalue_batch: keyvalue::batch::GuestIndices::new(&instance_pre)
                .ok()
                .and_then(|indices| indices.load(&mut store, &instance).ok()),
        };
        Ok(Self { store, exports })
    }

    /// Build and instantiate the named component, its connector configured for the Valkey server
    /// of the `with_valkey` tests, see [`valkey_server`], with the values of `wasi:config`.
    pub async fn with_valkey(name: &str, config_values: &[(&str, &str)]) -> Result<Self> {
        let (host, port) = valkey_server();
        let port = port.to_string();
        let mut values = vec![("hostname", host.as_str()), ("port", port.as_str())];
        values.extend_from_slice(config_values);
        Self::with_config(name, &values).await
    }

    /// Drop a resource the component exports, running its destructor.
    ///
    /// Between runs, the store is only free to drop a resource while no run is in progress.
    pub async fn drop_resource(&mut self, resource: ResourceAny) -> Result<()> {
        resource.resource_drop_async(&mut self.store).await
    }

    /// Run a test body against the component's exports.
    pub async fn run<R: Send + 'static>(
        &mut self,
        f: impl AsyncFnOnce(&Accessor<Ctx>, &Exports) -> Result<R> + Send,
    ) -> Result<R> {
        let exports = &self.exports;
        self.store
            .run_concurrent(async move |accessor| f(accessor, exports).await)
            .await?
    }
}

/// The Valkey server of the `with_valkey` tests, from `VALKEY_HOST` and `VALKEY_PORT`, by default
/// 127.0.0.1:6379. `make test-valkey` runs one in a container, and sets them.
pub fn valkey_server() -> (String, u16) {
    let host = std::env::var("VALKEY_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("VALKEY_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(6379);
    (host, port)
}

/// Run the tests of the suite the component exports that `filter` selects, each in turn, failing
/// with every test that failed.
pub async fn run_suite(
    mut harness: Harness,
    filter: impl Fn(&str) -> bool + Send + Sync,
) -> Result<()> {
    let failures = harness
        .run(async |accessor, exports| {
            let suite = exports.test_suite();
            let mut failures = vec![];
            for test in suite.call_tests(accessor).await? {
                if !filter(&test) {
                    continue;
                }
                match suite.call_run(accessor, test.clone()).await? {
                    Ok(()) => println!("ok - {test}"),
                    Err(why) => {
                        println!("FAILED - {test}: {why}");
                        failures.push(format!("{test}: {why}"));
                    }
                }
            }
            Ok(failures)
        })
        .await?;
    if !failures.is_empty() {
        bail!("{} failed:\n{}", failures.len(), failures.join("\n"));
    }
    Ok(())
}
