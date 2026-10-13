//! The mechanics of a connection to a Valkey server, independent of the transport carrying it.
//!
//! A connection runs background tasks for as long as it is open, in the task of the call to
//! `connection.new` that created it:
//!
//! - the writer writes queued commands to the requests stream, in order
//! - the reader reads the replies stream, handing each reply to the oldest command waiting for one,
//!   and each push message, or value the server sent unprompted, to every reader of push messages
//! - the supervisor starts the work methods ask for that outlives them, e.g. keep-alive, since sync
//!   methods have no task of their own to spawn it into
//!
//! Dropping the connection drops its queue of commands, the writer then closes the requests stream
//! and the transport closes, which ends the reader.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::rc::{Rc, Weak};

use componentized_valkey::types::{NestedValue, Value};
use exports::componentized::valkey::connection::{
    ConnectOpts, Connection, DurationMilliseconds, Error, Guest, GuestConnection,
};
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use wasi::clocks::monotonic_clock::wait_for;
use wit_bindgen::{FutureReader, FutureWriter, StreamReader, StreamWriter, spawn_local};

/// How many push messages a reader may fall behind before its stream is closed.
const PUSH_BUFFER: usize = 256;

struct Component;

impl Guest for Component {
    type Connection = ConnectionImpl;
}

/// Where the reply to a command goes.
enum Reply {
    /// To the caller waiting for it.
    Call(oneshot::Sender<Result<Value, Error>>),
    /// Nowhere, the command was sent without waiting for its reply.
    Discard,
}

/// A command for the writer, with a signal for once it is written.
struct Request {
    command: Value,
    written: Option<oneshot::Sender<()>>,
}

/// Work for the supervisor to start.
enum Task {
    /// Forward push messages from the channel to a reader's stream.
    Pushes(mpsc::Receiver<Value>, StreamWriter<Value>),
    /// Keep the connection alive, until the keep-alive is set again.
    KeepAlive {
        generation: u64,
        interval: DurationMilliseconds,
    },
}

/// State shared by a connection and its background tasks.
#[derive(Default)]
struct State {
    /// Where each reply goes, in the order the commands were sent.
    pending: VecDeque<Reply>,
    /// Why the connection closed, once it has.
    closed: Option<Error>,
    /// Counts the commands written and the values read, it stays the same while the connection is
    /// idle.
    activity: u64,
    /// The readers of push messages.
    pushes: Vec<mpsc::Sender<Value>>,
    /// The server's properties from the reply to HELLO.
    server_properties: Vec<(String, Value)>,
    /// Bumped each time the keep-alive is set, a keep-alive task stops once it is outdated.
    keep_alive_generation: u64,
}

/// A connection's queues to its background tasks.
///
/// Only the connection holds it, the background tasks hold the state alone or a weak reference,
/// so dropping the connection drops the queues and ends the tasks.
struct Inner {
    state: Rc<RefCell<State>>,
    requests: mpsc::UnboundedSender<Request>,
    tasks: mpsc::UnboundedSender<Task>,
}

pub struct ConnectionImpl(Rc<Inner>);

impl GuestConnection for ConnectionImpl {
    async fn new(
        replies: StreamReader<Value>,
        closed: FutureReader<Result<(), Error>>,
        opts: Option<ConnectOpts>,
    ) -> (StreamReader<Value>, FutureReader<Result<Connection, Error>>) {
        let (output, requests) = wit_stream::new();
        // the default is only written if the connection is dropped before it is established
        let (connection_writer, connection) = wit_future::new(|| Err(Error::Closed(None)));

        let state = Rc::new(RefCell::new(State::default()));
        let (requests_tx, requests_rx) = mpsc::unbounded();
        let (tasks_tx, tasks_rx) = mpsc::unbounded();
        let inner = Rc::new(Inner {
            state: state.clone(),
            requests: requests_tx,
            tasks: tasks_tx,
        });

        spawn_local(write_requests(state.clone(), requests_rx, output));
        spawn_local(read_replies(state, replies, closed));
        spawn_local(supervise(tasks_rx, Rc::downgrade(&inner)));
        spawn_local(establish(inner, opts, connection_writer));

        (requests, connection)
    }

    fn server_properties(&self) -> Vec<(String, Value)> {
        self.0.state.borrow().server_properties.clone()
    }

    async fn exchange(&self, command: Vec<String>) -> Result<Value, Error> {
        exchange(&self.0, command).await
    }

    async fn exchange_all(&self, commands: Vec<Vec<String>>) -> Vec<Result<Value, Error>> {
        exchange_all(&self.0, commands).await
    }

    async fn emit(&self, command: Vec<String>) -> Result<(), Error> {
        emit(&self.0, command).await
    }

    async fn watch(&self, keys: Vec<String>) -> Result<(), Error> {
        if keys.is_empty() {
            return Err(Error::Client("keys requires at least one item".to_string()));
        }
        let command = std::iter::once("WATCH".to_string()).chain(keys).collect();
        ok(exchange(&self.0, command).await?)
    }

    async fn unwatch(&self) -> Result<(), Error> {
        ok(exchange(&self.0, words(&["UNWATCH"])).await?)
    }

    async fn transaction(&self, commands: Vec<Vec<String>>) -> Result<Option<Vec<Value>>, Error> {
        transaction(&self.0, commands).await
    }

    fn pushes(&self) -> StreamReader<Value> {
        let (writer, reader) = wit_stream::new();
        let (tx, rx) = mpsc::channel(PUSH_BUFFER);
        let mut state = self.0.state.borrow_mut();
        // a closed connection drops the writer, closing the stream at once
        if state.closed.is_none()
            && self
                .0
                .tasks
                .unbounded_send(Task::Pushes(rx, writer))
                .is_ok()
        {
            state.pushes.push(tx);
        }
        reader
    }

    fn set_keep_alive(&self, interval: Option<DurationMilliseconds>) {
        let generation = {
            let mut state = self.0.state.borrow_mut();
            state.keep_alive_generation += 1;
            state.keep_alive_generation
        };
        // a zero interval would ping whenever the connection is idle, treated as disabled
        if let Some(interval) = interval.filter(|interval| interval.px > 0) {
            let _ = self.0.tasks.unbounded_send(Task::KeepAlive {
                generation,
                interval,
            });
        }
    }
}

/// Queue a command, and where its reply goes, as one step so replies pair with commands in order.
/// A command without a reply takes no place in the order.
fn enqueue(
    inner: &Inner,
    command: Vec<String>,
    reply: Option<Reply>,
    written: Option<oneshot::Sender<()>>,
) -> Result<(), Error> {
    if command.is_empty() {
        return Err(Error::Client(
            "a command requires at least one word".to_string(),
        ));
    }
    let mut state = inner.state.borrow_mut();
    if let Some(error) = &state.closed {
        return Err(error.clone());
    }
    // the words of a command are bulk strings
    let command = Value::Array(
        command
            .into_iter()
            .map(|word| componentized_valkey::codec::encode(Value::BulkString(word)))
            .collect(),
    );
    inner
        .requests
        .unbounded_send(Request { command, written })
        .map_err(|_| Error::Closed(None))?;
    state.pending.extend(reply);
    Ok(())
}

async fn exchange(inner: &Inner, command: Vec<String>) -> Result<Value, Error> {
    if replies_with_pushes(&command) {
        return emit(inner, command).await.map(|()| Value::Null);
    }
    let (reply_tx, reply_rx) = oneshot::channel();
    enqueue(inner, command, Some(Reply::Call(reply_tx)), None)?;
    // the reply is dropped unsent when the connection closes, the reader fails it first
    reply_rx
        .await
        .unwrap_or_else(|_| Err(closed_error(&inner.state)))
}

/// Queue every command before waiting for any reply, so no other command is sent between them.
async fn exchange_all(inner: &Inner, commands: Vec<Vec<String>>) -> Vec<Result<Value, Error>> {
    // no await until every command is queued
    let queued: Vec<Result<Option<oneshot::Receiver<_>>, Error>> = commands
        .into_iter()
        .map(|command| {
            if replies_with_pushes(&command) {
                // no reply, as for `exchange`
                return enqueue(inner, command, None, None).map(|()| None);
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            enqueue(inner, command, Some(Reply::Call(reply_tx)), None).map(|()| Some(reply_rx))
        })
        .collect();
    let mut replies = Vec::with_capacity(queued.len());
    for reply in queued {
        replies.push(match reply {
            Err(error) => Err(error),
            Ok(None) => Ok(Value::Null),
            // the reply is dropped unsent when the connection closes, the reader fails it first
            Ok(Some(reply)) => reply
                .await
                .unwrap_or_else(|_| Err(closed_error(&inner.state))),
        });
    }
    replies
}

/// MULTI, the commands, then EXEC, with no other command between them.
async fn transaction(
    inner: &Inner,
    commands: Vec<Vec<String>>,
) -> Result<Option<Vec<Value>>, Error> {
    // before MULTI is sent, so an invalid command fails the transaction rather than being left out
    if commands.iter().any(Vec::is_empty) {
        return Err(Error::Client(
            "a command requires at least one word".to_string(),
        ));
    }
    let batch = std::iter::once(words(&["MULTI"]))
        .chain(commands)
        .chain(std::iter::once(words(&["EXEC"])))
        .collect();
    let mut replies = exchange_all(inner, batch).await;
    let exec = replies.pop().expect("a reply for each command");
    // MULTI's OK and each command's QUEUED, the first command rejected while queuing is a clearer
    // error than EXEC's EXECABORT
    for reply in replies {
        reply?;
    }
    match exec? {
        // a watched key changed
        Value::Null => Ok(None),
        Value::Array(replies) => replies
            .into_iter()
            .map(|reply| componentized_valkey::codec::decode(reply).map_err(Error::Resp))
            .collect::<Result<_, _>>()
            .map(Some),
        reply => Err(unexpected(&reply)),
    }
}

async fn emit(inner: &Inner, command: Vec<String>) -> Result<(), Error> {
    let reply = (!replies_with_pushes(&command)).then_some(Reply::Discard);
    let (written_tx, written_rx) = oneshot::channel();
    enqueue(inner, command, reply, Some(written_tx))?;
    // the signal is dropped unsent when the transport closed first
    written_rx.await.map_err(|_| closed_error(&inner.state))
}

/// Whether the command's replies are push messages, e.g. SUBSCRIBE, rather than a reply.
///
/// In RESP3 the server confirms each channel subscribed to, or unsubscribed from, with a push
/// message. An error, e.g. for a command missing its channels, is a reply the server sends
/// unprompted.
fn replies_with_pushes(command: &[String]) -> bool {
    const COMMANDS: [&str; 6] = [
        "SUBSCRIBE",
        "PSUBSCRIBE",
        "SSUBSCRIBE",
        "UNSUBSCRIBE",
        "PUNSUBSCRIBE",
        "SUNSUBSCRIBE",
    ];
    command.first().is_some_and(|name| {
        COMMANDS
            .iter()
            .any(|command| name.eq_ignore_ascii_case(command))
    })
}

/// The error a command fails with once the connection closed.
fn closed_error(state: &RefCell<State>) -> Error {
    state.borrow().closed.clone().unwrap_or(Error::Closed(None))
}

/// Close the connection, failing every command waiting for a reply and closing the readers of push
/// messages.
fn close(state: &RefCell<State>, error: Error) {
    let mut state = state.borrow_mut();
    let error = state.closed.get_or_insert(error).clone();
    for reply in state.pending.drain(..) {
        if let Reply::Call(reply) = reply {
            let _ = reply.send(Err(error.clone()));
        }
    }
    state.pushes.clear();
}

/// The next item of a channel.
async fn next<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

fn words(words: &[&str]) -> Vec<String> {
    words.iter().map(|word| word.to_string()).collect()
}

/// Write each queued command to the requests stream, closing it once the connection is dropped.
async fn write_requests(
    state: Rc<RefCell<State>>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    mut output: StreamWriter<Value>,
) {
    while let Some(Request { command, written }) = next(&mut requests).await {
        state.borrow_mut().activity += 1;
        if output.write_one(command).await.is_some() {
            // the transport stopped reading, the reader closes the connection once it ends
            break;
        }
        if let Some(written) = written {
            let _ = written.send(());
        }
    }
}

/// Hand each reply to the oldest command waiting for one, and each push message, or value the server
/// sent unprompted, to the readers of push messages, until the transport closes.
async fn read_replies(
    state: Rc<RefCell<State>>,
    mut replies: StreamReader<Value>,
    closed: FutureReader<Result<(), Error>>,
) {
    while let Some(value) = replies.next().await {
        let mut state = state.borrow_mut();
        state.activity += 1;
        let value = match value {
            Value::Push(_) => value,
            value => match state.pending.pop_front() {
                Some(Reply::Call(reply)) => {
                    let _ = reply.send(reply_result(value));
                    continue;
                }
                Some(Reply::Discard) => continue,
                // sent unprompted, e.g. the feed of MONITOR
                None => value,
            },
        };
        // a reader that is full or gone is dropped, rather than holding up replies
        state
            .pushes
            .retain_mut(|pushes| pushes.try_send(value.clone()).is_ok());
    }
    let reason = match closed.await {
        Ok(()) => None,
        Err(error) => describe(error),
    };
    close(&state, Error::Closed(reason));
}

/// Start the work methods ask for that outlives them.
async fn supervise(mut tasks: mpsc::UnboundedReceiver<Task>, inner: Weak<Inner>) {
    while let Some(task) = next(&mut tasks).await {
        match task {
            Task::Pushes(pushes, writer) => spawn_local(forward_pushes(pushes, writer)),
            Task::KeepAlive {
                generation,
                interval,
            } => spawn_local(keep_alive(inner.clone(), generation, interval)),
        }
    }
}

/// Forward push messages to a reader's stream, until the reader or the connection is gone.
async fn forward_pushes(mut pushes: mpsc::Receiver<Value>, mut writer: StreamWriter<Value>) {
    while let Some(value) = next(&mut pushes).await {
        if writer.write_one(value).await.is_some() {
            break;
        }
    }
}

/// Send PING whenever the connection was idle for `interval`, closing it if PING fails.
async fn keep_alive(inner: Weak<Inner>, generation: u64, interval: DurationMilliseconds) {
    loop {
        let Some(seen) = keep_alive_activity(&inner, generation) else {
            return;
        };
        // a wasi:clocks duration is in nanoseconds
        wait_for(interval.px.saturating_mul(1_000_000)).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let idle = {
            let state = inner.state.borrow();
            if state.keep_alive_generation != generation || state.closed.is_some() {
                return;
            }
            state.activity == seen && state.pending.is_empty()
        };
        if idle {
            if let Err(error) = exchange(&inner, words(&["PING"])).await {
                close(&inner.state, error);
                inner.requests.close_channel();
                return;
            }
        }
    }
}

/// The connection's activity, `None` once the keep-alive should stop.
fn keep_alive_activity(inner: &Weak<Inner>, generation: u64) -> Option<u64> {
    let inner = inner.upgrade()?;
    let state = inner.state.borrow();
    (state.keep_alive_generation == generation && state.closed.is_none()).then_some(state.activity)
}

/// Send HELLO, then hand over the connection, or the error that stopped it.
async fn establish(
    inner: Rc<Inner>,
    opts: Option<ConnectOpts>,
    connection: FutureWriter<Result<Connection, Error>>,
) {
    let (auth, client_name, keep_alive_interval) = match opts {
        Some(opts) => (opts.auth, opts.client_name, opts.keep_alive_interval),
        None => (None, None, None),
    };

    let mut hello = words(&["HELLO", "3"]);
    if let Some((username, password)) = auth {
        hello.extend(["AUTH".to_string(), username, password]);
    }
    if let Some(client_name) = client_name {
        hello.extend(["SETNAME".to_string(), client_name]);
    }

    let result = match exchange(&inner, hello).await.and_then(server_properties) {
        Ok(properties) => {
            inner.state.borrow_mut().server_properties = properties;
            let established = ConnectionImpl(inner);
            established.set_keep_alive(keep_alive_interval);
            Ok(Connection::new(established))
        }
        // dropping `inner` closes the connection
        Err(error) => Err(error),
    };
    // the connection is dropped, and closed, when nobody waits for it anymore
    let _ = connection.write(result).await;
}

/// The server's properties from the reply to HELLO, a map of property names to values.
fn server_properties(reply: Value) -> Result<Vec<(String, Value)>, Error> {
    let Value::Map(entries) = reply else {
        return Err(Error::Client(format!(
            "unexpected reply to HELLO: {reply:?}"
        )));
    };
    entries
        .into_iter()
        .map(|(name, value)| {
            let name = match decode(name)? {
                Value::String(name) | Value::BulkString(name) => name,
                name => {
                    return Err(Error::Client(format!(
                        "unexpected property name in reply to HELLO: {name:?}"
                    )));
                }
            };
            Ok((name, decode(value)?))
        })
        .collect()
}

fn decode(value: NestedValue) -> Result<Value, Error> {
    componentized_valkey::codec::decode(value).map_err(Error::Resp)
}

/// An error value from the server fails the command.
/// A simple string `OK`.
fn ok(reply: Value) -> Result<(), Error> {
    match reply {
        Value::String(text) if text == "OK" => Ok(()),
        reply => Err(unexpected(&reply)),
    }
}

fn unexpected(reply: &Value) -> Error {
    Error::Client(format!("unexpected reply: {reply:?}"))
}

fn reply_result(value: Value) -> Result<Value, Error> {
    match value {
        Value::Error(message) | Value::BulkError(message) => Err(Error::Valkey(message)),
        value => Ok(value),
    }
}

/// A transport's error as the reason a connection closed.
fn describe(error: Error) -> Option<String> {
    match error {
        Error::Closed(reason) => reason,
        Error::Client(message) | Error::Resp(message) | Error::Valkey(message) => Some(message),
    }
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/connection/`
    path: "../wit",
    world: "connection",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Component);
