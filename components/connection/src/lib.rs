//! The mechanics of a connection to a Valkey server, independent of the transport carrying it.
//!
//! A connection runs background tasks for as long as it is open, in the task of the call to
//! `connection.new` that created it:
//!
//! - the writer writes queued commands to the requests stream, in order
//! - the reader reads the replies stream, handing each reply to the oldest command waiting for one,
//!   each message to the subscriptions holding its target, and each other push message, or value
//!   the server sent unprompted, to every reader of dead letters
//! - the supervisor starts the work methods ask for that outlives them, e.g. keep-alive, since sync
//!   methods have no task of their own to spawn it into
//!
//! Dropping the connection drops its queue of commands, the writer then closes the requests stream
//! and the transport closes, which ends the reader. A subscription keeps the connection open.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(not(feature = "pubsub"))]
#[path = "pubsub_disabled.rs"]
mod pubsub;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::{Rc, Weak};

use componentized_valkey::types::{RespValue, StringOrBytes};
use exports::componentized::valkey::connection::{
    ConnectOpts, Connection, DurationMilliseconds, ErrorCode, Guest, GuestConnection, Subscription,
};
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use wasi::clocks::monotonic_clock::wait_for;
use wit_bindgen::{FutureReader, FutureWriter, StreamReader, StreamWriter, spawn_local};

/// The library the connection reports to the server, see CLIENT SETINFO.
const LIB_NAME: &str = "componentized:valkey";
/// The version the connection reports to the server, that of the package.
const LIB_VER: &str = env!("CARGO_PKG_VERSION");

/// A word of a command, as sent, UTF-8 or not.
type Word = Vec<u8>;

/// How many values a reader may fall behind before its stream is closed, and how many events a
/// subscription keeps while no stream is open.
const PUSH_BUFFER: usize = 256;

struct Component;

impl Guest for Component {
    type Connection = ConnectionImpl;
    type Subscription = pubsub::SubscriptionImpl;
}

/// Where the reply to a command goes.
enum Reply {
    /// To the caller waiting for it.
    Call(oneshot::Sender<Result<RespValue, ErrorCode>>),
    /// Nowhere, the command was sent without waiting for its reply.
    Discard,
    /// A subscription's SUBSCRIBE, UNSUBSCRIBE or the like, see `pubsub::Pending`.
    #[cfg_attr(not(feature = "pubsub"), allow(dead_code))]
    Pubsub(pubsub::Pending),
    /// RESET, whose reply ends the subscriptions the server dropped.
    Reset(oneshot::Sender<Result<(), ErrorCode>>),
    /// The HELLO after RESET, the writer waits for its reply before sending another command, and
    /// the connection closes when it fails.
    Rehello {
        proceed: oneshot::Sender<bool>,
        done: oneshot::Sender<Result<(), ErrorCode>>,
    },
}

/// A command for the writer, with a signal for once it is written.
struct Request {
    command: RespValue,
    written: Option<oneshot::Sender<()>>,
    /// Whether to go on writing after this command, e.g. once the HELLO after RESET succeeded,
    /// otherwise the writer stops, and the commands after it are never sent.
    barrier: Option<oneshot::Receiver<bool>>,
}

/// Work for the supervisor to start.
enum Task {
    /// Run the future in a task of its own.
    Spawn(Pin<Box<dyn Future<Output = ()>>>),
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
    closed: Option<ErrorCode>,
    /// Counts the commands written and the values read, it stays the same while the connection is
    /// idle.
    activity: u64,
    /// The readers of dead letters.
    dead_letters: Vec<mpsc::Sender<RespValue>>,
    /// The subscriptions, and the channels they hold.
    subscriptions: pubsub::Subscriptions,
    /// The server's properties from the reply to HELLO.
    server_properties: Vec<(String, RespValue)>,
    /// The HELLO the connection was established with, repeated after RESET.
    hello: Vec<Word>,
    /// Bumped each time the keep-alive is set, a keep-alive task stops once it is outdated.
    keep_alive_generation: u64,
    /// Set by the first `subscription`, and never cleared: from then on the connection
    /// alone sends SUBSCRIBE and the like, so the subscriptions stay in step with the server.
    owns_subscriptions: bool,
}

/// A connection's queues to its background tasks.
///
/// Only the connection and its subscriptions hold it, the background tasks hold the state alone
/// or a weak reference, so dropping them drops the queues and ends the tasks.
struct Inner {
    state: Rc<RefCell<State>>,
    requests: mpsc::UnboundedSender<Request>,
    tasks: mpsc::UnboundedSender<Task>,
}

pub struct ConnectionImpl(Rc<Inner>);

impl GuestConnection for ConnectionImpl {
    async fn new(
        replies: StreamReader<RespValue>,
        closed: FutureReader<Result<(), ErrorCode>>,
        opts: Option<ConnectOpts>,
    ) -> (
        StreamReader<RespValue>,
        FutureReader<Result<Connection, ErrorCode>>,
    ) {
        let (output, requests) = wit_stream::new();
        // the default is only written if the connection is dropped before it is established
        let (connection_writer, connection) = wit_future::new(|| Err(ErrorCode::Closed(None)));

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

    fn server_properties(&self) -> Vec<(String, RespValue)> {
        self.0.state.borrow().server_properties.clone()
    }

    async fn exchange(&self, command: Vec<StringOrBytes>) -> Result<RespValue, ErrorCode> {
        exchange(&self.0, bytes(command)).await
    }

    async fn exchange_all(
        &self,
        commands: Vec<Vec<StringOrBytes>>,
    ) -> Vec<Result<RespValue, ErrorCode>> {
        exchange_all(&self.0, commands.into_iter().map(bytes).collect()).await
    }

    async fn emit(&self, command: Vec<StringOrBytes>) -> Result<(), ErrorCode> {
        emit(&self.0, bytes(command)).await
    }

    async fn watch(&self, keys: Vec<String>) -> Result<(), ErrorCode> {
        if keys.is_empty() {
            return Err(ErrorCode::Client(
                "keys requires at least one item".to_string(),
            ));
        }
        let keys = keys.into_iter().map(String::into_bytes);
        let command = std::iter::once(b"WATCH".to_vec()).chain(keys).collect();
        ok(exchange(&self.0, command).await?)
    }

    async fn unwatch(&self) -> Result<(), ErrorCode> {
        ok(exchange(&self.0, words(&["UNWATCH"])).await?)
    }

    async fn transaction(
        &self,
        commands: Vec<Vec<StringOrBytes>>,
    ) -> Result<Option<Vec<RespValue>>, ErrorCode> {
        transaction(&self.0, commands.into_iter().map(bytes).collect()).await
    }

    fn subscription(&self) -> Result<Subscription, ErrorCode> {
        pubsub::subscription(&self.0)
    }

    async fn reset(&self) -> Result<(), ErrorCode> {
        reset(&self.0).await
    }

    fn dead_letters(&self) -> StreamReader<RespValue> {
        let (writer, reader) = wit_stream::new();
        let (tx, rx) = mpsc::channel(PUSH_BUFFER);
        let mut state = self.0.state.borrow_mut();
        // a closed connection drops the writer, closing the stream at once
        if state.closed.is_none()
            && self
                .0
                .tasks
                .unbounded_send(Task::Spawn(Box::pin(forward_values(rx, writer))))
                .is_ok()
        {
            state.dead_letters.push(tx);
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

/// RESET, then HELLO again, with no other command between them, closing the connection when HELLO
/// fails.
///
/// The writer sends no other command until HELLO succeeds, so none runs on the connection RESET
/// leaves: RESP2, the default user, no name. When HELLO fails, e.g. the credentials no longer work,
/// the reader closes the connection, and the commands queued after it fail without being sent.
async fn reset(inner: &Inner) -> Result<(), ErrorCode> {
    let mut state = inner.state.borrow_mut();
    if let Some(error) = &state.closed {
        return Err(error.clone());
    }
    let hello = state.hello.clone();
    let (reset_tx, reset_rx) = oneshot::channel();
    let (proceed_tx, proceed_rx) = oneshot::channel();
    let (hello_tx, hello_rx) = oneshot::channel();
    let requests = [(words(&["RESET"]), None), (hello, Some(proceed_rx))];
    for (command, barrier) in requests {
        let command = RespValue::Array(
            command
                .into_iter()
                .map(|word| componentized_valkey::resp::encode(RespValue::BulkString(word)))
                .collect(),
        );
        inner
            .requests
            .unbounded_send(Request {
                command,
                written: None,
                barrier,
            })
            .map_err(|_| ErrorCode::Closed(None))?;
    }
    state.pending.push_back(Reply::Reset(reset_tx));
    state.pending.push_back(Reply::Rehello {
        proceed: proceed_tx,
        done: hello_tx,
    });
    drop(state);
    // the replies are dropped unsent when the connection closes, the reader fails them first
    let reset = reset_rx
        .await
        .unwrap_or_else(|_| Err(closed_error(&inner.state)));
    hello_rx
        .await
        .unwrap_or_else(|_| Err(closed_error(&inner.state)))?;
    reset
}

/// Queue a command, and where its reply goes, as one step so replies pair with commands in order.
/// A command without a reply takes no place in the order.
fn enqueue(
    inner: &Inner,
    command: Vec<Word>,
    reply: Option<Reply>,
    written: Option<oneshot::Sender<()>>,
) -> Result<(), ErrorCode> {
    if command.is_empty() {
        return Err(ErrorCode::Client(
            "a command requires at least one word".to_string(),
        ));
    }
    let mut state = inner.state.borrow_mut();
    if let Some(error) = &state.closed {
        return Err(error.clone());
    }
    // the words of a command are bulk strings
    let command = RespValue::Array(
        command
            .into_iter()
            .map(|word| componentized_valkey::resp::encode(RespValue::BulkString(word)))
            .collect(),
    );
    inner
        .requests
        .unbounded_send(Request {
            command,
            written,
            barrier: None,
        })
        .map_err(|_| ErrorCode::Closed(None))?;
    state.pending.extend(reply);
    Ok(())
}

async fn exchange(inner: &Inner, command: Vec<Word>) -> Result<RespValue, ErrorCode> {
    reject_managed(inner, &command)?;
    if replies_with_pushes(&command) {
        return emit(inner, command).await.map(|()| RespValue::Null);
    }
    request(inner, command).await
}

/// Send a command and wait for its reply, for the commands the connection sends itself, e.g. HELLO,
/// which callers may not.
async fn request(inner: &Inner, command: Vec<Word>) -> Result<RespValue, ErrorCode> {
    let (reply_tx, reply_rx) = oneshot::channel();
    enqueue(inner, command, Some(Reply::Call(reply_tx)), None)?;
    // the reply is dropped unsent when the connection closes, the reader fails it first
    reply_rx
        .await
        .unwrap_or_else(|_| Err(closed_error(&inner.state)))
}

/// Queue every command before waiting for any reply, so no other command is sent between them.
async fn exchange_all(
    inner: &Inner,
    commands: Vec<Vec<Word>>,
) -> Vec<Result<RespValue, ErrorCode>> {
    // no await until every command is queued
    let queued: Vec<Result<Option<oneshot::Receiver<_>>, ErrorCode>> = commands
        .into_iter()
        .map(|command| {
            reject_managed(inner, &command)?;
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
            Ok(None) => Ok(RespValue::Null),
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
    commands: Vec<Vec<Word>>,
) -> Result<Option<Vec<RespValue>>, ErrorCode> {
    // before MULTI is sent, so an invalid command fails the transaction rather than being left out
    if commands.iter().any(Vec::is_empty) {
        return Err(ErrorCode::Client(
            "a command requires at least one word".to_string(),
        ));
    }
    for command in &commands {
        reject_managed(inner, command)?;
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
        RespValue::Null => Ok(None),
        RespValue::Array(replies) => replies
            .into_iter()
            .map(|reply| decode(&reply))
            .collect::<Result<_, _>>()
            .map(Some),
        reply => Err(unexpected(&reply)),
    }
}

async fn emit(inner: &Inner, command: Vec<Word>) -> Result<(), ErrorCode> {
    reject_managed(inner, &command)?;
    let reply = (!replies_with_pushes(&command)).then_some(Reply::Discard);
    let (written_tx, written_rx) = oneshot::channel();
    enqueue(inner, command, reply, Some(written_tx))?;
    // the signal is dropped unsent when the transport closed first
    written_rx.await.map_err(|_| closed_error(&inner.state))
}

/// Rejects the commands that would leave the server's state out of step with the connection's:
///
/// - RESET, which switches the connection to RESP2, see `reset`
/// - HELLO, which may switch the connection to RESP2, or another user, see `server-properties`
/// - CLIENT REPLY, which stops replies from pairing with commands
/// - SUBSCRIBE and the like once the connection owns the subscriptions, see
///   `State::owns_subscriptions`, e.g. an UNSUBSCRIBE would end a subscription without it asking
fn reject_managed(inner: &Inner, command: &[Word]) -> Result<(), ErrorCode> {
    let is = |index: usize, word: &str| {
        command
            .get(index)
            .is_some_and(|actual| actual.eq_ignore_ascii_case(word.as_bytes()))
    };
    let reason = if is(0, "RESET") {
        Some("RESET is rejected, use the connection's reset")
    } else if is(0, "HELLO") {
        Some(
            "HELLO is rejected, the connection sent it when it was established, see its server-properties",
        )
    } else if is(0, "CLIENT") && is(1, "REPLY") {
        Some("CLIENT REPLY is rejected, every command has a reply, see the connection's emit")
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(ErrorCode::Client(reason.to_string()));
    }
    if replies_with_pushes(command) && inner.state.borrow().owns_subscriptions {
        let name = String::from_utf8_lossy(&command[0]).to_uppercase();
        return Err(ErrorCode::Client(format!(
            "{name} is rejected once the connection has a subscription, use the subscription's methods"
        )));
    }
    Ok(())
}

/// Whether the command's replies are push messages, e.g. SUBSCRIBE, rather than a reply.
///
/// In RESP3 the server confirms each channel subscribed to, or unsubscribed from, with a push
/// message. An error, e.g. for a command missing its channels, is a reply the server sends
/// unprompted.
fn replies_with_pushes(command: &[Word]) -> bool {
    const COMMANDS: [&[u8]; 6] = [
        b"SUBSCRIBE",
        b"PSUBSCRIBE",
        b"SSUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PUNSUBSCRIBE",
        b"SUNSUBSCRIBE",
    ];
    command.first().is_some_and(|name| {
        COMMANDS
            .iter()
            .any(|command| name.eq_ignore_ascii_case(command))
    })
}

/// The error a command fails with once the connection closed.
fn closed_error(state: &RefCell<State>) -> ErrorCode {
    state
        .borrow()
        .closed
        .clone()
        .unwrap_or(ErrorCode::Closed(None))
}

/// Close the connection, failing every command waiting for a reply, closing the readers of dead
/// letters, and ending the subscriptions.
fn close(state: &RefCell<State>, error: ErrorCode) {
    let mut state = state.borrow_mut();
    let error = state.closed.get_or_insert(error).clone();
    for reply in state.pending.drain(..).collect::<Vec<_>>() {
        match reply {
            Reply::Call(reply) => {
                let _ = reply.send(Err(error.clone()));
            }
            Reply::Pubsub(pending) => pending.fail(error.clone()),
            Reply::Reset(done) => {
                let _ = done.send(Err(error.clone()));
            }
            Reply::Rehello { proceed, done } => {
                let _ = proceed.send(false);
                let _ = done.send(Err(error.clone()));
            }
            Reply::Discard => {}
        }
    }
    state.dead_letters.clear();
    state.subscriptions.close(&error);
}

/// The next item of a channel.
async fn next<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

/// The words of a command the connection sends itself, e.g. `words(&["PING"])`.
fn words(words: &[&str]) -> Vec<Word> {
    words.iter().map(|word| word.as_bytes().to_vec()).collect()
}

/// The words of a command, taken from strings or bytes as they are, without copying.
fn bytes(command: Vec<StringOrBytes>) -> Vec<Word> {
    command
        .into_iter()
        .map(|word| match word {
            StringOrBytes::String(text) => text.into_bytes(),
            StringOrBytes::Bytes(bytes) => bytes,
        })
        .collect()
}

/// Write each queued command to the requests stream, closing it once the connection is dropped.
async fn write_requests(
    state: Rc<RefCell<State>>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    mut output: StreamWriter<RespValue>,
) {
    while let Some(Request {
        command,
        written,
        barrier,
    }) = next(&mut requests).await
    {
        state.borrow_mut().activity += 1;
        if output.write_one(command).await.is_some() {
            // the transport stopped reading, the reader closes the connection once it ends
            break;
        }
        if let Some(written) = written {
            let _ = written.send(());
        }
        if let Some(barrier) = barrier
            && !barrier.await.unwrap_or(false)
        {
            // the commands after it are never sent, those waiting to be written fail
            requests.close();
            while requests.try_recv().is_ok() {}
            // dropping the requests stream closes the transport, the reader closed the connection
            break;
        }
    }
}

/// Hand each reply to the oldest command waiting for one, each message to its subscriptions, and
/// each other push message, or value the server sent unprompted, to the readers of dead letters,
/// until the transport closes.
async fn read_replies(
    state: Rc<RefCell<State>>,
    mut replies: StreamReader<RespValue>,
    closed: FutureReader<Result<(), ErrorCode>>,
) {
    // the error to close the connection with, before the transport closes
    let mut closing = None;
    while let Some(value) = replies.next().await {
        let mut state = state.borrow_mut();
        let state = &mut *state;
        state.activity += 1;
        let value = match value {
            push @ RespValue::Push(_) => {
                if pubsub::claim_push(state, &push) {
                    continue;
                }
                push
            }
            value => match state.pending.pop_front() {
                Some(Reply::Call(reply)) => {
                    let _ = reply.send(reply_result(value));
                    continue;
                }
                Some(Reply::Discard) => continue,
                Some(Reply::Rehello { proceed, done }) => {
                    match reply_result(value).and_then(server_properties) {
                        Ok(properties) => {
                            state.server_properties = properties;
                            let _ = proceed.send(true);
                            let _ = done.send(Ok(()));
                        }
                        Err(error) => {
                            // the connection is not the one it was established with
                            let _ = proceed.send(false);
                            let _ = done.send(Err(error.clone()));
                            closing = Some(error);
                            break;
                        }
                    }
                    continue;
                }
                Some(Reply::Reset(done)) => {
                    let result = match reply_result(value) {
                        Ok(RespValue::String(reply)) if reply == "RESET" => {
                            state.subscriptions.reset();
                            Ok(())
                        }
                        Ok(reply) => Err(unexpected(&reply)),
                        Err(error) => Err(error),
                    };
                    let _ = done.send(result);
                    continue;
                }
                // confirmed with push messages, a reply in their place is an error
                Some(Reply::Pubsub(pending)) => {
                    let error = match reply_result(value) {
                        Ok(reply) => unexpected(&reply),
                        Err(error) => error,
                    };
                    pending.refused(&mut state.subscriptions, error);
                    continue;
                }
                // sent unprompted, e.g. the feed of MONITOR
                None => value,
            },
        };
        // a reader that is full or gone is dropped, rather than holding up replies
        state
            .dead_letters
            .retain_mut(|dead_letters| dead_letters.try_send(value.clone()).is_ok());
    }
    if let Some(error) = closing {
        // the writer stopped, dropping the requests stream closes the transport
        close(&state, error);
        return;
    }
    let reason = match closed.await {
        Ok(()) => None,
        Err(error) => describe(error),
    };
    close(&state, ErrorCode::Closed(reason));
}

/// Start the work methods ask for that outlives them.
async fn supervise(mut tasks: mpsc::UnboundedReceiver<Task>, inner: Weak<Inner>) {
    while let Some(task) = next(&mut tasks).await {
        match task {
            Task::Spawn(future) => spawn_local(future),
            Task::KeepAlive {
                generation,
                interval,
            } => spawn_local(keep_alive(inner.clone(), generation, interval)),
        };
    }
}

/// Forward values to a reader's stream, until the reader or the connection is gone.
async fn forward_values(
    mut values: mpsc::Receiver<RespValue>,
    mut writer: StreamWriter<RespValue>,
) {
    while let Some(value) = next(&mut values).await {
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
    connection: FutureWriter<Result<Connection, ErrorCode>>,
) {
    let (auth, client_name, keep_alive_interval) = match opts {
        Some(opts) => (opts.auth, opts.client_name, opts.keep_alive_interval),
        None => (None, None, None),
    };

    let mut hello = words(&["HELLO", "3"]);
    if let Some((username, password)) = auth {
        hello.extend([
            b"AUTH".to_vec(),
            username.into_bytes(),
            password.into_bytes(),
        ]);
    }
    if let Some(client_name) = client_name {
        hello.extend([b"SETNAME".to_vec(), client_name.into_bytes()]);
    }

    inner.state.borrow_mut().hello = hello.clone();
    let (hello_tx, hello_rx) = oneshot::channel();
    // the library, best effort, a server before 7.2 does not know CLIENT SETINFO, its error is
    // discarded. It survives RESET, so it is only sent once.
    let setinfo = [("LIB-NAME", LIB_NAME), ("LIB-VER", LIB_VER)];
    let queued = enqueue(&inner, hello, Some(Reply::Call(hello_tx)), None).and_then(|()| {
        for (attr, value) in setinfo {
            let command = words(&["CLIENT", "SETINFO", attr, value]);
            enqueue(&inner, command, Some(Reply::Discard), None)?;
        }
        Ok(())
    });
    let hello = match queued {
        // dropped unsent when the connection closes, the reader fails it first
        Ok(()) => hello_rx
            .await
            .unwrap_or_else(|_| Err(closed_error(&inner.state))),
        Err(error) => Err(error),
    };
    let result = match hello.and_then(server_properties) {
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
fn server_properties(reply: RespValue) -> Result<Vec<(String, RespValue)>, ErrorCode> {
    let RespValue::Map(entries) = reply else {
        return Err(ErrorCode::Client(format!(
            "unexpected reply to HELLO: {reply}"
        )));
    };
    entries
        .into_iter()
        .map(|(name, value)| {
            let invalid = |name: RespValue| {
                ErrorCode::Client(format!(
                    "unexpected property name in reply to HELLO: {name}"
                ))
            };
            let name = match decode(&name)? {
                RespValue::String(name) => name,
                // the bytes come back with the error, for its message
                RespValue::BulkString(bytes) => String::from_utf8(bytes)
                    .map_err(|error| invalid(RespValue::BulkString(error.into_bytes())))?,
                name => return Err(invalid(name)),
            };
            Ok((name, decode(&value)?))
        })
        .collect()
}

fn decode(value: &[u8]) -> Result<RespValue, ErrorCode> {
    componentized_valkey::resp::decode(value).map_err(ErrorCode::Resp)
}

/// An error value from the server fails the command.
/// A simple string `OK`.
fn ok(reply: RespValue) -> Result<(), ErrorCode> {
    match reply {
        RespValue::String(text) if text == "OK" => Ok(()),
        reply => Err(unexpected(&reply)),
    }
}

fn unexpected(reply: &RespValue) -> ErrorCode {
    ErrorCode::Client(format!("unexpected reply: {reply:?}"))
}

fn reply_result(value: RespValue) -> Result<RespValue, ErrorCode> {
    match value {
        RespValue::Error(message) => Err(ErrorCode::Server(StringOrBytes::String(message))),
        // binary safe, like a bulk string
        RespValue::BulkError(message) => Err(ErrorCode::Server(StringOrBytes::from_bytes(message))),
        value => Ok(value),
    }
}

/// A transport's error as the reason a connection closed.
fn describe(error: ErrorCode) -> Option<String> {
    match error {
        ErrorCode::Closed(reason) => reason,
        ErrorCode::Client(message) | ErrorCode::Resp(message) => Some(message),
        ErrorCode::Server(message) => Some(message.to_string()),
        ErrorCode::Lagged => Some("a stream fell behind".to_string()),
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
