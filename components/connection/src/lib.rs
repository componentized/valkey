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

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::{Rc, Weak};

use componentized_valkey::types::{
    RespNestedValue, RespValue, SubscriptionEvent, SubscriptionMessage, SubscriptionTarget,
    UnsubscribeReason,
};
use exports::componentized::valkey::connection::{
    ConnectOpts, Connection, DurationMilliseconds, Error, Guest, GuestConnection,
    GuestSubscription, Subscription,
};
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use wasi::clocks::monotonic_clock::wait_for;
use wit_bindgen::{FutureReader, FutureWriter, StreamReader, StreamWriter, spawn_local};

/// How many values a reader may fall behind before its stream is closed, and how many events a
/// subscription keeps while no stream is open.
const PUSH_BUFFER: usize = 256;

struct Component;

impl Guest for Component {
    type Connection = ConnectionImpl;
    type Subscription = SubscriptionImpl;
}

/// Where the reply to a command goes.
enum Reply {
    /// To the caller waiting for it.
    Call(oneshot::Sender<Result<RespValue, Error>>),
    /// Nowhere, the command was sent without waiting for its reply.
    Discard,
    /// A subscription's SUBSCRIBE, confirmed by a push message for each channel rather than a
    /// reply.
    Subscribe {
        kind: Kind,
        /// The channels not confirmed yet.
        channels: Vec<String>,
        id: u64,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// RESET, whose reply ends the subscriptions the server dropped.
    Reset(oneshot::Sender<Result<(), Error>>),
    /// The HELLO after RESET, the writer waits for its reply before sending another command, and
    /// the connection closes when it fails.
    Rehello {
        proceed: oneshot::Sender<bool>,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// A subscription's UNSUBSCRIBE, confirmed by a push message for each channel rather than a
    /// reply.
    Unsubscribe {
        kind: Kind,
        /// The channels not confirmed yet.
        channels: Vec<String>,
        done: oneshot::Sender<Result<(), Error>>,
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
    closed: Option<Error>,
    /// Counts the commands written and the values read, it stays the same while the connection is
    /// idle.
    activity: u64,
    /// The readers of dead letters.
    dead_letters: Vec<mpsc::Sender<RespValue>>,
    /// The subscriptions, and the channels they hold.
    subscriptions: Subscriptions,
    /// The server's properties from the reply to HELLO.
    server_properties: Vec<(String, RespValue)>,
    /// The HELLO the connection was established with, repeated after RESET.
    hello: Vec<String>,
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
        closed: FutureReader<Result<(), Error>>,
        opts: Option<ConnectOpts>,
    ) -> (
        StreamReader<RespValue>,
        FutureReader<Result<Connection, Error>>,
    ) {
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

    fn server_properties(&self) -> Vec<(String, RespValue)> {
        self.0.state.borrow().server_properties.clone()
    }

    async fn exchange(&self, command: Vec<String>) -> Result<RespValue, Error> {
        exchange(&self.0, command).await
    }

    async fn exchange_all(&self, commands: Vec<Vec<String>>) -> Vec<Result<RespValue, Error>> {
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

    async fn transaction(
        &self,
        commands: Vec<Vec<String>>,
    ) -> Result<Option<Vec<RespValue>>, Error> {
        transaction(&self.0, commands).await
    }

    fn subscription(&self) -> Subscription {
        let id = {
            let mut state = self.0.state.borrow_mut();
            state.owns_subscriptions = true;
            let closed = state.closed.clone();
            state.subscriptions.create(closed)
        };
        Subscription::new(SubscriptionImpl(Handle {
            inner: self.0.clone(),
            id,
        }))
    }

    async fn reset(&self) -> Result<(), Error> {
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

pub struct SubscriptionImpl(Handle);

impl GuestSubscription for SubscriptionImpl {
    fn targets(&self) -> Vec<SubscriptionTarget> {
        let state = self.0.inner.state.borrow();
        let Some(sub) = state.subscriptions.subs.get(&self.0.id) else {
            return vec![];
        };
        sub.targets
            .iter()
            .filter(|(_, hold)| *hold == Hold::Active)
            .map(|((kind, name), _)| kind.target(name.clone()))
            .collect()
    }

    async fn subscribe(&self, targets: Vec<SubscriptionTarget>) -> Result<(), Error> {
        let groups = by_kind(targets)?;
        subscribe(&self.0.inner, self.0.id, groups).await
    }

    async fn unsubscribe(&self, targets: Option<Vec<SubscriptionTarget>>) -> Result<(), Error> {
        let groups = match targets {
            Some(targets) => by_kind(targets)?
                .into_iter()
                .map(|(kind, names)| (kind, Some(names)))
                .collect(),
            None => Kind::ALL.into_iter().map(|kind| (kind, None)).collect(),
        };
        unsubscribe(&self.0.inner, self.0.id, groups).await
    }

    fn events(&self) -> (StreamReader<SubscriptionEvent>, FutureReader<Ended>) {
        let (writer, stream) = wit_stream::new();
        // the default is only written if the subscription is gone before the stream ends
        let (end_writer, end) = wit_future::new(|| Err(Error::Closed(None)));
        let (mut events_tx, events_rx) = mpsc::channel(PUSH_BUFFER);
        let (end_tx, end_rx) = oneshot::channel();
        {
            let mut state = self.0.inner.state.borrow_mut();
            if let Some(sub) = state.subscriptions.subs.get_mut(&self.0.id) {
                // the backlog fits, it is no larger than the channel
                for event in sub.backlog.drain(..) {
                    let _ = events_tx.try_send(event);
                }
                match &sub.closed {
                    Some(error) => {
                        let _ = end_tx.send(Err(error.clone()));
                    }
                    None => sub.readers.push(Reader {
                        events: events_tx,
                        end: end_tx,
                    }),
                }
            }
        }
        let forward = forward_events(events_rx, end_rx, writer, end_writer);
        let _ = self
            .0
            .inner
            .tasks
            .unbounded_send(Task::Spawn(Box::pin(forward)));
        (stream, end)
    }
}

/// The kind of a subscription's target, each with its own commands and push messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Channel,
    Pattern,
    Shard,
}

impl Kind {
    const ALL: [Kind; 3] = [Kind::Channel, Kind::Pattern, Kind::Shard];

    fn subscribe(self) -> &'static str {
        match self {
            Kind::Channel => "SUBSCRIBE",
            Kind::Pattern => "PSUBSCRIBE",
            Kind::Shard => "SSUBSCRIBE",
        }
    }

    fn unsubscribe(self) -> &'static str {
        match self {
            Kind::Channel => "UNSUBSCRIBE",
            Kind::Pattern => "PUNSUBSCRIBE",
            Kind::Shard => "SUNSUBSCRIBE",
        }
    }

    fn target(self, name: String) -> SubscriptionTarget {
        match self {
            Kind::Channel => SubscriptionTarget::Channel(name),
            Kind::Pattern => SubscriptionTarget::Pattern(name),
            Kind::Shard => SubscriptionTarget::Shardchannel(name),
        }
    }
}

/// The names of the targets of each kind, in the order each kind first appears, failing unless
/// there is at least one target.
fn by_kind(targets: Vec<SubscriptionTarget>) -> Result<Vec<(Kind, Vec<String>)>, Error> {
    if targets.is_empty() {
        return Err(Error::Client(
            "targets requires at least one item".to_string(),
        ));
    }
    let mut groups: Vec<(Kind, Vec<String>)> = vec![];
    for target in targets {
        let (kind, name) = match target {
            SubscriptionTarget::Channel(name) => (Kind::Channel, name),
            SubscriptionTarget::Pattern(name) => (Kind::Pattern, name),
            SubscriptionTarget::Shardchannel(name) => (Kind::Shard, name),
        };
        match groups.iter_mut().find(|(grouped, _)| *grouped == kind) {
            Some((_, names)) => names.push(name),
            None => groups.push((kind, vec![name])),
        }
    }
    Ok(groups)
}

/// A channel, pattern or shard channel, by its kind and name.
type Target = (Kind, String);

/// How a subscription holds a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hold {
    /// Waiting for the server to confirm SUBSCRIBE.
    Pending,
    /// Confirmed, its messages arrive.
    Active,
    /// Unsubscribing, its messages arrive until the server confirms UNSUBSCRIBE.
    Leaving,
}

/// How a stream of events ended, `ok` once the subscription is dropped.
type Ended = Result<(), Error>;

/// A reader of a subscription's events, and where to send how its stream ended.
struct Reader {
    events: mpsc::Sender<SubscriptionEvent>,
    end: oneshot::Sender<Ended>,
}

#[derive(Default)]
struct Sub {
    /// The targets, in the order they were subscribed to.
    targets: Vec<(Target, Hold)>,
    readers: Vec<Reader>,
    /// How to tell readers whose stream was dropped how the subscription ended.
    orphans: Vec<oneshot::Sender<Ended>>,
    /// The events arriving while no stream is open, for the next stream.
    backlog: VecDeque<SubscriptionEvent>,
    /// The error that closed the connection, once it has.
    closed: Option<Error>,
}

impl Sub {
    fn hold(&self, target: &Target) -> Option<Hold> {
        self.targets
            .iter()
            .find(|(held, _)| held == target)
            .map(|(_, hold)| *hold)
    }

    fn set_hold(&mut self, target: &Target, hold: Hold) {
        match self.targets.iter_mut().find(|(held, _)| held == target) {
            Some((_, current)) => *current = hold,
            None => self.targets.push((target.clone(), hold)),
        }
    }

    fn release(&mut self, target: &Target) {
        self.targets.retain(|(held, _)| held != target);
    }

    /// Fan the event out to every reader, a reader that is full is cut off, one whose stream was
    /// dropped is forgotten.
    fn emit(&mut self, event: SubscriptionEvent) {
        if self.readers.is_empty() {
            if self.backlog.len() < PUSH_BUFFER {
                self.backlog.push_back(event);
            }
            return;
        }
        for mut reader in std::mem::take(&mut self.readers) {
            match reader.events.try_send(event.clone()) {
                Ok(()) => self.readers.push(reader),
                Err(error) if error.is_full() => {
                    let _ = reader.end.send(Err(Error::Lagged));
                }
                Err(_) => self.orphans.push(reader.end),
            }
        }
    }

    /// End the readers' streams.
    fn end(&mut self, ended: Ended) {
        for reader in self.readers.drain(..) {
            let _ = reader.end.send(ended.clone());
        }
        for orphan in self.orphans.drain(..) {
            let _ = orphan.send(ended.clone());
        }
    }
}

fn unsubscribed(target: &Target, reason: UnsubscribeReason) -> SubscriptionEvent {
    SubscriptionEvent::Unsubscribed((target.0.target(target.1.clone()), reason))
}

#[derive(Default)]
struct Subscriptions {
    next_id: u64,
    subs: HashMap<u64, Sub>,
    /// The subscriptions holding each target, in any way.
    holders: HashMap<Target, Vec<u64>>,
}

impl Subscriptions {
    fn create(&mut self, closed: Option<Error>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.subs.insert(
            id,
            Sub {
                closed,
                ..Sub::default()
            },
        );
        id
    }

    fn hold(&mut self, id: u64, target: &Target, hold: Hold) {
        let Some(sub) = self.subs.get_mut(&id) else {
            return;
        };
        if sub.hold(target).is_none() {
            self.holders.entry(target.clone()).or_default().push(id);
        }
        sub.set_hold(target, hold);
    }

    /// The subscription no longer holds the target.
    fn release(&mut self, id: u64, target: &Target) {
        if let Some(sub) = self.subs.get_mut(&id) {
            sub.release(target);
        }
        if let Some(ids) = self.holders.get_mut(target) {
            ids.retain(|holder| *holder != id);
            if ids.is_empty() {
                self.holders.remove(target);
            }
        }
    }

    /// The subscriptions holding the target in one of the ways.
    fn holding(&self, target: &Target, holds: &[Hold]) -> Vec<u64> {
        self.holders
            .get(target)
            .into_iter()
            .flatten()
            .copied()
            .filter(|id| {
                self.subs
                    .get(id)
                    .and_then(|sub| sub.hold(target))
                    .is_some_and(|hold| holds.contains(&hold))
            })
            .collect()
    }

    fn emit(&mut self, id: u64, event: SubscriptionEvent) {
        if let Some(sub) = self.subs.get_mut(&id) {
            sub.emit(event);
        }
    }

    /// The server confirmed SUBSCRIBE for the subscription's target.
    fn subscribed(&mut self, id: u64, target: &Target) {
        let Some(sub) = self.subs.get_mut(&id) else {
            return;
        };
        match sub.hold(target) {
            Some(Hold::Pending) => sub.set_hold(target, Hold::Active),
            // unsubscribed from while waiting, UNSUBSCRIBE follows
            Some(Hold::Leaving) => {}
            _ => return,
        }
        sub.emit(SubscriptionEvent::Subscribed(
            target.0.target(target.1.clone()),
        ));
    }

    /// A message for the subscriptions receiving the target's messages, false when none holds it.
    fn message(&mut self, target: &Target, message: SubscriptionMessage) -> bool {
        if !self.holders.contains_key(target) {
            return false;
        }
        for id in self.holding(target, &[Hold::Active, Hold::Leaving]) {
            self.emit(id, SubscriptionEvent::Message(message.clone()));
        }
        true
    }

    /// The server confirmed UNSUBSCRIBE for the target, the subscriptions leaving it let go.
    fn unsubscribe_confirmed(&mut self, target: &Target) {
        for id in self.holding(target, &[Hold::Leaving]) {
            self.release(id, target);
            self.emit(id, unsubscribed(target, UnsubscribeReason::Requested));
        }
    }

    /// The server unsubscribed from the target on its own, false when none holds it.
    fn unsubscribed_by_server(&mut self, target: &Target) -> bool {
        if !self.holders.contains_key(target) {
            return false;
        }
        for id in self.holding(target, &[Hold::Active, Hold::Leaving]) {
            let leaving =
                self.subs.get(&id).and_then(|sub| sub.hold(target)) == Some(Hold::Leaving);
            self.release(id, target);
            let reason = if leaving {
                UnsubscribeReason::Requested
            } else {
                UnsubscribeReason::ByServer
            };
            self.emit(id, unsubscribed(target, reason));
        }
        true
    }

    /// RESET unsubscribed from every target the server confirmed, without a push message. Those
    /// still waiting for the server subscribe after RESET.
    fn reset(&mut self) {
        let mut reset = vec![];
        for (id, sub) in &self.subs {
            for (target, hold) in &sub.targets {
                if *hold != Hold::Pending {
                    reset.push((*id, target.clone()));
                }
            }
        }
        for (id, target) in reset {
            self.release(id, &target);
            self.emit(id, unsubscribed(&target, UnsubscribeReason::Reset));
        }
    }

    /// The connection closed, ending every stream.
    fn close(&mut self, error: &Error) {
        for sub in self.subs.values_mut() {
            sub.end(Err(error.clone()));
            sub.closed = Some(error.clone());
        }
        self.holders.clear();
    }

    /// The subscription was dropped and unsubscribed, ending its streams.
    fn remove(&mut self, id: u64) {
        let Some(mut sub) = self.subs.remove(&id) else {
            return;
        };
        sub.end(Ok(()));
        for (target, _) in sub.targets {
            if let Some(ids) = self.holders.get_mut(&target) {
                ids.retain(|holder| *holder != id);
                if ids.is_empty() {
                    self.holders.remove(&target);
                }
            }
        }
    }
}

/// A subscription, for its resource.
///
/// Dropping it unsubscribes from its targets, in the background, then ends its streams.
struct Handle {
    inner: Rc<Inner>,
    id: u64,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let id = self.id;
        let _ = self
            .inner
            .tasks
            .unbounded_send(Task::Spawn(Box::pin(async move {
                let every = Kind::ALL.into_iter().map(|kind| (kind, None)).collect();
                let _ = unsubscribe(&inner, id, every).await;
                inner.state.borrow_mut().subscriptions.remove(id);
            })));
    }
}

/// The names given once each, in order.
fn unique(names: Vec<String>) -> Vec<String> {
    let mut unique: Vec<String> = vec![];
    for name in names {
        if !unique.contains(&name) {
            unique.push(name);
        }
    }
    unique
}

/// A command for one kind of targets, queued and waiting for the server to confirm it.
type Confirmation = oneshot::Receiver<Result<(), Error>>;

/// Subscribe to the targets, one command for each kind, queued with no other command between
/// them. Every command is sent, the error holds each that failed.
async fn subscribe(inner: &Inner, id: u64, groups: Vec<(Kind, Vec<String>)>) -> Result<(), Error> {
    let several = groups.len() > 1;
    // no await until every command is queued
    let queued: Vec<_> = groups
        .into_iter()
        .map(|(kind, names)| (kind, queue_subscribe(inner, id, kind, names)))
        .collect();
    let mut errors = vec![];
    for (kind, queued) in queued {
        let result = match queued {
            Ok(None) => Ok(()),
            // dropped unsent when the connection closes, the reader fails it first
            Ok(Some((names, confirmation))) => {
                let result = confirmation
                    .await
                    .unwrap_or_else(|_| Err(closed_error(&inner.state)));
                if result.is_err() {
                    abandon(
                        &mut inner.state.borrow_mut().subscriptions,
                        id,
                        kind,
                        &names,
                    );
                }
                result
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            errors.push((kind.subscribe(), error));
        }
    }
    aggregate(errors, several)
}

/// Queue SUBSCRIBE for the targets the subscription does not hold yet, none when it holds them all.
fn queue_subscribe(
    inner: &Inner,
    id: u64,
    kind: Kind,
    names: Vec<String>,
) -> Result<Option<(Vec<String>, Confirmation)>, Error> {
    let names: Vec<String> = {
        let mut state = inner.state.borrow_mut();
        // before the targets are touched, they stay as they were when the connection closed
        if let Some(error) = &state.closed {
            return Err(error.clone());
        }
        let subscriptions = &mut state.subscriptions;
        let Some(sub) = subscriptions.subs.get(&id) else {
            return Ok(None);
        };
        let names: Vec<String> = unique(names)
            .into_iter()
            .filter(|name| {
                // a target being left is subscribed to again, after its UNSUBSCRIBE
                matches!(sub.hold(&(kind, name.clone())), None | Some(Hold::Leaving))
            })
            .collect();
        for name in &names {
            subscriptions.hold(id, &(kind, name.clone()), Hold::Pending);
        }
        names
    };
    if names.is_empty() {
        return Ok(None);
    }
    let command = std::iter::once(kind.subscribe().to_string())
        .chain(names.iter().cloned())
        .collect();
    let (done_tx, done_rx) = oneshot::channel();
    let reply = Reply::Subscribe {
        kind,
        channels: names.clone(),
        id,
        done: done_tx,
    };
    match enqueue(inner, command, Some(reply), None) {
        Ok(()) => Ok(Some((names, done_rx))),
        Err(error) => {
            abandon(
                &mut inner.state.borrow_mut().subscriptions,
                id,
                kind,
                &names,
            );
            Err(error)
        }
    }
}

/// SUBSCRIBE failed, the subscription lets go of the targets it was waiting for.
fn abandon(subscriptions: &mut Subscriptions, id: u64, kind: Kind, names: &[String]) {
    for name in names {
        let target = (kind, name.clone());
        let pending = subscriptions
            .subs
            .get(&id)
            .and_then(|sub| sub.hold(&target))
            == Some(Hold::Pending);
        if pending {
            subscriptions.release(id, &target);
        }
    }
}

/// Unsubscribe from the targets, none for every target of the kind, one command for each kind,
/// queued with no other command between them. Every command is sent, the error holds each that
/// failed.
async fn unsubscribe(
    inner: &Inner,
    id: u64,
    groups: Vec<(Kind, Option<Vec<String>>)>,
) -> Result<(), Error> {
    let several = groups.len() > 1;
    // no await until every command is queued
    let queued: Vec<_> = groups
        .into_iter()
        .map(|(kind, names)| (kind, queue_unsubscribe(inner, id, kind, names)))
        .collect();
    let mut errors = vec![];
    for (kind, queued) in queued {
        let result = match queued {
            Ok(None) => Ok(()),
            // dropped unsent when the connection closes, the reader fails it first
            Ok(Some(confirmation)) => confirmation
                .await
                .unwrap_or_else(|_| Err(closed_error(&inner.state))),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            errors.push((kind.unsubscribe(), error));
        }
    }
    aggregate(errors, several)
}

/// Queue UNSUBSCRIBE for the targets no other subscription holds, the subscription lets go of the
/// others at once. None when there is nothing to send.
fn queue_unsubscribe(
    inner: &Inner,
    id: u64,
    kind: Kind,
    names: Option<Vec<String>>,
) -> Result<Option<Confirmation>, Error> {
    let release = {
        let mut state = inner.state.borrow_mut();
        // before the targets are touched, they stay as they were when the connection closed
        if let Some(error) = &state.closed {
            return Err(error.clone());
        }
        let subscriptions = &mut state.subscriptions;
        let Some(sub) = subscriptions.subs.get(&id) else {
            return Ok(None);
        };
        let held = |name: &String| {
            matches!(
                sub.hold(&(kind, name.clone())),
                Some(Hold::Pending | Hold::Active)
            )
        };
        let names: Vec<String> = match names {
            Some(names) => unique(names).into_iter().filter(held).collect(),
            None => sub
                .targets
                .iter()
                .filter(|((held_kind, name), _)| *held_kind == kind && held(name))
                .map(|((_, name), _)| name.clone())
                .collect(),
        };
        let mut release = vec![];
        for name in names {
            let target = (kind, name);
            let kept = subscriptions
                .holding(&target, &[Hold::Pending, Hold::Active])
                .iter()
                .any(|other| *other != id);
            if kept {
                subscriptions.release(id, &target);
                subscriptions.emit(id, unsubscribed(&target, UnsubscribeReason::Requested));
            } else {
                subscriptions.hold(id, &target, Hold::Leaving);
                release.push(target.1);
            }
        }
        release
    };
    if release.is_empty() {
        return Ok(None);
    }
    let command = std::iter::once(kind.unsubscribe().to_string())
        .chain(release.iter().cloned())
        .collect();
    let (done_tx, done_rx) = oneshot::channel();
    let reply = Reply::Unsubscribe {
        kind,
        channels: release,
        done: done_tx,
    };
    enqueue(inner, command, Some(reply), None)?;
    Ok(Some(done_rx))
}

/// The errors of the commands of a call as one, each prefixed with its command when the call sent
/// several. An error of one kind keeps it, e.g. every command was refused by the server, otherwise
/// it is a client error.
fn aggregate(errors: Vec<(&str, Error)>, several: bool) -> Result<(), Error> {
    let mut errors = errors.into_iter();
    let Some((command, first)) = errors.next() else {
        return Ok(());
    };
    if !several {
        return Err(first);
    }
    let kind = std::mem::discriminant(&first);
    let mut same = true;
    let mut messages = vec![format!("{command}: {}", describe_error(&first))];
    for (command, error) in errors {
        same &= std::mem::discriminant(&error) == kind;
        messages.push(format!("{command}: {}", describe_error(&error)));
    }
    if messages.len() == 1 {
        return Err(match first {
            Error::Valkey(_) => Error::Valkey(messages.remove(0)),
            Error::Client(_) => Error::Client(messages.remove(0)),
            Error::Resp(_) => Error::Resp(messages.remove(0)),
            error => error,
        });
    }
    let message = messages.join("; ");
    Err(match first {
        // the connection closed, every command failed the same way
        error @ (Error::Closed(_) | Error::Lagged) if same => error,
        Error::Valkey(_) if same => Error::Valkey(message),
        Error::Resp(_) if same => Error::Resp(message),
        _ => Error::Client(message),
    })
}

/// An error for people to read, in an aggregate of errors.
fn describe_error(error: &Error) -> String {
    match error {
        Error::Closed(None) => "connection closed".to_string(),
        Error::Closed(Some(reason)) => format!("connection closed: {reason}"),
        Error::Client(message) | Error::Resp(message) | Error::Valkey(message) => message.clone(),
        Error::Lagged => "fell behind".to_string(),
    }
}

/// RESET, then HELLO again, with no other command between them, closing the connection when HELLO
/// fails.
///
/// The writer sends no other command until HELLO succeeds, so none runs on the connection RESET
/// leaves: RESP2, the default user, no name. When HELLO fails, e.g. the credentials no longer work,
/// the reader closes the connection, and the commands queued after it fail without being sent.
async fn reset(inner: &Inner) -> Result<(), Error> {
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
            .map_err(|_| Error::Closed(None))?;
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
        .map_err(|_| Error::Closed(None))?;
    state.pending.extend(reply);
    Ok(())
}

async fn exchange(inner: &Inner, command: Vec<String>) -> Result<RespValue, Error> {
    reject_managed(inner, &command)?;
    if replies_with_pushes(&command) {
        return emit(inner, command).await.map(|()| RespValue::Null);
    }
    request(inner, command).await
}

/// Send a command and wait for its reply, for the commands the connection sends itself, e.g. HELLO,
/// which callers may not.
async fn request(inner: &Inner, command: Vec<String>) -> Result<RespValue, Error> {
    let (reply_tx, reply_rx) = oneshot::channel();
    enqueue(inner, command, Some(Reply::Call(reply_tx)), None)?;
    // the reply is dropped unsent when the connection closes, the reader fails it first
    reply_rx
        .await
        .unwrap_or_else(|_| Err(closed_error(&inner.state)))
}

/// Queue every command before waiting for any reply, so no other command is sent between them.
async fn exchange_all(inner: &Inner, commands: Vec<Vec<String>>) -> Vec<Result<RespValue, Error>> {
    // no await until every command is queued
    let queued: Vec<Result<Option<oneshot::Receiver<_>>, Error>> = commands
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
    commands: Vec<Vec<String>>,
) -> Result<Option<Vec<RespValue>>, Error> {
    // before MULTI is sent, so an invalid command fails the transaction rather than being left out
    if commands.iter().any(Vec::is_empty) {
        return Err(Error::Client(
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
            .map(|reply| componentized_valkey::resp::decode(reply).map_err(Error::Resp))
            .collect::<Result<_, _>>()
            .map(Some),
        reply => Err(unexpected(&reply)),
    }
}

async fn emit(inner: &Inner, command: Vec<String>) -> Result<(), Error> {
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
fn reject_managed(inner: &Inner, command: &[String]) -> Result<(), Error> {
    let word = |index: usize| command.get(index).map(|word| word.to_uppercase());
    let name = word(0).unwrap_or_default();
    let reason = match (name.as_str(), word(1).as_deref()) {
        ("RESET", _) => Some("RESET is rejected, use the connection's reset"),
        ("HELLO", _) => Some(
            "HELLO is rejected, the connection sent it when it was established, see its server-properties",
        ),
        ("CLIENT", Some("REPLY")) => {
            Some("CLIENT REPLY is rejected, every command has a reply, see the connection's emit")
        }
        _ => None,
    };
    if let Some(reason) = reason {
        return Err(Error::Client(reason.to_string()));
    }
    if replies_with_pushes(command) && inner.state.borrow().owns_subscriptions {
        return Err(Error::Client(format!(
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

/// A push message the connection understands.
enum Push {
    /// The server subscribed to the target.
    Subscribed(Target),
    /// The server unsubscribed from the target, none when nothing was subscribed.
    Unsubscribed(Kind, Option<String>),
    /// A message, for the target it was subscribed with.
    Message(Target, SubscriptionMessage),
}

fn parse_push(value: &RespValue) -> Option<Push> {
    let RespValue::Push(items) = value else {
        return None;
    };
    let items = items
        .iter()
        .map(|item| decode(item.clone()).ok())
        .collect::<Option<Vec<_>>>()?;
    let text = |index: usize| match items.get(index) {
        Some(RespValue::String(text) | RespValue::BulkString(text)) => Some(text.clone()),
        _ => None,
    };
    let push = match text(0)?.as_str() {
        "subscribe" => Push::Subscribed((Kind::Channel, text(1)?)),
        "psubscribe" => Push::Subscribed((Kind::Pattern, text(1)?)),
        "ssubscribe" => Push::Subscribed((Kind::Shard, text(1)?)),
        "unsubscribe" => Push::Unsubscribed(Kind::Channel, text(1)),
        "punsubscribe" => Push::Unsubscribed(Kind::Pattern, text(1)),
        "sunsubscribe" => Push::Unsubscribed(Kind::Shard, text(1)),
        "message" => {
            let channel = text(1)?;
            let message = SubscriptionMessage {
                payload: text(2)?,
                channel: channel.clone(),
                target: SubscriptionTarget::Channel(channel.clone()),
            };
            Push::Message((Kind::Channel, channel), message)
        }
        "pmessage" => {
            let pattern = text(1)?;
            let message = SubscriptionMessage {
                payload: text(3)?,
                channel: text(2)?,
                target: SubscriptionTarget::Pattern(pattern.clone()),
            };
            Push::Message((Kind::Pattern, pattern), message)
        }
        "smessage" => {
            let shardchannel = text(1)?;
            let message = SubscriptionMessage {
                payload: text(2)?,
                channel: shardchannel.clone(),
                target: SubscriptionTarget::Shardchannel(shardchannel.clone()),
            };
            Push::Message((Kind::Shard, shardchannel), message)
        }
        _ => return None,
    };
    Some(push)
}

/// Hand a push message to the subscriptions, or the command, it belongs to, false when it belongs
/// to none.
fn claim_push(state: &mut State, value: &RespValue) -> bool {
    let State {
        pending,
        subscriptions,
        ..
    } = state;
    match parse_push(value) {
        Some(Push::Subscribed(target)) => {
            // the confirmations of a SUBSCRIBE arrive in the place of its reply
            let Some(Reply::Subscribe {
                kind, channels, id, ..
            }) = pending.front_mut()
            else {
                return false;
            };
            if *kind != target.0 {
                return false;
            }
            let Some(index) = channels.iter().position(|name| *name == target.1) else {
                return false;
            };
            channels.remove(index);
            let (id, done) = (*id, channels.is_empty());
            subscriptions.subscribed(id, &target);
            if done && let Some(Reply::Subscribe { done, .. }) = pending.pop_front() {
                let _ = done.send(Ok(()));
            }
            true
        }
        Some(Push::Unsubscribed(kind, Some(name))) => {
            let target = (kind, name);
            // the confirmations of an UNSUBSCRIBE arrive in the place of its reply, otherwise the
            // server unsubscribed on its own
            let confirmation = match pending.front_mut() {
                Some(Reply::Unsubscribe { kind, channels, .. }) if *kind == target.0 => channels
                    .iter()
                    .position(|name| *name == target.1)
                    .map(|index| {
                        channels.remove(index);
                        channels.is_empty()
                    }),
                _ => None,
            };
            match confirmation {
                Some(done) => {
                    subscriptions.unsubscribe_confirmed(&target);
                    if done && let Some(Reply::Unsubscribe { done, .. }) = pending.pop_front() {
                        let _ = done.send(Ok(()));
                    }
                    true
                }
                None => subscriptions.unsubscribed_by_server(&target),
            }
        }
        Some(Push::Message(target, message)) => subscriptions.message(&target, message),
        Some(Push::Unsubscribed(_, None)) | None => false,
    }
}

/// The error a command fails with once the connection closed.
fn closed_error(state: &RefCell<State>) -> Error {
    state.borrow().closed.clone().unwrap_or(Error::Closed(None))
}

/// Close the connection, failing every command waiting for a reply, closing the readers of dead
/// letters, and ending the subscriptions.
fn close(state: &RefCell<State>, error: Error) {
    let mut state = state.borrow_mut();
    let error = state.closed.get_or_insert(error).clone();
    for reply in state.pending.drain(..).collect::<Vec<_>>() {
        match reply {
            Reply::Call(reply) => {
                let _ = reply.send(Err(error.clone()));
            }
            Reply::Subscribe { done, .. }
            | Reply::Unsubscribe { done, .. }
            | Reply::Reset(done) => {
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

fn words(words: &[&str]) -> Vec<String> {
    words.iter().map(|word| word.to_string()).collect()
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
    closed: FutureReader<Result<(), Error>>,
) {
    // the error to close the connection with, before the transport closes
    let mut closing = None;
    while let Some(value) = replies.next().await {
        let mut state = state.borrow_mut();
        let state = &mut *state;
        state.activity += 1;
        let value = match value {
            push @ RespValue::Push(_) => {
                if claim_push(state, &push) {
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
                Some(Reply::Subscribe {
                    kind,
                    channels,
                    id,
                    done,
                }) => {
                    abandon(&mut state.subscriptions, id, kind, &channels);
                    let _ =
                        done.send(reply_result(value).and_then(|reply| Err(unexpected(&reply))));
                    continue;
                }
                Some(Reply::Unsubscribe { done, .. }) => {
                    let _ =
                        done.send(reply_result(value).and_then(|reply| Err(unexpected(&reply))));
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
    close(&state, Error::Closed(reason));
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
        }
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

/// Forward a subscription's events to a reader's stream, then close the stream and tell the reader
/// how it ended.
async fn forward_events(
    mut events: mpsc::Receiver<SubscriptionEvent>,
    end: oneshot::Receiver<Ended>,
    mut writer: StreamWriter<SubscriptionEvent>,
    end_writer: FutureWriter<Ended>,
) {
    while let Some(event) = next(&mut events).await {
        if writer.write_one(event).await.is_some() {
            // the stream was dropped, the subscription carries on without it
            break;
        }
    }
    drop(events);
    drop(writer);
    let end = end.await.unwrap_or_else(|_| Err(Error::Closed(None)));
    let _ = end_writer.write(end).await;
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

    inner.state.borrow_mut().hello = hello.clone();
    let result = match request(&inner, hello).await.and_then(server_properties) {
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
fn server_properties(reply: RespValue) -> Result<Vec<(String, RespValue)>, Error> {
    let RespValue::Map(entries) = reply else {
        return Err(Error::Client(format!(
            "unexpected reply to HELLO: {reply:?}"
        )));
    };
    entries
        .into_iter()
        .map(|(name, value)| {
            let name = match decode(name)? {
                RespValue::String(name) | RespValue::BulkString(name) => name,
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

fn decode(value: RespNestedValue) -> Result<RespValue, Error> {
    componentized_valkey::resp::decode(value).map_err(Error::Resp)
}

/// An error value from the server fails the command.
/// A simple string `OK`.
fn ok(reply: RespValue) -> Result<(), Error> {
    match reply {
        RespValue::String(text) if text == "OK" => Ok(()),
        reply => Err(unexpected(&reply)),
    }
}

fn unexpected(reply: &RespValue) -> Error {
    Error::Client(format!("unexpected reply: {reply:?}"))
}

fn reply_result(value: RespValue) -> Result<RespValue, Error> {
    match value {
        RespValue::Error(message) | RespValue::BulkError(message) => Err(Error::Valkey(message)),
        value => Ok(value),
    }
}

/// A transport's error as the reason a connection closed.
fn describe(error: Error) -> Option<String> {
    match error {
        Error::Closed(reason) => reason,
        Error::Client(message) | Error::Resp(message) | Error::Valkey(message) => Some(message),
        Error::Lagged => Some("a stream fell behind".to_string()),
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
