//! Pub/sub: subscriptions to channels, patterns and shard channels, see `subscription` in the
//! connection's WIT. Left out of builds without the `pubsub` feature, see `pubsub_disabled.rs`.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use componentized_valkey::types::{
    RespValue, StringOrBytes, SubscriptionEvent, SubscriptionMessage, SubscriptionTarget,
    UnsubscribeReason,
};
use futures_channel::{mpsc, oneshot};
use wit_bindgen::{FutureReader, FutureWriter, StreamReader, StreamWriter};

use crate::exports::componentized::valkey::connection::{Error, GuestSubscription, Subscription};
use crate::{
    Inner, PUSH_BUFFER, Reply, State, Task, closed_error, decode, enqueue, next, wit_future,
    wit_stream,
};

/// A new subscription, setting the ratchet, see `State::owns_subscriptions`.
pub(crate) fn subscription(inner: &Rc<Inner>) -> Result<Subscription, Error> {
    let id = {
        let mut state = inner.state.borrow_mut();
        state.owns_subscriptions = true;
        let closed = state.closed.clone();
        state.subscriptions.create(closed)
    };
    Ok(Subscription::new(SubscriptionImpl(Handle {
        inner: inner.clone(),
        id,
    })))
}

/// A subscription's command waiting for the server, confirmed by a push message for each target
/// rather than a reply.
pub(crate) enum Pending {
    /// SUBSCRIBE and the like.
    Subscribe {
        kind: Kind,
        /// The targets not confirmed yet.
        channels: Vec<String>,
        id: u64,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// UNSUBSCRIBE and the like.
    Unsubscribe {
        kind: Kind,
        /// The targets not confirmed yet.
        channels: Vec<String>,
        done: oneshot::Sender<Result<(), Error>>,
    },
}

impl Pending {
    /// A reply arrived in the place of the confirmations, the server refused the command.
    pub(crate) fn refused(self, subscriptions: &mut Subscriptions, error: Error) {
        match self {
            Pending::Subscribe {
                kind,
                channels,
                id,
                done,
            } => {
                abandon(subscriptions, id, kind, &channels);
                let _ = done.send(Err(error));
            }
            Pending::Unsubscribe { done, .. } => {
                let _ = done.send(Err(error));
            }
        }
    }

    /// The connection closed.
    pub(crate) fn fail(self, error: Error) {
        match self {
            Pending::Subscribe { done, .. } | Pending::Unsubscribe { done, .. } => {
                let _ = done.send(Err(error));
            }
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
pub(crate) enum Kind {
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
pub(crate) struct Subscriptions {
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
    pub(crate) fn reset(&mut self) {
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
    pub(crate) fn close(&mut self, error: &Error) {
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
    let command = std::iter::once(kind.subscribe().as_bytes().to_vec())
        .chain(names.iter().map(|name| name.as_bytes().to_vec()))
        .collect();
    let (done_tx, done_rx) = oneshot::channel();
    let reply = Reply::Pubsub(Pending::Subscribe {
        kind,
        channels: names.clone(),
        id,
        done: done_tx,
    });
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
    let command = std::iter::once(kind.unsubscribe().as_bytes().to_vec())
        .chain(release.iter().map(|name| name.as_bytes().to_vec()))
        .collect();
    let (done_tx, done_rx) = oneshot::channel();
    let reply = Reply::Pubsub(Pending::Unsubscribe {
        kind,
        channels: release,
        done: done_tx,
    });
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
    let mut items = items
        .iter()
        .map(|item| decode(item).ok())
        .collect::<Option<Vec<_>>>()?;
    // each item is taken once, rather than cloned
    let mut take = |index: usize| {
        items
            .get_mut(index)
            .map(|item| std::mem::replace(item, RespValue::Null))
    };
    let push = match text(take(0))?.as_str() {
        "subscribe" => Push::Subscribed((Kind::Channel, text(take(1))?)),
        "psubscribe" => Push::Subscribed((Kind::Pattern, text(take(1))?)),
        "ssubscribe" => Push::Subscribed((Kind::Shard, text(take(1))?)),
        "unsubscribe" => Push::Unsubscribed(Kind::Channel, text(take(1))),
        "punsubscribe" => Push::Unsubscribed(Kind::Pattern, text(take(1))),
        "sunsubscribe" => Push::Unsubscribed(Kind::Shard, text(take(1))),
        "message" => {
            let channel = text(take(1))?;
            let message = SubscriptionMessage {
                payload: payload(take(2))?,
                channel: channel.clone(),
                target: SubscriptionTarget::Channel(channel.clone()),
            };
            Push::Message((Kind::Channel, channel), message)
        }
        "pmessage" => {
            let pattern = text(take(1))?;
            let message = SubscriptionMessage {
                channel: text(take(2))?,
                payload: payload(take(3))?,
                target: SubscriptionTarget::Pattern(pattern.clone()),
            };
            Push::Message((Kind::Pattern, pattern), message)
        }
        "smessage" => {
            let shardchannel = text(take(1))?;
            let message = SubscriptionMessage {
                payload: payload(take(2))?,
                channel: shardchannel.clone(),
                target: SubscriptionTarget::Shardchannel(shardchannel.clone()),
            };
            Push::Message((Kind::Shard, shardchannel), message)
        }
        _ => return None,
    };
    Some(push)
}

/// The text of an item of a push message, a simple or bulk string that is UTF-8, e.g. its kind or
/// a channel.
fn text(item: Option<RespValue>) -> Option<String> {
    match item? {
        RespValue::String(text) => Some(text),
        RespValue::BulkString(bytes) => String::from_utf8(bytes).ok(),
        _ => None,
    }
}

/// The payload of a message, a string when it is UTF-8, otherwise its bytes.
fn payload(item: Option<RespValue>) -> Option<StringOrBytes> {
    match item? {
        // the bytes come back with the error, rather than being cloned to try
        RespValue::BulkString(bytes) => Some(match String::from_utf8(bytes) {
            Ok(text) => StringOrBytes::String(text),
            Err(error) => StringOrBytes::Bytes(error.into_bytes()),
        }),
        RespValue::String(text) => Some(StringOrBytes::String(text)),
        _ => None,
    }
}

/// Hand a push message to the subscriptions, or the command, it belongs to, false when it belongs
/// to none.
pub(crate) fn claim_push(state: &mut State, value: &RespValue) -> bool {
    let State {
        pending,
        subscriptions,
        ..
    } = state;
    match parse_push(value) {
        Some(Push::Subscribed(target)) => {
            // the confirmations of a SUBSCRIBE arrive in the place of its reply
            let Some(Reply::Pubsub(Pending::Subscribe {
                kind, channels, id, ..
            })) = pending.front_mut()
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
            if done
                && let Some(Reply::Pubsub(Pending::Subscribe { done, .. })) = pending.pop_front()
            {
                let _ = done.send(Ok(()));
            }
            true
        }
        Some(Push::Unsubscribed(kind, Some(name))) => {
            let target = (kind, name);
            // the confirmations of an UNSUBSCRIBE arrive in the place of its reply, otherwise the
            // server unsubscribed on its own
            let confirmation = match pending.front_mut() {
                Some(Reply::Pubsub(Pending::Unsubscribe { kind, channels, .. }))
                    if *kind == target.0 =>
                {
                    channels
                        .iter()
                        .position(|name| *name == target.1)
                        .map(|index| {
                            channels.remove(index);
                            channels.is_empty()
                        })
                }
                _ => None,
            };
            match confirmation {
                Some(done) => {
                    subscriptions.unsubscribe_confirmed(&target);
                    if done
                        && let Some(Reply::Pubsub(Pending::Unsubscribe { done, .. })) =
                            pending.pop_front()
                    {
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
