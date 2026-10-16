//! Pub/sub left out, for builds without the `pubsub` feature, see `pubsub.rs`.
//!
//! The interface stays the same: the subscription resource is exported, but none is ever made,
//! `connection.subscription` fails, and every push message is a dead letter.

use std::rc::Rc;

use componentized_valkey::types::{RespValue, SubscriptionEvent, SubscriptionTarget};
use wit_bindgen::{FutureReader, StreamReader};

use crate::exports::componentized::valkey::connection::{
    ErrorCode, GuestSubscription, Subscription,
};
use crate::{Inner, State};

/// Fails, there are no subscriptions in this build.
pub(crate) fn subscription(_inner: &Rc<Inner>) -> Result<Subscription, ErrorCode> {
    Err(ErrorCode::Client(
        "subscriptions are not supported by this build of the connection, push messages arrive in dead-letters"
            .to_string(),
    ))
}

/// A subscription's command waiting for the server, never queued in this build.
pub(crate) enum Pending {}

impl Pending {
    pub(crate) fn refused(self, _subscriptions: &mut Subscriptions, _error: ErrorCode) {
        match self {}
    }

    pub(crate) fn fail(self, _error: ErrorCode) {
        match self {}
    }
}

/// No subscriptions.
#[derive(Default)]
pub(crate) struct Subscriptions;

impl Subscriptions {
    pub(crate) fn reset(&mut self) {}

    pub(crate) fn close(&mut self, _error: &ErrorCode) {}
}

/// No push message belongs to a subscription, each is a dead letter.
pub(crate) fn claim_push(_state: &mut State, _value: &RespValue) -> bool {
    false
}

/// A subscription, of which this build never makes one.
pub enum SubscriptionImpl {}

impl GuestSubscription for SubscriptionImpl {
    fn targets(&self) -> Vec<SubscriptionTarget> {
        match *self {}
    }

    async fn subscribe(&self, _targets: Vec<SubscriptionTarget>) -> Result<(), ErrorCode> {
        match *self {}
    }

    async fn unsubscribe(
        &self,
        _targets: Option<Vec<SubscriptionTarget>>,
    ) -> Result<(), ErrorCode> {
        match *self {}
    }

    fn events(
        &self,
    ) -> (
        StreamReader<SubscriptionEvent>,
        FutureReader<Result<(), ErrorCode>>,
    ) {
        match *self {}
    }
}
