//! Tests for the `connection` component, over a stub transport playing the server.

use std::time::Duration;

use test_harness::types::{
    DurationMilliseconds, SubscriptionEvent, SubscriptionMessage, SubscriptionTarget,
    UnsubscribeReason,
};
use test_harness::{
    ConnectOpts, Error, Harness, RespValue, Transport, bulk, command, connect, establish,
    handshake, hello_reply, ok, push, receive, resolve, words,
};

/// Long enough for the connection to act on what it was given, short enough to keep tests quick.
const SETTLE: Duration = Duration::from_millis(100);

fn simple(text: &str) -> RespValue {
    RespValue::String(text.to_string())
}

fn channel(name: &str) -> SubscriptionTarget {
    SubscriptionTarget::Channel(name.to_string())
}

fn pattern(name: &str) -> SubscriptionTarget {
    SubscriptionTarget::Pattern(name.to_string())
}

fn shardchannel(name: &str) -> SubscriptionTarget {
    SubscriptionTarget::Shardchannel(name.to_string())
}

/// A push message confirming a target, e.g. `confirm("subscribe", "news")`.
fn confirm(kind: &str, name: &str) -> RespValue {
    push(vec![bulk(kind), bulk(name), RespValue::Integer(1)])
}

fn message(payload: &str, channel: &str, target: SubscriptionTarget) -> Option<SubscriptionEvent> {
    Some(SubscriptionEvent::Message(SubscriptionMessage {
        payload: payload.to_string(),
        channel: channel.to_string(),
        target,
    }))
}

fn unsubscribed(
    target: SubscriptionTarget,
    reason: UnsubscribeReason,
) -> Option<SubscriptionEvent> {
    Some(SubscriptionEvent::Unsubscribed((target, reason)))
}

/// The commands the connection wrote, until the server reads nothing more for a while.
async fn requests(transport: &mut Transport) -> Vec<Vec<RespValue>> {
    let mut requests = vec![];
    while let Ok(Some(request)) = tokio::time::timeout(SETTLE, transport.request()).await {
        requests.push(request);
    }
    requests
}

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
                    ("proto".to_string(), RespValue::Integer(3)),
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
            let hello = ["HELLO", "3", "AUTH", "user", "secret", "SETNAME", "tests"];
            handshake(&mut transport, &hello).await;
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
            transport.reply(RespValue::Error("WRONGPASS invalid password".to_string()));
            let Err(error) = resolve(accessor, connection).await? else {
                panic!("established despite the error reply");
            };
            assert_eq!(
                error,
                Error::Valkey("WRONGPASS invalid password".to_string())
            );
            // the CLIENT SETINFO commands queued with HELLO, then the connection is dropped, which
            // closes the requests stream
            assert_eq!(
                transport.request().await,
                Some(test_harness::setinfo()[0].clone())
            );
            assert_eq!(
                transport.request().await,
                Some(test_harness::setinfo()[1].clone())
            );
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
async fn exchanges_are_paired_with_replies_in_order() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let first = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let second = guest.call_exchange(accessor, connection, words(&["GET", "b"]));
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
async fn error_replies_fail_the_exchange() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let call = guest.call_exchange(accessor, connection, words(&["INCR", "text"]));
            let server = async {
                transport.request().await;
                transport.reply(RespValue::Error("ERR value is not an integer".to_string()));
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
async fn emit_discards_its_reply() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            guest
                .call_emit(accessor, connection, words(&["SET", "a", "1"]))
                .await??;
            let call = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["SET", "a", "1"])));
                assert_eq!(transport.request().await, Some(command(&["GET", "a"])));
                // the reply to SET is discarded, even an error
                transport.reply(RespValue::Error("ERR discarded".to_string()));
                transport.reply(bulk("1"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(bulk("1")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dead_letters_go_to_every_reader() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let first = guest.call_dead_letters(accessor, connection).await?;
            let second = guest.call_dead_letters(accessor, connection).await?;
            let mut first = receive(accessor, first)?;
            let mut second = receive(accessor, second)?;

            // a message no subscription owns
            let message = push(vec![bulk("message"), bulk("news"), bulk("hello")]);
            let call = guest.call_exchange(accessor, connection, words(&["PING"]));
            let server = async {
                transport.request().await;
                // a push message arriving ahead of a reply is not mistaken for it
                transport.reply(message.clone());
                transport.reply(simple("PONG"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(simple("PONG")));
            assert_eq!(first.recv().await, Some(message.clone()));
            assert_eq!(second.recv().await, Some(message));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn unprompted_values_are_dead_letters() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let dead_letters = guest.call_dead_letters(accessor, connection).await?;
            let mut dead_letters = receive(accessor, dead_letters)?;

            let call = guest.call_exchange(accessor, connection, words(&["MONITOR"]));
            let server = async {
                transport.request().await;
                transport.reply(ok());
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(ok()));

            // the feed of MONITOR, simple strings with no command waiting for them
            let feed = simple(r#"1760190000.123456 [0 127.0.0.1:52144] "ping""#);
            transport.reply(feed.clone());
            assert_eq!(dead_letters.recv().await, Some(feed));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_dead_letter_readers_are_closed() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            // a reader that never reads
            let _slow = guest.call_dead_letters(accessor, connection).await?;
            for n in 0..1000 {
                transport.reply(push(vec![
                    bulk("message"),
                    bulk("news"),
                    bulk(&n.to_string()),
                ]));
            }
            // replies are not held up behind the reader
            let call = guest.call_exchange(accessor, connection, words(&["PING"]));
            let server = async {
                transport.request().await;
                transport.reply(simple("PONG"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(simple("PONG")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_fails_waiting_and_later_exchanges() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let reset = Error::Closed(Some("connection reset".to_string()));
            let call = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let server = async {
                transport.request().await;
                transport.close(Err(reset.clone()));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Err(reset.clone()));
            let later = guest
                .call_exchange(accessor, connection, words(&["GET", "a"]))
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
            let interval = DurationMilliseconds { px: 20 };
            guest
                .call_set_keep_alive(accessor, connection, Some(interval))
                .await?;
            assert_eq!(transport.request().await, Some(command(&["PING"])));
            transport.reply(simple("PONG"));
            assert_eq!(transport.request().await, Some(command(&["PING"])));
            transport.reply(simple("PONG"));

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

#[tokio::test(flavor = "multi_thread")]
async fn commands_the_connection_manages_are_rejected() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let rejected = |message: &str| Err(Error::Client(message.to_string()));
            let reset = "RESET is rejected, use the connection's reset";
            let hello = "HELLO is rejected, the connection sent it when it was established, see its server-properties";
            let reply = "CLIENT REPLY is rejected, every command has a reply, see the connection's emit";
            for (command, message) in [
                (words(&["reset"]), reset),
                (words(&["HELLO", "2"]), hello),
                (words(&["client", "reply", "off"]), reply),
            ] {
                let result = guest.call_exchange(accessor, connection, command).await?;
                assert_eq!(result, rejected(message));
            }
            let result = guest
                .call_transaction(accessor, connection, vec![words(&["RESET"])])
                .await?;
            assert_eq!(result.err(), Some(Error::Client(reset.to_string())));

            // subscribing with exchange is allowed until the connection has a subscription
            let result = guest
                .call_exchange(accessor, connection, words(&["SUBSCRIBE", "raw"]))
                .await?;
            assert_eq!(result, Ok(RespValue::Null));
            let _subscription = guest.call_subscription(accessor, connection).await?;
            let result = guest
                .call_exchange(accessor, connection, words(&["unsubscribe"]))
                .await?;
            assert_eq!(
                result,
                rejected(
                    "UNSUBSCRIBE is rejected once the connection has a subscription, use the subscription's methods"
                )
            );
            // only the SUBSCRIBE from before the subscription was sent
            assert_eq!(requests(&mut transport).await, vec![command(&["SUBSCRIBE", "raw"])]);
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn subscriptions_are_confirmed_by_push_messages() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let subscription = guest.call_subscription(accessor, connection).await?;
            let (events, _ended) = subscriptions.call_events(accessor, subscription).await?;
            let mut events = receive(accessor, events)?;
            let dead_letters = guest.call_dead_letters(accessor, connection).await?;
            let mut dead_letters = receive(accessor, dead_letters)?;

            let call = subscriptions.call_subscribe(
                accessor,
                subscription,
                vec![channel("news"), channel("sports")],
            );
            let server = async {
                assert_eq!(
                    transport.request().await,
                    Some(command(&["SUBSCRIBE", "news", "sports"]))
                );
                transport.reply(push(vec![
                    bulk("subscribe"),
                    bulk("news"),
                    RespValue::Integer(1),
                ]));
                transport.reply(push(vec![
                    bulk("subscribe"),
                    bulk("sports"),
                    RespValue::Integer(2),
                ]));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            assert_eq!(
                events.recv().await,
                Some(SubscriptionEvent::Subscribed(channel("news")))
            );
            assert_eq!(
                events.recv().await,
                Some(SubscriptionEvent::Subscribed(channel("sports")))
            );
            assert_eq!(
                subscriptions.call_targets(accessor, subscription).await?,
                vec![channel("news"), channel("sports")]
            );

            // a message for a target goes to the subscription, others are dead letters
            transport.reply(push(vec![bulk("message"), bulk("news"), bulk("hello")]));
            let unowned = push(vec![bulk("message"), bulk("weather"), bulk("rain")]);
            transport.reply(unowned.clone());
            assert_eq!(
                events.recv().await,
                Some(SubscriptionEvent::Message(SubscriptionMessage {
                    payload: "hello".to_string(),
                    channel: "news".to_string(),
                    target: channel("news"),
                }))
            );
            assert_eq!(dead_letters.recv().await, Some(unowned));

            let call =
                subscriptions.call_unsubscribe(accessor, subscription, Some(vec![channel("news")]));
            let server = async {
                assert_eq!(
                    transport.request().await,
                    Some(command(&["UNSUBSCRIBE", "news"]))
                );
                transport.reply(push(vec![
                    bulk("unsubscribe"),
                    bulk("news"),
                    RespValue::Integer(1),
                ]));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            assert_eq!(
                events.recv().await,
                Some(SubscriptionEvent::Unsubscribed((
                    channel("news"),
                    UnsubscribeReason::Requested
                )))
            );
            assert_eq!(
                subscriptions.call_targets(accessor, subscription).await?,
                vec![channel("sports")]
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn error_replies_fail_a_subscribe() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let subscription = guest.call_subscription(accessor, connection).await?;
            let subscribe =
                subscriptions.call_subscribe(accessor, subscription, vec![channel("secret")]);
            let get = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let server = async {
                transport.request().await;
                transport.request().await;
                // in the place of the confirmation, then the reply to GET
                transport.reply(RespValue::Error(
                    "NOPERM No permissions to access a channel".to_string(),
                ));
                transport.reply(bulk("1"));
            };
            let (subscribed, got, ()) = tokio::join!(subscribe, get, server);
            assert_eq!(
                subscribed?,
                Err(Error::Valkey(
                    "NOPERM No permissions to access a channel".to_string()
                ))
            );
            assert_eq!(got?, Ok(bulk("1")));
            assert_eq!(
                subscriptions.call_targets(accessor, subscription).await?,
                Vec::<SubscriptionTarget>::new()
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_subscription_unsubscribes() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    let (mut transport, subscription) = harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let subscription = guest.call_subscription(accessor, connection).await?;
            let call = subscriptions.call_subscribe(accessor, subscription, vec![channel("news")]);
            let server = async {
                transport.request().await;
                transport.reply(push(vec![
                    bulk("subscribe"),
                    bulk("news"),
                    RespValue::Integer(1),
                ]));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            Ok((transport, subscription))
        })
        .await?;
    harness.drop_resource(subscription).await?;
    harness
        .run(async |_accessor, _exports| {
            assert_eq!(
                transport.request().await,
                Some(command(&["UNSUBSCRIBE", "news"]))
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_holds_commands_until_hello_succeeds() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let reset = guest.call_reset(accessor, connection);
            let get = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["RESET"])));
                assert_eq!(transport.request().await, Some(command(&["HELLO", "3"])));
                // GET is not sent while the connection is on RESP2, without its user
                assert!(
                    tokio::time::timeout(SETTLE, transport.request())
                        .await
                        .is_err()
                );
                transport.reply(simple("RESET"));
                transport.reply(hello_reply());
                assert_eq!(transport.request().await, Some(command(&["GET", "a"])));
                transport.reply(bulk("1"));
            };
            let (reset, get, ()) = tokio::join!(reset, get, server);
            assert_eq!(reset?, Ok(()));
            assert_eq!(get?, Ok(bulk("1")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_closes_the_connection_when_hello_fails() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let wrongpass = Error::Valkey("WRONGPASS invalid username-password pair".to_string());
            let reset = guest.call_reset(accessor, connection);
            let get = guest.call_exchange(accessor, connection, words(&["GET", "a"]));
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["RESET"])));
                assert_eq!(transport.request().await, Some(command(&["HELLO", "3"])));
                transport.reply(simple("RESET"));
                transport.reply(RespValue::Error(
                    "WRONGPASS invalid username-password pair".to_string(),
                ));
                // GET was never sent, the connection closed its requests stream
                assert_eq!(transport.request().await, None);
            };
            let (reset, get, ()) = tokio::join!(reset, get, server);
            assert_eq!(reset?, Err(wrongpass.clone()));
            assert_eq!(get?, Err(wrongpass.clone()));
            let later = guest
                .call_exchange(accessor, connection, words(&["PING"]))
                .await?;
            assert_eq!(later, Err(wrongpass));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_all_sends_the_commands_together() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let call = guest.call_exchange_all(
                accessor,
                connection,
                vec![words(&["SET", "a", "1"]), vec![], words(&["GET", "a"])],
            );
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["SET", "a", "1"])));
                assert_eq!(transport.request().await, Some(command(&["GET", "a"])));
                transport.reply(RespValue::Error("ERR oops".to_string()));
                transport.reply(bulk("1"));
            };
            let (results, ()) = tokio::join!(call, server);
            assert_eq!(
                results?,
                vec![
                    Err(Error::Valkey("ERR oops".to_string())),
                    // an empty command fails in its place, the others are sent
                    Err(Error::Client(
                        "a command requires at least one word".to_string()
                    )),
                    Ok(bulk("1")),
                ]
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_wrap_the_commands_in_multi_and_exec() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let queued = || simple("QUEUED");

            // committed, a command that fails while running is an error value in its place
            let call = guest.call_transaction(
                accessor,
                connection,
                vec![words(&["SET", "a", "1"]), words(&["INCR", "text"])],
            );
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["MULTI"])));
                assert_eq!(transport.request().await, Some(command(&["SET", "a", "1"])));
                assert_eq!(transport.request().await, Some(command(&["INCR", "text"])));
                assert_eq!(transport.request().await, Some(command(&["EXEC"])));
                transport.reply(ok());
                transport.reply(queued());
                transport.reply(queued());
                transport.reply(RespValue::Array(vec![
                    test_harness::encode(ok()),
                    test_harness::encode(RespValue::Error("ERR not an integer".to_string())),
                ]));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(
                result?,
                Ok(Some(vec![
                    ok(),
                    RespValue::Error("ERR not an integer".to_string())
                ]))
            );

            // a watched key changed
            let call =
                guest.call_transaction(accessor, connection, vec![words(&["SET", "a", "2"])]);
            let server = async {
                for _ in 0..3 {
                    transport.request().await;
                }
                transport.reply(ok());
                transport.reply(queued());
                transport.reply(RespValue::Null);
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(None));

            // a command rejected while queuing fails the transaction with its own error
            let call = guest.call_transaction(accessor, connection, vec![words(&["NOPE"])]);
            let server = async {
                for _ in 0..3 {
                    transport.request().await;
                }
                transport.reply(ok());
                transport.reply(RespValue::Error("ERR unknown command 'NOPE'".to_string()));
                transport.reply(RespValue::Error(
                    "EXECABORT Transaction discarded because of previous errors.".to_string(),
                ));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(
                result?,
                Err(Error::Valkey("ERR unknown command 'NOPE'".to_string()))
            );

            // an empty command fails before MULTI is sent
            let result = guest
                .call_transaction(accessor, connection, vec![words(&["GET", "a"]), vec![]])
                .await?;
            assert_eq!(
                result,
                Err(Error::Client(
                    "a command requires at least one word".to_string()
                ))
            );
            assert_eq!(requests(&mut transport).await, Vec::<Vec<RespValue>>::new());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_and_unwatch() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let call = guest.call_watch(accessor, connection, words(&["a", "b"]));
            let server = async {
                assert_eq!(
                    transport.request().await,
                    Some(command(&["WATCH", "a", "b"]))
                );
                transport.reply(ok());
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));

            let call = guest.call_unwatch(accessor, connection);
            let server = async {
                assert_eq!(transport.request().await, Some(command(&["UNWATCH"])));
                transport.reply(ok());
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));

            let result = guest.call_watch(accessor, connection, vec![]).await?;
            assert_eq!(
                result,
                Err(Error::Client("keys requires at least one item".to_string()))
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_kinds_are_subscribed_with_a_command_each() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let subscription = guest.call_subscription(accessor, connection).await?;
            let (events, _ended) = subscriptions.call_events(accessor, subscription).await?;
            let mut events = receive(accessor, events)?;

            let targets = vec![
                channel("news"),
                pattern("n*"),
                shardchannel("orders"),
                channel("sports"),
            ];
            let call = subscriptions.call_subscribe(accessor, subscription, targets);
            let server = async {
                assert_eq!(
                    transport.request().await,
                    Some(command(&["SUBSCRIBE", "news", "sports"]))
                );
                assert_eq!(
                    transport.request().await,
                    Some(command(&["PSUBSCRIBE", "n*"]))
                );
                assert_eq!(
                    transport.request().await,
                    Some(command(&["SSUBSCRIBE", "orders"]))
                );
                transport.reply(confirm("subscribe", "news"));
                transport.reply(confirm("subscribe", "sports"));
                // every command is sent, one failing does not stop the others
                transport.reply(RespValue::Error("NOPERM no patterns".to_string()));
                transport.reply(confirm("ssubscribe", "orders"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(
                result?,
                Err(Error::Valkey("PSUBSCRIBE: NOPERM no patterns".to_string()))
            );
            for target in [channel("news"), channel("sports"), shardchannel("orders")] {
                assert_eq!(
                    events.recv().await,
                    Some(SubscriptionEvent::Subscribed(target))
                );
            }
            assert_eq!(
                subscriptions.call_targets(accessor, subscription).await?,
                vec![channel("news"), channel("sports"), shardchannel("orders")]
            );

            // the targets given back to unsubscribe, a command for each kind
            let targets = subscriptions.call_targets(accessor, subscription).await?;
            let call = subscriptions.call_unsubscribe(accessor, subscription, Some(targets));
            let server = async {
                assert_eq!(
                    transport.request().await,
                    Some(command(&["UNSUBSCRIBE", "news", "sports"]))
                );
                assert_eq!(
                    transport.request().await,
                    Some(command(&["SUNSUBSCRIBE", "orders"]))
                );
                transport.reply(confirm("unsubscribe", "news"));
                transport.reply(confirm("unsubscribe", "sports"));
                transport.reply(confirm("sunsubscribe", "orders"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            assert_eq!(
                subscriptions.call_targets(accessor, subscription).await?,
                Vec::<SubscriptionTarget>::new()
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapping_subscriptions_share_the_servers() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let first = guest.call_subscription(accessor, connection).await?;
            let second = guest.call_subscription(accessor, connection).await?;
            let (first_events, _) = subscriptions.call_events(accessor, first).await?;
            let mut first_events = receive(accessor, first_events)?;
            let (second_events, _) = subscriptions.call_events(accessor, second).await?;
            let mut second_events = receive(accessor, second_events)?;

            for subscription in [first, second] {
                let call =
                    subscriptions.call_subscribe(accessor, subscription, vec![channel("news")]);
                let server = async {
                    assert_eq!(
                        transport.request().await,
                        Some(command(&["SUBSCRIBE", "news"]))
                    );
                    transport.reply(confirm("subscribe", "news"));
                };
                let (result, ()) = tokio::join!(call, server);
                assert_eq!(result?, Ok(()));
            }
            transport.reply(push(vec![bulk("message"), bulk("news"), bulk("both")]));
            for events in [&mut first_events, &mut second_events] {
                assert_eq!(
                    events.recv().await,
                    Some(SubscriptionEvent::Subscribed(channel("news")))
                );
                assert_eq!(
                    events.recv().await,
                    message("both", "news", channel("news"))
                );
            }

            // the other subscription keeps the channel, nothing is sent
            let result = subscriptions
                .call_unsubscribe(accessor, first, Some(vec![channel("news")]))
                .await?;
            assert_eq!(result, Ok(()));
            assert_eq!(
                first_events.recv().await,
                unsubscribed(channel("news"), UnsubscribeReason::Requested)
            );
            assert_eq!(requests(&mut transport).await, Vec::<Vec<RespValue>>::new());
            transport.reply(push(vec![bulk("message"), bulk("news"), bulk("second")]));
            assert_eq!(
                second_events.recv().await,
                message("second", "news", channel("news"))
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn pattern_messages_carry_the_channel() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let subscription = guest.call_subscription(accessor, connection).await?;
            let (events, _) = subscriptions.call_events(accessor, subscription).await?;
            let mut events = receive(accessor, events)?;
            let call = subscriptions.call_subscribe(accessor, subscription, vec![pattern("n*")]);
            let server = async {
                transport.request().await;
                transport.reply(confirm("psubscribe", "n*"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            transport.reply(push(vec![
                bulk("pmessage"),
                bulk("n*"),
                bulk("news"),
                bulk("hello"),
            ]));
            assert_eq!(
                events.recv().await,
                Some(SubscriptionEvent::Subscribed(pattern("n*")))
            );
            assert_eq!(events.recv().await, message("hello", "news", pattern("n*")));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn targets_end_by_the_server_reset_and_closing() -> wasmtime::Result<()> {
    let mut harness = Harness::new("connection").await?;
    harness
        .run(async |accessor, exports| {
            let guest = exports.connection();
            let subscriptions = exports.subscription();
            let (mut transport, connection) = establish(accessor, exports).await?;
            let subscription = guest.call_subscription(accessor, connection).await?;
            let (events, ended) = subscriptions.call_events(accessor, subscription).await?;
            let mut events = receive(accessor, events)?;
            let targets = vec![shardchannel("orders"), channel("news")];
            let call = subscriptions.call_subscribe(accessor, subscription, targets);
            let server = async {
                transport.request().await;
                transport.request().await;
                transport.reply(confirm("ssubscribe", "orders"));
                transport.reply(confirm("subscribe", "news"));
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            events.recv().await;
            events.recv().await;

            // the slot of the shard channel moved, the server unsubscribed on its own
            transport.reply(confirm("sunsubscribe", "orders"));
            assert_eq!(
                events.recv().await,
                unsubscribed(shardchannel("orders"), UnsubscribeReason::ByServer)
            );

            // RESET unsubscribes from every target, the subscription stays open
            let call = guest.call_reset(accessor, connection);
            let server = async {
                transport.request().await;
                transport.request().await;
                transport.reply(simple("RESET"));
                transport.reply(hello_reply());
            };
            let (result, ()) = tokio::join!(call, server);
            assert_eq!(result?, Ok(()));
            assert_eq!(
                events.recv().await,
                unsubscribed(channel("news"), UnsubscribeReason::Reset)
            );

            // closing the connection closes the stream with the error
            let reason = Error::Closed(Some("connection reset".to_string()));
            transport.close(Err(reason.clone()));
            assert_eq!(events.recv().await, None);
            assert_eq!(resolve(accessor, ended).await?, Err(reason));
            Ok(())
        })
        .await
}

/// The tests against a Valkey server, ignored unless run with `make test-valkey`, which runs one in
/// a container and only runs the ignored tests in `with_valkey` modules. They connect with
/// the connector of `test-tcp`, and leave nothing behind on the server.
mod with_valkey {
    use test_harness::{Ctx, Exports};
    use wasmtime::component::{Accessor, ResourceAny};

    use super::*;

    async fn connect(accessor: &Accessor<Ctx>, exports: &Exports) -> wasmtime::Result<ResourceAny> {
        exports
            .connector()
            .call_connect(accessor)
            .await?
            .map_err(|error| wasmtime::format_err!("failed to connect: {error:?}"))
    }

    async fn exchange(
        accessor: &Accessor<Ctx>,
        exports: &Exports,
        connection: ResourceAny,
        command: &[&str],
    ) -> wasmtime::Result<Result<RespValue, Error>> {
        exports
            .connection()
            .call_exchange(accessor, connection, words(command))
            .await
    }

    /// PUBSUB NUMSUB of one channel.
    async fn numsub(
        accessor: &Accessor<Ctx>,
        exports: &Exports,
        connection: ResourceAny,
        channel: &str,
    ) -> wasmtime::Result<RespValue> {
        let reply = exchange(
            accessor,
            exports,
            connection,
            &["PUBSUB", "NUMSUB", channel],
        )
        .await?
        .map_err(|error| wasmtime::format_err!("{error:?}"))?;
        let RespValue::Array(mut items) = reply else {
            wasmtime::bail!("unexpected {reply:?}");
        };
        test_harness::decode(items.remove(1)).map_err(|error| wasmtime::format_err!("{error}"))
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn subscriptions_against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::with_valkey("test-tcp", &[]).await?;
        harness
            .run(async |accessor, exports| {
                let subscriptions = exports.subscription();
                let connection = connect(accessor, exports).await?;
                let publisher = connect(accessor, exports).await?;
                let subscription = exports
                    .connection()
                    .call_subscription(accessor, connection)
                    .await?;
                let (events, ended) = subscriptions.call_events(accessor, subscription).await?;
                let mut events = receive(accessor, events)?;

                let targets = vec![
                    channel("t:conn:a"),
                    pattern("t:conn:p*"),
                    shardchannel("t:conn:s"),
                ];
                let result = subscriptions
                    .call_subscribe(accessor, subscription, targets.clone())
                    .await?;
                assert_eq!(result, Ok(()));
                for target in &targets {
                    assert_eq!(
                        events.recv().await,
                        Some(SubscriptionEvent::Subscribed(target.clone()))
                    );
                }

                exchange(
                    accessor,
                    exports,
                    publisher,
                    &["PUBLISH", "t:conn:a", "one"],
                )
                .await??;
                assert_eq!(
                    events.recv().await,
                    message("one", "t:conn:a", channel("t:conn:a"))
                );
                exchange(
                    accessor,
                    exports,
                    publisher,
                    &["PUBLISH", "t:conn:p1", "two"],
                )
                .await??;
                assert_eq!(
                    events.recv().await,
                    message("two", "t:conn:p1", pattern("t:conn:p*"))
                );
                exchange(
                    accessor,
                    exports,
                    publisher,
                    &["SPUBLISH", "t:conn:s", "three"],
                )
                .await??;
                assert_eq!(
                    events.recv().await,
                    message("three", "t:conn:s", shardchannel("t:conn:s"))
                );

                // another subscription to the same channel shares the server's subscription
                let other = exports
                    .connection()
                    .call_subscription(accessor, connection)
                    .await?;
                subscriptions
                    .call_subscribe(accessor, other, vec![channel("t:conn:a")])
                    .await??;
                assert_eq!(
                    numsub(accessor, exports, publisher, "t:conn:a").await?,
                    RespValue::Integer(1)
                );
                subscriptions
                    .call_unsubscribe(accessor, subscription, None)
                    .await??;
                for target in &targets {
                    assert_eq!(
                        events.recv().await,
                        unsubscribed(target.clone(), UnsubscribeReason::Requested)
                    );
                }
                assert_eq!(
                    numsub(accessor, exports, publisher, "t:conn:a").await?,
                    RespValue::Integer(1),
                    "kept for the other subscription"
                );
                subscriptions
                    .call_unsubscribe(accessor, other, None)
                    .await??;
                assert_eq!(
                    numsub(accessor, exports, publisher, "t:conn:a").await?,
                    RespValue::Integer(0)
                );

                // the stream stays open until the subscription is dropped
                assert!(
                    tokio::time::timeout(SETTLE, events.recv()).await.is_err(),
                    "the stream closed"
                );
                drop(ended);
                Ok(())
            })
            .await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn partial_subscribe_failure_against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::with_valkey("test-tcp", &[]).await?;
        harness
            .run(async |accessor, exports| {
                let subscriptions = exports.subscription();
                let admin = connect(accessor, exports).await?;
                let user = [
                    "ACL",
                    "SETUSER",
                    "t:conn-limited",
                    "on",
                    "nopass",
                    "resetchannels",
                    "&t:conn:ok",
                    "+@all",
                    "~*",
                ];
                exchange(accessor, exports, admin, &user).await??;
                let result = async {
                    let connection = connect(accessor, exports).await?;
                    let auth = ["AUTH", "t:conn-limited", "any"];
                    exchange(accessor, exports, connection, &auth).await??;
                    let subscription = exports
                        .connection()
                        .call_subscription(accessor, connection)
                        .await?;
                    let targets = vec![
                        pattern("t:conn:no*"),
                        channel("t:conn:ok"),
                        shardchannel("t:conn:no"),
                    ];
                    let result = subscriptions
                        .call_subscribe(accessor, subscription, targets)
                        .await?;
                    let Err(Error::Valkey(message)) = result else {
                        wasmtime::bail!("expected a valkey error, got {result:?}");
                    };
                    assert!(
                        message.starts_with("PSUBSCRIBE: NOPERM")
                            && message.contains("; SSUBSCRIBE: NOPERM"),
                        "{message}"
                    );
                    assert_eq!(
                        subscriptions.call_targets(accessor, subscription).await?,
                        vec![channel("t:conn:ok")]
                    );
                    Ok(())
                }
                .await;
                exchange(
                    accessor,
                    exports,
                    admin,
                    &["ACL", "DELUSER", "t:conn-limited"],
                )
                .await??;
                result
            })
            .await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn reset_against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::with_valkey("test-tcp", &[]).await?;
        harness
            .run(async |accessor, exports| {
                let guest = exports.connection();
                let subscriptions = exports.subscription();
                let connection = connect(accessor, exports).await?;
                exchange(accessor, exports, connection, &["SELECT", "1"]).await??;
                exchange(
                    accessor,
                    exports,
                    connection,
                    &["SET", "t:conn:reset", "db1"],
                )
                .await??;
                let subscription = guest.call_subscription(accessor, connection).await?;
                let (events, _ended) = subscriptions.call_events(accessor, subscription).await?;
                let mut events = receive(accessor, events)?;
                subscriptions
                    .call_subscribe(accessor, subscription, vec![channel("t:conn:r")])
                    .await??;
                events.recv().await;

                guest.call_reset(accessor, connection).await??;
                assert_eq!(
                    events.recv().await,
                    unsubscribed(channel("t:conn:r"), UnsubscribeReason::Reset)
                );
                assert_eq!(
                    exchange(accessor, exports, connection, &["GET", "t:conn:reset"]).await?,
                    Ok(RespValue::Null),
                    "reset selects database 0"
                );
                let properties = guest.call_server_properties(accessor, connection).await?;
                assert!(properties.contains(&("proto".to_string(), RespValue::Integer(3))));
                let info = exchange(accessor, exports, connection, &["CLIENT", "INFO"]).await??;
                let RespValue::VerbatimString((_, info)) = info else {
                    wasmtime::bail!("unexpected {info:?}");
                };
                assert!(info.contains("lib-name=componentized:valkey"), "{info}");

                // the subscription is usable after the reset
                subscriptions
                    .call_subscribe(accessor, subscription, vec![channel("t:conn:r")])
                    .await??;
                assert_eq!(
                    events.recv().await,
                    Some(SubscriptionEvent::Subscribed(channel("t:conn:r")))
                );

                exchange(accessor, exports, connection, &["SELECT", "1"]).await??;
                exchange(accessor, exports, connection, &["DEL", "t:conn:reset"]).await??;
                Ok(())
            })
            .await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn expired_credentials_close_on_reset_against_valkey() -> wasmtime::Result<()> {
        let mut setup = Harness::with_valkey("test-tcp", &[]).await?;
        let user = [
            "ACL",
            "SETUSER",
            "t:conn-expire",
            "on",
            ">old",
            "~*",
            "&*",
            "+@all",
        ];
        setup
            .run(async |accessor, exports| {
                let admin = connect(accessor, exports).await?;
                exchange(accessor, exports, admin, &user).await??;
                Ok(())
            })
            .await?;
        let config = [("username", "t:conn-expire"), ("password", "old")];
        let mut harness = Harness::with_valkey("test-tcp", &config).await?;
        let result = harness
            .run(async |accessor, exports| {
                let guest = exports.connection();
                let connection = connect(accessor, exports).await?;
                let other = connect(accessor, exports).await?;
                // connections already authenticated stay so
                let expire = ["ACL", "SETUSER", "t:conn-expire", "resetpass", ">new"];
                exchange(accessor, exports, other, &expire).await??;
                let reset = guest.call_reset(accessor, connection);
                let set = guest.call_exchange(
                    accessor,
                    connection,
                    words(&["SET", "t:conn:leak", "1"]),
                );
                let (reset, set) = tokio::join!(reset, set);
                let wrongpass = |result: &Result<(), Error>| {
                    matches!(result, Err(Error::Valkey(message)) if message.starts_with("WRONGPASS"))
                };
                assert!(wrongpass(&reset?), "the reset fails");
                assert!(wrongpass(&set?.map(|_| ())), "the command after it fails");
                assert_eq!(
                    exchange(accessor, exports, other, &["GET", "t:conn:leak"]).await?,
                    Ok(RespValue::Null),
                    "and never ran"
                );
                Ok(())
            })
            .await;
        setup
            .run(async |accessor, exports| {
                let admin = connect(accessor, exports).await?;
                exchange(
                    accessor,
                    exports,
                    admin,
                    &["ACL", "DELUSER", "t:conn-expire"],
                )
                .await??;
                Ok(())
            })
            .await?;
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a Valkey server, see make test-valkey"]
    async fn killed_connections_keep_their_targets_against_valkey() -> wasmtime::Result<()> {
        let mut harness = Harness::with_valkey("test-tcp", &[]).await?;
        harness
            .run(async |accessor, exports| {
                let subscriptions = exports.subscription();
                let connection = connect(accessor, exports).await?;
                let admin = connect(accessor, exports).await?;
                let RespValue::Integer(id) =
                    exchange(accessor, exports, connection, &["CLIENT", "ID"]).await??
                else {
                    wasmtime::bail!("unexpected CLIENT ID");
                };
                let subscription = exports
                    .connection()
                    .call_subscription(accessor, connection)
                    .await?;
                let (events, ended) = subscriptions.call_events(accessor, subscription).await?;
                let mut events = receive(accessor, events)?;
                let targets = vec![channel("t:conn:k"), pattern("t:conn:k*")];
                subscriptions
                    .call_subscribe(accessor, subscription, targets.clone())
                    .await??;
                let kill = ["CLIENT", "KILL", "ID", &id.to_string()];
                exchange(accessor, exports, admin, &kill).await??;
                while events.recv().await.is_some() {}
                assert!(matches!(
                    resolve(accessor, ended).await?,
                    Err(Error::Closed(_))
                ));
                assert_eq!(
                    subscriptions.call_targets(accessor, subscription).await?,
                    targets,
                    "the confirmed targets are still listed"
                );
                let result = subscriptions
                    .call_subscribe(accessor, subscription, vec![channel("t:conn:k2")])
                    .await?;
                assert!(matches!(result, Err(Error::Closed(_))));
                Ok(())
            })
            .await
    }
}
