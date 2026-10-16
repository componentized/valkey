//! The tests of the `commands` component, run inside a component by the test harness.
//!
//! Each test plays the server over the streams of a connection it establishes with
//! `connection.new`: it reads the command a function sends, answers it, and checks how the function
//! converts the reply. The tests named `with-valkey ...` run against a Valkey server instead, from
//! the connector.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;

use componentized::valkey::commands;
use componentized::valkey::connection::{Connection, ErrorCode};
use componentized::valkey::connector;
use componentized_valkey::types::{
    AclRule, AclUserRule, Bytes, Cursor, DurationMilliseconds, DurationSeconds,
    FunctionRestorePolicy, HexpireResult, RespValue, RestoreTtl, StringOrBytes, TtlMode,
    UnixTimeMilliseconds, UnixTimeSeconds,
};
use exports::componentized::valkey_components::test_suite::Guest;
use wit_bindgen::rt::Map;
use wit_bindgen::{FutureWriter, StreamReader, StreamWriter};

struct Suite;

/// The tests, by name, and the function running each.
macro_rules! suite {
    ($($name:literal => $test:ident),* $(,)?) => {
        const TESTS: &[&str] = &[$($name),*];

        async fn run_test(name: &str) -> Result<(), String> {
            match name {
                $($name => $test().await,)*
                _ => Err(format!("no test named {name:?}")),
            }
        }
    };
}

suite! {
    "string commands" => string_commands,
    "keyspace commands" => keyspace_commands,
    "hash commands" => hash_commands,
    "binary values" => binary_values,
    "dump and restore commands" => dump_and_restore_commands,
    "connection commands" => connection_commands,
    "admin and pubsub commands" => admin_and_pubsub_commands,
    "invalid arguments fail before anything is sent" => invalid_arguments,
    "unexpected replies fail the command" => unexpected_replies,
    "error replies fail the command" => error_replies,
    "with-valkey commands" => with_valkey,
}

impl Guest for Suite {
    fn tests() -> Vec<String> {
        TESTS.iter().map(|name| name.to_string()).collect()
    }

    async fn run(name: String) -> Result<(), String> {
        run_test(&name).await
    }
}

/// Fails the test unless the values are equal, with where and both values.
macro_rules! check_eq {
    ($left:expr, $right:expr $(,)?) => {{
        let (left, right) = (&$left, &$right);
        if left != right {
            return Err(format!(
                "{}:{}: {:?} != {:?}",
                file!(),
                line!(),
                left,
                right
            ));
        }
    }};
}

/// The words of a command, e.g. `words(&["GET", "key"])`.
fn words(words: &[&str]) -> Vec<String> {
    words.iter().map(|word| word.to_string()).collect()
}

fn bwords(words: &[&str]) -> Vec<Bytes> {
    words.iter().map(|word| word.as_bytes().to_vec()).collect()
}

fn s(value: &str) -> String {
    value.to_string()
}

/// A value that is text.
fn v(value: &str) -> StringOrBytes {
    value.into()
}

/// Bytes that are not UTF-8, a value that is not text.
const BINARY: &[u8] = b"\x80\xff\x00value";

fn bulk(value: &str) -> RespValue {
    RespValue::BulkString(value.as_bytes().to_vec())
}

fn ok() -> RespValue {
    RespValue::String("OK".to_string())
}

fn array(items: Vec<RespValue>) -> RespValue {
    RespValue::Array(
        items
            .into_iter()
            .map(componentized_valkey::resp::encode)
            .collect(),
    )
}

fn map(entries: Vec<(RespValue, RespValue)>) -> RespValue {
    RespValue::Map(
        entries
            .into_iter()
            .map(|(key, value)| {
                (
                    componentized_valkey::resp::encode(key),
                    componentized_valkey::resp::encode(value),
                )
            })
            .collect(),
    )
}

/// Poll both futures, `a` first each time, until both are ready.
async fn both<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let (mut a_output, mut b_output) = (None, None);
    poll_fn(|cx| {
        if a_output.is_none()
            && let Poll::Ready(output) = a.as_mut().poll(cx)
        {
            a_output = Some(output);
        }
        if b_output.is_none()
            && let Poll::Ready(output) = b.as_mut().poll(cx)
        {
            b_output = Some(output);
        }
        if a_output.is_some() && b_output.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    (a_output.expect("ready"), b_output.expect("ready"))
}

/// The server's side of a connection, played by the test.
struct Server {
    requests: StreamReader<RespValue>,
    replies: StreamWriter<RespValue>,
    /// Resolved to `ok` when the server is dropped, the transport closed gracefully.
    _closed: FutureWriter<Result<(), ErrorCode>>,
}

impl Server {
    /// The words of the next command the connection sent.
    async fn request(&mut self) -> Result<Vec<Bytes>, String> {
        let request = self.requests.next().await;
        let words = match request {
            Some(RespValue::Array(words)) => words,
            Some(request) => return Err(format!("expected a command, got {request}")),
            None => return Err("expected a command, the requests closed".to_string()),
        };
        words
            .into_iter()
            .map(|word| match componentized_valkey::resp::decode(&word) {
                Ok(RespValue::BulkString(word)) => Ok(word),
                Ok(word) => Err(format!("expected a bulk-string, got {word}")),
                Err(err) => Err(format!("expected a bulk-string, got {err}")),
            })
            .collect()
    }

    async fn reply(&mut self, value: RespValue) -> Result<(), String> {
        match self.replies.write_one(value).await {
            None => Ok(()),
            Some(_) => Err("the connection stopped reading".to_string()),
        }
    }
}

/// A connection over streams the server is played on, once it answered the handshake.
async fn establish() -> Result<(Server, Connection), String> {
    let (replies, replies_reader) = wit_stream::new();
    let (closed, closed_reader) = wit_future::new(|| Ok(()));
    let (requests, connection) = Connection::new(replies_reader, closed_reader, None).await;
    let mut server = Server {
        requests,
        replies,
        _closed: closed,
    };
    check_eq!(server.request().await?, bwords(&["HELLO", "3"]));
    let version = env!("CARGO_PKG_VERSION");
    let setinfo = [
        bwords(&["CLIENT", "SETINFO", "LIB-NAME", "componentized:valkey"]),
        bwords(&["CLIENT", "SETINFO", "LIB-VER", version]),
    ];
    for expected in &setinfo {
        check_eq!(&server.request().await?, expected);
    }
    let hello = map(vec![
        (bulk("server"), bulk("valkey")),
        (bulk("version"), bulk("9.1.2")),
        (bulk("proto"), RespValue::Integer(3)),
    ]);
    server.reply(hello).await?;
    for _ in &setinfo {
        server.reply(ok()).await?;
    }
    let connection = connection
        .await
        .map_err(|error| format!("failed to establish: {error:?}"))?;
    Ok((server, connection))
}

/// Answer the next command with `reply` while `call` waits for it, returning the call's result and
/// the command sent.
async fn round_trip<T>(
    server: &mut Server,
    reply: RespValue,
    call: impl Future<Output = T>,
) -> Result<(T, Vec<Bytes>), String> {
    let serve = async {
        let request = server.request().await?;
        server.reply(reply).await?;
        Ok::<_, String>(request)
    };
    let (result, request) = both(call, serve).await;
    Ok((result, request?))
}

async fn string_commands() -> Result<(), String> {
    let (mut server, conn) = establish().await?;

    let (result, sent) =
        round_trip(&mut server, bulk("value"), commands::get(&conn, s("key"))).await?;
    check_eq!(sent, bwords(&["GET", "key"]));
    check_eq!(result, Ok(Some(v("value"))));

    let (result, _) = round_trip(
        &mut server,
        RespValue::Null,
        commands::get(&conn, s("missing")),
    )
    .await?;
    check_eq!(result, Ok(None));

    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::set(&conn, s("key"), v("value"), None, None),
    )
    .await?;
    check_eq!(sent, bwords(&["SET", "key", "value"]));
    check_eq!(result, Ok(true));

    // null when the precondition is not met
    let (result, sent) = round_trip(
        &mut server,
        RespValue::Null,
        commands::set(
            &conn,
            s("key"),
            v("value"),
            Some(componentized_valkey::types::SetPrecondition::Nx),
            Some(componentized_valkey::types::SetTtl::Ex(DurationSeconds {
                ex: 10,
            })),
        ),
    )
    .await?;
    check_eq!(sent, bwords(&["SET", "key", "value", "NX", "EX", "10"]));
    check_eq!(result, Ok(false));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(1),
        commands::incr(&conn, s("count")),
    )
    .await?;
    check_eq!(sent, bwords(&["INCR", "count"]));
    check_eq!(result, Ok(1));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(-4),
        commands::incrby(&conn, s("count"), -5),
    )
    .await?;
    check_eq!(sent, bwords(&["INCRBY", "count", "-5"]));
    check_eq!(result, Ok(-4));
    Ok(())
}

async fn keyspace_commands() -> Result<(), String> {
    let (mut server, conn) = establish().await?;

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(1),
        commands::del(&conn, words(&["a", "b"])),
    )
    .await?;
    check_eq!(sent, bwords(&["DEL", "a", "b"]));
    check_eq!(result, Ok(1));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(0),
        commands::exists(&conn, words(&["key"])),
    )
    .await?;
    check_eq!(sent, bwords(&["EXISTS", "key"]));
    check_eq!(result, Ok(0));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(1),
        commands::expire(
            &conn,
            s("key"),
            DurationSeconds { ex: 91 },
            Some(TtlMode::Nx),
        ),
    )
    .await?;
    check_eq!(sent, bwords(&["EXPIRE", "key", "91", "NX"]));
    check_eq!(result, Ok(true));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(0),
        commands::expireat(
            &conn,
            s("key"),
            UnixTimeSeconds {
                exat: 1_900_000_000,
            },
            None,
        ),
    )
    .await?;
    check_eq!(sent, bwords(&["EXPIREAT", "key", "1900000000"]));
    check_eq!(result, Ok(false));

    let (result, sent) = round_trip(
        &mut server,
        array(vec![bulk("a"), bulk("b")]),
        commands::keys(&conn, s("*")),
    )
    .await?;
    check_eq!(sent, bwords(&["KEYS", "*"]));
    check_eq!(result, Ok(words(&["a", "b"])));

    // TYPE replies `none` for a key that does not exist
    let (result, sent) = round_trip(
        &mut server,
        RespValue::String(s("none")),
        commands::type_(&conn, s("missing")),
    )
    .await?;
    check_eq!(sent, bwords(&["TYPE", "missing"]));
    check_eq!(result, Ok(None));
    Ok(())
}

async fn hash_commands() -> Result<(), String> {
    let (mut server, conn) = establish().await?;
    let key = || s("hash");

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(2),
        commands::hset(&conn, key(), vec![(s("f"), v("v")), (s("g"), v("w"))]),
    )
    .await?;
    check_eq!(sent, bwords(&["HSET", "hash", "f", "v", "g", "w"]));
    check_eq!(result, Ok(2));

    let (result, sent) =
        round_trip(&mut server, bulk("v"), commands::hget(&conn, key(), s("f"))).await?;
    check_eq!(sent, bwords(&["HGET", "hash", "f"]));
    check_eq!(result, Ok(Some(v("v"))));

    // RESP3 replies to HGETALL with a map
    let (result, sent) = round_trip(
        &mut server,
        map(vec![(bulk("f"), bulk("v")), (bulk("g"), bulk("w"))]),
        commands::hgetall(&conn, key()),
    )
    .await?;
    check_eq!(sent, bwords(&["HGETALL", "hash"]));
    check_eq!(result, Ok(Map::from([(s("f"), v("v")), (s("g"), v("w"))])));

    let (result, _) = round_trip(
        &mut server,
        RespValue::Integer(1),
        commands::hsetnx(&conn, key(), s("f"), v("v")),
    )
    .await?;
    check_eq!(result, Ok(true));

    let (result, _) = round_trip(
        &mut server,
        RespValue::Integer(0),
        commands::hexists(&conn, key(), s("x")),
    )
    .await?;
    check_eq!(result, Ok(false));

    let (result, sent) = round_trip(
        &mut server,
        array(vec![bulk("v"), RespValue::Null]),
        commands::hmget(&conn, key(), words(&["f", "x"])),
    )
    .await?;
    check_eq!(sent, bwords(&["HMGET", "hash", "f", "x"]));
    check_eq!(result, Ok(vec![Some(v("v")), None]));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(6),
        commands::hincrby(&conn, key(), s("n"), 5),
    )
    .await?;
    check_eq!(sent, bwords(&["HINCRBY", "hash", "n", "5"]));
    check_eq!(result, Ok(6));

    // a double sent as a bulk string, even with RESP3
    let (result, sent) = round_trip(
        &mut server,
        bulk("2.5"),
        commands::hincrbyfloat(&conn, key(), s("n"), 1.5),
    )
    .await?;
    check_eq!(sent, bwords(&["HINCRBYFLOAT", "hash", "n", "1.5"]));
    check_eq!(result, Ok(2.5));

    let (result, sent) = round_trip(
        &mut server,
        array(vec![RespValue::Integer(1), RespValue::Integer(-2)]),
        commands::hexpire(
            &conn,
            key(),
            DurationSeconds { ex: 60 },
            None,
            words(&["f", "x"]),
        ),
    )
    .await?;
    check_eq!(
        sent,
        bwords(&["HEXPIRE", "hash", "60", "FIELDS", "2", "f", "x"])
    );
    check_eq!(
        result,
        Ok(vec![HexpireResult::Applied, HexpireResult::DoesNotExist])
    );

    // without a count, the field itself, or null
    let (result, sent) = round_trip(
        &mut server,
        bulk("f"),
        commands::hrandfield(&conn, key(), None),
    )
    .await?;
    check_eq!(sent, bwords(&["HRANDFIELD", "hash"]));
    check_eq!(result, Ok(words(&["f"])));

    // RESP3 replies with values as [field, value] pairs
    let (result, sent) = round_trip(
        &mut server,
        array(vec![
            array(vec![bulk("f"), bulk("v")]),
            array(vec![bulk("g"), bulk("w")]),
        ]),
        commands::hrandfield_withvalues(&conn, key(), 2),
    )
    .await?;
    check_eq!(sent, bwords(&["HRANDFIELD", "hash", "2", "WITHVALUES"]));
    check_eq!(result, Ok(vec![(s("f"), v("v")), (s("g"), v("w"))]));

    let (result, sent) = round_trip(
        &mut server,
        array(vec![bulk("17"), array(vec![bulk("f"), bulk("v")])]),
        commands::hscan(&conn, key(), Cursor::Start, Some(s("f*")), Some(10)),
    )
    .await?;
    check_eq!(
        sent,
        bwords(&["HSCAN", "hash", "0", "MATCH", "f*", "COUNT", "10"])
    );
    check_eq!(
        result,
        Ok((Some(Cursor::Resume(s("17"))), Map::from([(s("f"), v("v"))])))
    );

    // the last page has a cursor of 0
    let (result, sent) = round_trip(
        &mut server,
        array(vec![bulk("0"), array(vec![bulk("g")])]),
        commands::hscan_novalues(&conn, key(), Cursor::Resume(s("17")), None, None),
    )
    .await?;
    check_eq!(sent, bwords(&["HSCAN", "hash", "17", "NOVALUES"]));
    check_eq!(result, Ok((None, words(&["g"]))));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(1),
        commands::hdel(&conn, key(), words(&["f"])),
    )
    .await?;
    check_eq!(sent, bwords(&["HDEL", "hash", "f"]));
    check_eq!(result, Ok(1));
    Ok(())
}

async fn binary_values() -> Result<(), String> {
    let (mut server, conn) = establish().await?;
    let binary = || StringOrBytes::Bytes(BINARY.to_vec());

    // sent as is
    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::set(&conn, s("key"), binary(), None, None),
    )
    .await?;
    check_eq!(
        sent,
        vec![b"SET".to_vec(), b"key".to_vec(), BINARY.to_vec()]
    );
    check_eq!(result, Ok(true));

    // a reply that is not UTF-8 is bytes, one that is is a string
    let (result, _) = round_trip(
        &mut server,
        RespValue::BulkString(BINARY.to_vec()),
        commands::get(&conn, s("key")),
    )
    .await?;
    check_eq!(result, Ok(Some(binary())));

    let (result, _) = round_trip(
        &mut server,
        RespValue::BulkString(b"text".to_vec()),
        commands::get(&conn, s("key")),
    )
    .await?;
    check_eq!(result, Ok(Some(v("text"))));

    let (result, _) = round_trip(
        &mut server,
        map(vec![
            (bulk("f"), RespValue::BulkString(BINARY.to_vec())),
            (bulk("g"), bulk("w")),
        ]),
        commands::hgetall(&conn, s("hash")),
    )
    .await?;
    check_eq!(
        result,
        Ok(Map::from([(s("f"), binary()), (s("g"), v("w"))]))
    );

    // a field, a string, that is not UTF-8 fails the command
    let (result, _) = round_trip(
        &mut server,
        map(vec![(RespValue::BulkString(BINARY.to_vec()), bulk("v"))]),
        commands::hgetall(&conn, s("hash")),
    )
    .await?;
    check_eq!(result.is_err(), true);
    Ok(())
}

async fn dump_and_restore_commands() -> Result<(), String> {
    let (mut server, conn) = establish().await?;

    let (result, sent) = round_trip(
        &mut server,
        RespValue::BulkString(BINARY.to_vec()),
        commands::dump(&conn, s("key")),
    )
    .await?;
    check_eq!(sent, bwords(&["DUMP", "key"]));
    check_eq!(result, Ok(Some(BINARY.to_vec())));

    let (result, _) = round_trip(
        &mut server,
        RespValue::Null,
        commands::dump(&conn, s("missing")),
    )
    .await?;
    check_eq!(result, Ok(None));

    // without a ttl, 0
    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::restore(&conn, s("key"), None, BINARY.to_vec(), false, None, None),
    )
    .await?;
    check_eq!(
        sent,
        vec![
            b"RESTORE".to_vec(),
            b"key".to_vec(),
            b"0".to_vec(),
            BINARY.to_vec()
        ]
    );
    check_eq!(result, Ok(()));

    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::restore(
            &conn,
            s("key"),
            Some(RestoreTtl::ExpireIn(DurationMilliseconds { px: 1500 })),
            BINARY.to_vec(),
            true,
            Some(DurationSeconds { ex: 10 }),
            Some(5),
        ),
    )
    .await?;
    let mut expected = bwords(&["RESTORE", "key", "1500"]);
    expected.push(BINARY.to_vec());
    expected.extend(bwords(&["REPLACE", "IDLETIME", "10", "FREQ", "5"]));
    check_eq!(sent, expected);
    check_eq!(result, Ok(()));

    // an expiration at a time is ABSTTL
    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::restore_asking(
            &conn,
            s("key"),
            Some(RestoreTtl::ExpireAt(UnixTimeMilliseconds {
                pxat: 1_900_000_000_000,
            })),
            BINARY.to_vec(),
            false,
            None,
            None,
        ),
    )
    .await?;
    let mut expected = bwords(&["RESTORE-ASKING", "key", "1900000000000"]);
    expected.push(BINARY.to_vec());
    expected.extend(bwords(&["ABSTTL"]));
    check_eq!(sent, expected);
    check_eq!(result, Ok(()));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::BulkString(BINARY.to_vec()),
        commands::function_dump(&conn),
    )
    .await?;
    check_eq!(sent, bwords(&["FUNCTION", "DUMP"]));
    check_eq!(result, Ok(BINARY.to_vec()));

    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::function_restore(&conn, BINARY.to_vec(), Some(FunctionRestorePolicy::Replace)),
    )
    .await?;
    let mut expected = bwords(&["FUNCTION", "RESTORE"]);
    expected.push(BINARY.to_vec());
    expected.extend(bwords(&["REPLACE"]));
    check_eq!(sent, expected);
    check_eq!(result, Ok(()));
    Ok(())
}

async fn connection_commands() -> Result<(), String> {
    let (mut server, conn) = establish().await?;

    let (result, sent) = round_trip(
        &mut server,
        RespValue::String(s("PONG")),
        commands::ping(&conn, None),
    )
    .await?;
    check_eq!(sent, bwords(&["PING"]));
    check_eq!(result, Ok(()));

    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::auth(&conn, Some(s("user")), s("secret")),
    )
    .await?;
    check_eq!(sent, bwords(&["AUTH", "user", "secret"]));
    check_eq!(result, Ok(()));

    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::client_setname(&conn, s("tests")),
    )
    .await?;
    check_eq!(sent, bwords(&["CLIENT", "SETNAME", "tests"]));
    check_eq!(result, Ok(()));
    Ok(())
}

async fn admin_and_pubsub_commands() -> Result<(), String> {
    let (mut server, conn) = establish().await?;

    let rules = vec![
        AclRule::User(AclUserRule::On),
        AclRule::User(AclUserRule::Password(s("secret"))),
    ];
    let (result, sent) = round_trip(
        &mut server,
        ok(),
        commands::acl_setuser(&conn, s("user"), Some(rules)),
    )
    .await?;
    check_eq!(sent, bwords(&["ACL", "SETUSER", "user", "on", ">secret"]));
    check_eq!(result, Ok(()));

    let pass = "dd721260bfe1b3d9601e7fbab36de6d04e2e67b0ef1c53de59d45950db0dd3cc";
    let (result, sent) =
        round_trip(&mut server, bulk(pass), commands::acl_genpass(&conn, None)).await?;
    check_eq!(sent, bwords(&["ACL", "GENPASS"]));
    check_eq!(result, Ok(s(pass)));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(1),
        commands::acl_deluser(&conn, words(&["user"])),
    )
    .await?;
    check_eq!(sent, bwords(&["ACL", "DELUSER", "user"]));
    check_eq!(result, Ok(1));

    let (result, sent) = round_trip(
        &mut server,
        RespValue::Integer(2),
        commands::publish(&conn, s("news"), v("hello")),
    )
    .await?;
    check_eq!(sent, bwords(&["PUBLISH", "news", "hello"]));
    check_eq!(result, Ok(2));
    Ok(())
}

async fn invalid_arguments() -> Result<(), String> {
    let (mut server, conn) = establish().await?;
    check_eq!(
        commands::del(&conn, vec![]).await,
        Err(ErrorCode::Client(s("keys requires at least one item")))
    );
    check_eq!(
        commands::scan(&conn, Cursor::Resume(s("0")), None, None, None).await,
        Err(ErrorCode::Client(s(
            "resume requires the cursor a scan returned, use start to begin a scan"
        )))
    );
    // nothing was sent, the next command is the first the server sees
    let (_, sent) = round_trip(
        &mut server,
        RespValue::String(s("PONG")),
        commands::ping(&conn, None),
    )
    .await?;
    check_eq!(sent, bwords(&["PING"]));
    Ok(())
}

async fn unexpected_replies() -> Result<(), String> {
    let (mut server, conn) = establish().await?;
    let (result, _) = round_trip(
        &mut server,
        RespValue::Integer(7),
        commands::get(&conn, s("key")),
    )
    .await?;
    check_eq!(
        result,
        Err(ErrorCode::Client(s("unexpected reply: integer<7>")))
    );
    Ok(())
}

async fn error_replies() -> Result<(), String> {
    let (mut server, conn) = establish().await?;
    let (result, _) = round_trip(
        &mut server,
        RespValue::Error(s(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        commands::incr(&conn, s("key")),
    )
    .await?;
    check_eq!(
        result,
        Err(ErrorCode::Server(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into()
        ))
    );
    Ok(())
}

/// Against a Valkey server, from the connector.
async fn with_valkey() -> Result<(), String> {
    let conn = connector::connect()
        .await
        .map_err(|error| format!("failed to connect: {error:?}"))?;
    let check = |result: Result<(), ErrorCode>| result.map_err(|error| format!("{error:?}"));
    let key = || s("commands-test:string");

    check(commands::ping(&conn, None).await)?;
    check_eq!(
        commands::set(&conn, key(), v("1"), None, None).await,
        Ok(true)
    );
    check_eq!(commands::get(&conn, key()).await, Ok(Some(v("1"))));
    check_eq!(commands::incr(&conn, key()).await, Ok(2));
    check_eq!(commands::incrby(&conn, key(), 3).await, Ok(5));
    check_eq!(commands::exists(&conn, vec![key()]).await, Ok(1));
    check_eq!(
        commands::expire(&conn, key(), DurationSeconds { ex: 60 }, None).await,
        Ok(true)
    );
    check_eq!(
        commands::keys(&conn, s("commands-test:str*")).await,
        Ok(vec![key()])
    );
    check_eq!(commands::del(&conn, vec![key()]).await, Ok(1));
    check_eq!(commands::get(&conn, key()).await, Ok(None));

    let key = || s("commands-test:hash");
    check_eq!(
        commands::hset(&conn, key(), vec![(s("f"), v("v")), (s("g"), v("w"))]).await,
        Ok(2)
    );
    check_eq!(
        commands::hgetall(&conn, key()).await,
        Ok(Map::from([(s("f"), v("v")), (s("g"), v("w"))]))
    );
    check_eq!(commands::hlen(&conn, key()).await, Ok(2));
    check_eq!(
        commands::hmget(&conn, key(), words(&["f", "x"])).await,
        Ok(vec![Some(v("v")), None])
    );
    let fields = commands::hrandfield_withvalues(&conn, key(), 2)
        .await
        .map_err(|error| format!("{error:?}"))?;
    check_eq!(fields.len(), 2);
    check_eq!(
        commands::hscan(&conn, key(), Cursor::Start, None, None).await,
        Ok((None, Map::from([(s("f"), v("v")), (s("g"), v("w"))])))
    );
    check_eq!(
        commands::hincrbyfloat(&conn, key(), s("n"), 1.5).await,
        Ok(1.5)
    );
    check_eq!(
        commands::hexpire(
            &conn,
            key(),
            DurationSeconds { ex: 60 },
            None,
            vec!["f".to_string(), "x".to_string()]
        )
        .await,
        Ok(vec![HexpireResult::Applied, HexpireResult::DoesNotExist])
    );
    check_eq!(commands::del(&conn, vec![key()]).await, Ok(1));

    // binary values round trip
    let key = || s("commands-test:binary");
    let binary = || StringOrBytes::Bytes(BINARY.to_vec());
    check_eq!(
        commands::set(&conn, key(), binary(), None, None).await,
        Ok(true)
    );
    check_eq!(commands::get(&conn, key()).await, Ok(Some(binary())));

    // the serialized value restores the key, with its value
    let serialized = commands::dump(&conn, key())
        .await
        .map_err(|error| format!("{error:?}"))?
        .ok_or("expected the key to dump")?;
    check_eq!(
        commands::restore(&conn, key(), None, serialized.clone(), false, None, None)
            .await
            .is_err(),
        true
    );
    let ttl = RestoreTtl::ExpireIn(DurationMilliseconds { px: 60_000 });
    check_eq!(
        commands::restore(&conn, key(), Some(ttl), serialized, true, None, None).await,
        Ok(())
    );
    check_eq!(commands::get(&conn, key()).await, Ok(Some(binary())));
    check_eq!(commands::del(&conn, vec![key()]).await, Ok(1));

    // the serialized libraries restore, appending none when there are none
    let serialized = commands::function_dump(&conn)
        .await
        .map_err(|error| format!("{error:?}"))?;
    check_eq!(
        commands::function_restore(&conn, serialized, Some(FunctionRestorePolicy::Append)).await,
        Ok(())
    );

    check_eq!(
        commands::publish(&conn, s("commands-test"), v("hello")).await,
        Ok(0)
    );
    let pass = commands::acl_genpass(&conn, None)
        .await
        .map_err(|error| format!("{error:?}"))?;
    check_eq!(pass.len(), 64);
    Ok(())
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/test-commands-suite/`
    path: "../wit",
    world: "test-commands",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Suite);
