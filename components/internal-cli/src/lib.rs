//! A cli sending commands to Valkey, printing the replies.
//!
//! With arguments, they are sent as one command. A command without a reply, e.g. SUBSCRIBE, prints
//! the push messages that arrive until the cli is interrupted or the connection closes.
//!
//! Without arguments, each line of stdin is sent as a command over one connection, kept open until
//! stdin closes. Push messages are printed as they arrive.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::Poll;

use componentized::valkey::connection::{Connection, Error};
use componentized::valkey::connector;
use componentized_valkey::types::{RespNestedValue, RespValue, StringOrBytes};
use exports::wasi::cli::run::Guest;
use futures_channel::mpsc;
use futures_core::Stream;
use wasi::cli::{environment, stderr, stdin, stdout, types::ErrorCode};
use wit_bindgen::{FutureReader, StreamReader, StreamResult, StreamWriter};

const USAGE: &str = "\
Usage: cli [COMMAND [ARG]...]

Sends the command to Valkey and prints the reply. A command without a reply, e.g. SUBSCRIBE,
prints the push messages that arrive until interrupted.

Without a command, each line of stdin is sent as a command, until stdin closes. Words are split on
whitespace, quote a word to include whitespace, e.g. \"hello world\", in double quotes escapes like
\\n and \\x41 are expanded.

The server is configured with wasi:config, see the tcp-connector component.";

/// Commands without a reply, the server confirms each channel with a push message.
const SUBSCRIBE: [&str; 3] = ["SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE"];
const UNSUBSCRIBE: [&str; 3] = ["UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE"];

struct Cli;

impl Guest for Cli {
    async fn run() -> Result<(), ()> {
        let command: Vec<StringOrBytes> = environment::get_arguments()
            .into_iter()
            .skip(1)
            .map(StringOrBytes::String)
            .collect();
        if let [StringOrBytes::String(flag)] = command.as_slice()
            && (flag == "-h" || flag == "--help")
        {
            print(stdout::write_via_stream, USAGE).await;
            return Ok(());
        }

        let mut stdout = Output::new(stdout::write_via_stream);
        let result = if command.is_empty() {
            session(&mut stdout).await
        } else {
            exec(command, &mut stdout).await
        };
        stdout.close().await;
        if let Err(error) = result {
            print(stderr::write_via_stream, &render_error(error)).await;
            return Err(());
        }
        Ok(())
    }
}

/// Sends the command, printing its reply, or the push messages a command without one causes.
async fn exec(command: Vec<StringOrBytes>, out: &mut Output) -> Result<(), Error> {
    let connection = connector::connect().await?;
    // before the command is sent, so none of the push messages it causes are missed
    let mut pushes = connection.dead_letters();
    let name = name(&command);
    let channels = command.len() - 1;

    if SUBSCRIBE.contains(&name.as_str()) {
        connection.emit(command).await?;
        print_pushes(&mut pushes, out, None).await
    } else if UNSUBSCRIBE.contains(&name.as_str()) {
        // a push message for each channel, or one when no channel is given, as a new connection is
        // not subscribed to any
        connection.emit(command).await?;
        print_pushes(&mut pushes, out, Some(channels.max(1))).await
    } else if name == "RESET" && channels == 0 {
        // RESET is the connection's to send, it then repeats its handshake
        connection.reset().await?;
        out.line("RESET").await;
        Ok(())
    } else if name == "MONITOR" {
        // the feed follows the reply, values sent unprompted that the connection treats as push
        // messages
        let reply = connection.exchange(command).await?;
        out.line(&render(reply)).await;
        print_pushes(&mut pushes, out, None).await
    } else {
        let reply = connection.exchange(command).await?;
        out.line(&render(reply)).await;
        Ok(())
    }
}

/// Prints push messages, up to `limit` of them, failing once they stop.
async fn print_pushes(
    pushes: &mut StreamReader<RespValue>,
    out: &mut Output,
    limit: Option<usize>,
) -> Result<(), Error> {
    for _ in 0..limit.unwrap_or(usize::MAX) {
        match pushes.next().await {
            // sent unprompted, e.g. for a SUBSCRIBE missing its channels
            Some(RespValue::Error(message) | RespValue::BulkError(message)) => {
                return Err(Error::Valkey(message));
            }
            Some(push) => out.line(&render(push)).await,
            None => {
                return Err(Error::Client(
                    "push messages stopped, the connection closed or they were not read fast enough"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Sends each line of stdin as a command, printing the replies, and push messages as they arrive,
/// until stdin closes.
async fn session(out: &mut Output) -> Result<(), Error> {
    let connection = connector::connect().await?;
    let pushes = connection.dead_letters();
    let (stdin, _stdin_result) = stdin::read_via_stream();

    // replies and push messages arrive independently, both are printed by one writer
    let (lines_tx, lines_rx) = mpsc::unbounded();
    let mut commands = Some(Box::pin(send_lines(&connection, stdin, lines_tx.clone())));
    let mut forwarding = Some(Box::pin(forward_pushes(pushes, lines_tx)));
    let mut writing = pin!(write_lines(lines_rx, out));
    let mut result = Ok(());
    poll_fn(|cx| {
        let sent = match &mut commands {
            Some(commands) => commands.as_mut().poll(cx),
            None => Poll::Pending,
        };
        if let Poll::Ready(sent) = sent {
            result = sent;
            // dropping the senders lets the writer finish
            commands = None;
            forwarding = None;
        }
        if let Some(pushes) = &mut forwarding
            && pushes.as_mut().poll(cx).is_ready()
        {
            forwarding = None;
        }
        writing.as_mut().poll(cx)
    })
    .await;
    result
}

async fn send_lines(
    connection: &Connection,
    stdin: StreamReader<u8>,
    lines: mpsc::UnboundedSender<String>,
) -> Result<(), Error> {
    let mut input = Lines::new(stdin);
    while let Some(line) = input.next().await {
        let words: Vec<StringOrBytes> = match split(&line) {
            Ok(words) => words.into_iter().map(StringOrBytes::Bytes).collect(),
            Err(message) => {
                let _ = lines.unbounded_send(format!("(error) {message}"));
                continue;
            }
        };
        if words.is_empty() {
            continue;
        }
        let name = name(&words);
        if SUBSCRIBE.contains(&name.as_str()) || UNSUBSCRIBE.contains(&name.as_str()) {
            // no reply, the push messages confirming it are printed as they arrive
            connection.emit(words).await?;
            continue;
        }
        let reply = if name == "RESET" && words.len() == 1 {
            // RESET is the connection's to send, it then repeats its handshake
            connection
                .reset()
                .await
                .map(|()| RespValue::String("RESET".to_string()))
        } else {
            connection.exchange(words).await
        };
        let text = match reply {
            Ok(reply) => render(reply),
            // the command failed, not the session
            Err(Error::Valkey(message)) => format!("(error) {message}"),
            Err(Error::Client(message)) => format!("(error) {message}"),
            Err(error) => return Err(error),
        };
        let _ = lines.unbounded_send(text);
    }
    Ok(())
}

async fn forward_pushes(mut pushes: StreamReader<RespValue>, lines: mpsc::UnboundedSender<String>) {
    while let Some(push) = pushes.next().await {
        let _ = lines.unbounded_send(render(push));
    }
}

/// Prints lines until every sender is gone.
async fn write_lines(mut lines: mpsc::UnboundedReceiver<String>, out: &mut Output) {
    while let Some(line) = poll_fn(|cx| Pin::new(&mut lines).poll_next(cx)).await {
        out.line(&line).await;
    }
}

/// The lines of a stream of bytes.
struct Lines {
    stream: StreamReader<u8>,
    buffer: Vec<u8>,
    closed: bool,
}

impl Lines {
    fn new(stream: StreamReader<u8>) -> Self {
        Lines {
            stream,
            buffer: Vec::new(),
            closed: false,
        }
    }

    /// The next line, without its line ending, or none once the stream closed.
    async fn next(&mut self) -> Option<String> {
        loop {
            if let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = self.buffer.drain(..=end).collect();
                let line = String::from_utf8_lossy(&line);
                return Some(line.trim_end_matches(['\n', '\r']).to_string());
            }
            if self.closed {
                // the last line, without a line ending
                let line = std::mem::take(&mut self.buffer);
                return (!line.is_empty()).then(|| String::from_utf8_lossy(&line).into_owned());
            }
            let mut buffer = std::mem::take(&mut self.buffer);
            buffer.reserve(4096);
            let (result, buffer) = self.stream.read(buffer).await;
            self.buffer = buffer;
            if !matches!(result, StreamResult::Complete(_)) {
                self.closed = true;
            }
        }
    }
}

/// Splits a line into words, like valkey-cli.
///
/// Words are separated by whitespace. A double quoted word may contain whitespace and escapes, e.g.
/// `\"`, `\n` and `\x41`, a single quoted word is taken as is, except for `\'`. A `\x` escape is a
/// byte, so a word may be binary, e.g. `"\x80\xff"`.
fn split(line: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut words = vec![];
    let mut chars = line.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else {
            return Ok(words);
        };
        let mut word = vec![];
        if first == '"' || first == '\'' {
            chars.next();
            loop {
                match (chars.next(), first) {
                    (None, _) => return Err("unbalanced quotes".to_string()),
                    (Some(c), quote) if c == quote => break,
                    (Some('\\'), '"') => unescape(&mut chars, &mut word)?,
                    (Some('\\'), '\'') if chars.peek() == Some(&'\'') => {
                        push(&mut word, chars.next().unwrap_or_default())
                    }
                    (Some(c), _) => push(&mut word, c),
                }
            }
            if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                return Err("closing quote must be followed by a space".to_string());
            }
        } else {
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                push(&mut word, c);
            }
        }
        words.push(word);
    }
}

/// Appends the character, as UTF-8.
fn push(word: &mut Vec<u8>, c: char) {
    word.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
}

/// Appends the escape in a double quoted word, after its `\`.
fn unescape(chars: &mut impl Iterator<Item = char>, word: &mut Vec<u8>) -> Result<(), String> {
    match chars.next() {
        Some('n') => word.push(b'\n'),
        Some('r') => word.push(b'\r'),
        Some('t') => word.push(b'\t'),
        Some('b') => word.push(0x08),
        Some('a') => word.push(0x07),
        Some('x') => {
            let hex: String = chars.take(2).collect();
            match u8::from_str_radix(&hex, 16) {
                Ok(byte) if hex.len() == 2 => word.push(byte),
                _ => return Err(format!("unsupported escape: \\x{hex}")),
            }
        }
        Some(c) => push(word, c),
        None => return Err("unbalanced quotes".to_string()),
    }
    Ok(())
}

/// The name of a command, to tell those that need handling apart, e.g. `SUBSCRIBE`.
fn name(command: &[StringOrBytes]) -> String {
    match command.first() {
        Some(StringOrBytes::String(name)) => name.to_ascii_uppercase(),
        Some(StringOrBytes::Bytes(name)) => String::from_utf8_lossy(name).to_ascii_uppercase(),
        None => String::new(),
    }
}

/// A stream to stdout or stderr.
struct Output {
    writer: StreamWriter<u8>,
    written: FutureReader<Result<(), ErrorCode>>,
}

impl Output {
    fn new(write_via_stream: fn(StreamReader<u8>) -> FutureReader<Result<(), ErrorCode>>) -> Self {
        let (writer, reader) = wit_stream::new();
        let written = write_via_stream(reader);
        Output { writer, written }
    }

    async fn line(&mut self, text: &str) {
        self.writer
            .write_all(format!("{text}\n").into_bytes())
            .await;
    }

    async fn close(self) {
        // the host finishes writing once the writable end is dropped
        drop(self.writer);
        // nowhere left to report a failure to print
        let _ = self.written.await;
    }
}

/// Prints a line to a new stream to stdout or stderr.
async fn print(
    write_via_stream: fn(StreamReader<u8>) -> FutureReader<Result<(), ErrorCode>>,
    text: &str,
) {
    let mut output = Output::new(write_via_stream);
    output.line(text).await;
    output.close().await;
}

fn render_error(error: Error) -> String {
    match error {
        Error::Valkey(message) => format!("(error) {message}"),
        Error::Closed(None) => "connection closed".to_string(),
        Error::Closed(Some(reason)) => format!("connection closed: {reason}"),
        Error::Client(message) => message,
        Error::Resp(message) => format!("invalid RESP: {message}"),
        Error::Lagged => "fell behind, values were lost".to_string(),
    }
}

/// Renders a value for people to read, in the style of valkey-cli.
fn render(value: RespValue) -> String {
    match value {
        RespValue::String(text) => text,
        RespValue::Error(message) | RespValue::BulkError(message) => format!("(error) {message}"),
        RespValue::Integer(value) => format!("(integer) {value}"),
        // quoted, bytes outside printable ASCII escaped as valkey-cli does, binary or not
        RespValue::BulkString(bytes) => format!("\"{}\"", bytes.escape_ascii()),
        RespValue::Null => "(nil)".to_string(),
        RespValue::Boolean(value) => format!("({value})"),
        RespValue::Double(value) => format!("(double) {value}"),
        RespValue::BigNumber(value) => format!("(big number) {value}"),
        // text to show as is, e.g. from INFO, the format is `txt` or `mkd`
        RespValue::VerbatimString((_format, text)) => text,
        RespValue::Array(items) | RespValue::Push(items) => {
            numbered(items.into_iter().map(render_nested).collect(), ')', "array")
        }
        RespValue::Set(items) => {
            numbered(items.into_iter().map(render_nested).collect(), '~', "set")
        }
        RespValue::Map(entries) => numbered(
            entries
                .into_iter()
                .map(|(key, value)| {
                    indent(
                        &format!("{} => ", render_nested(key)),
                        &render_nested(value),
                    )
                })
                .collect(),
            '#',
            "map",
        ),
    }
}

fn render_nested(value: RespNestedValue) -> String {
    match componentized_valkey::resp::decode(&value) {
        Ok(value) => render(value),
        Err(error) => format!("(invalid) {error}"),
    }
}

/// Numbers each item, e.g. `1) `, the numbers right aligned.
fn numbered(items: Vec<String>, marker: char, kind: &str) -> String {
    if items.is_empty() {
        return format!("(empty {kind})");
    }
    let width = items.len().to_string().len();
    items
        .iter()
        .enumerate()
        .map(|(i, item)| indent(&format!("{:>width$}{marker} ", i + 1), item))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Prefixes the text, lining its other lines up under the first.
fn indent(prefix: &str, text: &str) -> String {
    let last_line = prefix.rsplit('\n').next().unwrap_or_default();
    let padding = format!("\n{}", " ".repeat(last_line.chars().count()));
    format!("{prefix}{}", text.replace('\n', &padding))
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/internal-cli/`
    path: "../wit",
    world: "cli",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Cli);

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(value: RespValue) -> RespNestedValue {
        componentized_valkey::resp::encode(value)
    }

    fn bulk(text: &str) -> RespValue {
        RespValue::BulkString(text.as_bytes().to_vec())
    }

    /// The words a line splits into, as bytes.
    fn split_words(words: &[&str]) -> Result<Vec<Vec<u8>>, String> {
        Ok(words.iter().map(|word| word.as_bytes().to_vec()).collect())
    }

    #[test]
    fn split_on_whitespace() {
        assert_eq!(
            split("  SET  key\tvalue "),
            split_words(&["SET", "key", "value"])
        );
        assert_eq!(split(""), Ok(vec![]));
        assert_eq!(split("   "), Ok(vec![]));
    }

    #[test]
    fn split_double_quotes() {
        assert_eq!(
            split(r#"SET key "hello world" "a\"b" "\n\r\t\b\a" "\x41\x7a" "\q" "€""#),
            split_words(&[
                "SET",
                "key",
                "hello world",
                "a\"b",
                "\n\r\t\u{8}\u{7}",
                "Az",
                "q",
                "€",
            ])
        );
        assert_eq!(split(r#""""#), Ok(vec![vec![]]));
    }

    #[test]
    fn split_binary_escapes() {
        // a `\x` escape is a byte, UTF-8 or not
        assert_eq!(
            split(r#"SET key "\x80\x00\xff""#),
            Ok(vec![
                b"SET".to_vec(),
                b"key".to_vec(),
                vec![0x80, 0x00, 0xff]
            ])
        );
    }

    #[test]
    fn split_single_quotes() {
        assert_eq!(
            split(r#"'hello world' 'it\'s' 'a\nb'"#),
            split_words(&["hello world", "it's", "a\\nb"])
        );
    }

    #[test]
    fn split_errors() {
        assert_eq!(split(r#"SET "key"#), Err("unbalanced quotes".to_string()));
        assert_eq!(split("'key"), Err("unbalanced quotes".to_string()));
        assert_eq!(split(r#""key\"#), Err("unbalanced quotes".to_string()));
        assert_eq!(
            split(r#""key"value"#),
            Err("closing quote must be followed by a space".to_string())
        );
        assert_eq!(
            split(r#""\xzz""#),
            Err("unsupported escape: \\xzz".to_string())
        );
        assert_eq!(
            split(r#""\x4""#),
            Err("unsupported escape: \\x4\"".to_string())
        );
    }

    #[test]
    fn render_scalars() {
        assert_eq!(render(RespValue::String("OK".to_string())), "OK");
        assert_eq!(
            render(RespValue::Error("ERR oops".to_string())),
            "(error) ERR oops"
        );
        assert_eq!(
            render(RespValue::BulkError("SYNTAX".to_string())),
            "(error) SYNTAX"
        );
        assert_eq!(render(RespValue::Integer(-1)), "(integer) -1");
        assert_eq!(render(bulk("a\"b\n")), r#""a\"b\n""#);
        assert_eq!(render(RespValue::Null), "(nil)");
        assert_eq!(render(RespValue::Boolean(true)), "(true)");
        assert_eq!(render(RespValue::Double(1.5)), "(double) 1.5");
        assert_eq!(
            render(RespValue::BigNumber("12345678901234567890".to_string())),
            "(big number) 12345678901234567890"
        );
        assert_eq!(
            render(RespValue::VerbatimString((
                "txt".to_string(),
                "line 1\nline 2".to_string()
            ))),
            "line 1\nline 2"
        );
    }

    #[test]
    fn render_bulk_strings_like_valkey_cli() {
        // bytes outside printable ASCII are escaped, like valkey-cli, binary or not
        assert_eq!(
            render(RespValue::BulkString(vec![0x80, b'a', 0x00])),
            r#""\x80a\x00""#
        );
        assert_eq!(render(bulk("€")), r#""\xe2\x82\xac""#);
        assert_eq!(render(bulk("tab\tquote\"")), r#""tab\tquote\"""#);
    }

    #[test]
    fn render_aggregates() {
        assert_eq!(render(RespValue::Array(vec![])), "(empty array)");
        assert_eq!(render(RespValue::Set(vec![])), "(empty set)");
        assert_eq!(render(RespValue::Map(vec![])), "(empty map)");
        assert_eq!(
            render(RespValue::Array(vec![
                nested(bulk("a")),
                nested(RespValue::Integer(1))
            ])),
            "1) \"a\"\n2) (integer) 1"
        );
        assert_eq!(render(RespValue::Set(vec![nested(bulk("a"))])), "1~ \"a\"");
        assert_eq!(
            render(RespValue::Push(vec![
                nested(bulk("message")),
                nested(bulk("news"))
            ])),
            "1) \"message\"\n2) \"news\""
        );
        assert_eq!(
            render(RespValue::Map(vec![(
                nested(bulk("proto")),
                nested(RespValue::Integer(3))
            )])),
            "1# \"proto\" => (integer) 3"
        );
    }

    #[test]
    fn render_nested_aggregates_line_up() {
        let inner = RespValue::Array(vec![nested(bulk("x")), nested(bulk("y"))]);
        assert_eq!(
            render(RespValue::Array(vec![nested(bulk("a")), nested(inner)])),
            "1) \"a\"\n2) 1) \"x\"\n   2) \"y\""
        );
        let entry = RespValue::Array(vec![nested(bulk("x")), nested(bulk("y"))]);
        assert_eq!(
            render(RespValue::Map(vec![(nested(bulk("k")), nested(entry))])),
            "1# \"k\" => 1) \"x\"\n          2) \"y\""
        );
    }

    #[test]
    fn render_numbers_right_aligned() {
        let items = (0..10).map(|n| nested(RespValue::Integer(n))).collect();
        let rendered = render(RespValue::Array(items));
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines[0], " 1) (integer) 0");
        assert_eq!(lines[9], "10) (integer) 9");
    }

    #[test]
    fn render_invalid_nested_values() {
        assert!(render(RespValue::Array(vec![b"?".to_vec()])).starts_with("1) (invalid) "));
    }

    #[test]
    fn render_errors() {
        assert_eq!(
            render_error(Error::Valkey("ERR oops".to_string())),
            "(error) ERR oops"
        );
        assert_eq!(render_error(Error::Closed(None)), "connection closed");
        assert_eq!(
            render_error(Error::Closed(Some("reset".to_string()))),
            "connection closed: reset"
        );
        assert_eq!(render_error(Error::Client("bad".to_string())), "bad");
        assert_eq!(
            render_error(Error::Resp("bad".to_string())),
            "invalid RESP: bad"
        );
        assert_eq!(render_error(Error::Lagged), "fell behind, values were lost");
    }
}
