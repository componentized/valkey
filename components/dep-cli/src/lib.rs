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
use componentized_valkey::types::{NestedValue, Value};
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
        let command: Vec<String> = environment::get_arguments().into_iter().skip(1).collect();
        if let [flag] = command.as_slice()
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
async fn exec(command: Vec<String>, out: &mut Output) -> Result<(), Error> {
    let connection = connector::connect().await?;
    // before the command is sent, so none of the push messages it causes are missed
    let mut pushes = connection.pushes();
    let name = command[0].to_ascii_uppercase();
    let channels = command.len() - 1;

    if SUBSCRIBE.contains(&name.as_str()) {
        connection.emit(command).await?;
        print_pushes(&mut pushes, out, None).await
    } else if UNSUBSCRIBE.contains(&name.as_str()) {
        // a push message for each channel, or one when no channel is given, as a new connection is
        // not subscribed to any
        connection.emit(command).await?;
        print_pushes(&mut pushes, out, Some(channels.max(1))).await
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
    pushes: &mut StreamReader<Value>,
    out: &mut Output,
    limit: Option<usize>,
) -> Result<(), Error> {
    for _ in 0..limit.unwrap_or(usize::MAX) {
        match pushes.next().await {
            // sent unprompted, e.g. for a SUBSCRIBE missing its channels
            Some(Value::Error(message) | Value::BulkError(message)) => {
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
    let pushes = connection.pushes();
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
        let words = match split(&line) {
            Ok(words) => words,
            Err(message) => {
                let _ = lines.unbounded_send(format!("(error) {message}"));
                continue;
            }
        };
        let Some(name) = words.first().map(|name| name.to_ascii_uppercase()) else {
            continue;
        };
        if SUBSCRIBE.contains(&name.as_str()) || UNSUBSCRIBE.contains(&name.as_str()) {
            // no reply, the push messages confirming it are printed as they arrive
            connection.emit(words).await?;
            continue;
        }
        let text = match connection.exchange(words).await {
            Ok(reply) => render(reply),
            // the command failed, not the session
            Err(Error::Valkey(message)) => format!("(error) {message}"),
            Err(error) => return Err(error),
        };
        let _ = lines.unbounded_send(text);
    }
    Ok(())
}

async fn forward_pushes(mut pushes: StreamReader<Value>, lines: mpsc::UnboundedSender<String>) {
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
/// `\"`, `\n` and `\x41`, a single quoted word is taken as is, except for `\'`.
fn split(line: &str) -> Result<Vec<String>, String> {
    let mut words = vec![];
    let mut chars = line.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else {
            return Ok(words);
        };
        let mut word = String::new();
        if first == '"' || first == '\'' {
            chars.next();
            loop {
                match (chars.next(), first) {
                    (None, _) => return Err("unbalanced quotes".to_string()),
                    (Some(c), quote) if c == quote => break,
                    (Some('\\'), '"') => word.push(unescape(&mut chars)?),
                    (Some('\\'), '\'') if chars.peek() == Some(&'\'') => {
                        word.push(chars.next().unwrap_or_default())
                    }
                    (Some(c), _) => word.push(c),
                }
            }
            if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                return Err("closing quote must be followed by a space".to_string());
            }
        } else {
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                word.push(c);
            }
        }
        words.push(word);
    }
}

/// The character of an escape in a double quoted word, after its `\`.
fn unescape(chars: &mut impl Iterator<Item = char>) -> Result<char, String> {
    Ok(match chars.next() {
        Some('n') => '\n',
        Some('r') => '\r',
        Some('t') => '\t',
        Some('b') => '\u{8}',
        Some('a') => '\u{7}',
        Some('x') => {
            let hex: String = chars.take(2).collect();
            match u8::from_str_radix(&hex, 16) {
                // a string holds UTF-8, a byte on its own is only a character when it is ASCII
                Ok(byte) if byte.is_ascii() => byte as char,
                _ => return Err(format!("unsupported escape: \\x{hex}")),
            }
        }
        Some(c) => c,
        None => return Err("unbalanced quotes".to_string()),
    })
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
    }
}

/// Renders a value for people to read, in the style of valkey-cli.
fn render(value: Value) -> String {
    match value {
        Value::String(text) => text,
        Value::Error(message) | Value::BulkError(message) => format!("(error) {message}"),
        Value::Integer(value) => format!("(integer) {value}"),
        // quoted, with special characters escaped
        Value::BulkString(text) => format!("{text:?}"),
        Value::Null => "(nil)".to_string(),
        Value::Boolean(value) => format!("({value})"),
        Value::Double(value) => format!("(double) {value}"),
        Value::BigNumber(value) => format!("(big number) {value}"),
        // text to show as is, e.g. from INFO, the format is `txt` or `mkd`
        Value::VerbatimString((_format, text)) => text,
        Value::Array(items) | Value::Push(items) => {
            numbered(items.into_iter().map(render_nested).collect(), ')', "array")
        }
        Value::Set(items) => numbered(items.into_iter().map(render_nested).collect(), '~', "set"),
        Value::Map(entries) => numbered(
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

fn render_nested(value: NestedValue) -> String {
    match componentized_valkey::codec::decode(value) {
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
    // `path` is relative to the manifest, `components/dep-cli/`
    path: "../wit",
    world: "cli",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Cli);
