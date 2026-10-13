//! Connections to a Valkey server over wasi:sockets TCP.
//!
//! Opening a transport connects the socket and starts two background tasks, in the task of the
//! call that opened it:
//!
//! - the receive pump frames the bytes the socket receives into RESP values
//! - the send pump encodes the RESP values handed to `send` into bytes for the socket
//!
//! `send` and `receive` are sync methods, which have no task of their own to start background
//! work in, so they only hand over the streams the pumps already work with, and leave resolving
//! the futures of repeated calls to a third task. The pumps keep the socket open until both
//! directions are done, so a connection keeps its transport alive.

// the test harness provides a main
#![cfg_attr(not(test), no_main)]

use std::cell::RefCell;
use std::future::poll_fn;
use std::net::IpAddr;
use std::pin::Pin;
use std::rc::Rc;

use componentized::valkey::connection::{ConnectOpts, Connection, Error};
use componentized_valkey::types::Value;
use exports::componentized::valkey::tcp::{Guest, GuestTransport, Transport};
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use wasi::sockets::ip_name_lookup::{ErrorCode as LookupErrorCode, resolve_addresses};
use wasi::sockets::types::{
    ErrorCode, IpAddress, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress,
    TcpSocket,
};
use wit_bindgen::{
    FutureReader, FutureWriter, StreamReader, StreamResult, StreamWriter, spawn_local,
};

/// How many bytes to read from the socket at a time.
const READ_SIZE: usize = 4096;

struct Component;

impl Guest for Component {
    type Transport = TransportImpl;

    async fn connect(
        host: String,
        port: u16,
        opts: Option<ConnectOpts>,
    ) -> Result<Connection, Error> {
        let transport = open(&host, port).await?;
        let (replies, closed) = transport.receive();
        let (requests, connection) = Connection::new(replies, closed, opts).await;
        // the connection resolves with the send pump's errors, the result of sending is not needed
        let _sent = transport.send(requests);
        connection.await
    }
}

/// The streams of an open socket, handed over once each.
pub struct TransportImpl {
    requests: RefCell<
        Option<(
            oneshot::Sender<StreamReader<Value>>,
            FutureReader<Result<(), Error>>,
        )>,
    >,
    replies: RefCell<Option<(StreamReader<Value>, FutureReader<Result<(), Error>>)>>,
    /// Futures for the resolver to fail, a sync method can't write them itself.
    failures: mpsc::UnboundedSender<(FutureWriter<Result<(), Error>>, &'static str)>,
}

impl TransportImpl {
    /// A future the resolver fails with `message`.
    fn failed(&self, message: &'static str) -> FutureReader<Result<(), Error>> {
        let (writer, reader) = wit_future::new(|| Err(Error::Closed(None)));
        // a transport whose resolver stopped drops the writer, resolving it to the default
        let _ = self.failures.unbounded_send((writer, message));
        reader
    }
}

impl GuestTransport for TransportImpl {
    async fn open(host: String, port: u16) -> Result<Transport, Error> {
        Ok(Transport::new(open(&host, port).await?))
    }

    fn send(&self, requests: StreamReader<Value>) -> FutureReader<Result<(), Error>> {
        match self.requests.borrow_mut().take() {
            Some((pump, sent)) => {
                // the pump only stops waiting for the requests once the socket is closed
                let _ = pump.send(requests);
                sent
            }
            None => self.failed("send may be called at most once"),
        }
    }

    fn receive(&self) -> (StreamReader<Value>, FutureReader<Result<(), Error>>) {
        self.replies.borrow_mut().take().unwrap_or_else(|| {
            let (_, replies) = wit_stream::new();
            (replies, self.failed("receive may be called at most once"))
        })
    }
}

/// Fail each future handed over, until the transport is dropped.
async fn resolve_failures(
    mut failures: mpsc::UnboundedReceiver<(FutureWriter<Result<(), Error>>, &'static str)>,
) {
    while let Some((writer, message)) = poll_fn(|cx| Pin::new(&mut failures).poll_next(cx)).await {
        let _ = writer.write(Err(Error::Client(message.to_string()))).await;
    }
}

/// Connect a socket to `host` and start its pumps.
async fn open(host: &str, port: u16) -> Result<TransportImpl, Error> {
    let socket = Rc::new(connect_socket(host, port).await?);

    let (bytes_out, bytes_out_reader) = wit_stream::new();
    let socket_sent = socket.send(bytes_out_reader);
    let (bytes_in, socket_received) = socket.receive();

    let (replies_writer, replies) = wit_stream::new();
    // the defaults are only written if a pump stops without a result, the socket closed under it
    let (closed_writer, closed) = wit_future::new(|| Err(Error::Closed(None)));
    let (sent_writer, sent) = wit_future::new(|| Err(Error::Closed(None)));
    let (requests_tx, requests_rx) = oneshot::channel();
    let (failures_tx, failures_rx) = mpsc::unbounded();

    spawn_local(receive_pump(
        socket.clone(),
        bytes_in,
        socket_received,
        replies_writer,
        closed_writer,
    ));
    spawn_local(send_pump(
        socket,
        requests_rx,
        bytes_out,
        socket_sent,
        sent_writer,
    ));
    spawn_local(resolve_failures(failures_rx));

    Ok(TransportImpl {
        requests: RefCell::new(Some((requests_tx, sent))),
        replies: RefCell::new(Some((replies, closed))),
        failures: failures_tx,
    })
}

/// Frame the bytes the socket receives into values, until the socket closes or the reader of the
/// values is gone.
async fn receive_pump(
    _socket: Rc<TcpSocket>,
    mut bytes: StreamReader<u8>,
    received: FutureReader<Result<(), ErrorCode>>,
    mut replies: StreamWriter<Value>,
    closed: FutureWriter<Result<(), Error>>,
) {
    // bytes read from the socket that are not part of a whole value yet
    let mut buffered: Vec<u8> = Vec::new();
    let result = 'pump: loop {
        // hand over each whole value buffered, a value may span several reads
        loop {
            match componentized_valkey::codec::frame_len(&buffered) {
                Ok(Some(len)) => {
                    let rest = buffered.split_off(len);
                    let frame = std::mem::replace(&mut buffered, rest);
                    match componentized_valkey::codec::decode(frame) {
                        Ok(value) => {
                            if replies.write_one(value).await.is_some() {
                                // nobody reads the values anymore
                                break 'pump Ok(());
                            }
                        }
                        Err(error) => break 'pump Err(Error::Resp(error)),
                    }
                }
                Ok(None) => break,
                Err(error) => break 'pump Err(Error::Resp(error)),
            }
        }
        match bytes.read(Vec::with_capacity(READ_SIZE)).await {
            (StreamResult::Complete(_), chunk) => buffered.extend_from_slice(&chunk),
            (StreamResult::Dropped | StreamResult::Cancelled, _) => {
                // the socket closed, its future says whether it closed gracefully
                break received.await.map_err(socket_error);
            }
        }
    };
    // closing the values ends the connection's reader, which then waits for the result
    drop(replies);
    let _ = closed.write(result).await;
}

/// Encode the values handed to `send` into bytes for the socket, until the values end, which
/// half-closes the socket.
async fn send_pump(
    _socket: Rc<TcpSocket>,
    requests: oneshot::Receiver<StreamReader<Value>>,
    mut bytes: StreamWriter<u8>,
    socket_sent: FutureReader<Result<(), ErrorCode>>,
    sent: FutureWriter<Result<(), Error>>,
) {
    // a transport dropped without sending half-closes the socket
    let Ok(mut requests) = requests.await else {
        return;
    };
    while let Some(value) = requests.next().await {
        if !bytes
            .write_all(componentized_valkey::codec::encode(value))
            .await
            .is_empty()
        {
            // the socket stopped sending
            break;
        }
    }
    drop(bytes);
    let result = socket_sent.await.map_err(socket_error);
    let _ = sent.write(result).await;
}

/// Connect a socket to the first address of `host` that accepts it.
async fn connect_socket(host: &str, port: u16) -> Result<TcpSocket, Error> {
    let mut last_error = None;
    for address in resolve(host, port).await? {
        let family = match address {
            IpSocketAddress::Ipv4(_) => IpAddressFamily::Ipv4,
            IpSocketAddress::Ipv6(_) => IpAddressFamily::Ipv6,
        };
        let socket = TcpSocket::create(family).map_err(socket_error)?;
        match socket.connect(address).await {
            Ok(()) => return Ok(socket),
            Err(error) => last_error = Some(error),
        }
    }
    let reason = match last_error {
        Some(error) => format!("unable to connect to {host}:{port}: {error:?}"),
        None => format!("unable to connect to {host}:{port}: no addresses"),
    };
    Err(Error::Closed(Some(reason)))
}

/// The socket addresses of `host`, an IP address or a hostname to resolve.
async fn resolve(host: &str, port: u16) -> Result<Vec<IpSocketAddress>, Error> {
    let addresses = match host.parse() {
        Ok(IpAddr::V4(address)) => {
            let [a, b, c, d] = address.octets();
            vec![IpAddress::Ipv4((a, b, c, d))]
        }
        Ok(IpAddr::V6(address)) => {
            let [a, b, c, d, e, f, g, h] = address.segments();
            vec![IpAddress::Ipv6((a, b, c, d, e, f, g, h))]
        }
        Err(_) => resolve_addresses(host.to_string())
            .await
            .map_err(|error| lookup_error(host, error))?,
    };
    Ok(addresses
        .into_iter()
        .map(|address| match address {
            IpAddress::Ipv4(address) => IpSocketAddress::Ipv4(Ipv4SocketAddress { port, address }),
            IpAddress::Ipv6(address) => IpSocketAddress::Ipv6(Ipv6SocketAddress {
                port,
                flow_info: 0,
                address,
                scope_id: 0,
            }),
        })
        .collect())
}

fn socket_error(error: ErrorCode) -> Error {
    Error::Closed(Some(format!("{error:?}")))
}

fn lookup_error(host: &str, error: LookupErrorCode) -> Error {
    Error::Closed(Some(format!("unable to resolve {host}: {error:?}")))
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/tcp/`
    path: "../wit",
    world: "tcp",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Component);
