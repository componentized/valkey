#![no_main]

use exports::componentized::valkey::resp::{
    Error as RespError, Guest as RespGuest, NestedValue, Value,
};
use exports::componentized::valkey::store::{
    Connection, Duration, Error, ExpireMode, ExpireResult, Guest as StoreGuest, GuestConnection,
    HelloOpts, HrandfieldOpts, HscanOpts, Instant,
};
use resp::{decode, encode};
use std::cell::RefCell;
use std::net::IpAddr;
use std::vec;
use wasi::sockets::ip_name_lookup::{resolve_addresses, ErrorCode as LookupErrorCode};
use wasi::sockets::types::{
    ErrorCode, IpAddress, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress,
    TcpSocket,
};
use wit_bindgen::{FutureReader, StreamReader, StreamResult, StreamWriter};

pub mod resp;

#[derive(Debug, Clone)]
struct ValkeyOps;

impl ValkeyOps {
    async fn open(address: IpSocketAddress) -> Result<ValkeyConnection, Error> {
        let socket = match address {
            IpSocketAddress::Ipv4(_) => TcpSocket::create(IpAddressFamily::Ipv4)?,
            IpSocketAddress::Ipv6(_) => TcpSocket::create(IpAddressFamily::Ipv6)?,
        };
        socket.connect(address).await?;

        // send and receive may each be called once per socket, the streams are kept for the life
        // of the connection
        let (output, send_stream) = wit_stream::new();
        let sent = socket.send(send_stream);
        let (input, received) = socket.receive();

        Ok(ValkeyConnection {
            streams: RefCell::new(Some((output, input))),
            _sent: sent,
            _received: received,
            _socket: socket,
        })
    }

    async fn resolve_ip_socket_addresses(
        host: &str,
        port: u16,
    ) -> Result<Vec<IpSocketAddress>, Error> {
        let ip_addresses = match host.parse() {
            Ok(IpAddr::V4(addr)) => {
                // host is an ipv4 address
                let octets = addr.octets();
                vec![IpAddress::Ipv4((
                    octets[0], octets[1], octets[2], octets[3],
                ))]
            }
            Ok(IpAddr::V6(addr)) => {
                // host is an ipv6 address
                let segments = addr.segments();
                vec![IpAddress::Ipv6((
                    segments[0],
                    segments[1],
                    segments[2],
                    segments[3],
                    segments[4],
                    segments[5],
                    segments[6],
                    segments[7],
                ))]
            }
            Err(_) => {
                // resolve as a hostname
                resolve_addresses(host.to_string()).await?
            }
        };

        // convert resolved ip address into socket address
        let socket_addresses = ip_addresses
            .into_iter()
            .map(|ip_address| match ip_address {
                IpAddress::Ipv4(address) => {
                    IpSocketAddress::Ipv4(Ipv4SocketAddress { address, port })
                }
                IpAddress::Ipv6(address) => {
                    let flow_info = 0;
                    let scope_id = 0;
                    IpSocketAddress::Ipv6(Ipv6SocketAddress {
                        port,
                        flow_info,
                        address,
                        scope_id,
                    })
                }
            })
            .collect();
        Ok(socket_addresses)
    }
}

impl StoreGuest for ValkeyOps {
    type Connection = ValkeyConnection;

    async fn connect(
        host: String,
        port: u16,
        opts: Option<HelloOpts>,
    ) -> Result<Connection, Error> {
        for addr in Self::resolve_ip_socket_addresses(&host, port).await? {
            if let Ok(conn) = Self::open(addr).await {
                // check the connection is alive
                if conn.hello(opts.clone()).await.is_ok() {
                    // it's alive
                    return Ok(Connection::new(conn));
                }
            }
        }
        Err(Error::Client(format!("unable to connect to {host}:{port}")))
    }
}

struct ValkeyConnection {
    /// The streams writing to and reading from the socket, taken while a command is in flight,
    /// a connection handles one command at a time.
    streams: RefCell<Option<(StreamWriter<u8>, StreamReader<u8>)>>,
    _sent: FutureReader<Result<(), ErrorCode>>,
    _received: FutureReader<Result<(), ErrorCode>>,
    _socket: TcpSocket,
}

impl ValkeyConnection {
    async fn exchange(
        output: &mut StreamWriter<u8>,
        input: &mut StreamReader<u8>,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        if !output.write_all(request).await.is_empty() {
            Err(Error::Client("Stream Closed".to_string()))?
        }

        // TODO handle responses spanning multiple windows
        match input.read(Vec::with_capacity(1024)).await {
            (StreamResult::Complete(_), response) => Ok(response),
            (StreamResult::Dropped, _) => Err(Error::Client("Stream Closed".to_string())),
            (StreamResult::Cancelled, _) => Err(Error::Client("Stream Cancelled".to_string())),
        }
    }
}

impl GuestConnection for ValkeyConnection {
    async fn send(&self, command: Vec<Value>) -> Result<Value, Error> {
        let request = encode(Value::Array(
            command.into_iter().map(|c| c.into()).collect(),
        ));

        let (mut output, mut input) = self
            .streams
            .borrow_mut()
            .take()
            .ok_or_else(|| Error::Client("Connection busy with another command".to_string()))?;
        let response = Self::exchange(&mut output, &mut input, request).await;
        *self.streams.borrow_mut() = Some((output, input));

        decode(response?).map(|r| r.into())
    }

    async fn acl_deluser(&self, username: String) -> Result<(), Error> {
        // https://valkey.io/commands/acl-deluser/
        // ACL DELUSER username [ username ... ]

        let response = self
            .send(vec![
                Value::BulkString("ACL".to_string()),
                Value::BulkString("DELUSER".to_string()),
                Value::BulkString(username),
            ])
            .await?;
        match response {
            Value::Integer(_) => Ok(()),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn acl_genpass(&self) -> Result<String, Error> {
        // https://valkey.io/commands/acl-genpass/
        // ACL GENPASS [ bits ]

        let response = self
            .send(vec![
                Value::BulkString("ACL".to_string()),
                Value::BulkString("GENPASS".to_string()),
            ])
            .await?;
        match response {
            Value::BulkString(pass) => Ok(pass),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn acl_setuser(&self, username: String, rules: Vec<String>) -> Result<(), Error> {
        // https://valkey.io/commands/acl-setuser/
        // ACL SETUSER username [ rule ] [ [ rule ] ... ]

        let mut command = vec![
            Value::BulkString("ACL".to_string()),
            Value::BulkString("SETUSER".to_string()),
            Value::BulkString(username),
        ];
        for rule in rules {
            command.push(Value::BulkString(rule));
        }
        let response = self.send(command).await?;
        match response {
            Value::String(msg) => match msg.as_str() {
                "OK" => Ok(()),
                msg => Err(Error::Client(format!("Not OK: {msg}")))?,
            },
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn auth(&self, username: String, password: String) -> Result<(), Error> {
        // https://valkey.io/commands/auth/
        // AUTH [ username ] password

        let response = self
            .send(vec![
                Value::BulkString("AUTH".to_string()),
                Value::BulkString(username),
                Value::BulkString(password),
            ])
            .await?;
        match response {
            Value::String(msg) => match msg.as_str() {
                "OK" => Ok(()),
                msg => Err(Error::Client(format!("Not OK: {msg}")))?,
            },
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn del(&self, key: String) -> Result<(), Error> {
        // https://valkey.io/commands/del/
        // DEL key [ key ... ]

        // TODO handle multiple keys
        let response = self
            .send(vec![
                Value::BulkString("DEL".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::Integer(_) => Ok(()),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn exists(&self, key: String) -> Result<bool, Error> {
        // https://valkey.io/commands/exists/
        // EXISTS key [ key ... ]

        // TODO handle multiple keys
        let response = self
            .send(vec![
                Value::BulkString("EXISTS".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    #[allow(async_fn_in_trait)]
    async fn expire(
        &self,
        key: String,
        after: Duration,
        mode: Option<ExpireMode>,
    ) -> Result<ExpireResult, Error> {
        // https://valkey.io/commands/exire/
        // EXPIRE key seconds [ NX | XX | GT | LT ]

        let mut cmd = vec![
            Value::BulkString("EXPIRE".to_string()),
            Value::BulkString(key),
            Value::Integer((after / 1000) as i64),
        ];
        if let Some(mode) = mode {
            cmd.push(mode.into());
        }

        let response = self.send(cmd).await?;
        match response {
            Value::Integer(code) => match code {
                0 => Ok(ExpireResult::ConditionNotMet),
                1 => Ok(ExpireResult::Applied),
                _ => Err(Error::Client(format!("Unknown reply: {code}")))?,
            },
            Value::Null => Err(Error::Client("Operation aborted".to_string()))?,
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    #[allow(async_fn_in_trait)]
    async fn expireat(
        &self,
        key: String,
        after: Instant,
        mode: Option<ExpireMode>,
    ) -> Result<ExpireResult, Error> {
        // https://valkey.io/commands/exireat/
        // EXPIREAT key unix-time-seconds [ NX | XX | GT | LT ]

        let mut cmd = vec![
            Value::BulkString("EXPIRE".to_string()),
            Value::BulkString(key),
            Value::Integer(after.seconds),
        ];
        if let Some(mode) = mode {
            cmd.push(mode.into());
        }

        let response = self.send(cmd).await?;
        match response {
            Value::Integer(code) => match code {
                0 => Ok(ExpireResult::ConditionNotMet),
                1 => Ok(ExpireResult::Applied),
                _ => Err(Error::Client(format!("Unknown reply: {code}")))?,
            },
            Value::Null => Err(Error::Client("Operation aborted".to_string()))?,
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn get(&self, key: String) -> Result<Option<String>, Error> {
        // https://valkey.io/commands/get/
        // GET key

        let response = self
            .send(vec![
                Value::BulkString("GET".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::BulkString(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hdel(&self, key: String, field: String) -> Result<(), Error> {
        // https://valkey.io/commands/hdel/
        // HDEL key field [ field ... ]

        let response = self
            .send(vec![
                Value::BulkString("HDEL".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
            ])
            .await?;
        match response {
            Value::Integer(_) => Ok(()),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hello(&self, opts: Option<HelloOpts>) -> Result<Vec<(String, Value)>, Error> {
        // https://valkey.io/commands/hello/
        // HELLO [ protover [ AUTH username password ] [ SETNAME clientname ] ]

        let mut cmd = vec![Value::BulkString("HELLO".to_string())];
        if let Some(opts) = opts {
            let has_proto = opts.proto_ver.is_none();
            if let Some(proto_ver) = opts.proto_ver {
                cmd.push(Value::BulkString(proto_ver));
            }
            if let Some((username, password)) = opts.auth {
                if has_proto {
                    Err(Error::Client(
                        "proto-ver must be specified to use auth".to_string(),
                    ))?
                }
                cmd.push(Value::BulkString("AUTH".to_string()));
                cmd.push(Value::BulkString(username));
                cmd.push(Value::BulkString(password));
            }
            if let Some(client_name) = opts.client_name {
                if has_proto {
                    Err(Error::Client(
                        "proto-ver must be specified to use client-name".to_string(),
                    ))?
                }
                cmd.push(Value::BulkString("SETNAME".to_string()));
                cmd.push(Value::BulkString(client_name));
            }
        }
        let response = self.send(cmd).await?;
        match response {
            // convert RESP2 array
            Value::Array(items) => {
                let mut hello: Vec<(String, Value)> = vec![];
                for item in items.chunks(2) {
                    let key = item[0].clone().into();
                    let value = item[1].clone().into();
                    let key = match key {
                        Value::BulkString(key) => key,
                        key => Err(Error::Client(format!("Unexpected key type: {:?}", key)))?,
                    };
                    hello.push((key, value))
                }
                Ok(hello)
            }
            // RESP3
            Value::Map(items) => {
                let mut hello: Vec<(String, Value)> = vec![];
                for (key, value) in items {
                    let key = match key.into() {
                        Value::BulkString(key) => key,
                        key => Err(Error::Client(format!("Unexpected key type: {:?}", key)))?,
                    };
                    hello.push((key, value.into()))
                }
                Ok(hello)
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hexists(&self, key: String, field: String) -> Result<bool, Error> {
        // https://valkey.io/commands/hexists/
        // HEXISTS key field

        let response = self
            .send(vec![
                Value::BulkString("HEXISTS".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
            ])
            .await?;
        match response {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    #[allow(async_fn_in_trait)]
    async fn hexpire(
        &self,
        key: String,
        after: Duration,
        mode: Option<ExpireMode>,
        fields: Vec<String>,
    ) -> Result<Vec<ExpireResult>, Error> {
        // https://valkey.io/commands/hexire/
        // HEXPIRE key seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]

        let mut cmd = vec![
            Value::BulkString("HEXPIRE".to_string()),
            Value::BulkString(key),
            Value::Integer((after / 1000) as i64),
        ];
        if let Some(mode) = mode {
            cmd.push(mode.into());
        }
        cmd.push(Value::BulkString("FIELDS".to_string()));
        cmd.push(Value::Integer(fields.len() as i64));
        for field in fields {
            cmd.push(Value::BulkString(field));
        }

        let response = self.send(cmd).await?;
        match response {
            Value::Array(codes) => {
                let mut replies = vec![];
                for code in codes {
                    let code = code.into();
                    match code {
                        Value::Integer(code) => {
                            replies.push(match code {
                                -2 => ExpireResult::DoesNotExist,
                                0 => ExpireResult::ConditionNotMet,
                                1 => ExpireResult::Applied,
                                2 => ExpireResult::ZeroSeconds,
                                _ => Err(Error::Client(format!("Unknown reply: {code}")))?,
                            });
                        }
                        value => Err(Error::Client(format!(
                            "Unexpected array item type: {:?}",
                            value
                        )))?,
                    }
                }
                Ok(replies)
            }
            Value::Null => Err(Error::Client("Operation aborted".to_string()))?,
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    #[allow(async_fn_in_trait)]
    async fn hexpireat(
        &self,
        key: String,
        after: Instant,
        mode: Option<ExpireMode>,
        fields: Vec<String>,
    ) -> Result<Vec<ExpireResult>, Error> {
        // https://valkey.io/commands/hexireat/
        // HEXPIREAT key unix-time-seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]

        let mut cmd = vec![
            Value::BulkString("HEXPIREAT".to_string()),
            Value::BulkString(key),
            Value::Integer(after.seconds),
        ];
        if let Some(mode) = mode {
            cmd.push(mode.into());
        }
        cmd.push(Value::BulkString("FIELDS".to_string()));
        cmd.push(Value::Integer(fields.len() as i64));
        for field in fields {
            cmd.push(Value::BulkString(field));
        }

        let response = self.send(cmd).await?;
        match response {
            Value::Array(codes) => {
                let mut replies = vec![];
                for code in codes {
                    let code = code.into();
                    match code {
                        Value::Integer(code) => {
                            replies.push(match code {
                                -2 => ExpireResult::DoesNotExist,
                                0 => ExpireResult::ConditionNotMet,
                                1 => ExpireResult::Applied,
                                2 => ExpireResult::ZeroSeconds,
                                _ => Err(Error::Client(format!("Unknown reply: {code}")))?,
                            });
                        }
                        value => Err(Error::Client(format!(
                            "Unexpected array item type: {:?}",
                            value
                        )))?,
                    }
                }
                Ok(replies)
            }
            Value::Null => Err(Error::Client("Operation aborted".to_string()))?,
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hget(&self, key: String, field: String) -> Result<Option<String>, Error> {
        // https://valkey.io/commands/hget/
        // HGET key field

        let response = self
            .send(vec![
                Value::BulkString("HGET".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
            ])
            .await?;
        match response {
            Value::BulkString(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hgetall(&self, key: String) -> Result<Vec<(String, String)>, Error> {
        // https://valkey.io/commands/hgetall/
        // HGETALL key

        let response = self
            .send(vec![
                Value::BulkString("HGETALL".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            // RESP2
            Value::Array(items) => {
                let mut fields = vec![];
                for item in items.chunks(2) {
                    let key = match item[0].clone().into() {
                        Value::BulkString(key) => key,
                        key => Err(Error::Client(format!("Unexpected key type: {:?}", key)))?,
                    };
                    let value = match item[1].clone().into() {
                        Value::BulkString(value) => value,
                        value => Err(Error::Client(format!("Unexpected value type: {:?}", value)))?,
                    };
                    fields.push((key, value));
                }
                Ok(fields)
            }
            // RESP3
            Value::Map(items) => {
                let mut fields = vec![];
                for (key, value) in items {
                    let key = match key.into() {
                        Value::BulkString(key) => key,
                        key => Err(Error::Client(format!("Unexpected key type: {:?}", key)))?,
                    };
                    let value = match value.into() {
                        Value::BulkString(value) => value,
                        value => Err(Error::Client(format!("Unexpected value type: {:?}", value)))?,
                    };
                    fields.push((key, value));
                }
                Ok(fields)
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hincrby(&self, key: String, field: String, increment: i64) -> Result<i64, Error> {
        // https://valkey.io/commands/hincrby/
        // HINCRBY key field increment

        let response = self
            .send(vec![
                Value::BulkString("HINCRBY".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
                Value::BulkString(increment.to_string()),
            ])
            .await?;
        match response {
            Value::Integer(value) => Ok(value),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hincrbyfloat(
        &self,
        key: String,
        field: String,
        increment: f64,
    ) -> Result<String, Error> {
        // https://valkey.io/commands/hincrbyfloat/
        // HINCRBYFLOAT key field increment

        let response = self
            .send(vec![
                Value::BulkString("HINCRBYFLOAT".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
                Value::BulkString(increment.to_string()),
            ])
            .await?;
        match response {
            Value::BulkString(value) => Ok(value),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hkeys(&self, key: String) -> Result<Vec<String>, Error> {
        // https://valkey.io/commands/hkeys/
        // HKEYS key

        let response = self
            .send(vec![
                Value::BulkString("HKEYS".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::Array(values) => {
                let mut keys = vec![];
                for value in values {
                    let value = value.into();
                    match value {
                        Value::BulkString(key) => keys.push(key),
                        value => Err(Error::Client(format!(
                            "Unexpected array item type: {:?}",
                            value
                        )))?,
                    }
                }
                Ok(keys)
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hlen(&self, key: String) -> Result<u64, Error> {
        // https://valkey.io/commands/hlen/
        // HLEN key

        let response = self
            .send(vec![
                Value::BulkString("HLEN".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::Integer(value) => Ok(value as u64),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hmget(&self, key: String, fields: Vec<String>) -> Result<Vec<Option<String>>, Error> {
        // https://valkey.io/commands/hmget/
        // HMGET key field [ field ... ]

        let mut cmd = vec![
            Value::BulkString("HMGET".to_string()),
            Value::BulkString(key),
        ];
        for field in fields {
            cmd.push(Value::BulkString(field));
        }
        let response = self.send(cmd).await?;
        match response {
            Value::Array(items) => {
                let mut values = vec![];
                for item in items {
                    match item.into() {
                        Value::BulkString(val) => values.push(Some(val)),
                        Value::Null => values.push(None),
                        item => Err(Error::Client(format!(
                            "Unexpected array item type: {:?}",
                            item
                        )))?,
                    }
                }
                Ok(values)
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hmset(&self, key: String, fields: Vec<(String, String)>) -> Result<(), Error> {
        // https://valkey.io/commands/hmset/
        // HMSET key field value [ field value ... ]

        let mut cmd = vec![
            Value::BulkString("HMSET".to_string()),
            Value::BulkString(key),
        ];
        for (field, value) in fields {
            cmd.push(Value::BulkString(field));
            cmd.push(Value::BulkString(value));
        }
        let response = self.send(cmd).await?;
        match response {
            Value::String(msg) => match msg.as_str() {
                "OK" => Ok(()),
                msg => Err(Error::Client(format!("Not OK: {msg}")))?,
            },
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hrandfield(
        &self,
        key: String,
        opts: Option<HrandfieldOpts>,
    ) -> Result<Option<Vec<(String, Option<String>)>>, Error> {
        // https://valkey.io/commands/hrandfield/
        // HRANDFIELD key [ count [ WITHVALUES ] ]

        let mut cmd = vec![
            Value::BulkString("HRANDFIELD".to_string()),
            Value::BulkString(key),
        ];
        if let Some(opts) = opts {
            if let Some(count) = opts.count {
                cmd.push(Value::BulkString(count.to_string()));
            }
            if let Some(with_values) = opts.with_values {
                if with_values {
                    if opts.count.is_none() {
                        Err(Error::Client(
                            "count must be specified to use with-values".to_string(),
                        ))?
                    }
                    cmd.push(Value::BulkString("WITHVALUES".to_string()));
                }
            }
        }
        let response = self.send(cmd).await?;
        match response {
            Value::BulkString(value) => Ok(Some(vec![(value, None)])),
            Value::Array(items) => match items.len() {
                0 => Ok(None),
                _ => {
                    let mut foo = vec![];
                    match opts {
                        Some(HrandfieldOpts {
                            count: _,
                            with_values: Some(true),
                        }) => {
                            for item in items.chunks(2) {
                                let key = match item[0].clone().into() {
                                    Value::BulkString(key) => key,
                                    key => Err(Error::Client(format!(
                                        "Unexpected key type: {:?}",
                                        key
                                    )))?,
                                };
                                let value = match item[1].clone().into() {
                                    Value::BulkString(value) => value,
                                    value => Err(Error::Client(format!(
                                        "Unexpected value type: {:?}",
                                        value
                                    )))?,
                                };
                                foo.push((key, Some(value)));
                            }
                        }
                        _ => {
                            for key in items {
                                let key = match key.into() {
                                    Value::BulkString(key) => key,
                                    key => Err(Error::Client(format!(
                                        "Unexpected key type: {:?}",
                                        key
                                    )))?,
                                };
                                foo.push((key, None));
                            }
                        }
                    }
                    Ok(Some(foo))
                }
            },
            Value::Null => Ok(None),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hscan(
        &self,
        key: String,
        cursor: Option<String>,
        opts: Option<HscanOpts>,
    ) -> Result<(Option<String>, Vec<(String, Option<String>)>), Error> {
        // https://valkey.io/commands/hscan/
        // HSCAN key cursor [ MATCH pattern ] [ COUNT count ] [ NOVALUES ]

        let mut cmd = vec![
            Value::BulkString("HSCAN".to_string()),
            Value::BulkString(key),
            Value::BulkString(cursor.unwrap_or("0".to_string())),
        ];
        if let Some(opts) = opts.clone() {
            if let Some(match_) = opts.match_ {
                cmd.push(Value::BulkString("MATCH".to_string()));
                cmd.push(Value::BulkString(match_));
            }
            if let Some(count) = opts.count {
                cmd.push(Value::BulkString("COUNT".to_string()));
                cmd.push(Value::BulkString(count.to_string()));
            }
            if let Some(no_values) = opts.no_values {
                if no_values {
                    cmd.push(Value::BulkString("NOVALUES".to_string()));
                }
            }
        }
        let response = self.send(cmd).await?;
        match response {
            Value::Array(items) => {
                let cursor = match items[0].clone().into() {
                    Value::BulkString(cursor) => cursor,
                    cursor => Err(Error::Client(format!(
                        "Unexpected cursor type: {:?}",
                        cursor
                    )))?,
                };
                let elements = match items[1].clone().into() {
                    Value::Array(elements) => elements,
                    elements => Err(Error::Client(format!(
                        "Unexpected elements type: {:?}",
                        elements
                    )))?,
                };
                let mut fields = vec![];
                match opts {
                    Some(HscanOpts {
                        match_: _,
                        count: _,
                        no_values: Some(true),
                    }) => {
                        for field in elements {
                            match field.clone().into() {
                                Value::BulkString(field) => fields.push((field, None)),
                                field => Err(Error::Client(format!(
                                    "Unexpected field type: {:?}",
                                    field
                                )))?,
                            }
                        }
                    }
                    _ => {
                        for item in elements.chunks(2) {
                            let key = match item[0].clone().into() {
                                Value::BulkString(key) => key,
                                key => {
                                    Err(Error::Client(format!("Unexpected key type: {:?}", key)))?
                                }
                            };
                            let value = match item[1].clone().into() {
                                Value::BulkString(value) => value,
                                value => Err(Error::Client(format!(
                                    "Unexpected value type: {:?}",
                                    value
                                )))?,
                            };
                            fields.push((key, Some(value)));
                        }
                    }
                };

                let cursor = match cursor.as_str() {
                    "0" => None,
                    _ => Some(cursor),
                };

                Ok((cursor, fields))
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hset(&self, key: String, field: String, value: String) -> Result<(), Error> {
        // https://valkey.io/commands/hset/
        // HSET key field value [ field value ... ]

        let response = self
            .send(vec![
                Value::BulkString("HSET".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
                Value::BulkString(value),
            ])
            .await?;
        match response {
            Value::Integer(_) => Ok(()),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hsetnx(&self, key: String, field: String, value: String) -> Result<bool, Error> {
        // https://valkey.io/commands/hsetnx/
        // HSETNX key field value

        let response = self
            .send(vec![
                Value::BulkString("HSETNX".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
                Value::BulkString(value),
            ])
            .await?;
        match response {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hstrlen(&self, key: String, field: String) -> Result<u64, Error> {
        // https://valkey.io/commands/hstrlen/
        // HSTRLEN key field

        let response = self
            .send(vec![
                Value::BulkString("HSTRLEN".to_string()),
                Value::BulkString(key),
                Value::BulkString(field),
            ])
            .await?;
        match response {
            Value::Integer(len) => Ok(len as u64),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn hvals(&self, key: String) -> Result<Vec<String>, Error> {
        // https://valkey.io/commands/hvals/
        // HVALS key

        let response = self
            .send(vec![
                Value::BulkString("HVALS".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::Array(items) => {
                let mut fields = vec![];
                for item in items {
                    match item.into() {
                        Value::BulkString(field) => fields.push(field),
                        field => Err(Error::Client(format!("Unexpected field type: {:?}", field)))?,
                    }
                }
                Ok(fields)
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn incr(&self, key: String) -> Result<i64, Error> {
        // https://valkey.io/commands/incr/
        // INCR key

        let response = self
            .send(vec![
                Value::BulkString("INCR".to_string()),
                Value::BulkString(key),
            ])
            .await?;
        match response {
            Value::Integer(value) => Ok(value),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn incrby(&self, key: String, increment: i64) -> Result<i64, Error> {
        // https://valkey.io/commands/incrby/
        // INCRBY key increment

        let response = self
            .send(vec![
                Value::BulkString("INCRBY".to_string()),
                Value::BulkString(key),
                Value::BulkString(increment.to_string()),
            ])
            .await?;
        match response {
            Value::Integer(value) => Ok(value),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn keys(&self, pattern: String) -> Result<Vec<String>, Error> {
        // https://valkey.io/commands/keys/
        // KEYS pattern

        let response = self
            .send(vec![
                Value::BulkString("KEYS".to_string()),
                Value::BulkString(pattern),
            ])
            .await?;
        match response {
            Value::Array(values) => {
                let mut keys = vec![];
                for value in values {
                    let value = value.into();
                    match value {
                        Value::BulkString(key) => keys.push(key),
                        value => Err(Error::Client(format!(
                            "Unexpected array item type: {:?}",
                            value
                        )))?,
                    }
                }
                Ok(keys)
            }
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn ping(&self) -> Result<(), Error> {
        // https://valkey.io/commands/ping/
        // PING [ message ]

        // TODO support command options
        let response = self
            .send(vec![Value::BulkString("PING".to_string())])
            .await?;
        match response {
            Value::String(msg) => match msg.as_str() {
                "PONG" => Ok(()),
                msg => Err(Error::Client(format!("Not PONG: {msg}")))?,
            },
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn quit(&self) -> Result<(), Error> {
        // https://valkey.io/commands/quit/
        // QUIT

        let response = self
            .send(vec![Value::BulkString("QUIT".to_string())])
            .await?;
        match response {
            Value::String(msg) => match msg.as_str() {
                "OK" => Ok(()),
                msg => Err(Error::Client(format!("Not OK: {msg}")))?,
            },
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn publish(&self, channel: String, message: String) -> Result<i64, Error> {
        // https://valkey.io/commands/publish/
        // PUBLISH channel message

        let response = self
            .send(vec![
                Value::BulkString("PUBLISH".to_string()),
                Value::BulkString(channel),
                Value::BulkString(message),
            ])
            .await?;
        match response {
            Value::Integer(value) => Ok(value),
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }

    async fn set(&self, key: String, value: String) -> Result<(), Error> {
        // https://valkey.io/commands/set/
        // SET key value
        //   [ NX | XX | IFEQ comparison-value ]
        //   [ GET ]
        //   [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]

        // TODO support command options
        let response = self
            .send(vec![
                Value::BulkString("SET".to_string()),
                Value::BulkString(key),
                Value::BulkString(value),
            ])
            .await?;
        match response {
            Value::String(msg) => match msg.as_str() {
                "OK" => Ok(()),
                msg => Err(Error::Client(format!("Not OK: {msg}")))?,
            },
            Value::Null => Err(Error::Client("Operation aborted".to_string()))?,
            Value::Error(err) => Err(Error::Valkey(err))?,
            response => Err(Error::Client(format!(
                "Unexpected response type: {:?}",
                response
            )))?,
        }
    }
}

impl From<ErrorCode> for Error {
    fn from(e: ErrorCode) -> Self {
        match e {
            ErrorCode::AccessDenied => Self::Client("Network AccessDenied".to_string()),
            ErrorCode::NotSupported => Self::Client("Network NotSupported".to_string()),
            ErrorCode::InvalidArgument => Self::Client("Network InvalidArgument".to_string()),
            ErrorCode::OutOfMemory => Self::Client("Network OutOfMemory".to_string()),
            ErrorCode::Timeout => Self::Client("Network Timeout".to_string()),
            ErrorCode::InvalidState => Self::Client("Network InvalidState".to_string()),
            ErrorCode::AddressNotBindable => Self::Client("Network AddressNotBindable".to_string()),
            ErrorCode::AddressInUse => Self::Client("Network AddressInUse".to_string()),
            ErrorCode::RemoteUnreachable => Self::Client("Network RemoteUnreachable".to_string()),
            ErrorCode::ConnectionRefused => Self::Client("Network ConnectionRefused".to_string()),
            ErrorCode::ConnectionBroken => Self::Client("Network ConnectionBroken".to_string()),
            ErrorCode::ConnectionReset => Self::Client("Network ConnectionReset".to_string()),
            ErrorCode::ConnectionAborted => Self::Client("Network ConnectionAborted".to_string()),
            ErrorCode::DatagramTooLarge => Self::Client("Network DatagramTooLarge".to_string()),
            ErrorCode::Other(None) => Self::Client("Network Other".to_string()),
            ErrorCode::Other(Some(message)) => Self::Client(format!("Network Other: {message}")),
        }
    }
}

impl From<LookupErrorCode> for Error {
    fn from(e: LookupErrorCode) -> Self {
        match e {
            LookupErrorCode::AccessDenied => Self::Client("Network AccessDenied".to_string()),
            LookupErrorCode::InvalidArgument => Self::Client("Network InvalidArgument".to_string()),
            LookupErrorCode::NameUnresolvable => {
                Self::Client("Network NameUnresolvable".to_string())
            }
            LookupErrorCode::TemporaryResolverFailure => {
                Self::Client("Network TemporaryResolverFailure".to_string())
            }
            LookupErrorCode::PermanentResolverFailure => {
                Self::Client("Network PermanentResolverFailure".to_string())
            }
            LookupErrorCode::Other(None) => Self::Client("Network Other".to_string()),
            LookupErrorCode::Other(Some(message)) => {
                Self::Client(format!("Network Other: {message}"))
            }
        }
    }
}

impl RespGuest for ValkeyOps {
    fn decode(data: Vec<u8>) -> Result<Value, RespError> {
        decode(data).map_err(|e| e.to_string())
    }

    fn encode(value: Value) -> Result<Vec<u8>, RespError> {
        Ok(encode(value.into()))
    }
}

impl From<Value> for NestedValue {
    fn from(value: Value) -> Self {
        ValkeyOps::encode(value).expect("values must encode cleanly")
    }
}

impl From<NestedValue> for Value {
    fn from(value: NestedValue) -> Self {
        ValkeyOps::decode(value).expect("nested values must decode cleanly")
    }
}

impl From<ExpireMode> for Value {
    fn from(mode: ExpireMode) -> Self {
        match mode {
            ExpireMode::Nx => Value::BulkString("NX".to_string()),
            ExpireMode::Xx => Value::BulkString("XX".to_string()),
            ExpireMode::Gt => Value::BulkString("GT".to_string()),
            ExpireMode::Lt => Value::BulkString("LT".to_string()),
        }
    }
}

wit_bindgen::generate!({
    world: "ops",
    path: "../wit",
    generate_all
});

export!(ValkeyOps);
