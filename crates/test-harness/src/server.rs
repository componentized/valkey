//! A server over TCP, played by the test.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::{RespValue, decode, encode};

/// Listens on localhost for the component's connections.
pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind() -> std::io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind("127.0.0.1:0").await?,
        })
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().expect("bound").port()
    }

    /// The next connection to the server.
    pub async fn accept(&self) -> std::io::Result<Client> {
        let (stream, _) = self.listener.accept().await?;
        Ok(Client {
            stream,
            buffered: vec![],
        })
    }
}

/// A connection to the [`Server`], from the component.
pub struct Client {
    stream: TcpStream,
    /// Bytes read that are not part of a whole command yet.
    buffered: Vec<u8>,
}

impl Client {
    /// The next command the client sent, as its words, `None` once it closed its side.
    pub async fn request(&mut self) -> Option<Vec<RespValue>> {
        loop {
            if let Some(len) =
                componentized_valkey::codec::frame_len(&self.buffered).expect("valid RESP")
            {
                let rest = self.buffered.split_off(len);
                let frame = std::mem::replace(&mut self.buffered, rest);
                let RespValue::Array(words) = decode(frame).expect("valid RESP") else {
                    panic!("a command is an array");
                };
                return Some(
                    words
                        .into_iter()
                        .map(|word| decode(word).expect("encoded word"))
                        .collect(),
                );
            }
            let mut chunk = [0; 4096];
            let read = self.stream.read(&mut chunk).await.expect("read");
            if read == 0 {
                return None;
            }
            self.buffered.extend_from_slice(&chunk[..read]);
        }
    }

    /// Send a value to the client.
    pub async fn reply(&mut self, value: RespValue) {
        self.write(&encode(value)).await;
    }

    /// Send raw bytes to the client, e.g. part of a value.
    pub async fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("write");
        self.stream.flush().await.expect("flush");
    }

    /// Close the connection.
    pub async fn close(mut self) {
        let _ = self.stream.shutdown().await;
    }
}
