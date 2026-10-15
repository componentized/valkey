//! wasi:sockets without a network, for tests of what a gate allows: every operation succeeds, so an
//! error can only come from the gate.
//!
//! Names resolve to fixed addresses: an IP address to itself, `localhost` to `127.0.0.1` and `::1`,
//! any other name to `192.0.2.1`, an address for documentation.

use std::cell::{Cell, RefCell};
use std::net::IpAddr;

use exports::wasi::sockets::ip_name_lookup::{self, ErrorCode as LookupErrorCode};
use exports::wasi::sockets::types::{
    self, Duration, ErrorCode, GuestTcpSocket, GuestUdpSocket, IpAddress, IpAddressFamily,
    IpSocketAddress, Ipv4SocketAddress, TcpSocket, UdpSocket,
};
use wit_bindgen::{FutureReader, StreamReader};

/// The address names other than `localhost` resolve to.
const NAME_ADDRESS: IpAddress = IpAddress::Ipv4((192, 0, 2, 1));

struct Component;

impl ip_name_lookup::Guest for Component {
    async fn resolve_addresses(name: String) -> Result<Vec<IpAddress>, LookupErrorCode> {
        Ok(match name.parse::<IpAddr>() {
            Ok(address) => vec![ip_address(address)],
            Err(_) if name == "localhost" => vec![
                IpAddress::Ipv4((127, 0, 0, 1)),
                IpAddress::Ipv6((0, 0, 0, 0, 0, 0, 0, 1)),
            ],
            Err(_) => vec![NAME_ADDRESS],
        })
    }
}

fn ip_address(address: IpAddr) -> IpAddress {
    match address {
        IpAddr::V4(address) => {
            let [a, b, c, d] = address.octets();
            IpAddress::Ipv4((a, b, c, d))
        }
        IpAddr::V6(address) => {
            let [a, b, c, d, e, f, g, h] = address.segments();
            IpAddress::Ipv6((a, b, c, d, e, f, g, h))
        }
    }
}

/// The address of a socket that was not bound, any address and port.
fn unspecified() -> IpSocketAddress {
    IpSocketAddress::Ipv4(Ipv4SocketAddress {
        port: 0,
        address: (0, 0, 0, 0),
    })
}

impl types::Guest for Component {
    type TcpSocket = Tcp;
    type UdpSocket = Udp;
}

/// A socket that remembers its addresses, and succeeds at everything else.
struct Tcp {
    family: IpAddressFamily,
    local: Cell<Option<IpSocketAddress>>,
    remote: Cell<Option<IpSocketAddress>>,
    listening: Cell<bool>,
}

impl GuestTcpSocket for Tcp {
    fn create(address_family: IpAddressFamily) -> Result<TcpSocket, ErrorCode> {
        Ok(TcpSocket::new(Tcp {
            family: address_family,
            local: Cell::new(None),
            remote: Cell::new(None),
            listening: Cell::new(false),
        }))
    }

    fn bind(&self, local_address: IpSocketAddress) -> Result<(), ErrorCode> {
        self.local.set(Some(local_address));
        Ok(())
    }

    async fn connect(&self, remote_address: IpSocketAddress) -> Result<(), ErrorCode> {
        self.remote.set(Some(remote_address));
        Ok(())
    }

    /// No connections arrive.
    fn listen(&self) -> Result<StreamReader<TcpSocket>, ErrorCode> {
        self.listening.set(true);
        let (_, connections) = wit_stream::new();
        Ok(connections)
    }

    /// The data is dropped.
    fn send(&self, _data: StreamReader<u8>) -> FutureReader<Result<(), ErrorCode>> {
        let (_, sent) = wit_future::new(|| Ok(()));
        sent
    }

    /// No data arrives.
    fn receive(&self) -> (StreamReader<u8>, FutureReader<Result<(), ErrorCode>>) {
        let (_, data) = wit_stream::new();
        let (_, received) = wit_future::new(|| Ok(()));
        (data, received)
    }

    fn get_local_address(&self) -> Result<IpSocketAddress, ErrorCode> {
        Ok(self.local.get().unwrap_or_else(unspecified))
    }

    fn get_remote_address(&self) -> Result<IpSocketAddress, ErrorCode> {
        self.remote.get().ok_or(ErrorCode::InvalidState)
    }

    fn get_is_listening(&self) -> bool {
        self.listening.get()
    }

    fn get_address_family(&self) -> IpAddressFamily {
        self.family
    }

    fn set_listen_backlog_size(&self, _value: u64) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_keep_alive_enabled(&self) -> Result<bool, ErrorCode> {
        Ok(false)
    }

    fn set_keep_alive_enabled(&self, _value: bool) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_keep_alive_idle_time(&self) -> Result<Duration, ErrorCode> {
        Ok(0)
    }

    fn set_keep_alive_idle_time(&self, _value: Duration) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_keep_alive_interval(&self) -> Result<Duration, ErrorCode> {
        Ok(0)
    }

    fn set_keep_alive_interval(&self, _value: Duration) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_keep_alive_count(&self) -> Result<u32, ErrorCode> {
        Ok(0)
    }

    fn set_keep_alive_count(&self, _value: u32) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_hop_limit(&self) -> Result<u8, ErrorCode> {
        Ok(64)
    }

    fn set_hop_limit(&self, _value: u8) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_receive_buffer_size(&self) -> Result<u64, ErrorCode> {
        Ok(0)
    }

    fn set_receive_buffer_size(&self, _value: u64) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_send_buffer_size(&self) -> Result<u64, ErrorCode> {
        Ok(0)
    }

    fn set_send_buffer_size(&self, _value: u64) -> Result<(), ErrorCode> {
        Ok(())
    }
}

/// A socket that remembers its addresses, and succeeds at everything else.
struct Udp {
    family: IpAddressFamily,
    local: Cell<Option<IpSocketAddress>>,
    remote: RefCell<Option<IpSocketAddress>>,
}

impl GuestUdpSocket for Udp {
    fn create(address_family: IpAddressFamily) -> Result<UdpSocket, ErrorCode> {
        Ok(UdpSocket::new(Udp {
            family: address_family,
            local: Cell::new(None),
            remote: RefCell::new(None),
        }))
    }

    fn bind(&self, local_address: IpSocketAddress) -> Result<(), ErrorCode> {
        self.local.set(Some(local_address));
        Ok(())
    }

    fn connect(&self, remote_address: IpSocketAddress) -> Result<(), ErrorCode> {
        self.remote.replace(Some(remote_address));
        Ok(())
    }

    fn disconnect(&self) -> Result<(), ErrorCode> {
        self.remote.replace(None);
        Ok(())
    }

    /// The datagram is dropped.
    async fn send(
        &self,
        _data: Vec<u8>,
        _remote_address: Option<IpSocketAddress>,
    ) -> Result<(), ErrorCode> {
        Ok(())
    }

    /// No datagram arrives, there is no network to wait on.
    async fn receive(&self) -> Result<(Vec<u8>, IpSocketAddress), ErrorCode> {
        Err(ErrorCode::Other(Some(
            "no datagrams without a network".to_string(),
        )))
    }

    fn get_local_address(&self) -> Result<IpSocketAddress, ErrorCode> {
        Ok(self.local.get().unwrap_or_else(unspecified))
    }

    fn get_remote_address(&self) -> Result<IpSocketAddress, ErrorCode> {
        self.remote.borrow().ok_or(ErrorCode::InvalidState)
    }

    fn get_address_family(&self) -> IpAddressFamily {
        self.family
    }

    fn get_unicast_hop_limit(&self) -> Result<u8, ErrorCode> {
        Ok(64)
    }

    fn set_unicast_hop_limit(&self, _value: u8) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_receive_buffer_size(&self) -> Result<u64, ErrorCode> {
        Ok(0)
    }

    fn set_receive_buffer_size(&self, _value: u64) -> Result<(), ErrorCode> {
        Ok(())
    }

    fn get_send_buffer_size(&self) -> Result<u64, ErrorCode> {
        Ok(0)
    }

    fn set_send_buffer_size(&self, _value: u64) -> Result<(), ErrorCode> {
        Ok(())
    }
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/test-stub-sockets/`
    path: "../wit",
    world: "test-stub-sockets",
    generate_all,
});

export!(Component);
