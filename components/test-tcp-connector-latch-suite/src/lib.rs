//! The tests of the `tcp-connector-latch`, run inside a component behind a gate with the latch,
//! over `test-stub-sockets`, wasi:sockets without a network. Every socket operation of the stub
//! succeeds, so a test that fails with `access-denied` was denied by the gate, and one that
//! succeeds was allowed.
//!
//! The tests are named for the connector config they expect, set by the test harness:
//!
//! - `by hostname ...`: `hostname=valkey.test`, `port=6380`
//! - `by address ...`: `hostname=127.0.0.1`, `port=6380`
//! - `by default ...`: neither, the connector's defaults, `127.0.0.1` and `6379`
//!
//! The stub resolves names other than `localhost` to `192.0.2.1`.

use exports::componentized::valkey_components::test_suite::Guest;
use wasi::sockets::ip_name_lookup::{ErrorCode as LookupErrorCode, resolve_addresses};
use wasi::sockets::types::{
    ErrorCode, IpAddress, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress,
    TcpSocket, UdpSocket,
};

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
    "by hostname looks up the hostname" => hostname_looks_up_hostname,
    "by hostname connects to its addresses on the port" => hostname_connects,
    "by hostname denies another port" => hostname_denies_another_port,
    "by hostname denies looking up another name" => hostname_denies_another_name,
    "by hostname denies looking up its address" => hostname_denies_looking_up_its_address,
    "by hostname denies an address no lookup returned" => hostname_denies_address_without_lookup,
    "by hostname denies binding" => denies_binding,
    "by hostname denies udp" => denies_udp,
    "by address looks up the address" => address_looks_up_address,
    "by address connects to it on the port" => address_connects,
    "by address denies another port" => address_denies_another_port,
    "by address denies looking up a name" => address_denies_a_name,
    "by address denies another address" => address_denies_another_address,
    "by default connects to the default address and port" => default_connects,
    "by default denies another port" => default_denies_another_port,
}

impl Guest for Suite {
    fn tests() -> Vec<String> {
        TESTS.iter().map(|name| name.to_string()).collect()
    }

    async fn run(name: String) -> Result<(), String> {
        run_test(&name).await
    }
}

const HOSTNAME: &str = "valkey.test";
/// The address the stub resolves names to.
const HOSTNAME_ADDRESS: IpAddress = IpAddress::Ipv4((192, 0, 2, 1));
const LOOPBACK: IpAddress = IpAddress::Ipv4((127, 0, 0, 1));
const PORT: u16 = 6380;
const DEFAULT_PORT: u16 = 6379;

async fn lookup(name: &str) -> Result<Vec<IpAddress>, LookupErrorCode> {
    resolve_addresses(name.to_string()).await
}

async fn connect(address: IpAddress, port: u16) -> Result<(), ErrorCode> {
    let address = socket_address(address, port);
    let family = match address {
        IpSocketAddress::Ipv4(_) => IpAddressFamily::Ipv4,
        IpSocketAddress::Ipv6(_) => IpAddressFamily::Ipv6,
    };
    TcpSocket::create(family)?.connect(address).await
}

fn socket_address(address: IpAddress, port: u16) -> IpSocketAddress {
    match address {
        IpAddress::Ipv4(address) => IpSocketAddress::Ipv4(Ipv4SocketAddress { port, address }),
        IpAddress::Ipv6(address) => IpSocketAddress::Ipv6(Ipv6SocketAddress {
            port,
            flow_info: 0,
            address,
            scope_id: 0,
        }),
    }
}

/// Fails unless the lookup was allowed, returning the addresses.
async fn allowed_lookup(name: &str) -> Result<Vec<IpAddress>, String> {
    lookup(name)
        .await
        .map_err(|error| format!("looking up {name} failed with {error:?}, expected it allowed"))
}

/// Fails unless the gate denied the lookup.
async fn denied_lookup(name: &str) -> Result<(), String> {
    match lookup(name).await {
        Err(LookupErrorCode::AccessDenied) => Ok(()),
        Err(error) => Err(format!(
            "looking up {name} failed with {error:?}, expected access-denied"
        )),
        Ok(addresses) => Err(format!(
            "looking up {name} returned {addresses:?}, expected access-denied"
        )),
    }
}

/// Fails unless the connection was allowed.
async fn allowed_connect(address: IpAddress, port: u16) -> Result<(), String> {
    connect(address, port).await.map_err(|error| {
        format!("connecting to {address:?}:{port} failed with {error:?}, expected it allowed")
    })
}

/// Fails unless the gate denied the connection.
async fn denied_connect(address: IpAddress, port: u16) -> Result<(), String> {
    match connect(address, port).await {
        Err(ErrorCode::AccessDenied) => Ok(()),
        Err(error) => Err(format!(
            "connecting to {address:?}:{port} failed with {error:?}, expected access-denied"
        )),
        Ok(()) => Err(format!(
            "connected to {address:?}:{port}, expected access-denied"
        )),
    }
}

async fn hostname_looks_up_hostname() -> Result<(), String> {
    let addresses = allowed_lookup(HOSTNAME).await?;
    if addresses != [HOSTNAME_ADDRESS] {
        return Err(format!("looking up {HOSTNAME} returned {addresses:?}"));
    }
    Ok(())
}

async fn hostname_connects() -> Result<(), String> {
    for address in allowed_lookup(HOSTNAME).await? {
        allowed_connect(address, PORT).await?;
    }
    Ok(())
}

async fn hostname_denies_another_port() -> Result<(), String> {
    for address in allowed_lookup(HOSTNAME).await? {
        denied_connect(address, DEFAULT_PORT).await?;
        denied_connect(address, 22).await?;
    }
    Ok(())
}

async fn hostname_denies_another_name() -> Result<(), String> {
    denied_lookup("example.com").await?;
    denied_lookup("localhost").await?;
    // a pattern is not a name
    denied_lookup("*.test").await
}

async fn hostname_denies_looking_up_its_address() -> Result<(), String> {
    denied_lookup("192.0.2.1").await
}

/// Connecting by hand, to an address no allowed lookup returned.
async fn hostname_denies_address_without_lookup() -> Result<(), String> {
    denied_connect(IpAddress::Ipv4((198, 51, 100, 1)), PORT).await?;
    denied_connect(LOOPBACK, PORT).await
}

async fn denies_binding() -> Result<(), String> {
    let socket = TcpSocket::create(IpAddressFamily::Ipv4)
        .map_err(|error| format!("creating a socket failed with {error:?}"))?;
    let local = socket_address(IpAddress::Ipv4((0, 0, 0, 0)), PORT);
    match socket.bind(local) {
        Err(ErrorCode::AccessDenied) => Ok(()),
        Err(error) => Err(format!(
            "binding failed with {error:?}, expected access-denied"
        )),
        Ok(()) => Err("bound, expected access-denied".to_string()),
    }
}

async fn denies_udp() -> Result<(), String> {
    match UdpSocket::create(IpAddressFamily::Ipv4) {
        Err(ErrorCode::AccessDenied) => Ok(()),
        Err(error) => Err(format!(
            "creating a udp socket failed with {error:?}, expected access-denied"
        )),
        Ok(_) => Err("created a udp socket, expected access-denied".to_string()),
    }
}

async fn address_looks_up_address() -> Result<(), String> {
    let addresses = allowed_lookup("127.0.0.1").await?;
    if addresses != [LOOPBACK] {
        return Err(format!("looking up 127.0.0.1 returned {addresses:?}"));
    }
    Ok(())
}

async fn address_connects() -> Result<(), String> {
    for address in allowed_lookup("127.0.0.1").await? {
        allowed_connect(address, PORT).await?;
    }
    Ok(())
}

async fn address_denies_another_port() -> Result<(), String> {
    allowed_lookup("127.0.0.1").await?;
    denied_connect(LOOPBACK, DEFAULT_PORT).await
}

async fn address_denies_a_name() -> Result<(), String> {
    // localhost resolves to the same address, but is not the configured hostname
    denied_lookup("localhost").await?;
    denied_lookup(HOSTNAME).await
}

async fn address_denies_another_address() -> Result<(), String> {
    allowed_lookup("127.0.0.1").await?;
    denied_connect(IpAddress::Ipv4((127, 0, 0, 2)), PORT).await?;
    denied_connect(IpAddress::Ipv6((0, 0, 0, 0, 0, 0, 0, 1)), PORT).await
}

async fn default_connects() -> Result<(), String> {
    for address in allowed_lookup("127.0.0.1").await? {
        allowed_connect(address, DEFAULT_PORT).await?;
    }
    Ok(())
}

async fn default_denies_another_port() -> Result<(), String> {
    allowed_lookup("127.0.0.1").await?;
    denied_connect(LOOPBACK, PORT).await
}

wit_bindgen::generate!({
    // `path` is relative to the manifest, `components/test-tcp-connector-latch-suite/`
    path: "../wit",
    world: "test-tcp-connector-latch-suite",
    // to compare the addresses a lookup returned
    additional_derives: [PartialEq],
    generate_all,
});

export!(Suite);
