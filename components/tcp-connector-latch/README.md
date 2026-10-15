# tcp-connector-latch

A `componentized:sockets` latch limiting network access to the Valkey server in the `tcp-connector` config, denying every other socket.

A [componentized:sockets](https://github.com/componentized/sockets) [`gate`](../dep-componentized-sockets-gate/) asks the latch before each socket operation, and denies the operation if the latch does. The connector's config is read with wasi:config, see [`tcp-connector`](../tcp-connector/).

Put the components that connect with the `tcp-connector` behind a gate with this latch, e.g. the [`tcp`](../tcp/) component. They can connect to the configured `hostname`, on the configured `port`, over TCP, and nothing else.

The latch is a [`latch-n5`](../dep-componentized-sockets-latch-n5/), which denies an operation if any of these latches denies it:

| Latch                                                                                                                | Denies                                                                                                             |
| -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| [`latch-deny-udp`](../dep-componentized-sockets-latch-deny-udp/)                                                     | all UDP, Valkey only uses TCP                                                                                      |
| [`latch-deny-bind`](../dep-componentized-sockets-latch-deny-bind/)                                                   | binding a socket to a local address, e.g. to listen for incoming connections, the client initiates all connections |
| [`latch-deny-connect-unless-lookup-address`](../dep-componentized-sockets-latch-deny-connect-unless-lookup-address/) | connections to an IP address, unless an allowed lookup returned it                                                 |
| [`latch-ip-name-lookup-glob`](../dep-componentized-sockets-latch-ip-name-lookup-glob/)                               | name lookups, except for the server's runtime configured `hostname`                                                |
| [`latch-cidr-egress`](../dep-componentized-sockets-latch-cidr-egress/)                                               | connections, except to the server's runtime configured `port`                                                      |

The lookup and egress latches are configured from the connector's own config, with the connector's keys and defaults from [`tcp-connector-constants`](../tcp-connector-constants/):

- [`internal-tcp-connector-lookup-config`](../internal-tcp-connector-lookup-config/): `hostname=valkey.example.com` becomes `default=deny` and `defer=valkey.example.com`
- [`internal-tcp-connector-egress-config`](../internal-tcp-connector-egress-config/): `port=6380` becomes `default=deny` and `defer=:6380`, any address on that port

The server is configured in one place, and the latch follows it.

A `hostname` that is an IP address, like the connector's default of `127.0.0.1`, is allowed too, to that address only. The `tcp` component looks up an IP address as well, wasi:sockets returns it as is, so the lookup of `127.0.0.1` is allowed and vouches for the address `127.0.0.1`. Another address, or a name that resolves to the same address, is still denied.

The [`cli`](../cli/) is behind a gate with this latch, see also its test, [`test-tcp-connector-latch`](../test-tcp-connector-latch/).

## Interfaces

Imports:

- `wasi:config/store@0.2.0-rc.1`: the connector's config, `hostname` and `port`
- `wasi:logging/logging@0.1.0-draft`: logs an invalid config
- `wasi:sockets/types@0.3.0`: the socket types of the operations being authorized

Exports:

- `componentized:sockets/latch@0.1.0`: the latch, for a gate

The sockets components need the component model's `implements` and `threading` features, `-Wcomponent-model-implements -Wcomponent-model-threading`.

## Custom latches

The latches an environment needs and their config can differ. Attempting to use this latch without establishing the connection through the `tcp-connector` will likely fail.

Client that need to open their own connections should build a custom latch from the same pieces, see [`tcp-connector-latch.wac`](./tcp-connector-latch.wac), and the other [latches](https://github.com/componentized/sockets#latches), or implement one by exporting the `componentized:sockets/latch` interface. For example:

- `latch-cidr-egress` can also limit the addresses of the server's network, e.g. `defer=10.0.0.0/8:6379`.
- `latch-dry-run` and `latch-trace` let you roll out a latch before enforcing it.
