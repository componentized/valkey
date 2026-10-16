# test-stub-sockets

`wasi:sockets` without a network, for tests of what a gate allows.

Every socket operation succeeds, so an error can only come from a gate in front of it. Names resolve to fixed addresses: an IP address to itself, `localhost` to `127.0.0.1` and `::1`, any other name to `192.0.2.1`. Connections carry no data and listening sockets accept none. See [`test-tcp-connector-latch`](../test-tcp-connector-latch/).
