# tcp

Connections to a Valkey server over `wasi:sockets` TCP.

Frames the bytes the socket carries into RESP values in each direction, and establishes a [`connection`](../connection/) over them. Compose with `connection`.

The host is always looked up with wasi:sockets/ip-name-lookup, an IP address too, which wasi:sockets returns as is. So each address a socket connects to was returned by a lookup, e.g. for the [`tcp-connector-latch`](../tcp-connector-latch/).
