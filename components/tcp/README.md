# tcp

Connections to a Valkey server over wasi:sockets TCP.

Frames the bytes the socket carries into RESP values in each direction, and establishes a [`connection`](../connection/) over them. Compose with `connection` and [`resp`](../resp/).
