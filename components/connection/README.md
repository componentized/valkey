# connection

The mechanics of a connection to a Valkey server, independent of the transport carrying it.

Pairs each command with its reply, keeps push messages apart for any number of readers, establishes the connection with `HELLO 3` and optionally keeps it alive with `PING`. A transport, e.g. [`tcp`](../tcp/), carries the RESP values to and from the server.
