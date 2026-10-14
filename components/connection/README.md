# connection

The mechanics of a connection to a Valkey server, independent of the transport carrying it.

Pairs each command with its reply, keeps push messages apart for any number of readers, owns pub/sub subscriptions, runs transactions with `WATCH`, `MULTI` and `EXEC`, establishes the connection with `HELLO 3`, resets it, and optionally keeps it alive with `PING`. A transport, e.g. [`tcp`](../tcp/), carries the RESP values to and from the server.

[`connection-minimal`](../connection-minimal/) is a smaller build without pub/sub, with the same interface.
