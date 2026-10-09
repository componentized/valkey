# tcp-connector

A connector for connections over TCP, to the server configured with wasi:config.

| Key | Default | |
| --- | --- | --- |
| `hostname` | `127.0.0.1` | hostname or IP address of the server |
| `port` | `6379` | port of the server |
| `username` | `default` | user to authenticate as, with `password` |
| `password` | | authenticates when set |
| `client-name` | | name of the connection, see `CLIENT SETNAME` |
| `keep-alive-interval-ms` | | sends `PING` after this many milliseconds idle, disabled when unset or `0` |

Compose with [`tcp`](../tcp/), e.g. to provide connections to [`as-keyvalue`](../as-keyvalue/).
