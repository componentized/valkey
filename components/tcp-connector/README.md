# tcp-connector

A connector for connections over TCP, to the server configured with `componentized:secrets@0.1.1`, every key is a secret, a secret that is not found is unset.

| Key | Default | |
| --- | --- | --- |
| `hostname` | `127.0.0.1` | hostname or IP address of the server |
| `port` | `6379` | port of the server |
| `username` | `default` | user to authenticate as, with `password` |
| `password` | | authenticates when set |
| `client-name` | | name of the connection, see `CLIENT SETNAME` |
| `keep-alive-interval-ms` | | sends `PING` after this many milliseconds idle, disabled when unset or `0` |

A host without `componentized:secrets`, like wasmtime, can compose [`dep-componentized-secrets-from-config`](../dep-componentized-secrets-from-config/) to provide them from `wasi:config`, as the [`cli`](../cli/) does.

The keys and their defaults are constants, from [`tcp-connector-constants`](../tcp-connector-constants/), for the components that derive their config from the connector's.

Compose with [`tcp`](../tcp/), e.g. to provide connections to [`as-keyvalue`](../as-keyvalue/).
