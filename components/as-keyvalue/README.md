# as-keyvalue

Adapter to back `wasmcloud:keyvalue` with the `componentized:valkey` client.

> [!NOTE]
> `wasi:keyvalue` is currently unsupportable with an async based client. It will be restored when either it updates it's own API to be async, or the component model is fully able to make async calls from sync functions.

Each bucket is a Valkey hash, its keys are the hash's fields. The hash is named for the bucket's identifier, after an optional prefix:

| Key | Default | |
| --- | --- | --- |
| `key-prefix` | | prefix of the hash for each bucket, e.g. `app:` stores the bucket `users` in the hash `app:users` |

Opening a bucket establishes a new connection with the imported `connector`, which decides the transport and the server, e.g. [`tcp-connector`](../tcp-connector/) with its own configuration. A connector that fails to connect makes the store unavailable. [`client`](../client/) composes it with `tcp-connector`.

- values are stored as strings, a value must be UTF-8
- a ttl expires the key with `HEXPIRE`, which needs Valkey 9 or later and has whole second precision, a ttl is rounded up to whole seconds
- keys are listed with `HSCAN`, a prefix matches literally
- compare and swap, `wasmcloud:keyvalue/cas`, is not supported
