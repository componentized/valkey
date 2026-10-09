# client

Client for interacting with a Valkey server, as componentized:valkey and wasmcloud:keyvalue.

> [!NOTE]
> `wasi:keyvalue` is currently unsupportable with an async based client. It will be restored when either it updates it's own API to be async, or the component model is fully able to make async calls from sync functions.

Composes [`resp`](../resp/), [`connection`](../connection/), [`tcp`](../tcp/), [`tcp-connector`](../tcp-connector/), [`commands`](../commands/) and [`as-keyvalue`](../as-keyvalue/), exporting each of their interfaces.
