# as-keyvalue

Adapter to back wasmcloud:keyvalue with the componentized:valkey client.

> [!NOTE]
> `wasi:keyvalue` is currently unsupportable with an async based client. It will be restored when either it updates it's own API to be async, or the component model is fully able to make async calls from sync functions.
