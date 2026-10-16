# Valkey CLI

A command line client for Valkey, sending its arguments as a command and printing the reply.

Composed of [`internal-cli`](../internal-cli/) and a [`tcp-connector`](../tcp-connector/) configured with wasi:config.

Every connection the cli makes is through the connector, so the cli is behind a [componentized:sockets](https://github.com/componentized/sockets) [`gate`](../dep-componentized-sockets-gate/) with the [`tcp-connector-latch`](../tcp-connector-latch/). It can only reach the server it is configured for, see [network access](../../README.md#network-access). The gate and the latch log denials with [`to-stderr`](../dep-componentized-logging-to-stderr/), keeping them out of the cli's output.

## Run

Start a Valkey server:

```sh
docker run --rm -p 6379:6379 valkey/valkey:9
```

```sh
wasmtime run -Sinherit-network -Sallow-ip-name-lookup -Sconfig -Wcomponent-model-map -Wcomponent-model-implements -Wcomponent-model-threading \
  target/components/cli/cli.wasm ping
```
```txt
PONG
```

The server is configured with wasi:config variables, e.g. `-Sconfig-var=hostname=localhost`, see [`tcp-connector`](../tcp-connector/) for the keys and their defaults. The latch follows the same config.

The gate needs the component model's `implements` and `threading` features, `-Wcomponent-model-implements -Wcomponent-model-threading`.

See [`cli.wac`](./cli.wac) for the composition.
