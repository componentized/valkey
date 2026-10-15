# Valkey Components <!-- omit in toc -->

A WASM component client for Valkey (and Redis).

- [Components](#components)
- [Network access](#network-access)
- [Build](#build)
- [Test](#test)
- [Run](#run)
  - [Samples](#samples)
- [Community](#community)
  - [Code of Conduct](#code-of-conduct)
  - [Communication](#communication)
  - [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)
- [License](#license)

## Components

The `componentized:valkey` WIT package, in [`wit`](./wit), separates the connection to a server from the transport carrying it, and from the commands sent over it. Connections always speak RESP3.

| Component                                                          |                                                                                                                                                                                          |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`connection`](./components/connection/)                           | the mechanics of a connection: pairs commands with replies, keeps push messages apart, establishes it with `HELLO 3`, optionally keeps it alive                                          |
| [`tcp`](./components/tcp/)                                         | connections over wasi:sockets TCP                                                                                                                                                        |
| [`tcp-connector`](./components/tcp-connector/)                     | connections over TCP to the server configured with wasi:config, for components that need a connection without a say in how it is made                                                    |
| [`tcp-connector-constants`](./components/tcp-connector-constants/) | the keys of the `tcp-connector` config and their defaults, for components that derive their config from it                                                                               |
| [`tcp-connector-latch`](./components/tcp-connector-latch/)         | a componentized:sockets latch, for a gate, limiting sockets to the server in the `tcp-connector` config                                                                                  |
| [`commands`](./components/commands/)                               | the commands, an interface for each of Valkey's ACL categories, e.g. `hash-commands`, each also available as a component of its own, e.g. [`hash-commands`](./components/hash-commands/) |
| [`as-keyvalue`](./components/as-keyvalue/)                         | wasmcloud:keyvalue, backed by Valkey hashes                                                                                                                                              |
| [`client`](./components/client/)                                   | everything above, composed into one component                                                                                                                                            |
| [`cli`](./components/cli/)                                         | a command line client, behind a gate limiting it to the server it is configured for                                                                                                      |

A composition picks the transport, e.g. `tcp` and `tcp-connector`, other transports can provide connections to the same commands. The [`*.wac`](./components/client/client.wac) files show how the components are composed.

## Network access

The network access of a client can be limited with a [componentized:sockets](https://github.com/componentized/sockets) gate. A gate imports `wasi:sockets` and exports it to the components it is put in front of, asking a latch before each socket operation, and denying the operation if the latch does.

A critical aspect of gates and latches is how they protect a slice of the composed component. Behavior is selectively applied to parts of an application by wrapping that portion of the composition with a gate. For example, network access to a Valkey server can be segregated from network access to other services. Host based controls apply equally to all parts of the component. A gate can never increase privilege, only restrict otherwise granted capabilities.

The [`tcp-connector-latch`](./components/tcp-connector-latch/) allows normal access to the Valkey server in the `tcp-connector` config, and denies every other socket. Suppose a client connects to a malicious server that is able to trigger a remote code execution within the client. The gate prevents the client from communicating with any other network resources, dramatically limiting the blast radius of exploitation. The [`cli`](./components/cli/) is always behind a gate with this latch.

Many other latches are available in the [`componentized/sockets`](https://github.com/componentized/sockets) repo, or a custom latch can be implemented by exporting the `componentized:sockets/latch` interface.

## Build

A [dev container](https://containers.dev) is available that contains the necessary tools and configuration out of the box.

Prereqs:
- a rust toolchain
- [`cargo-binstall`](https://github.com/cargo-bins/cargo-binstall), optional, to download prebuilt tools instead of building them
- Docker, to run the integration tests `make test-valkey`

```sh
make components
```

The build creates each component in [`components`](./components) into `target/components`, e.g. the cli at `target/components/cli/cli.wasm`, along with `target/components/interface.wasm`, the `componentized:valkey` WIT package. Each component is also built with debug info, e.g. `target/components/cli/cli.debug.wasm`.

Every component is published, except those with these prefixes:

| Prefix      |                                                                                    |
| ----------- | ---------------------------------------------------------------------------------- |
| `dep-`      | pulled from elsewhere, named for its source, e.g. `dep-componentized-sockets-gate` |
| `internal-` | built here, for use by other components, e.g. `internal-cli` composed into `cli`   |
| `test-`     | tests, e.g. `test-commands`                                                        |

The cli tools the build uses, [`constants`](https://github.com/componentized/constants), [`wasm-tools`](https://github.com/bytecodealliance/wasm-tools), [`wac`](https://github.com/bytecodealliance/wac), [`wasmtime`](https://github.com/bytecodealliance/wasmtime) and [`wkg`](https://github.com/bytecodealliance/wasm-pkg-tools), are pinned in [`tools/Cargo.toml`](./tools/Cargo.toml) and installed into `target/tools/<platform>`, e.g. `target/tools/aarch64-apple-darwin`, as needed, or ahead of time with `make tools`. Dependabot bumps the pinned versions.

## Test

```sh
make test
```

Tests the components without a Valkey server, playing the server themselves, then runs the cli against a Valkey server on `localhost:6379`. The tests against a Valkey server, ignored by `cargo test`, run with:

```sh
make test-valkey
```

## Run

Prereqs:
- build the components (see above)
- a `wasi:cli/command` compatible runtime (like [wasmtime](https://github.com/bytecodealliance/wasmtime))
- access to a running [Valkey](https://valkey.io) server

```sh
wasmtime run -Sinherit-network -Sallow-ip-name-lookup -Sconfig -Wcomponent-model-map -Wcomponent-model-implements -Wcomponent-model-threading \
  target/components/cli/cli.wasm keys '*'
```

The arguments are sent as a command and the reply is printed. A command without a reply, like `SUBSCRIBE`, prints the push messages that arrive until interrupted or the connection closes.

- the server is configured with wasi:config variables, e.g. `-Sconfig-var=hostname=valkey.example.com -Sconfig-var=port=6380`, see [`tcp-connector`](./components/tcp-connector/) for the keys and their defaults
- `-Sallow-ip-name-lookup` is required, the `tcp` component looks up the `hostname`, an IP address too, which wasi:sockets returns as is

To aid incremental development a make target is available to rebuild and run the CLI:

```sh
make run cmd="hello"
```

### Samples

- [`http-incrementor`](./components/sample-http-incrementor/)

## Community

### Code of Conduct

The Componentized project follow the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md). In short, be kind and treat others with respect.

### Communication

General discussion and questions about the project can occur in the project's [GitHub discussions](https://github.com/orgs/componentized/discussions).

### Contributing

The Componentized project team welcomes contributions from the community. A contributor license agreement (CLA) is not required. You own full rights to your contribution and agree to license the work to the community under the Apache License v2.0, via a [Developer Certificate of Origin (DCO)](https://developercertificate.org). For more detailed information, refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## Acknowledgements

This project was conceived in discussion between [Mark Fisher](https://github.com/markfisher) and [Scott Andrews](https://github.com/scothis).

## License

Apache License v2.0: see [LICENSE](./LICENSE) for details.
