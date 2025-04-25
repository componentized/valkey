# Valkey Components <!-- omit in toc -->

A WASM component client for Valkey (and Redis).

- [Build](#build)
- [Run](#run)
  - [Samples](#samples)
- [Community](#community)
  - [Code of Conduct](#code-of-conduct)
  - [Communication](#communication)
  - [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)
- [License](#license)


## Build

Prereqs:
- a rust toolchain with a recent nightly (`rustup toolchain install nightly`)
- [`cargo component`](https://github.com/bytecodealliance/cargo-component)
- [`wac`](https://github.com/bytecodealliance/wac)
- [`wkg`](https://github.com/bytecodealliance/wasm-pkg-tools)

```sh
make components
```

## Run

Prereqs:
- build the components (see above)
- a `wasi:cli/command` compatible runtime (like [wasmtime](https://github.com/bytecodealliance/wasmtime))
- access to a running [Valkey](https://valkey.io) server

```sh
wasmtime run -Sinherit-network -Sallow-ip-name-lookup lib/cli.wasm keys '*'
```

- use `--host` to specify a host other than `127.0.0.1`
- use `--port` to specify a port other than `6379`
- `-Sallow-ip-name-lookup` is only required if a hostname is used for the connection instead of an IP address.

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
