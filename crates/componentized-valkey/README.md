# componentized-valkey

Rust support for the `componentized:valkey` WIT package: the shared types, a RESP codec, and the
conversions between Rust values and commands and replies.

## License

Licensed under the Apache License, Version 2.0, see [LICENSE](./LICENSE).

The RESP codec, `src/codec.rs`, is adapted from https://github.com/iorust/resp/tree/v1.0.3, used
under the MIT license, see [NOTICE](./NOTICE).
