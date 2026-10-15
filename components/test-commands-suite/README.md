# test-commands-suite

The tests of the [`commands`](../commands/) component, a component that plays the server over the streams of a [`connection`](../connection/), checking the command each function sends and how it converts the reply. Composed as [`test-commands`](../test-commands/), and run by `cargo test -p commands`.

Tests named `with-valkey ...` run against a Valkey server on localhost:6379, with `make test-valkey`.
