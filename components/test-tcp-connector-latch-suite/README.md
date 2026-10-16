# test-tcp-connector-latch-suite

The tests of the [`tcp-connector-latch`](../tcp-connector-latch/), run inside a component behind a gate with the latch.

Each test looks up names, connects and binds with wasi:sockets, expecting each operation either allowed or denied with `access-denied`. The tests are named for the connector config they expect, `by hostname`, `by address` and `by default`. Composed as [`test-tcp-connector-latch`](../test-tcp-connector-latch/).
