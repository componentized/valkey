# test-tcp-connector-latch

The tests of the [`tcp-connector-latch`](../tcp-connector-latch/), behind a [`gate`](../dep-componentized-sockets-gate/), over [`test-stub-sockets`](../test-stub-sockets/), wasi:sockets without a network.

[`test-tcp-connector-latch-suite`](../test-tcp-connector-latch-suite/) gets its wasi:sockets from the gate. Every operation of the stub succeeds, so an operation that fails with `access-denied` was denied by the latch, and one that succeeds was allowed, with no server, DNS or firewall in the way.

[`tests/latch.rs`](./tests/latch.rs) runs the suite with the test harness, once for each connector config, each in a new instance whose latch has seen no lookups:

| Config | Allowed | Denied |
| --- | --- | --- |
| `hostname=valkey.test`, `port=6380` | looking up `valkey.test`, connecting to its address on `6380` | other ports, other names, looking up its address, addresses no lookup returned, binding, UDP |
| `hostname=127.0.0.1`, `port=6380` | looking up `127.0.0.1`, connecting to it on `6380` | other ports, names, even `localhost`, other addresses |
| neither, the connector's defaults | looking up `127.0.0.1`, connecting to it on `6379` | other ports |

Run by `cargo test`, and `make test`.
