# internal-tcp-connector-lookup-config

The config of a [`latch-ip-name-lookup-glob`](https://github.com/componentized/sockets/tree/main/components/latch-ip-name-lookup-glob), from the config of a [`tcp-connector`](../tcp-connector/): name lookups are denied, except for the server's `hostname`.

| Connector config | Latch config |
| --- | --- |
| `hostname=valkey.example.com` | `default=deny`, `defer=valkey.example.com` |
| `hostname=127.0.0.1`, an IP address | `default=deny`, `defer=127.0.0.1` |
| unset | `default=deny`, `defer=127.0.0.1`, the connector's default |
| `hostname=*.example.com`, with glob characters | `default=deny` |

The [`tcp`](../tcp/) component looks up an IP address too, wasi:sockets returns it as is, so the address is allowed like a resolved hostname. See [`tcp-connector-latch`](../tcp-connector-latch/).
