# internal-tcp-connector-egress-config

The config of a [`latch-cidr-egress`](https://github.com/componentized/sockets/tree/main/components/latch-cidr-egress), from the config of a [`tcp-connector`](../tcp-connector/): outbound traffic is denied, except to the server's `port`, on any address.

| Connector config | Latch config |
| --- | --- |
| `port=6380` | `default=deny`, `defer=:6380` |
| unset | `default=deny`, `defer=:6379`, the connector's default |
| not a port number | `default=deny` |

See [`tcp-connector-latch`](../tcp-connector-latch/).
