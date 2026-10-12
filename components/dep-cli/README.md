# dep-cli

The Valkey CLI before the client components are composed in, see [`cli`](../cli/).

Sends its arguments as a command over a connection from a `connector` and prints the reply. A command without a reply, like `SUBSCRIBE`, prints the push messages that arrive until interrupted or the connection closes.

`UNSUBSCRIBE`, `PUNSUBSCRIBE` and `SUNSUBSCRIBE` print their confirmations and exit. `MONITOR` prints its feed until interrupted or the connection closes.

Without arguments, each line of stdin is sent as a command over one connection, kept open until stdin closes, printing replies and push messages as they arrive. Words are split on whitespace, quotes keep a word with whitespace together, e.g. `set greeting "hello world"`.
