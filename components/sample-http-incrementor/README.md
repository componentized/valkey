# Sample HTTP incrementor

Sample component that exposes `wasi:http` incrementing a counter based on the http path.


Compose with the valkey client:

```sh
wac plug sample-http-incrementor.wasm --plug client.wasm -o valkey-http-incrementor.wasm
```

Start a Valkey server:

```sh
docker run --rm -p 6379:6379 valkey/valkey:9
```

Start the incrementor:

```sh
wasmtime serve -Sconfig -Sinherit-network -Sallow-ip-name-lookup -Scli valkey-http-incrementor.wasm
```

The client connects to `127.0.0.1:6379` unless configured otherwise, e.g. `-Sconfig-var=hostname=valkey.example.com -Sconfig-var=port=6380`, see [`tcp-connector`](../tcp-connector/) for the settings, and [`as-keyvalue`](../as-keyvalue/) for `key-prefix`.

Increment all the things:

```sh
curl http://localhost:8080/hello
```
```txt
1
```


```sh
curl http://localhost:8080/hello
```
```txt
2
```
