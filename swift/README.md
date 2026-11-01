# Bolthole for Swift

A Swift client for [Bolthole](../README.md). It runs on iOS, macOS, tvOS,
watchOS and visionOS on CryptoKit, and on Linux on swift-crypto.

It covers connect codes, the Noise_NNpsk0 handshake, `mux/1`, the `http`,
`ws` and `tcp` stream kinds, and races the relay against direct paths. It is
checked byte for byte against [spec/test-vectors.json](../spec/test-vectors.json)
and live against the Go relay and agent.

## Install

```swift
// Package.swift
.package(url: "https://github.com/abcdlsj/bolthole", branch: "main"),
// target dependency
.product(name: "Bolthole", package: "bolthole"),
```

## Use

```swift
import Bolthole

let client = try BoltholeClient("https://relay.example.com/#k=6Xo2…")

// HTTP
let res = try await client.request("GET", "/api/status")
print(res.status, String(decoding: try await res.data(), as: UTF8.self))

// Or with a URLRequest
let (body, http) = try await client.data(for: URLRequest(url: URL(string: "http://agent/api/status")!))

// WebSockets
let ws = try await client.webSocket("/live")
try await ws.send(.text("hi"))
let reply = try await ws.receive()

// Raw TCP to a target the agent allows
let stream = try await client.tcp("ssh")

// Your own stream kind
let (chat, acceptMeta) = try await client.open(kind: "example.chat", meta: ["room": "lobby"])
```

### Paths

By default the client races every direct candidate the agent advertised
against the relay. The relay starts 250 ms late, so a direct path on the LAN
or VPN wins when one exists. After a relayed connection the client keeps
trying to move to a direct path in the background. Pin a path with
`options.mode = .relayOnly` or `.directOnly`, and watch it change with
`options.onPath`.

```swift
var o = BoltholeClient.Options()
o.direct = ["ws://192.168.1.20:7777/.bolthole/e2e"]   // known candidates
o.onPath = { print("now via", $0) }
let client = try BoltholeClient(conn, options: o)
```

### Custom transports

`options.makeTransport` builds the WebSocket for a URL. The default is
`URLSessionWebSocketTransport`. Supply your own `MessageTransport` to use
another stack (Network.framework, a proxy, a test pipe).

## Test

```bash
swift test   # vectors and mux tests
BOLTHOLE_CONN="http://127.0.0.1:8787/#k=…" swift test   # plus live interop
```

For interop, run an agent with `bolthole agent --http <upstream> --direct 127.0.0.1:0`
whose upstream answers `GET /hello` and echoes WebSocket messages. On Linux
the tests use a small socket WebSocket client, because Foundation's libcurl
is usually built without WebSocket support.
