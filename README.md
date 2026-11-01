# Bolthole

**Reach any service from anywhere, through a relay that can't read your traffic.**

Your app runs on a laptop, a Raspberry Pi or a server behind NAT. Bolthole
lets a phone, a browser or another program talk to it from anywhere, with no
port forwarding, no VPN and no trust in whoever runs the relay.

```
 your service ── agent ──outbound WSS──▶ relay ◀──HTTPS/WSS── client (phone, browser, CLI, server)
                   └──────────── end-to-end encrypted (Noise) ───────────┘
                   └──── or directly, when they share a network ──────────┘
```

- **Any app, any language.** Use the Go or JavaScript SDK, or put the
  `bolthole` binary next to your app and keep speaking plain HTTP,
  WebSocket or TCP on localhost.
- **No server needed.** Deploy the relay to Cloudflare Workers on the free
  plan in one command, or run the single Go binary anywhere.
- **The relay is blind.** Clients and agents run a Noise handshake keyed by a
  secret only they hold. The relay forwards ciphertext and stores nothing.
- **LAN when possible, relay when not.** Clients race direct addresses
  against the relay, and move to a direct path in the background when one
  appears.
- **Built to stay up.** Per-stream flow control, heartbeats, reconnect with
  jittered backoff, graceful relay restarts (GOAWAY, make-before-break),
  rate limits and bounded memory.
- **Extensible.** HTTP, WebSocket and TCP are just stream *kinds*. Define your
  own (`myapp.sync`, `rpc.v2`…) in a few lines, let the agent open streams
  back to clients, or negotiate your own protocol inside the encrypted
  channel.

## Five-minute tour

```bash
go install github.com/abcdlsj/bolthole/cmd/bolthole@latest

# 1. A relay (or skip this and use one you deployed to Cloudflare)
bolthole serve --addr :8787

# 2. On the machine with the service
bolthole agent --relay http://localhost:8787 --http http://127.0.0.1:3000 --qr
#    → Connection string: http://localhost:8787/#k=6Xo2…   (treat it like a password)

# 3. Anywhere else
bolthole connect 'http://localhost:8787/#k=6Xo2…' --listen 127.0.0.1:8080
curl http://127.0.0.1:8080/        # your service, end-to-end encrypted
```

The **connection string** is the relay's URL with a 256-bit code in the
`#fragment`. Fragments are never sent to servers, so the relay never sees it.

## Use it from code

### Go

```go
// Agent: expose an http.Handler.
code, _ := bolthole.LoadOrCreateCode("~/.config/myapp/bolthole.code")
a := bolthole.Expose("https://relay.example.com", code, myHandler)
cs, _ := a.ConnString()
fmt.Println("connect with:", cs)
a.Run(ctx) // reconnects forever

// Client: a normal *http.Client.
c, _ := bolthole.Dial(cs)
resp, _ := c.HTTPClient().Get("http://agent/api/status")
```

WebSockets, raw TCP and your own stream kinds use `agent.Router`:

```go
r := agent.NewRouter().
	HTTP(api).
	TCP(map[string]string{"ssh": "127.0.0.1:22"}).
	HandleFunc("myapp.sync", func(ctx context.Context, st *mux.Stream, info agent.Info) {
		st.Accept(nil)
		io.Copy(st, st) // a stream is an io.ReadWriteCloser
	})
```

### JavaScript (browser, Node 22+, Deno, Bun, Workers)

```js
import { Client, Agent } from 'bolthole';

// Client: fetch() and WebSockets to the agent.
const c = new Client('https://relay.example.com/#k=6Xo2…');
const res = await c.fetch('/api/status');
const ws = await c.websocket('/live');

// Agent: any fetch-style handler.
const agent = new Agent({ relay: 'https://relay.example.com', fetch: (req) => new Response('hi') });
agent.run();
console.log(agent.connString);
```

No build step and no dependencies: the SDK is plain ES modules on WebCrypto.

### Any other language

Run `bolthole agent` beside your server and `bolthole connect` beside your
client; both ends speak plain localhost HTTP/WebSocket/TCP. Or implement
[the protocol](spec/PROTOCOL.md) directly: it is short, versioned, and comes
with byte-exact [test vectors](spec/test-vectors.json).

## Deploy a relay

| | |
| --- | --- |
| **Cloudflare Workers** (free, no server) | `npx wrangler deploy` (see [docs/deploy.md](docs/deploy.md)) |
| **Docker + automatic HTTPS** | `RELAY_DOMAIN=relay.example.com docker compose up -d` |
| **Single binary** | `bolthole serve --addr :8787` behind any TLS proxy |
| **Inside your Go server** | `http.Handle("/", relay.New(relay.Config{}))` |

All editions speak the same protocol; the same test suite runs against the
Go relay and the Worker. To keep strangers off your relay, set
`BOLTHOLE_KEYS` (shared keys) or `BOLTHOLE_JWT_SECRET` (per-agent tokens from
`bolthole token`). That controls who may *use your relay*; who may reach an
agent is decided only by its code.

## Examples

| | |
| --- | --- |
| [`examples/phone-remote`](examples/phone-remote) | Open a link on your phone: live CPU/memory of your laptop and a button that pops a message on its terminal. Browser SDK, SSE over E2E. |
| [`examples/ssh-anywhere`](examples/ssh-anywhere) | `ssh laptop` from anywhere with a one-line `ProxyCommand`; goes direct on the same Wi-Fi. |
| [`examples/webhook-inbox`](examples/webhook-inbox) | A public webhook URL that lands on your laptop, pretty-prints deliveries and replays them to your dev server. |
| [`examples/chat`](examples/chat) | A terminal chat room hosted on your machine, built on a custom stream kind. |
| [`examples/python-sidecar`](examples/python-sidecar) | A Python app and client that never import anything: the sidecar does it all. |

## How it works

1. The agent generates a 32-byte **code**. From it, HKDF derives a **routing
   token** (what clients put in the relay URL), its SHA-256 **token hash**
   (what the agent registers with), and a **PSK** (never leaves the
   endpoints).
2. The agent dials out to `/v1/agent` and registers its token hash. The relay
   knows nothing else about it.
3. A client opens `wss://relay/c/<routing token>/.bolthole/e2e`. The relay
   bridges it to the agent as a stream.
4. Client and agent run `Noise_NNpsk0_25519_AESGCM_SHA256`. Without the PSK
   the handshake fails, so the relay can't read or impersonate either side.
5. Inside the encrypted channel they run the mux: many concurrent,
   flow-controlled streams of any kind, in both directions.

Read [spec/PROTOCOL.md](spec/PROTOCOL.md) for the details.

## Security notes

- The code is a password for the agent. Rotate it (delete the code file and
  restart the agent) to revoke everyone.
- E2E is on by default. `--public` / `Public: true` additionally serves
  plaintext HTTP through the relay, for things meant to be public (webhooks,
  a landing page). In public mode the relay can see and alter that traffic.
- A page served through the relay (as in `phone-remote`) could be altered by
  a malicious relay before it runs. For real deployments, host the client
  page yourself or ship it in your app.
- The relay sees timing, sizes and which token hash is talked to. Always put
  it behind TLS.

## Repository layout

| Path | |
| --- | --- |
| `spec/` | Protocol spec and test vectors |
| `mux/`, `e2e/`, `kinds/` | Go: stream multiplexer, Noise channel and codes, stream kinds |
| `agent/`, `client/`, `relay/` | Go: the three roles |
| `cmd/bolthole/` | The CLI: relay server and sidecar |
| `js/` | JavaScript SDK (client, agent, relay core) |
| `cloudflare/` | Workers edition of the relay |
| `examples/` | Runnable examples |

## Development

```bash
go test -race ./...                 # Go unit + integration tests
cd js && npm install && npm test    # JS tests, including Go⇄JS interop
npx wrangler dev &                  # Worker relay on :8787 …
BOLTHOLE_RELAY=http://127.0.0.1:8787 npm --prefix js test   # … same suite against it
```

## License

MIT
