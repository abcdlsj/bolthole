# bolthole (JavaScript)

**Reach any service from anywhere, through a relay that can't read your traffic.**

JavaScript SDK for [Bolthole](https://github.com/abcdlsj/bolthole). Runs in
browsers, Node 22+, Deno, Bun and Cloudflare Workers. Plain ES modules on
WebCrypto — no build step, no dependencies.

```
 your service ── agent ──outbound WSS──▶ relay ◀──HTTPS/WSS── client (phone, browser, server)
                   └──────────── end-to-end encrypted (Noise) ───────────┘
```

The agent dials **out** to the relay and keeps the connection open. Clients go
through the relay to the agent. Both ends derive keys from a 256-bit code that
travels in the URL fragment, so the relay only ever forwards ciphertext.

## Install

```bash
npm install bolthole
```

Need a relay? Deploy one to Cloudflare Workers for free, or run the Go binary:

```bash
npx wrangler deploy                      # see github.com/abcdlsj/bolthole
bolthole serve --addr :8787              # or a relay on your own box
```

## Client

```js
import { Client } from 'bolthole';

const c = new Client('https://relay.example.com/#k=6Xo2…');

const res = await c.fetch('/api/status', { method: 'POST', body: 'x' });
const ws = await c.websocket('/live');            // send() / read() / close()
const sock = await c.tcp();                       // raw byte stream
```

`c.fetch` and `c.websocket` take the same arguments as the globals; paths are
resolved against the agent's address, not the relay's.

| Method | |
| --- | --- |
| `fetch(input, init)` | HTTP through the agent, response streamed back |
| `websocket(path, protocols)` | WebSocket over the tunnel: `send()`, `read()`, `close()`, `protocol` |
| `tcp(target)` | Raw stream to a target the agent allows |
| `open(kind, meta)` | Any stream kind, including ones you define; `st.acceptMeta` holds the agent's reply |
| `session()` | The live encrypted session, dialing if needed |
| `close()` | Drop the connection |

Options: `{ mode: 'auto' | 'relay' | 'direct', direct: [url], relayDelayMs,
directTimeoutMs, WebSocket }`. In the default `auto` mode the client races
direct addresses against the relay and settles on whichever answers first.

## Agent

```js
import { Agent } from 'bolthole';

const agent = new Agent({
  relay: 'https://relay.example.com',
  // code: '…',                  // persistent 43-char code; random if omitted
  fetch: (req) => new Response('hi'),   // your service
});
agent.run();                    // reconnects forever
console.log(agent.connString);  // send this to clients — it is a password
```

Options: `{ relay, code, fetch, websocket, kinds, public, direct, auth, name,
 onStatus, onSession, WebSocket }`. `websocket` handles `ws` streams,
`kinds` maps a custom kind name to `(stream, info) => …`.

`fetch` is any fetch-style handler, so an existing `Request → Response`
function works unchanged. Set `public: true` to also serve plaintext HTTP
through the relay for things meant to be public (webhooks, a landing page) —
in that mode the relay can see and alter that traffic. `auth` carries a relay
registration key or JWT; `onStatus` reports `{connected, error, retryMs}`.

Shorthands: `connect(connString, opts)` and `serve(opts)`.

## Custom stream kinds

HTTP, WebSocket and TCP are just stream kinds. Define your own when you want
your own protocol inside the encrypted channel:

```js
// Agent side: register a handler per kind.
new Agent({
  relay: 'https://relay.example.com',
  fetch: handler,
  kinds: {
    'myapp.sync': async (st, info) => {
      await st.accept({ ok: true });      // send meta back to the caller
      for await (const chunk of st.readable()) { /* … */ }
      await st.write(reply);              // backpressure-aware
    },
  },
}).run();

// Client side: open that kind.
const st = await c.open('myapp.sync', { room: 'dev' });
console.log(st.acceptMeta);               // { ok: true }
await st.write(bytes);
await st.closeWrite();
for await (const chunk of st.readable()) { /* … */ }
```

A stream is a byte pipe with flow control: `write`, `read`, `readable()`
(a web `ReadableStream`), `pipeFrom`, `closeWrite`, `reset(code)`.

Stream kinds are documented in the
[protocol spec](https://github.com/abcdlsj/bolthole/blob/main/spec/PROTOCOL.md).

## Relay

`bolthole/relay` is the relay core, so you can host one without the Go binary:

```js
import { RelayHub } from 'bolthole/relay';
```

The Workers edition lives in the main repository under `cloudflare/`.

## What this package does not hide

The relay sees timing, sizes, and which token hash is talked to. Put it behind
TLS. A page served through the relay could be altered by a malicious relay
before it runs — for real deployments, host the client page yourself.

## Credits

<img src="https://alma.now/assets/alma.png" width="44" height="44" alt="Alma" align="left" hspace="12">

The approach came from [alma-relay](https://github.com/yetone/alma-relay), the
self-hosted relay for [Alma](https://alma.now) Remote Access: an outbound-only
agent connection, a relay that only sees ciphertext, and a Cloudflare Workers
deployment that costs nothing while idle. Thanks to
[@yetone](https://github.com/yetone) and its contributors.

<br clear="left">

## License

MIT
