# Deploying a relay

The relay is stateless and stores nothing on disk, so you can redeploy,
move or replace it at any time; agents reconnect on their own and clients
redial. Pick whichever edition fits.

## Cloudflare Workers (free plan, no server)

```bash
git clone https://github.com/abcdlsj/bolthole && cd bolthole
npx wrangler deploy            # prints https://bolthole-relay.<you>.workers.dev
```

Each agent gets its own Durable Object (named by its token hash), placed
near whoever connects first, usually the agent. Set `LOCATION_HINT`
(`apac`, `enam`, `weur`, …) in `wrangler.jsonc` to pin a region. To use your
own domain, uncomment `routes`.

Cost: an always-connected agent keeps its Durable Object resident, about
10 800 GB-s a day at 128 MB, inside the free plan's daily allowance for one
agent. Many agents need the paid plan.

Optional settings (secrets):

```bash
npx wrangler secret put BOLTHOLE_KEYS        # comma-separated registration keys
npx wrangler secret put BOLTHOLE_JWT_SECRET  # or: per-agent JWTs (bolthole token)
```

Request bodies through a Worker are limited to what Cloudflare allows on
your plan (100 MB on Free).

## Docker with automatic HTTPS

You need a host with ports 80 and 443 open and a DNS record for it.

```bash
cd deploy
RELAY_DOMAIN=relay.example.com docker compose up -d
```

Caddy obtains and renews the certificate. Optional variables:
`BOLTHOLE_KEYS`, `BOLTHOLE_JWT_SECRET`.

## Single binary

```bash
go install github.com/abcdlsj/bolthole/cmd/bolthole@latest
bolthole serve --addr 127.0.0.1:8787 --trust-proxy
```

Put any TLS proxy in front. WebSockets must pass through and responses must
not be buffered:

```nginx
location / {
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_buffering off;
    proxy_read_timeout 1h;
    client_max_body_size 128m;
}
```

Or give it a certificate directly: `--tls-cert cert.pem --tls-key key.pem`.
`SIGTERM` drains gracefully: agents get `GOAWAY` and reconnect while
in-flight requests finish.

| Flag | Env | |
| --- | --- | --- |
| `--addr` | `BOLTHOLE_ADDR` | listen address (`:8787`) |
| `--key` | `BOLTHOLE_KEYS` | comma-separated registration keys |
| `--jwt-secret` | `BOLTHOLE_JWT_SECRET` | accept HS256 agent tokens instead |
| `--trust-proxy` | `BOLTHOLE_TRUST_PROXY=1` | rate-limit by `X-Forwarded-For` / `CF-Connecting-IP` |
| `--max-body-mb` | | request body cap (128) |

## Inside an existing Go server

```go
r := relay.New(relay.Config{Auth: relay.SharedKey(os.Getenv("RELAY_KEY"))})
mux.Handle("/", r)            // or mount under a prefix with http.StripPrefix
defer r.Shutdown(ctx)
```

## Running more than one relay process

A token hash must reach the same process as its agent. Route on it at the
load balancer: hash the `th` query parameter of `/v1/agent` and the SHA-256
of the `/c/<token>` segment, consistently. (The Workers edition does exactly
this with Durable Objects.) Or implement `relay.Registry` over something
shared.
