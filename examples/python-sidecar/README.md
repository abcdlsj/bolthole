# Python behind the sidecar

The language-agnostic way to use bolthole: your app stays a normal local
HTTP server, and the `bolthole` binary does the relaying and encryption on
both ends. Nothing here imports an SDK.

```
 machine A                                             machine B
 app.py :5000 ◀── bolthole agent ──▶ relay ◀── bolthole connect :8080 ◀── client.py
```

On the machine with the app:

```bash
python3 app.py &
bolthole agent --relay https://relay.example.com --http http://127.0.0.1:5000
# prints: Connection string … https://relay.example.com/#k=…
```

Anywhere else:

```bash
bolthole connect 'https://relay.example.com/#k=…' --listen 127.0.0.1:8080 &
python3 client.py http://127.0.0.1:8080
```

Swap Python for Ruby, PHP, Rust or a shell script with `curl`: the contract
is plain HTTP (and WebSockets, which the sidecar forwards too) on localhost.
Add `--direct :7777` to the agent and clients on the same network skip the
relay automatically.
