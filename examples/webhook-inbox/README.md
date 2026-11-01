# Webhook inbox

A public HTTPS URL for webhooks (GitHub, Stripe, Slack…) that lands on your
laptop. Deliveries are pretty-printed, optionally replayed to a local dev
server, and the last 50 can be browsed privately over E2E.

```bash
go run ./examples/webhook-inbox --relay https://relay.example.com --forward http://localhost:3000
```

It shows the two modes side by side: the agent is `Public`, and its handler
looks at `info.Encrypted` to decide whether a request is an anonymous
webhook (plaintext through the relay) or you, browsing the inbox through
`bolthole connect`.
