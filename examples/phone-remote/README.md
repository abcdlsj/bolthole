# Phone remote

Open a link on your phone and see your laptop's CPU and memory live, measure
the round trip, and pop a message on its terminal. Everything after the page
loads runs over an end-to-end encrypted session from the browser.

```bash
node examples/phone-remote/agent.mjs https://relay.example.com
# Open on your phone:
#   https://relay.example.com/c/<token>/#k=<code>
```

How it fits together:

- The agent runs in **public** mode, but its plaintext handler only serves
  `index.html` and the SDK files. Anything that touches the laptop
  (`/stats`, `/ping`, `/notify`) is answered only when `info.encrypted` is
  true.
- The page reads the code from `location.hash` (never sent to the relay),
  opens `new Client(...)`, and uses `client.fetch()` like normal fetch, including
  a streaming Server-Sent Events body.

For a real app, host the page yourself or ship it inside the app: a page
delivered through a relay you don't trust could be modified on the way.
