# Terminal chat room

```bash
go run ./examples/chat host --relay https://relay.example.com
go run ./examples/chat join '<connection string>' --nick alice   # from anywhere
```

The room is a custom stream kind, `example.chat`. Each member opens one
stream with `{"nick": …}` as its meta; the host accepts it, then everything
typed is a line on that stream. That's the whole extension API:

```go
agent.NewRouter().HandleFunc("example.chat", func(ctx context.Context, st *mux.Stream, info agent.Info) { … })
// and on the other side
conn.Session.OpenAndWait(ctx, "example.chat", joinMeta{Nick: "alice"})
```
