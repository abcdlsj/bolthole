// Receive webhooks (GitHub, Stripe, Slack…) on your laptop without a public
// server, tunnel or port forward.
//
//	go run ./examples/webhook-inbox --relay https://relay.example.com
//
// It runs a *public* agent: the printed URL accepts plain HTTPS, so you can
// paste it into any webhook settings page. Each delivery is pretty-printed,
// optionally forwarded to a local dev server, and kept in a ring buffer you
// can browse end-to-end encrypted with `bolthole connect`.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/abcdlsj/bolthole"
	"github.com/abcdlsj/bolthole/agent"
	"github.com/abcdlsj/bolthole/mux"
)

type delivery struct {
	At      time.Time         `json:"at"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body,omitempty"`
	Text    string            `json:"text,omitempty"`
}

type inbox struct {
	mu   sync.Mutex
	last []delivery
}

func (in *inbox) add(d delivery) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.last = append(in.last, d)
	if len(in.last) > 50 {
		in.last = in.last[1:]
	}
}

func main() {
	relay := flag.String("relay", os.Getenv("BOLTHOLE_RELAY"), "relay URL")
	forward := flag.String("forward", "", "also forward each delivery to this local URL (e.g. http://localhost:3000)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	code, err := bolthole.LoadOrCreateCode("~/.config/bolthole/webhook-inbox.code")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	in := &inbox{}

	// Plaintext (public) streams: accept webhooks.
	hooks := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		d := delivery{At: time.Now(), Method: r.Method, Path: r.URL.RequestURI(), Headers: map[string]string{}}
		for k, v := range r.Header {
			d.Headers[strings.ToLower(k)] = strings.Join(v, ", ")
		}
		if json.Valid(body) {
			d.Body = body
		} else {
			d.Text = string(body)
		}
		in.add(d)
		show(d)
		if *forward != "" {
			go replay(*forward, r.Method, r.URL.RequestURI(), r.Header, body)
		}
		w.Header().Set("content-type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	})
	// Encrypted streams: browse the inbox privately.
	private := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in.mu.Lock()
		defer in.mu.Unlock()
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(in.last)
	})

	router := agent.NewRouter().HandleFunc("http", func(ctx context.Context, st *mux.Stream, info agent.Info) {
		if info.Encrypted {
			agent.NewRouter().HTTP(private).ServeStream(ctx, st, info)
		} else {
			agent.NewRouter().HTTP(hooks).ServeStream(ctx, st, info)
		}
	})
	a := &agent.Agent{Relay: *relay, Code: code, Name: "webhook-inbox", Public: true, Handler: router}
	cs, _ := a.ConnString()
	fmt.Printf("Webhook URL (paste it anywhere):\n\n  %shooks\n\nBrowse the last 50 deliveries privately:\n\n  bolthole connect '%s' --listen 127.0.0.1:8080 && curl localhost:8080/\n\n", a.PublicURL(), cs)
	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func show(d delivery) {
	fmt.Printf("\n\033[1m%s %s\033[0m  %s\n", d.Method, d.Path, d.At.Format("15:04:05"))
	keys := make([]string, 0, len(d.Headers))
	for k := range d.Headers {
		if strings.HasPrefix(k, "x-") || k == "content-type" || k == "user-agent" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  \033[2m%s:\033[0m %s\n", k, d.Headers[k])
	}
	if d.Body != nil {
		var pretty bytes.Buffer
		json.Indent(&pretty, d.Body, "  ", "  ")
		fmt.Printf("  %s\n", pretty.String())
	} else if d.Text != "" {
		fmt.Printf("  %s\n", d.Text)
	}
}

func replay(base, method, path string, h http.Header, body []byte) {
	req, err := http.NewRequest(method, strings.TrimSuffix(base, "/")+path, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header = h.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("  \033[31mforward failed: %v\033[0m\n", err)
		return
	}
	resp.Body.Close()
	fmt.Printf("  → forwarded: %s\n", resp.Status)
}
