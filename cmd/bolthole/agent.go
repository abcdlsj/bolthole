package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"

	"github.com/coder/websocket"
	"github.com/mdp/qrterminal/v3"

	"github.com/abcdlsj/bolthole"
	"github.com/abcdlsj/bolthole/agent"
	"github.com/abcdlsj/bolthole/kinds/httpkind"
	"github.com/abcdlsj/bolthole/kinds/wskind"
	"github.com/abcdlsj/bolthole/mux"
)

func cmdAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	relayURL := fs.String("relay", envOr("BOLTHOLE_RELAY", ""), "relay base URL, e.g. https://relay.example.com [BOLTHOLE_RELAY]")
	codeFile := fs.String("code-file", envOr("BOLTHOLE_CODE_FILE", "~/.config/bolthole/code"), "where the connect code lives (created if missing) [BOLTHOLE_CODE_FILE]")
	upstream := fs.String("http", envOr("BOLTHOLE_HTTP", ""), "local HTTP service to expose, e.g. http://127.0.0.1:3000 (WebSockets included) [BOLTHOLE_HTTP]")
	var tcps multiFlag
	fs.Var(&tcps, "tcp", "expose a TCP service as name=host:port (repeatable; a bare host:port is the default target)")
	public := fs.Bool("public", false, "also serve plaintext HTTP at <relay>/c/<token>/ (for webhooks or public pages)")
	direct := fs.String("direct", envOr("BOLTHOLE_DIRECT", ""), "also listen here for direct (LAN/VPN) clients, e.g. :7777 [BOLTHOLE_DIRECT]")
	name := fs.String("name", hostname(), "name shown to clients and in relay logs")
	auth := fs.String("auth", envOr("BOLTHOLE_AUTH", ""), "registration key or JWT, if the relay requires one [BOLTHOLE_AUTH]")
	qr := fs.Bool("qr", false, "print the connection string as a QR code")
	verbose := fs.Bool("v", false, "verbose logs")
	fs.Parse(args)

	if *relayURL == "" {
		return errors.New("agent: --relay is required")
	}
	if *upstream == "" && len(tcps) == 0 {
		return errors.New("agent: expose something with --http and/or --tcp")
	}
	code, err := bolthole.LoadOrCreateCode(*codeFile)
	if err != nil {
		return err
	}
	log := logger(*verbose)
	router := agent.NewRouter()
	if *upstream != "" {
		up, err := url.Parse(*upstream)
		if err != nil || (up.Scheme != "http" && up.Scheme != "https") {
			return fmt.Errorf("agent: --http must be an http(s) URL")
		}
		proxy := &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(up)
				r.Out.Host = up.Host
			},
			FlushInterval: -1, // stream responses (SSE) as they arrive
		}
		router.HTTP(proxy)
		router.HandleFunc(wskind.Kind, proxyWebSocket(up))
	}
	if len(tcps) > 0 {
		targets := map[string]string{}
		for _, t := range tcps {
			n, addr, ok := strings.Cut(t, "=")
			if !ok {
				n, addr = "", t
			}
			targets[n] = addr
		}
		router.TCP(targets)
	}

	a := &agent.Agent{Relay: *relayURL, Code: code, Name: *name, Auth: *auth, Public: *public, Handler: router, Logger: log}
	if *direct != "" {
		addr, err := a.ListenDirect(ctx, *direct)
		if err != nil {
			return err
		}
		log.Info("direct listener", "addr", addr.String(), "urls", a.Direct)
	}
	cs, err := a.ConnString()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "\nConnection string (treat it like a password):\n\n  %s\n\n", cs)
	if *qr {
		qrterminal.GenerateHalfBlock(cs, qrterminal.L, os.Stderr)
	}
	if *public {
		fmt.Fprintf(os.Stderr, "Public URL (plaintext, anyone with the link):\n\n  %s\n\n", a.PublicURL())
	}
	return a.Run(ctx)
}

// proxyWebSocket bridges ws streams to the upstream's WebSocket endpoint.
func proxyWebSocket(up *url.URL) agent.HandlerFunc {
	return func(ctx context.Context, st *mux.Stream, _ agent.Info) {
		var m wskind.OpenMeta
		if err := st.DecodeMeta(&m); err != nil {
			st.Reset(mux.CodeProtocolError, "bad ws meta")
			return
		}
		target := *up
		if target.Scheme == "https" {
			target.Scheme = "wss"
		} else {
			target.Scheme = "ws"
		}
		rel, err := url.Parse(m.Path)
		if err != nil {
			st.Reset(mux.CodeProtocolError, "bad path")
			return
		}
		target.Path = strings.TrimSuffix(target.Path, "/") + rel.Path
		target.RawQuery = rel.RawQuery
		hdr := httpkind.DecodeHeaders(m.Headers)
		hdr.Del("origin")
		c, resp, err := websocket.Dial(ctx, target.String(), &websocket.DialOptions{HTTPHeader: hdr, Subprotocols: m.Protocols})
		if err != nil {
			code := mux.CodeRefused
			if resp != nil && resp.StatusCode == http.StatusNotFound {
				code = mux.CodeNotFound
			}
			st.Reset(code, "upstream websocket failed")
			return
		}
		c.SetReadLimit(wskind.MaxRecord)
		if err := st.Accept(wskind.AcceptMeta{Protocol: c.Subprotocol()}); err != nil {
			c.CloseNow()
			return
		}
		wskind.Bridge(ctx, c, wskind.New(st))
	}
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
