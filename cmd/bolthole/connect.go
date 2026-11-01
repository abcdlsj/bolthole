package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/client"
	"github.com/abcdlsj/bolthole/kinds/wskind"
)

func clientFlags(fs *flag.FlagSet) (*string, *string, *bool) {
	mode := fs.String("mode", envOr("BOLTHOLE_MODE", "auto"), "path selection: auto | relay | direct [BOLTHOLE_MODE]")
	direct := fs.String("direct-url", "", "extra comma-separated direct candidates (ws://host:port/.bolthole/e2e)")
	verbose := fs.Bool("v", false, "verbose logs")
	return mode, direct, verbose
}

func newClient(cs, mode, direct string, log *slog.Logger) (*client.Client, error) {
	o := &client.Options{Name: "bolthole-cli " + hostname(), Logger: log}
	switch mode {
	case "auto", "":
	case "relay":
		o.Mode = client.RelayOnly
	case "direct":
		o.Mode = client.DirectOnly
	default:
		return nil, fmt.Errorf("unknown --mode %q", mode)
	}
	if direct != "" {
		o.Direct = strings.Split(direct, ",")
	}
	o.OnPath = func(p string) { o.Logger.Info("path", "via", p) }
	return client.New(cs, o)
}

func connString(fs *flag.FlagSet) (string, error) {
	cs := fs.Arg(0)
	if cs == "" {
		cs = os.Getenv("BOLTHOLE_CONN")
	}
	if cs == "" {
		return "", errors.New("pass the connection string as an argument or in BOLTHOLE_CONN")
	}
	return cs, nil
}

func cmdConnect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	listen := fs.String("listen", envOr("BOLTHOLE_LISTEN", "127.0.0.1:8080"), "local address to serve the agent's HTTP API on [BOLTHOLE_LISTEN]")
	var tcps multiFlag
	fs.Var(&tcps, "tcp", "forward a local port to an agent TCP target: localaddr=target (repeatable)")
	mode, direct, verbose := clientFlags(fs)
	fs.Parse(reorder(args))
	cs, err := connString(fs)
	if err != nil {
		return err
	}
	log := logger(*verbose)
	c, err := newClient(cs, *mode, *direct, log)
	if err != nil {
		return err
	}
	defer c.Close()

	// Connect up front so mistakes surface immediately, not on first request.
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if _, err := c.Conn(dctx); err != nil {
		cancel()
		return fmt.Errorf("connect: %w", err)
	}
	cancel()

	for _, t := range tcps {
		local, target, _ := strings.Cut(t, "=")
		ln, err := net.Listen("tcp", local)
		if err != nil {
			return err
		}
		log.Info("forwarding TCP", "local", ln.Addr().String(), "target", target)
		go serveTCP(ctx, ln, c, target)
	}

	proxy := &httputil.ReverseProxy{
		Rewrite:       func(r *httputil.ProxyRequest) { r.Out.URL.Scheme, r.Out.URL.Host = "http", "agent" },
		Transport:     c.Transport(),
		FlushInterval: -1,
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("upgrade"), "websocket") {
			bridgeLocalWebSocket(w, r, c)
			return
		}
		proxy.ServeHTTP(w, r)
	})
	srv := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 15 * time.Second}
	go func() { <-ctx.Done(); srv.Close() }()
	fmt.Fprintf(os.Stderr, "Agent available at http://%s/\n", *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func bridgeLocalWebSocket(w http.ResponseWriter, r *http.Request, c *client.Client) {
	var protos []string
	for _, v := range r.Header.Values("sec-websocket-protocol") {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				protos = append(protos, p)
			}
		}
	}
	remote, proto, err := c.DialWebSocket(r.Context(), r.URL.RequestURI(), protos...)
	if err != nil {
		http.Error(w, "agent websocket: "+err.Error(), http.StatusBadGateway)
		return
	}
	opts := &websocket.AcceptOptions{InsecureSkipVerify: true}
	if proto != "" {
		opts.Subprotocols = []string{proto}
	}
	local, err := websocket.Accept(w, r, opts)
	if err != nil {
		remote.Stream().Close()
		return
	}
	local.SetReadLimit(wskind.MaxRecord)
	wskind.Bridge(context.WithoutCancel(r.Context()), local, remote)
}

func serveTCP(ctx context.Context, ln net.Listener, c *client.Client, target string) {
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			remote, err := c.DialTCP(ctx, target)
			if err != nil {
				return
			}
			defer remote.Close()
			pipe(conn, remote)
		}()
	}
}

func cmdTCP(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tcp", flag.ExitOnError)
	mode, direct, verbose := clientFlags(fs)
	fs.Parse(reorder(args))
	cs, err := connString(fs)
	if err != nil {
		return err
	}
	// Stay quiet on stderr unless asked: ssh shows it to the user.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if *verbose {
		log = logger(true)
	}
	c, err := newClient(cs, *mode, *direct, log)
	if err != nil {
		return err
	}
	defer c.Close()
	remote, err := c.DialTCP(ctx, fs.Arg(1))
	if err != nil {
		return err
	}
	defer remote.Close()
	pipe(stdio{}, remote)
	return nil
}

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) CloseWrite() error           { return os.Stdout.Close() }

// pipe copies both ways, half-closing each side when the other finishes.
func pipe(a, b io.ReadWriter) {
	var wg sync.WaitGroup
	cp := func(dst, src io.ReadWriter) {
		defer wg.Done()
		io.Copy(dst, src)
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}
	wg.Add(2)
	go cp(a, b)
	go cp(b, a)
	wg.Wait()
}

// reorder lets flags follow positional arguments ("connect CONN --listen x").
func reorder(args []string) []string {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			if !strings.Contains(a, "=") && i+1 < len(args) && !isBoolFlag(a) {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		pos = append(pos, a)
	}
	return append(flags, pos...)
}

func isBoolFlag(a string) bool {
	a = strings.TrimLeft(a, "-")
	return a == "v"
}
