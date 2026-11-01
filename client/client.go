// Package client reaches an agent through a relay, or directly when it can,
// over an end-to-end encrypted session.
//
// The short version:
//
//	c, _ := client.New("https://relay.example.com/#k=…", nil)
//	resp, _ := c.HTTPClient().Get("http://agent/api/status")
//
// Client picks the fastest path (direct candidates race the relay), redials
// when the connection drops, and upgrades from the relay to a direct path
// in the background when one becomes reachable.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/e2e"
	"github.com/abcdlsj/bolthole/kinds/httpkind"
	"github.com/abcdlsj/bolthole/kinds/wskind"
	"github.com/abcdlsj/bolthole/mux"
)

// Mode pins the path selection.
type Mode int

const (
	// Auto races direct candidates against the relay and prefers direct.
	Auto Mode = iota
	// RelayOnly never tries direct candidates.
	RelayOnly
	// DirectOnly never uses the relay.
	DirectOnly
)

// Options configure a Client. All fields are optional.
type Options struct {
	Name   string   // sent to the agent inside the encrypted hello
	Mode   Mode     // path selection, Auto by default
	Direct []string // extra direct candidates (ws:// or wss:// URLs)
	// RelayDelay is how long direct candidates get a head start (default 250ms).
	RelayDelay time.Duration
	// DirectTimeout bounds one direct attempt (default 2s).
	DirectTimeout time.Duration
	// UpgradeInterval is how often a relayed client probes direct
	// candidates (default 30s; negative disables).
	UpgradeInterval time.Duration
	HTTPClient      *http.Client
	Logger          *slog.Logger
	// OnPath is called whenever the active path changes.
	OnPath func(path string)
	// Accept handles streams the agent opens toward this client. Optional.
	Accept func(ctx context.Context, st *mux.Stream)
}

// Client keeps one live encrypted session to an agent.
type Client struct {
	cs   e2e.ConnString
	keys e2e.Keys
	opts Options
	log  *slog.Logger

	mu      sync.Mutex
	cur     *Conn
	dialing chan struct{}
	direct  []string // learned + configured candidates
	closed  bool
	stop    context.CancelFunc
}

// New parses a connection string and returns a Client. It does not dial
// until the first use.
func New(connString string, opts *Options) (*Client, error) {
	cs, err := e2e.ParseConnString(connString)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &Options{}
	}
	o := *opts
	if o.RelayDelay == 0 {
		o.RelayDelay = 250 * time.Millisecond
	}
	if o.DirectTimeout == 0 {
		o.DirectTimeout = 2 * time.Second
	}
	if o.UpgradeInterval == 0 {
		o.UpgradeInterval = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{cs: cs, keys: cs.Code.Derive(), opts: o, log: o.Logger, direct: slices.Clone(o.Direct), stop: cancel}
	if o.UpgradeInterval > 0 && o.Mode == Auto {
		go c.upgradeLoop(ctx)
	}
	return c, nil
}

// Close ends the session and stops background work.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	cur := c.cur
	c.cur = nil
	c.mu.Unlock()
	c.stop()
	if cur != nil {
		cur.Close()
	}
	return nil
}

// Path reports the active path ("relay", "direct ws://…") or "" when idle.
func (c *Client) Path() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur == nil {
		return ""
	}
	return c.cur.Path
}

// Conn returns the live session, dialing (or redialing) if needed.
func (c *Client) Conn(ctx context.Context) (*Conn, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errors.New("client: closed")
		}
		if c.cur != nil && c.cur.alive() {
			cur := c.cur
			c.mu.Unlock()
			return cur, nil
		}
		if c.dialing != nil {
			wait := c.dialing
			c.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		c.dialing = make(chan struct{})
		cands := c.candidates()
		c.mu.Unlock()

		conn, err := c.race(ctx, cands)

		c.mu.Lock()
		close(c.dialing)
		c.dialing = nil
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		c.install(conn)
		c.mu.Unlock()
		return conn, nil
	}
}

// install makes conn current (c.mu held).
func (c *Client) install(conn *Conn) {
	old := c.cur
	c.cur = conn
	c.learn(conn.Hello.Direct)
	if c.opts.Accept != nil {
		go func() {
			for {
				st, err := conn.Session.Accept(context.Background())
				if err != nil {
					return
				}
				go c.opts.Accept(context.Background(), st)
			}
		}()
	}
	if old != nil {
		go old.drain()
	}
	if c.opts.OnPath != nil {
		go c.opts.OnPath(conn.Path)
	}
	c.log.Info("connected", "path", conn.Path)
}

func (c *Client) learn(urls []string) {
	for _, u := range urls {
		if !slices.Contains(c.direct, u) {
			c.direct = append(c.direct, u)
		}
	}
}

type candidate struct {
	path  string // "relay" or "direct <url>"
	url   string
	delay time.Duration
}

func (c *Client) candidates() []candidate {
	var out []candidate
	if c.opts.Mode != RelayOnly {
		for _, u := range c.direct {
			out = append(out, candidate{path: "direct " + u, url: u})
		}
	}
	if c.opts.Mode != DirectOnly {
		d := time.Duration(0)
		if len(out) > 0 {
			d = c.opts.RelayDelay
		}
		out = append(out, candidate{path: "relay", url: c.relayURL(), delay: d})
	}
	return out
}

func (c *Client) relayURL() string {
	u := *c.cs.Relay
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/c/" + c.keys.RoutingToken + e2e.Path
	return u.String()
}

// race dials every candidate (direct ones first, the relay after a short
// head start) and keeps the first that completes the handshake.
func (c *Client) race(ctx context.Context, cands []candidate) (*Conn, error) {
	if len(cands) == 0 {
		return nil, errors.New("client: no path to the agent (mode excludes all candidates)")
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		conn *Conn
		err  error
		path string
	}
	results := make(chan result, len(cands))
	for _, cand := range cands {
		go func() {
			if cand.delay > 0 {
				select {
				case <-time.After(cand.delay):
				case <-rctx.Done():
					results <- result{err: rctx.Err(), path: cand.path}
					return
				}
			}
			actx := rctx
			if cand.path != "relay" {
				var cl context.CancelFunc
				actx, cl = context.WithTimeout(rctx, c.opts.DirectTimeout)
				defer cl()
			}
			conn, err := c.dialOne(actx, cand)
			results <- result{conn, err, cand.path}
		}()
	}
	var errs []error
	var winner *Conn
	for range cands {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.path, r.err))
			continue
		}
		if winner == nil {
			winner = r.conn
			cancel() // stop the losers
		} else {
			r.conn.Close()
		}
	}
	if winner != nil {
		return winner, nil
	}
	return nil, errors.Join(errs...)
}

func (c *Client) dialOne(ctx context.Context, cand candidate) (*Conn, error) {
	ws, resp, err := websocket.Dial(ctx, cand.url, &websocket.DialOptions{HTTPClient: c.opts.HTTPClient})
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusBadGateway {
			return nil, ErrAgentOffline
		}
		return nil, err
	}
	ws.SetReadLimit(mux.DefaultMaxFrame)
	raw := mux.WebSocket(ws)
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	secure, hello, err := e2e.Client(ctx, raw, c.keys.PSK, e2e.ClientHello{Protos: []string{e2e.ProtoMux}, Name: c.opts.Name})
	if !stop() || err != nil {
		raw.Close()
		if err == nil {
			err = ctx.Err()
		}
		return nil, err
	}
	sess := mux.NewSession(secure, mux.Config{Dialer: true, Logger: c.log, MaxFrame: e2e.MaxRecord})
	return &Conn{Session: sess, Path: cand.path, Hello: hello}, nil
}

// ErrAgentOffline is returned when the relay has no agent for this code.
var ErrAgentOffline = errors.New("client: agent is offline")

func (c *Client) upgradeLoop(ctx context.Context) {
	t := time.NewTicker(c.opts.UpgradeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.mu.Lock()
		cur := c.cur
		var cands []candidate
		if cur != nil && cur.Path == "relay" && cur.alive() && c.dialing == nil {
			for _, u := range c.direct {
				cands = append(cands, candidate{path: "direct " + u, url: u})
			}
		}
		c.mu.Unlock()
		if len(cands) == 0 {
			continue
		}
		conn, err := c.race(ctx, cands)
		if err != nil {
			continue
		}
		c.mu.Lock()
		if c.closed || c.cur != cur {
			c.mu.Unlock()
			conn.Close()
			continue
		}
		c.install(conn)
		c.mu.Unlock()
	}
}

// HTTPClient returns an http.Client whose requests travel over the
// session. The URL's scheme and host are ignored; only path and query are
// sent. Each request redials if the session is gone.
func (c *Client) HTTPClient() *http.Client {
	return &http.Client{Transport: c.Transport()}
}

// Transport is the http.RoundTripper behind HTTPClient.
func (c *Client) Transport() http.RoundTripper {
	return &httpkind.Transport{Opener: func(ctx context.Context) (httpkind.Opener, error) {
		conn, err := c.Conn(ctx)
		if err != nil {
			return nil, err
		}
		return conn.Session, nil
	}}
}

// Open opens a stream of any kind on the current session.
func (c *Client) Open(ctx context.Context, kind string, meta any) (*mux.Stream, error) {
	conn, err := c.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Session.Open(ctx, kind, meta)
}

// DialWebSocket opens a WebSocket to path on the agent.
func (c *Client) DialWebSocket(ctx context.Context, path string, protocols ...string) (*wskind.Conn, string, error) {
	conn, err := c.Conn(ctx)
	if err != nil {
		return nil, "", err
	}
	st, raw, err := conn.Session.OpenAndWait(ctx, wskind.Kind, wskind.OpenMeta{Path: path, Protocols: protocols})
	if err != nil {
		return nil, "", err
	}
	var am wskind.AcceptMeta
	_ = json.Unmarshal(raw, &am)
	return wskind.New(st), am.Protocol, nil
}

// DialTCP opens a raw byte stream to one of the agent's named TCP targets
// ("" is the default target).
func (c *Client) DialTCP(ctx context.Context, target string) (net.Conn, error) {
	conn, err := c.Conn(ctx)
	if err != nil {
		return nil, err
	}
	st, _, err := conn.Session.OpenAndWait(ctx, "tcp", map[string]string{"target": target})
	if err != nil {
		return nil, err
	}
	return StreamConn(st, conn.Path), nil
}

// Conn is one established session.
type Conn struct {
	Session *mux.Session
	Path    string
	Hello   *e2e.AgentHello
}

func (c *Conn) alive() bool {
	select {
	case <-c.Session.Done():
		return false
	case <-c.Session.RemoteGoAway():
		return false
	default:
		return true
	}
}

// Close closes the session.
func (c *Conn) Close() { c.Session.Close() }

// drain lets in-flight streams finish before closing (used when switching
// paths so a path upgrade never cuts a running request).
func (c *Conn) drain() {
	c.Session.GoAway(mux.CodeGoingAway, "client switched path")
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) && c.Session.NumStreams() > 0 {
		select {
		case <-c.Session.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	c.Session.Close()
}
