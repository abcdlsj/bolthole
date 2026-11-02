// Package agent is the service side of the relay: it dials out to a relay,
// keeps that connection alive across failures, and dispatches incoming
// streams (plaintext or end-to-end encrypted) to handlers.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/e2e"
	"github.com/abcdlsj/bolthole/kinds/wskind"
	"github.com/abcdlsj/bolthole/mux"
)

// Version is reported to relays in the hello.
const Version = "0.2.0"

// Info describes where a stream came from.
type Info struct {
	// Encrypted is true for streams opened inside an E2E session.
	Encrypted bool
	// Session is the session the stream arrived on. Inside an E2E session
	// the agent can use it to open streams back to the client.
	Session *mux.Session
}

// Handler serves one inbound stream. It owns the stream and must close or
// reset it.
type Handler interface {
	ServeStream(ctx context.Context, st *mux.Stream, info Info)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, st *mux.Stream, info Info)

// ServeStream calls f.
func (f HandlerFunc) ServeStream(ctx context.Context, st *mux.Stream, info Info) { f(ctx, st, info) }

// Status is reported to Agent.OnStatus on every connection change.
type Status struct {
	Connected bool
	Attempt   int           // consecutive failed attempts
	Retry     time.Duration // delay before the next attempt (when !Connected)
	Err       error
}

// Agent connects a service to a relay. Fill the fields, then call Run.
type Agent struct {
	// Relay is the relay's base URL (http, https, ws or wss).
	Relay string
	// Code is the connect code. Clients need it (via the connection string)
	// to reach this agent.
	Code e2e.Code
	// Name is shown in relay logs. Optional.
	Name string
	// Auth is the registration key or JWT, if the relay requires one.
	Auth string
	// Public also serves plaintext streams opened by the relay, reachable
	// at <relay>/c/<routing token>/. Off by default: E2E only.
	Public bool
	// Handler serves streams. Usually a *Router.
	Handler Handler
	// OnSession is called for each new E2E mux session (one per connected
	// client), e.g. to open streams back to that client. Optional.
	OnSession func(ctx context.Context, s *mux.Session, hello *e2e.ClientHello)
	// Protos adds application protocols besides the built-in mux. A client
	// offering one of these names gets the raw encrypted message channel
	// instead of a mux session. Optional.
	Protos map[string]func(ctx context.Context, conn mux.Conn, hello *e2e.ClientHello)
	// Direct lists URLs where clients can reach this agent without the
	// relay (see ListenDirect). They are sent inside the encrypted
	// handshake, never to the relay.
	Direct []string
	// OnStatus observes connection changes. Optional.
	OnStatus func(Status)
	// Logger, MinBackoff, MaxBackoff and HTTPClient have sane defaults.
	Logger     *slog.Logger
	MinBackoff time.Duration
	MaxBackoff time.Duration
	HTTPClient *http.Client

	keys e2e.Keys
}

// PermanentError means retrying will not help (bad credentials, bad code).
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// ConnString returns the connection string clients use to reach this agent.
func (a *Agent) ConnString() (string, error) { return e2e.FormatConnString(httpBase(a.Relay), a.Code) }

// PublicURL is where plaintext traffic reaches this agent when Public is set.
func (a *Agent) PublicURL() string {
	return strings.TrimSuffix(httpBase(a.Relay), "/") + "/c/" + a.Code.Derive().RoutingToken + "/"
}

// Run connects and serves until ctx is cancelled or a permanent error occurs.
// Transient failures are retried with exponential backoff and full jitter.
func (a *Agent) Run(ctx context.Context) error {
	if a.Handler == nil {
		return errors.New("agent: Handler is required")
	}
	if a.Logger == nil {
		a.Logger = slog.New(slog.DiscardHandler)
	}
	if a.MinBackoff <= 0 {
		a.MinBackoff = 500 * time.Millisecond
	}
	if a.MaxBackoff <= 0 {
		a.MaxBackoff = 30 * time.Second
	}
	a.keys = a.Code.Derive()

	var wg sync.WaitGroup
	defer wg.Wait()
	attempt := 0
	for {
		start := time.Now()
		sess, err := a.connect(ctx)
		if err == nil {
			attempt = 0
			a.status(Status{Connected: true})
			a.Logger.Info("connected to relay", "relay", a.Relay)
			wg.Add(1)
			go func() { defer wg.Done(); a.serve(ctx, sess, false) }()
			select {
			case <-sess.Done():
				err = sess.Err()
			case <-sess.RemoteGoAway():
				// Make-before-break: the old session keeps serving its
				// streams while we dial a replacement right away.
				a.Logger.Info("relay is going away; reconnecting")
				go func() { <-time.After(2 * time.Minute); sess.Close() }()
				continue
			case <-ctx.Done():
				sess.GoAway(mux.CodeGoingAway, "agent shutting down")
				sess.Close()
				return ctx.Err()
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var perm *PermanentError
		if errors.As(err, &perm) {
			a.status(Status{Err: err})
			return err
		}
		if time.Since(start) > 30*time.Second {
			attempt = 0
		}
		delay := a.backoff(attempt)
		if websocket.CloseStatus(err) == 4000 {
			delay = a.MaxBackoff // replaced by another agent with our code
		}
		attempt++
		a.status(Status{Attempt: attempt, Retry: delay, Err: err})
		a.Logger.Warn("relay connection lost", "err", err, "retry_in", delay.Round(time.Millisecond))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *Agent) backoff(attempt int) time.Duration {
	d := a.MinBackoff << min(attempt, 16)
	if d > a.MaxBackoff || d <= 0 {
		d = a.MaxBackoff
	}
	return a.MinBackoff/2 + rand.N(d) // full jitter, with a small floor
}

func (a *Agent) status(s Status) {
	if a.OnStatus != nil {
		a.OnStatus(s)
	}
}

type hello struct {
	Type      string   `json:"type"`
	V         int      `json:"v"`
	TokenHash string   `json:"token_hash"`
	Name      string   `json:"name,omitempty"`
	Auth      string   `json:"auth,omitempty"`
	Version   string   `json:"version,omitempty"`
	Caps      []string `json:"caps,omitempty"`
}

type welcome struct {
	Type        string `json:"type"`
	V           int    `json:"v"`
	HeartbeatMS int    `json:"heartbeat_ms"`
	MaxStreams  int    `json:"max_streams"`
	MaxFrame    int    `json:"max_frame"`
	Code        string `json:"code"`
	Message     string `json:"message"`
}

func (a *Agent) connect(ctx context.Context) (*mux.Session, error) {
	u, err := url.Parse(wsBase(a.Relay))
	if err != nil {
		return nil, &PermanentError{fmt.Errorf("agent: bad relay URL: %w", err)}
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/agent"
	u.RawQuery = "th=" + a.keys.TokenHash

	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(dctx, u.String(), &websocket.DialOptions{
		Subprotocols: []string{"relay.v1"},
		HTTPClient:   a.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	c.SetReadLimit(mux.DefaultMaxFrame)
	h, _ := json.Marshal(hello{Type: "hello", V: 1, TokenHash: a.keys.TokenHash, Name: a.Name, Auth: a.Auth, Version: Version, Caps: []string{"mux/1"}})
	if err := c.Write(dctx, websocket.MessageText, h); err != nil {
		c.CloseNow()
		return nil, err
	}
	typ, msg, err := c.Read(dctx)
	if err != nil {
		switch websocket.CloseStatus(err) {
		case 4002, 4003:
			return nil, &PermanentError{fmt.Errorf("agent: relay refused registration: %w", err)}
		}
		return nil, err
	}
	var w welcome
	if typ != websocket.MessageText || json.Unmarshal(msg, &w) != nil {
		c.CloseNow()
		return nil, errors.New("agent: bad welcome from relay")
	}
	if w.Type == "error" {
		c.CloseNow()
		err := fmt.Errorf("agent: relay refused registration: %s: %s", w.Code, w.Message)
		if w.Code == "unauthorized" || w.Code == "invalid_token_hash" {
			return nil, &PermanentError{err}
		}
		return nil, err
	}
	if w.Type != "welcome" {
		c.CloseNow()
		return nil, errors.New("agent: unexpected first message from relay")
	}
	cfg := mux.Config{Dialer: true, Logger: a.Logger}
	if w.HeartbeatMS > 0 {
		cfg.Heartbeat = time.Duration(w.HeartbeatMS) * time.Millisecond
	}
	return mux.NewSession(mux.WebSocket(c), cfg), nil
}

// serve accepts streams on a session until it ends.
func (a *Agent) serve(ctx context.Context, sess *mux.Session, encrypted bool) {
	for {
		st, err := sess.Accept(ctx)
		if err != nil {
			return
		}
		go a.dispatch(ctx, sess, st, encrypted)
	}
}

func (a *Agent) dispatch(ctx context.Context, sess *mux.Session, st *mux.Stream, encrypted bool) {
	if !encrypted && st.Kind() == wskind.Kind {
		var m wskind.OpenMeta
		if st.DecodeMeta(&m) == nil && pathOnly(m.Path) == e2e.Path {
			a.serveE2E(ctx, st)
			return
		}
	}
	if !encrypted && !a.Public {
		st.Reset(mux.CodeE2ERequired, "this agent only accepts end-to-end encrypted sessions")
		return
	}
	a.Handler.ServeStream(ctx, st, Info{Encrypted: encrypted, Session: sess})
}

func (a *Agent) serveE2E(ctx context.Context, st *mux.Stream) {
	if err := st.Accept(wskind.AcceptMeta{}); err != nil {
		return
	}
	ws := wskind.New(st)
	a.ServeSecure(ctx, ws.MessageConn())
	st.Reset(mux.CodeCancel, "")
}

// ServeSecure runs the agent side of an E2E handshake on conn (a WebSocket
// from the relay or a direct one) and serves the session until it ends.
// It is exported so you can accept E2E connections from any transport.
func (a *Agent) ServeSecure(ctx context.Context, conn mux.Conn) {
	if a.keys.TokenHash == "" {
		a.keys = a.Code.Derive()
	}
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	stop := context.AfterFunc(hctx, func() { _ = conn.Close() })
	secure, hello, err := e2e.Agent(hctx, conn, a.keys.PSK, a.chooseProto)
	ok := stop()
	cancel()
	if err != nil || !ok {
		a.Logger.Debug("e2e handshake failed", "err", err)
		_ = conn.Close()
		return
	}
	proto := a.pickProto(hello.Protos)
	if proto != e2e.ProtoMux {
		a.Protos[proto](ctx, secure, hello)
		return
	}
	inner := mux.NewSession(secure, mux.Config{Dialer: false, Logger: a.Logger, MaxFrame: e2e.MaxRecord})
	defer inner.Close()
	if a.OnSession != nil {
		go a.OnSession(ctx, inner, hello)
	}
	go func() {
		select {
		case <-ctx.Done():
			inner.Close()
		case <-inner.Done():
		}
	}()
	a.serve(ctx, inner, true)
}

func (a *Agent) pickProto(offered []string) string {
	for _, p := range offered {
		if p == e2e.ProtoMux {
			return p
		}
		if _, ok := a.Protos[p]; ok {
			return p
		}
	}
	return ""
}

func (a *Agent) chooseProto(h *e2e.ClientHello) (*e2e.AgentHello, error) {
	p := a.pickProto(h.Protos)
	if p == "" {
		return nil, errors.New("agent: no common application protocol")
	}
	return &e2e.AgentHello{Proto: p, Name: a.Name, Direct: a.Direct}, nil
}

func pathOnly(p string) string {
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		return p[:i]
	}
	return p
}

func httpBase(s string) string {
	switch {
	case strings.HasPrefix(s, "ws://"):
		return "http://" + s[5:]
	case strings.HasPrefix(s, "wss://"):
		return "https://" + s[6:]
	}
	return s
}

func wsBase(s string) string {
	switch {
	case strings.HasPrefix(s, "http://"):
		return "ws://" + s[7:]
	case strings.HasPrefix(s, "https://"):
		return "wss://" + s[8:]
	}
	return s
}
