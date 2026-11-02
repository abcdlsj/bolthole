// Package relay is the reference relay: a small, stateless router that
// pairs agents (dialing out) with clients (dialing in) and forwards bytes.
// It stores nothing on disk; it keeps only token hashes in memory and, for
// end-to-end encrypted sessions, sees nothing but ciphertext.
//
// Server is an http.Handler, so it mounts anywhere: its own process
// (cmd/bolthole serve), an existing Go server, or behind any reverse proxy.
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/e2e"
	"github.com/abcdlsj/bolthole/kinds/httpkind"
	"github.com/abcdlsj/bolthole/kinds/wskind"
	"github.com/abcdlsj/bolthole/mux"
)

// Version is reported by /healthz.
const Version = "0.2.0"

// Config tunes a Server. The zero value is a usable open relay.
type Config struct {
	// Auth decides which agents may register. Nil means anyone.
	Auth Authenticator
	// Registry tracks live agents. Nil means an in-memory registry.
	Registry Registry
	// MaxBodyBytes caps a tunneled HTTP request body (default 128 MiB).
	MaxBodyBytes int64
	// AgentWait is how long a client request waits for an agent that is
	// momentarily offline, e.g. reconnecting (default 5s).
	AgentWait time.Duration
	// HeadTimeout bounds the wait for an HTTP response head (default 120s).
	HeadTimeout time.Duration
	// Heartbeat is sent to agents in the welcome (default 20s).
	Heartbeat time.Duration
	// RegisterRate and ConnectRate are per-IP limits per minute
	// (defaults 30 and 600; negative disables).
	RegisterRate, ConnectRate int
	// TrustProxy uses X-Forwarded-For / CF-Connecting-IP for rate limits.
	TrustProxy bool
	Logger     *slog.Logger
}

// Server is the relay. Create it with New.
type Server struct {
	cfg       Config
	log       *slog.Logger
	reg       Registry
	regLimit  *limiter
	connLimit *limiter
	draining  atomic.Bool
	inflight  atomic.Int64
	mux       *http.ServeMux
}

// New returns a Server.
func New(cfg Config) *Server {
	if cfg.Auth == nil {
		cfg.Auth = Open()
	}
	if cfg.Registry == nil {
		cfg.Registry = NewMemoryRegistry()
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 128 << 20
	}
	if cfg.AgentWait == 0 {
		cfg.AgentWait = 5 * time.Second
	}
	if cfg.HeadTimeout <= 0 {
		cfg.HeadTimeout = 120 * time.Second
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 20 * time.Second
	}
	if cfg.RegisterRate == 0 {
		cfg.RegisterRate = 30
	}
	if cfg.ConnectRate == 0 {
		cfg.ConnectRate = 600
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		cfg: cfg, log: cfg.Logger, reg: cfg.Registry,
		regLimit:  newLimiter(cfg.RegisterRate),
		connLimit: newLimiter(cfg.ConnectRate),
		mux:       http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /v1/agent", s.handleAgent)
	s.mux.HandleFunc("/c/{token}/{rest...}", s.handleClient)
	s.mux.HandleFunc("/c/{token}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
	})
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		io.WriteString(w, "bolthole relay "+Version+" is running.\n")
	})
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Shutdown sends GOAWAY to every agent so they reconnect elsewhere, then
// waits for in-flight client traffic to finish or ctx to expire.
func (s *Server) Shutdown(ctx context.Context) error {
	s.draining.Store(true)
	for _, a := range s.reg.All() {
		a.Session.GoAway(mux.CodeGoingAway, "relay restarting")
	}
	for s.inflight.Load() > 0 && ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	for _, a := range s.reg.All() {
		a.close(websocket.StatusServiceRestart, "relay restarting")
	}
	return ctx.Err()
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "edition": "go", "version": Version, "agents": s.reg.Len()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("cache-control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.TrustProxy {
		if ip := r.Header.Get("cf-connecting-ip"); ip != "" {
			return ip
		}
		if xff := r.Header.Get("x-forwarded-for"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

// Hello is the agent's first message.
type Hello struct {
	Type      string   `json:"type"`
	V         int      `json:"v"`
	TokenHash string   `json:"token_hash"`
	Name      string   `json:"name"`
	Auth      string   `json:"auth"`
	Version   string   `json:"version"`
	Caps      []string `json:"caps"`
}

// Close codes sent to agents.
const (
	CloseReplaced      websocket.StatusCode = 4000
	CloseProtocol      websocket.StatusCode = 4001
	CloseBadTokenHash  websocket.StatusCode = 4002
	CloseUnauthorized  websocket.StatusCode = 4003
	CloseRateLimited   websocket.StatusCode = 4004
	closeHelloDeadline                      = 10 * time.Second
)

func (s *Server) handleAgent(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "going_away"})
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"relay.v1"}, InsecureSkipVerify: true})
	if err != nil {
		return
	}
	c.SetReadLimit(mux.DefaultMaxFrame)
	fail := func(code websocket.StatusCode, errCode, msg string) {
		b, _ := json.Marshal(map[string]string{"type": "error", "code": errCode, "message": msg})
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		_ = c.Write(ctx, websocket.MessageText, b)
		_ = c.Close(code, msg)
	}
	if !s.regLimit.allow(s.clientIP(r)) {
		fail(CloseRateLimited, "rate_limited", "too many registrations")
		return
	}
	hctx, cancel := context.WithTimeout(r.Context(), closeHelloDeadline)
	typ, msg, err := c.Read(hctx)
	cancel()
	if err != nil {
		c.CloseNow()
		return
	}
	var h Hello
	if typ != websocket.MessageText || json.Unmarshal(msg, &h) != nil || h.Type != "hello" || h.V != 1 {
		fail(CloseProtocol, "protocol_error", "expected hello v1")
		return
	}
	if !e2e.ValidTokenHash(h.TokenHash) || (r.URL.Query().Has("th") && r.URL.Query().Get("th") != h.TokenHash) {
		fail(CloseBadTokenHash, "invalid_token_hash", "token_hash must be 64 lowercase hex characters")
		return
	}
	subject, err := s.cfg.Auth.Authenticate(r.Context(), r, h)
	if err != nil {
		fail(CloseUnauthorized, "unauthorized", err.Error())
		return
	}
	welcome, _ := json.Marshal(map[string]any{
		"type": "welcome", "v": 1,
		"heartbeat_ms": s.cfg.Heartbeat.Milliseconds(),
		"max_streams":  256, "max_frame": mux.DefaultMaxFrame,
	})
	if err := c.Write(r.Context(), websocket.MessageText, welcome); err != nil {
		c.CloseNow()
		return
	}
	sess := mux.NewSession(mux.WebSocket(c), mux.Config{Dialer: false, Heartbeat: s.cfg.Heartbeat, Logger: s.log})
	a := &AgentConn{Session: sess, ws: c, Name: limitString(h.Name, 64), Subject: subject, TokenHash: h.TokenHash}
	if old := s.reg.Register(h.TokenHash, a); old != nil {
		old.close(CloseReplaced, "replaced by a newer connection")
	}
	s.log.Info("agent registered", "name", a.Name, "subject", subject, "agents", s.reg.Len())
	// The relay never accepts streams from agents in v1; reject politely.
	go func() {
		for {
			st, err := sess.Accept(context.Background())
			if err != nil {
				return
			}
			st.Reset(mux.CodeUnsupportedKind, "relay services are not enabled")
		}
	}()
	<-sess.Done()
	s.reg.Unregister(h.TokenHash, a)
	s.log.Info("agent disconnected", "name", a.Name, "err", sess.Err(), "agents", s.reg.Len())
}

func limitString(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

func (s *Server) handleClient(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	if !e2e.ValidToken(token) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if !s.connLimit.allow(s.clientIP(r)) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate_limited"})
		return
	}
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	a := s.reg.Lookup(r.Context(), e2e.HashToken(token), s.cfg.AgentWait)
	if a == nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "agent_offline"})
		return
	}
	path := "/" + r.PathValue("rest")
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	if isWebSocket(r) {
		s.bridgeWebSocket(w, r, a, path)
		return
	}
	s.bridgeHTTP(w, r, a, path)
}

func isWebSocket(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("connection")), "upgrade")
}

// resetStatus maps a stream reset to an HTTP status (spec §2).
func resetStatus(err error) (int, string) {
	var re *mux.ResetError
	if !errors.As(err, &re) {
		return http.StatusBadGateway, "agent_unavailable"
	}
	switch re.Code {
	case mux.CodeNotFound:
		return http.StatusNotFound, re.Code
	case mux.CodeForbidden, mux.CodeE2ERequired:
		return http.StatusForbidden, re.Code
	case mux.CodeRefused, mux.CodeGoingAway, "overloaded":
		return http.StatusServiceUnavailable, re.Code
	case mux.CodeTimeout:
		return http.StatusGatewayTimeout, re.Code
	}
	return http.StatusBadGateway, re.Code
}

func (s *Server) bridgeHTTP(w http.ResponseWriter, r *http.Request, a *AgentConn, path string) {
	ctx := r.Context()
	headers := httpkind.EncodeHeaders(r.Header)
	delete(headers, "content-length")
	if r.ContentLength > 0 {
		headers["content-length"] = []string{itoa(r.ContentLength)}
	}
	st, err := a.Session.Open(ctx, httpkind.Kind, httpkind.RequestMeta{Method: r.Method, Path: path, Headers: headers})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "agent_unavailable"})
		return
	}
	defer st.Close()
	stop := context.AfterFunc(ctx, func() { st.Reset(mux.CodeCancel, "") })
	defer stop()

	go func() {
		body := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
		if _, err := io.Copy(st, body); err != nil {
			st.Reset(mux.CodeCancel, "request body failed")
			return
		}
		_ = st.CloseWrite()
	}()

	hctx, cancel := context.WithTimeout(ctx, s.cfg.HeadTimeout)
	raw, err := st.WaitAccept(hctx)
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			st.Reset(mux.CodeTimeout, "")
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timeout"})
			return
		}
		status, code := resetStatus(err)
		writeJSON(w, status, map[string]string{"error": code})
		return
	}
	var rm httpkind.ResponseMeta
	if json.Unmarshal(raw, &rm) != nil || rm.Status < 200 || rm.Status > 999 {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "bad_response"})
		return
	}
	for k, v := range httpkind.DecodeHeaders(rm.Headers) {
		w.Header()[k] = v
	}
	w.WriteHeader(rm.Status)
	rc := http.NewResponseController(w)
	_ = rc.Flush()
	buf := make([]byte, 32*1024)
	for {
		n, err := st.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			_ = rc.Flush() // stream as it arrives: SSE and long responses work
		}
		if err != nil {
			if err != io.EOF {
				panic(http.ErrAbortHandler) // truncate rather than pretend success
			}
			return
		}
	}
}

func (s *Server) bridgeWebSocket(w http.ResponseWriter, r *http.Request, a *AgentConn, path string) {
	ctx := r.Context()
	var protos []string
	for _, p := range r.Header.Values("sec-websocket-protocol") {
		for _, x := range strings.Split(p, ",") {
			if x = strings.TrimSpace(x); x != "" {
				protos = append(protos, x)
			}
		}
	}
	meta := wskind.OpenMeta{Path: path, Headers: httpkind.EncodeHeaders(r.Header), Protocols: protos}
	st, err := a.Session.Open(ctx, wskind.Kind, meta)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "agent_unavailable"})
		return
	}
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	raw, err := st.WaitAccept(hctx)
	cancel()
	if err != nil {
		st.Reset(mux.CodeCancel, "")
		status, code := resetStatus(err)
		writeJSON(w, status, map[string]string{"error": code})
		return
	}
	var am wskind.AcceptMeta
	_ = json.Unmarshal(raw, &am)
	opts := &websocket.AcceptOptions{InsecureSkipVerify: true}
	if am.Protocol != "" {
		opts.Subprotocols = []string{am.Protocol}
	}
	c, err := websocket.Accept(w, r, opts)
	if err != nil {
		st.Reset(mux.CodeCancel, "")
		return
	}
	c.SetReadLimit(wskind.MaxRecord)
	wskind.Bridge(context.WithoutCancel(ctx), c, wskind.New(st))
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
