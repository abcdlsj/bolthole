package bolthole_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/agent"
	"github.com/abcdlsj/bolthole/client"
	"github.com/abcdlsj/bolthole/e2e"
	"github.com/abcdlsj/bolthole/kinds/wskind"
	"github.com/abcdlsj/bolthole/mux"
	"github.com/abcdlsj/bolthole/relay"
)

type env struct {
	relay   *httptest.Server
	srv     *relay.Server
	agent   *agent.Agent
	conn    string
	cancel  context.CancelFunc
	echo    net.Listener
	restart func() *relay.Server // swaps in a fresh relay process
}

func appHandler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello %s", r.URL.Query().Get("name"))
	})
	m.HandleFunc("POST /sum", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var s int
		for _, c := range b {
			s += int(c)
		}
		w.Header().Set("x-len", fmt.Sprint(len(b)))
		fmt.Fprintf(w, "%d", s)
	})
	m.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/event-stream")
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	})
	return m
}

func setup(t *testing.T, public bool, auth relay.Authenticator, agentAuth string) *env {
	t.Helper()
	srv := relay.New(relay.Config{Auth: auth, AgentWait: 2 * time.Second})
	var current atomic.Pointer[relay.Server]
	current.Store(srv)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current.Load().ServeHTTP(w, r)
	}))
	echo, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	r := agent.NewRouter().
		HTTP(appHandler()).
		TCP(map[string]string{"echo": echo.Addr().String()}).
		WebSocket(func(ctx context.Context, c *wskind.Conn, m wskind.OpenMeta, _ agent.Info) {
			for {
				op, p, err := c.Read()
				if err != nil {
					c.Close(1000, "")
					return
				}
				c.Write(op, append([]byte(m.Path+":"), p...))
			}
		})
	a := &agent.Agent{Relay: ts.URL, Code: e2e.NewCode(), Handler: r, Public: public, Auth: agentAuth, Name: "test"}
	ctx, cancel := context.WithCancel(context.Background())
	connected := make(chan struct{}, 1)
	a.OnStatus = func(s agent.Status) {
		if s.Connected {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	}
	go a.Run(ctx)
	cs, _ := a.ConnString()
	e := &env{relay: ts, srv: srv, agent: a, conn: cs, cancel: cancel, echo: echo}
	e.restart = func() *relay.Server {
		next := relay.New(relay.Config{Auth: auth, AgentWait: 2 * time.Second})
		current.Store(next)
		return next
	}
	t.Cleanup(func() { cancel(); ts.Close(); echo.Close() })
	if agentAuth != "bad" {
		select {
		case <-connected:
		case <-time.After(5 * time.Second):
			t.Fatal("agent never connected")
		}
	}
	return e
}

func newClient(t *testing.T, cs string) *client.Client {
	c, err := client.New(cs, &client.Options{UpgradeInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestE2EHTTP(t *testing.T) {
	e := setup(t, false, nil, "")
	c := newClient(t, e.conn)
	hc := c.HTTPClient()

	resp, err := hc.Get("http://agent/hello?name=bolthole")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "hello bolthole" {
		t.Fatalf("got %q", b)
	}
	if c.Path() != "relay" {
		t.Fatalf("path %q", c.Path())
	}

	// A 3 MiB upload exercises flow control through the encrypted channel.
	body := make([]byte, 3<<20)
	rand.Read(body)
	var want int
	for _, x := range body {
		want += int(x)
	}
	resp, err = hc.Post("http://agent/sum", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != fmt.Sprint(want) || resp.Header.Get("x-len") != fmt.Sprint(len(body)) {
		t.Fatalf("sum %s (want %d), x-len %s", b, want, resp.Header.Get("x-len"))
	}

	// Server-sent events arrive incrementally.
	resp, err = hc.Get("http://agent/events")
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(resp.Body)
	var got []string
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data:") {
			got = append(got, sc.Text())
		}
	}
	resp.Body.Close()
	if strings.Join(got, ",") != "data: 0,data: 1,data: 2" {
		t.Fatalf("events %v", got)
	}
}

func TestE2EWebSocketAndTCP(t *testing.T) {
	e := setup(t, false, nil, "")
	c := newClient(t, e.conn)
	ctx := context.Background()

	ws, _, err := c.DialWebSocket(ctx, "/chat")
	if err != nil {
		t.Fatal(err)
	}
	ws.Write(wskind.OpText, []byte("hi"))
	_, p, err := ws.Read()
	if err != nil || string(p) != "/chat:hi" {
		t.Fatalf("ws got %q %v", p, err)
	}
	ws.Close(1000, "")

	conn, err := c.DialTCP(ctx, "echo")
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("ping over tcp"))
	buf := make([]byte, 13)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping over tcp" {
		t.Fatalf("tcp got %q %v", buf, err)
	}
	conn.Close()

	if _, err := c.DialTCP(ctx, "not-configured"); err == nil {
		t.Fatal("unknown TCP target must be refused")
	}
}

func TestPlaintextRefusedUnlessPublic(t *testing.T) {
	e := setup(t, false, nil, "")
	tok := e2e.Code(e.agent.Code).Derive().RoutingToken
	resp, err := http.Get(e.relay.URL + "/c/" + tok + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("plaintext to an E2E agent: status %d, want 403", resp.StatusCode)
	}
}

func TestPublicMode(t *testing.T) {
	e := setup(t, true, nil, "")
	resp, err := http.Get(e.agent.PublicURL() + "hello?name=web")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "hello web" {
		t.Fatalf("got %d %q", resp.StatusCode, b)
	}
	// Plain WebSocket through the relay.
	wsURL := "ws" + strings.TrimPrefix(e.agent.PublicURL(), "http") + "room"
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.Write(context.Background(), websocket.MessageText, []byte("yo"))
	_, p, err := conn.Read(context.Background())
	if err != nil || string(p) != "/room:yo" {
		t.Fatalf("got %q %v", p, err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

func TestWrongCodeFails(t *testing.T) {
	e := setup(t, false, nil, "")
	// Same relay, but a code no agent holds: the relay has nobody to route to.
	other, _ := e2e.FormatConnString(e.relay.URL, e2e.NewCode())
	c := newClient(t, other)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Conn(ctx); !errors.Is(err, client.ErrAgentOffline) {
		t.Fatalf("want ErrAgentOffline, got %v", err)
	}
}

// An attacker who knows the routing token (e.g. the relay operator) but
// not the code cannot complete the handshake.
func TestRelayCannotImpersonateClient(t *testing.T) {
	e := setup(t, false, nil, "")
	keys := e2e.Code(e.agent.Code).Derive()
	wsURL := "ws" + strings.TrimPrefix(e.relay.URL, "http") + "/c/" + keys.RoutingToken + e2e.Path
	ws, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wrongPSK [32]byte
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := e2e.Client(ctx, mux.WebSocket(ws), wrongPSK, e2e.ClientHello{}); err == nil {
		t.Fatal("handshake with a wrong PSK succeeded")
	}
}

func TestAuthKeys(t *testing.T) {
	e := setup(t, false, relay.SharedKey("s3cret"), "s3cret")
	c := newClient(t, e.conn)
	if _, err := c.HTTPClient().Get("http://agent/hello"); err != nil {
		t.Fatal(err)
	}

	srv := relay.New(relay.Config{Auth: relay.SharedKey("s3cret")})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	a := &agent.Agent{Relay: ts.URL, Code: e2e.NewCode(), Handler: agent.NewRouter(), Auth: "wrong"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var perm *agent.PermanentError
	if err := a.Run(ctx); !errors.As(err, &perm) {
		t.Fatalf("wrong key: want PermanentError, got %v", err)
	}
}

func TestJWT(t *testing.T) {
	secret := []byte("team-secret")
	tok := relay.MintJWT(secret, "alice", time.Hour)
	if sub, err := relay.VerifyJWT(secret, tok, time.Now()); err != nil || sub != "alice" {
		t.Fatalf("verify: %q %v", sub, err)
	}
	if _, err := relay.VerifyJWT(secret, tok, time.Now().Add(2*time.Hour)); err == nil {
		t.Fatal("expired token accepted")
	}
	if _, err := relay.VerifyJWT([]byte("other"), tok, time.Now()); err == nil {
		t.Fatal("wrong secret accepted")
	}
}

// The agent reconnects after the relay drops it, and clients recover.
func TestReconnectAfterRelayRestart(t *testing.T) {
	e := setup(t, false, nil, "")
	c := newClient(t, e.conn)
	if _, err := c.HTTPClient().Get("http://agent/hello"); err != nil {
		t.Fatal(err)
	}
	// Simulate a deploy: the old process drains (GOAWAY), a new one takes
	// over the same address with an empty registry.
	old := e.srv
	e.restart()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	old.Shutdown(ctx)
	cancel()
	e.relay.CloseClientConnections()

	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := c.HTTPClient().Get("http://agent/hello?name=again")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(b) == "hello again" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("never recovered: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A direct candidate wins the race against the relay, and a client pinned
// to the relay ignores it.
func TestDirectPathRace(t *testing.T) {
	e := setup(t, false, nil, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := e.agent.ListenDirect(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	direct := "ws://" + addr.String() + e2e.Path

	c, _ := client.New(e.conn, &client.Options{Direct: []string{direct}, UpgradeInterval: -1})
	defer c.Close()
	if _, err := c.HTTPClient().Get("http://agent/hello"); err != nil {
		t.Fatal(err)
	}
	if c.Path() != "direct "+direct {
		t.Fatalf("path %q, want direct", c.Path())
	}

	r, _ := client.New(e.conn, &client.Options{Direct: []string{direct}, Mode: client.RelayOnly, UpgradeInterval: -1})
	defer r.Close()
	if _, err := r.HTTPClient().Get("http://agent/hello"); err != nil {
		t.Fatal(err)
	}
	if r.Path() != "relay" {
		t.Fatalf("pinned client path %q", r.Path())
	}
}

// A relayed client learns direct candidates from the encrypted hello and
// upgrades to them in the background.
func TestUpgradeToDirect(t *testing.T) {
	e := setup(t, false, nil, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, _ := e.agent.ListenDirect(ctx, "127.0.0.1:0")
	e.agent.Direct = []string{"ws://" + addr.String() + e2e.Path}

	var switched atomic.Int32
	c, _ := client.New(e.conn, &client.Options{UpgradeInterval: 100 * time.Millisecond, OnPath: func(p string) {
		if strings.HasPrefix(p, "direct") {
			switched.Add(1)
		}
	}})
	defer c.Close()
	// First connection: no direct candidates known yet, so the relay is used.
	if _, err := c.HTTPClient().Get("http://agent/hello"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for switched.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("never upgraded; path %q", c.Path())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := c.HTTPClient().Get("http://agent/hello"); err != nil {
		t.Fatal(err)
	}
}
