package agent

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/abcdlsj/bolthole/kinds/httpkind"
	"github.com/abcdlsj/bolthole/kinds/wskind"
	"github.com/abcdlsj/bolthole/mux"
)

// Router dispatches streams by kind. It is the usual Agent.Handler:
//
//	r := agent.NewRouter()
//	r.HTTP(myHandler)                         // kind "http"
//	r.TCP(map[string]string{"ssh": "127.0.0.1:22"}) // kind "tcp"
//	r.Handle("com.example.chat", chatHandler) // your own kind
type Router struct {
	mu    sync.RWMutex
	kinds map[string]Handler
}

// NewRouter returns an empty router.
func NewRouter() *Router { return &Router{kinds: map[string]Handler{}} }

// Handle registers h for streams of the given kind.
func (r *Router) Handle(kind string, h Handler) *Router {
	r.mu.Lock()
	r.kinds[kind] = h
	r.mu.Unlock()
	return r
}

// HandleFunc registers a function for a kind.
func (r *Router) HandleFunc(kind string, f func(ctx context.Context, st *mux.Stream, info Info)) *Router {
	return r.Handle(kind, HandlerFunc(f))
}

// ServeStream implements Handler.
func (r *Router) ServeStream(ctx context.Context, st *mux.Stream, info Info) {
	r.mu.RLock()
	h := r.kinds[st.Kind()]
	r.mu.RUnlock()
	if h == nil {
		st.Reset(mux.CodeUnsupportedKind, st.Kind())
		return
	}
	h.ServeStream(ctx, st, info)
}

// HTTP serves `http` streams with an ordinary http.Handler.
func (r *Router) HTTP(h http.Handler) *Router {
	return r.HandleFunc(httpkind.Kind, func(ctx context.Context, st *mux.Stream, _ Info) {
		httpkind.Serve(h, st)
	})
}

// WebSocket serves `ws` streams with fn, which receives the message
// connection already accepted.
func (r *Router) WebSocket(fn func(ctx context.Context, c *wskind.Conn, meta wskind.OpenMeta, info Info)) *Router {
	return r.HandleFunc(wskind.Kind, func(ctx context.Context, st *mux.Stream, info Info) {
		var m wskind.OpenMeta
		if err := st.DecodeMeta(&m); err != nil {
			st.Reset(mux.CodeProtocolError, "bad ws meta")
			return
		}
		if err := st.Accept(wskind.AcceptMeta{}); err != nil {
			return
		}
		fn(ctx, wskind.New(st), m, info)
	})
}

// TCPMeta is the OPEN meta of a `tcp` stream.
type TCPMeta struct {
	Target string `json:"target"`
}

// TCP serves `tcp` streams by dialing one of the named targets. The empty
// name is the default target. Unknown names are refused: the agent never
// dials an address chosen by the client.
func (r *Router) TCP(targets map[string]string) *Router {
	return r.HandleFunc("tcp", func(ctx context.Context, st *mux.Stream, _ Info) {
		var m TCPMeta
		_ = st.DecodeMeta(&m)
		addr, ok := targets[m.Target]
		if !ok {
			st.Reset(mux.CodeNotFound, "unknown target")
			return
		}
		d := net.Dialer{Timeout: 10 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			st.Reset(mux.CodeRefused, "dial failed")
			return
		}
		if err := st.Accept(nil); err != nil {
			conn.Close()
			return
		}
		Pipe(st, conn)
	})
}

// Pipe copies both ways between a stream and a net.Conn until both sides
// are done, propagating half-closes.
func Pipe(st *mux.Stream, conn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := io.Copy(conn, st)
		if tc, ok := conn.(interface{ CloseWrite() error }); ok && err == nil {
			_ = tc.CloseWrite()
		} else {
			_ = conn.Close()
		}
	}()
	go func() {
		defer wg.Done()
		_, err := io.Copy(st, conn)
		if err != nil {
			st.Reset(mux.CodeCancel, "")
			return
		}
		_ = st.CloseWrite()
	}()
	wg.Wait()
	_ = conn.Close()
	st.Close()
}
