package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/e2e"
	"github.com/abcdlsj/bolthole/mux"
)

// DirectHandler serves the same E2E entry point the relay forwards to, so
// clients on the same network (LAN, VPN, Tailscale…) can skip the relay.
// The handshake is identical, so a direct connection is exactly as private
// and authenticated as a relayed one; it is just shorter.
func (a *Agent) DirectHandler(ctx context.Context) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET "+e2e.Path, func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		c.SetReadLimit(mux.DefaultMaxFrame)
		a.ServeSecure(ctx, mux.WebSocket(c))
	})
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		fmt.Fprint(w, `{"ok":true,"edition":"agent-direct"}`)
	})
	return m
}

// ListenDirect serves DirectHandler on addr (e.g. ":7777") until ctx ends,
// and adds a ws:// URL for every local interface address to a.Direct so
// connected clients learn where to find it. Call it before Run.
func (a *Agent) ListenDirect(ctx context.Context, addr string) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcp := ln.Addr().(*net.TCPAddr)
	if tcp.IP.IsUnspecified() {
		a.Direct = append(a.Direct, LocalURLs(tcp.Port)...)
	} else { // bound to one address: advertise exactly that
		a.Direct = append(a.Direct, fmt.Sprintf("ws://%s%s", tcp.String(), e2e.Path))
	}
	srv := &http.Server{Handler: a.DirectHandler(ctx), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && a.Logger != nil {
			a.Logger.Warn("direct listener stopped", "err", err)
		}
	}()
	return ln.Addr(), nil
}

// LocalURLs lists ws://<ip>:<port> for each non-loopback unicast address,
// private ranges first.
func LocalURLs(port int) []string {
	var private, other []string
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() || ipn.IP.IsLinkLocalUnicast() || ipn.IP.IsMulticast() {
			continue
		}
		u := fmt.Sprintf("ws://%s%s", net.JoinHostPort(ipn.IP.String(), fmt.Sprint(port)), e2e.Path)
		if ipn.IP.IsPrivate() || isCGNAT(ipn.IP) {
			private = append(private, u)
		} else {
			other = append(other, u)
		}
	}
	return append(private, other...)
}

// isCGNAT matches 100.64.0.0/10, which Tailscale uses.
func isCGNAT(ip net.IP) bool {
	v4 := ip.To4()
	return v4 != nil && v4[0] == 100 && v4[1]&0xc0 == 64
}
