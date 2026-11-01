package client

import (
	"net"

	"github.com/abcdlsj/bolthole/mux"
)

// StreamConn wraps a stream as a net.Conn (for SSH, database drivers, or
// anything else that wants a socket).
func StreamConn(st *mux.Stream, path string) net.Conn { return &streamConn{Stream: st, path: path} }

type streamConn struct {
	*mux.Stream
	path string
}

type addr string

func (a addr) Network() string { return "bolthole" }
func (a addr) String() string  { return string(a) }

func (c *streamConn) LocalAddr() net.Addr  { return addr("client") }
func (c *streamConn) RemoteAddr() net.Addr { return addr("agent via " + c.path) }

// CloseWrite half-closes, so protocols that rely on EOF work.
func (c *streamConn) CloseWrite() error { return c.Stream.CloseWrite() }
