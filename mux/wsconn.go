package mux

import (
	"context"
	"errors"
	"sync"

	"github.com/coder/websocket"
)

// WebSocket adapts a github.com/coder/websocket connection to Conn. Text
// messages are rejected: after the handshake, mux traffic is binary only.
func WebSocket(c *websocket.Conn) Conn { return &wsConn{c: c} }

type wsConn struct {
	c    *websocket.Conn
	once sync.Once
}

func (w *wsConn) ReadMessage(ctx context.Context) ([]byte, error) {
	typ, b, err := w.c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("mux: unexpected text message")
	}
	return b, nil
}

func (w *wsConn) WriteMessage(ctx context.Context, msg []byte) error {
	return w.c.Write(ctx, websocket.MessageBinary, msg)
}

func (w *wsConn) Close() error {
	w.once.Do(func() { _ = w.c.Close(websocket.StatusNormalClosure, "") })
	return nil
}

// Pipe returns two connected in-memory Conns, for tests and in-process use.
func Pipe() (Conn, Conn) {
	ab := make(chan []byte, 64)
	ba := make(chan []byte, 64)
	done := make(chan struct{})
	var once sync.Once
	closeFn := func() { once.Do(func() { close(done) }) }
	return &pipeConn{in: ba, out: ab, done: done, close: closeFn},
		&pipeConn{in: ab, out: ba, done: done, close: closeFn}
}

type pipeConn struct {
	in, out chan []byte
	done    chan struct{}
	close   func()
}

var errPipeClosed = errors.New("mux: pipe closed")

func (p *pipeConn) ReadMessage(ctx context.Context) ([]byte, error) {
	select {
	case m := <-p.in:
		return m, nil
	case <-p.done:
		return nil, errPipeClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeConn) WriteMessage(ctx context.Context, msg []byte) error {
	select {
	case p.out <- append([]byte(nil), msg...):
		return nil
	case <-p.done:
		return errPipeClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *pipeConn) Close() error { p.close(); return nil }
