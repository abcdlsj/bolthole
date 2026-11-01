// Package wskind implements the `ws` stream kind (spec §4.2): WebSocket
// messages carried as length-prefixed records on a mux stream.
package wskind

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/abcdlsj/bolthole/mux"
)

// Kind is the stream kind name.
const Kind = "ws"

// Record opcodes.
const (
	OpText   byte = 0x01
	OpBinary byte = 0x02
	OpClose  byte = 0x08
)

// MaxRecord is the largest record payload accepted.
const MaxRecord = 16 << 20

// OpenMeta is the OPEN meta of a ws stream.
type OpenMeta struct {
	Path      string              `json:"path"`
	Headers   map[string][]string `json:"headers,omitempty"`
	Protocols []string            `json:"protocols,omitempty"`
}

// AcceptMeta is the ACCEPT meta of a ws stream.
type AcceptMeta struct {
	Protocol string `json:"protocol,omitempty"`
}

// CloseError is returned by Read after the peer sent a close record.
type CloseError struct {
	Code   int
	Reason string
}

func (e *CloseError) Error() string { return fmt.Sprintf("ws closed: %d %s", e.Code, e.Reason) }

// Conn reads and writes WebSocket messages on a stream.
type Conn struct {
	st  *mux.Stream
	br  *bufio.Reader
	wmu sync.Mutex
}

// New wraps a stream.
func New(st *mux.Stream) *Conn { return &Conn{st: st, br: bufio.NewReaderSize(st, 32*1024)} }

// Stream returns the underlying stream.
func (c *Conn) Stream() *mux.Stream { return c.st }

// Read returns the next message. After a close record it returns *CloseError.
func (c *Conn) Read() (op byte, payload []byte, err error) {
	var hdr [5]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > MaxRecord {
		c.st.Reset(mux.CodeProtocolError, "ws record too large")
		return 0, nil, errors.New("wskind: record too large")
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return 0, nil, err
	}
	switch hdr[0] {
	case OpText, OpBinary:
		return hdr[0], payload, nil
	case OpClose:
		ce := &CloseError{Code: 1005}
		if len(payload) >= 2 {
			ce.Code = int(binary.BigEndian.Uint16(payload))
			ce.Reason = string(payload[2:])
		}
		return OpClose, nil, ce
	default:
		return c.Read() // unknown opcodes are skipped
	}
}

// Write sends one message.
func (c *Conn) Write(op byte, payload []byte) error {
	if len(payload) > MaxRecord {
		return errors.New("wskind: message too large")
	}
	buf := make([]byte, 5+len(payload))
	buf[0] = op
	binary.BigEndian.PutUint32(buf[1:], uint32(len(payload)))
	copy(buf[5:], payload)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.st.Write(buf)
	return err
}

// Close sends a close record and FIN.
func (c *Conn) Close(code int, reason string) error {
	if len(reason) > 123 {
		reason = reason[:123]
	}
	p := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(p, uint16(code))
	copy(p[2:], reason)
	err := c.Write(OpClose, p)
	if cerr := c.st.CloseWrite(); err == nil {
		err = cerr
	}
	return err
}

// MessageConn adapts a ws stream to mux.Conn (binary or text messages are
// delivered as-is), which is how an E2E channel rides on top of it.
func (c *Conn) MessageConn() mux.Conn { return msgConn{c} }

type msgConn struct{ c *Conn }

func (m msgConn) ReadMessage(ctx context.Context) ([]byte, error) {
	_, p, err := m.c.Read()
	if _, ok := err.(*CloseError); ok {
		return nil, io.EOF
	}
	return p, err
}

func (m msgConn) WriteMessage(ctx context.Context, msg []byte) error {
	return m.c.Write(OpBinary, msg)
}

func (m msgConn) Close() error {
	_ = m.c.Close(1000, "")
	m.c.st.Reset(mux.CodeCancel, "")
	return nil
}
