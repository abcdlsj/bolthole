// Package mux implements the relay stream multiplexer (spec §3): framed,
// flow-controlled, bidirectional byte streams over any message transport.
//
// The same session type runs on the agent↔relay WebSocket and inside an
// end-to-end encrypted channel, so everything built on streams works in both
// places.
package mux

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

// Frame types (spec §3.1).
const (
	TypeOpen   byte = 0x01
	TypeAccept byte = 0x02
	TypeData   byte = 0x03
	TypeFin    byte = 0x04
	TypeReset  byte = 0x05
	TypeWindow byte = 0x06
	TypePing   byte = 0x07
	TypePong   byte = 0x08
	TypeGoAway byte = 0x09
)

const (
	// HeaderSize is the fixed frame header: type (1) + stream id (4).
	HeaderSize = 5
	// DefaultMaxFrame is the largest frame accepted by default.
	DefaultMaxFrame = 1<<20 + HeaderSize
	// InitialWindow is the per-direction, per-stream starting credit.
	InitialWindow = 4 << 20
	// MaxChunk is the largest DATA payload this implementation sends.
	MaxChunk = 32 * 1024
)

// Frame is one decoded mux frame.
type Frame struct {
	Type     byte
	StreamID uint32
	Payload  []byte
}

// ErrShortFrame is returned for a message shorter than the header.
var ErrShortFrame = errors.New("mux: frame shorter than header")

// Encode serialises f into a new buffer.
func (f Frame) Encode() []byte {
	b := make([]byte, HeaderSize+len(f.Payload))
	b[0] = f.Type
	binary.BigEndian.PutUint32(b[1:5], f.StreamID)
	copy(b[HeaderSize:], f.Payload)
	return b
}

// Decode parses one frame. The payload aliases data.
func Decode(data []byte) (Frame, error) {
	if len(data) < HeaderSize {
		return Frame{}, ErrShortFrame
	}
	return Frame{Type: data[0], StreamID: binary.BigEndian.Uint32(data[1:5]), Payload: data[HeaderSize:]}, nil
}

// OpenPayload is the body of an OPEN frame.
type OpenPayload struct {
	Kind string          `json:"kind"`
	Meta json.RawMessage `json:"meta,omitempty"`
}

// ResetError is carried by RESET and GOAWAY frames, and is the error a
// stream operation returns after the peer resets it.
type ResetError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

func (e *ResetError) Error() string {
	if e.Message == "" {
		return "stream reset: " + e.Code
	}
	return fmt.Sprintf("stream reset: %s: %s", e.Code, e.Message)
}

// Reset codes (spec §3.5).
const (
	CodeCancel          = "cancel"
	CodeRefused         = "refused"
	CodeUnsupportedKind = "unsupported_kind"
	CodeNotFound        = "not_found"
	CodeForbidden       = "forbidden"
	CodeE2ERequired     = "e2e_required"
	CodeTimeout         = "timeout"
	CodeFlowControl     = "flow_control"
	CodeProtocolError   = "protocol_error"
	CodeInternal        = "internal"
	CodeGoingAway       = "going_away"
	CodeUnknownStream   = "unknown_stream"
)

func encodeJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // only called with types that always marshal
	}
	return b
}
