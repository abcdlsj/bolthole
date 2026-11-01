package e2e

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/flynn/noise"

	"github.com/abcdlsj/bolthole/mux"
)

// Path is the reserved WebSocket path for the E2E entry point.
const Path = "/.bolthole/e2e"

// Prologue is mixed into every handshake so a transcript from another
// protocol can never be replayed into this one.
const Prologue = "bolthole/v1"

// ProtoMux is the application protocol that runs a mux session inside the
// encrypted channel. It is what the SDK speaks unless told otherwise.
const ProtoMux = "mux/1"

// MaxRecord is the largest plaintext a single record carries (Noise caps a
// message at 65535 bytes, including the 16-byte tag).
const MaxRecord = noise.MaxMsgLen - 16

// ErrHandshake is returned for every handshake failure. The reason is not
// distinguished on purpose.
var ErrHandshake = errors.New("e2e: handshake failed")

var suite = noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

// ClientHello is the encrypted payload of handshake message 1.
type ClientHello struct {
	// Protos lists acceptable application protocols, most preferred first.
	Protos []string `json:"protos"`
	// Name optionally identifies the client to the agent (for logs/UI).
	Name string `json:"name,omitempty"`
	// Extra is free for applications (e.g. a resume token). Max ~60 KiB total.
	Extra json.RawMessage `json:"extra,omitempty"`
}

// AgentHello is the encrypted payload of handshake message 2.
type AgentHello struct {
	Proto string `json:"proto"`
	Name  string `json:"name,omitempty"`
	// Direct lists URLs where the agent can also be reached without the
	// relay (e.g. ws://192.168.1.20:7777). Clients may race them.
	Direct []string        `json:"direct,omitempty"`
	Extra  json.RawMessage `json:"extra,omitempty"`
}

// Result is the outcome of a handshake.
type Result struct {
	Conn  mux.Conn // the encrypted message channel
	Hello any      // *AgentHello on the client, *ClientHello on the agent
}

func newState(psk [32]byte, initiator bool, r io.Reader) (*noise.HandshakeState, error) {
	return noise.NewHandshakeState(noise.Config{
		CipherSuite:           suite,
		Random:                r,
		Pattern:               noise.HandshakeNN,
		Initiator:             initiator,
		Prologue:              []byte(Prologue),
		PresharedKey:          psk[:],
		PresharedKeyPlacement: 0,
	})
}

// Client runs Noise_NNpsk0_25519_AESGCM_SHA256 as initiator over conn and
// returns the encrypted channel plus the agent's hello.
func Client(ctx context.Context, conn mux.Conn, psk [32]byte, hello ClientHello) (mux.Conn, *AgentHello, error) {
	return clientWith(ctx, conn, psk, hello, rand.Reader)
}

func clientWith(ctx context.Context, conn mux.Conn, psk [32]byte, hello ClientHello, r io.Reader) (mux.Conn, *AgentHello, error) {
	if len(hello.Protos) == 0 {
		hello.Protos = []string{ProtoMux}
	}
	hs, err := newState(psk, true, r)
	if err != nil {
		return nil, nil, err
	}
	payload, _ := json.Marshal(hello)
	msg1, _, _, err := hs.WriteMessage(nil, payload)
	if err != nil {
		return nil, nil, err
	}
	if err := conn.WriteMessage(ctx, msg1); err != nil {
		return nil, nil, err
	}
	msg2, err := conn.ReadMessage(ctx)
	if err != nil {
		return nil, nil, err
	}
	p2, send, recv, err := hs.ReadMessage(nil, msg2)
	if err != nil || send == nil {
		return nil, nil, ErrHandshake
	}
	var ah AgentHello
	if err := json.Unmarshal(p2, &ah); err != nil || !slices.Contains(hello.Protos, ah.Proto) {
		return nil, nil, ErrHandshake
	}
	return newChannel(conn, send, recv), &ah, nil
}

// Agent runs the responder side. choose is called with the client's hello
// and returns the agent's hello (with the chosen Proto) or an error to
// refuse the client.
func Agent(ctx context.Context, conn mux.Conn, psk [32]byte, choose func(*ClientHello) (*AgentHello, error)) (mux.Conn, *ClientHello, error) {
	return agentWith(ctx, conn, psk, choose, rand.Reader)
}

func agentWith(ctx context.Context, conn mux.Conn, psk [32]byte, choose func(*ClientHello) (*AgentHello, error), r io.Reader) (mux.Conn, *ClientHello, error) {
	hs, err := newState(psk, false, r)
	if err != nil {
		return nil, nil, err
	}
	msg1, err := conn.ReadMessage(ctx)
	if err != nil {
		return nil, nil, err
	}
	p1, _, _, err := hs.ReadMessage(nil, msg1)
	if err != nil {
		return nil, nil, ErrHandshake
	}
	var ch ClientHello
	if err := json.Unmarshal(p1, &ch); err != nil {
		return nil, nil, ErrHandshake
	}
	ah, err := choose(&ch)
	if err != nil {
		return nil, nil, err
	}
	payload, _ := json.Marshal(ah)
	msg2, recv, send, err := hs.WriteMessage(nil, payload)
	if err != nil {
		return nil, nil, err
	}
	if err := conn.WriteMessage(ctx, msg2); err != nil {
		return nil, nil, err
	}
	// NNpsk0 authenticates the client implicitly: only a holder of the PSK
	// can produce a first transport record that decrypts.
	return newChannel(conn, send, recv), &ch, nil
}

// channel encrypts every message as one Noise transport record. Any
// authentication failure is fatal for the channel.
type channel struct {
	inner mux.Conn
	wmu   sync.Mutex
	send  *noise.CipherState
	recv  *noise.CipherState
}

func newChannel(inner mux.Conn, send, recv *noise.CipherState) *channel {
	return &channel{inner: inner, send: send, recv: recv}
}

// ErrDecrypt is returned (and the channel closed) on a bad record.
var ErrDecrypt = errors.New("e2e: record failed authentication")

func (c *channel) ReadMessage(ctx context.Context) ([]byte, error) {
	ct, err := c.inner.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}
	pt, err := c.recv.Decrypt(nil, nil, ct)
	if err != nil {
		_ = c.inner.Close()
		return nil, ErrDecrypt
	}
	return pt, nil
}

func (c *channel) WriteMessage(ctx context.Context, msg []byte) error {
	if len(msg) > MaxRecord {
		return fmt.Errorf("e2e: record of %d bytes exceeds %d", len(msg), MaxRecord)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	ct, err := c.send.Encrypt(nil, nil, msg)
	if err != nil {
		return err
	}
	return c.inner.WriteMessage(ctx, ct)
}

func (c *channel) Close() error { return c.inner.Close() }
