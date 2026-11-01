package e2e

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/flynn/noise"

	"github.com/abcdlsj/bolthole/mux"
)

// Vectors pins every derived value so other implementations can check
// themselves byte for byte. Regenerate with UPDATE_VECTORS=1 go test ./e2e.
type Vectors struct {
	Code         string `json:"code"`
	RoutingToken string `json:"routing_token"`
	TokenHash    string `json:"token_hash"`
	PSK          string `json:"psk"`
	ConnString   string `json:"conn_string"`
	Handshake    struct {
		ClientEphemeralPrivate string `json:"client_ephemeral_private"`
		AgentEphemeralPrivate  string `json:"agent_ephemeral_private"`
		ClientHello            string `json:"client_hello"`
		AgentHello             string `json:"agent_hello"`
		Msg1                   string `json:"msg1"`
		Msg2                   string `json:"msg2"`
		ClientRecord0          string `json:"client_record_0"` // encrypts "ping"
		AgentRecord0           string `json:"agent_record_0"`  // encrypts "pong"
	} `json:"handshake"`
	Frames []struct {
		Hex  string `json:"hex"`
		Type byte   `json:"type"`
		ID   uint32 `json:"stream_id"`
		Body string `json:"payload_hex"`
	} `json:"frames"`
}

const vectorsPath = "../spec/test-vectors.json"

func fixedKey(seed byte) noise.DHKey {
	// Built by hand: crypto/ecdh ignores custom randomness since Go 1.26.
	priv, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{seed}, 32))
	if err != nil {
		panic(err)
	}
	return noise.DHKey{Private: priv.Bytes(), Public: priv.PublicKey().Bytes()}
}

func computeVectors(t *testing.T) Vectors {
	var v Vectors
	var code Code
	for i := range code {
		code[i] = byte(i)
	}
	k := code.Derive()
	v.Code, v.RoutingToken, v.TokenHash, v.PSK = code.String(), k.RoutingToken, k.TokenHash, hex.EncodeToString(k.PSK[:])
	v.ConnString, _ = FormatConnString("https://relay.example.com", code)

	ce, ae := fixedKey(0x11), fixedKey(0x22)
	v.Handshake.ClientEphemeralPrivate = hex.EncodeToString(ce.Private)
	v.Handshake.AgentEphemeralPrivate = hex.EncodeToString(ae.Private)
	ch := `{"protos":["mux/1"],"name":"vector-client"}`
	ah := `{"proto":"mux/1","name":"vector-agent"}`
	v.Handshake.ClientHello, v.Handshake.AgentHello = ch, ah

	mk := func(init bool, e noise.DHKey) *noise.HandshakeState {
		hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: suite, Pattern: noise.HandshakeNN, Initiator: init,
			Prologue: []byte(Prologue), PresharedKey: k.PSK[:], PresharedKeyPlacement: 0,
			Random: bytes.NewReader(e.Private)}) // flynn/noise draws the ephemeral from Random
		if err != nil {
			t.Fatal(err)
		}
		return hs
	}
	c, a := mk(true, ce), mk(false, ae)
	m1, _, _, err := c.WriteMessage(nil, []byte(ch))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := a.ReadMessage(nil, m1); err != nil {
		t.Fatal(err)
	}
	m2, aRecv, aSend, err := a.WriteMessage(nil, []byte(ah))
	if err != nil {
		t.Fatal(err)
	}
	_, cSend, cRecv, err := c.ReadMessage(nil, m2)
	if err != nil {
		t.Fatal(err)
	}
	_ = aRecv
	_ = cRecv
	r0, _ := cSend.Encrypt(nil, nil, []byte("ping"))
	r1, _ := aSend.Encrypt(nil, nil, []byte("pong"))
	v.Handshake.Msg1, v.Handshake.Msg2 = hex.EncodeToString(m1), hex.EncodeToString(m2)
	v.Handshake.ClientRecord0, v.Handshake.AgentRecord0 = hex.EncodeToString(r0), hex.EncodeToString(r1)

	for _, f := range []mux.Frame{
		{Type: mux.TypeOpen, StreamID: 1, Payload: []byte(`{"kind":"http","meta":{"method":"GET","path":"/"}}`)},
		{Type: mux.TypeData, StreamID: 1, Payload: []byte("hello")},
		{Type: mux.TypeWindow, StreamID: 2, Payload: []byte{0, 2, 0, 0}},
		{Type: mux.TypeFin, StreamID: 3},
		{Type: mux.TypePing, StreamID: 0, Payload: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	} {
		v.Frames = append(v.Frames, struct {
			Hex  string `json:"hex"`
			Type byte   `json:"type"`
			ID   uint32 `json:"stream_id"`
			Body string `json:"payload_hex"`
		}{hex.EncodeToString(f.Encode()), f.Type, f.StreamID, hex.EncodeToString(f.Payload)})
	}
	return v
}

func TestVectors(t *testing.T) {
	got := computeVectors(t)
	b, _ := json.MarshalIndent(got, "", "  ")
	if os.Getenv("UPDATE_VECTORS") == "1" {
		if err := os.WriteFile(vectorsPath, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_VECTORS=1 to create)", err)
	}
	if !bytes.Equal(bytes.TrimSpace(want), bytes.TrimSpace(b)) {
		t.Fatal("test vectors changed: the wire format is not backward compatible")
	}
}

// The public Client/Agent functions interoperate over a pipe and refuse a
// wrong PSK.
func TestHandshake(t *testing.T) {
	ctx := context.Background()
	psk := NewCode().Derive().PSK
	for _, wrong := range []bool{false, true} {
		a, b := mux.Pipe()
		agentPSK := psk
		if wrong {
			agentPSK[0] ^= 1
		}
		done := make(chan error, 1)
		go func() {
			ch, hello, err := Agent(ctx, b, agentPSK, func(h *ClientHello) (*AgentHello, error) {
				return &AgentHello{Proto: ProtoMux, Direct: []string{"ws://lan"}}, nil
			})
			if err == nil {
				if hello.Name != "c" {
					err = ErrHandshake
				} else {
					var m []byte
					if m, err = ch.ReadMessage(ctx); err == nil && string(m) != "hi" {
						err = ErrHandshake
					}
				}
			}
			if err != nil {
				b.Close() // as a real agent would: drop the connection
			}
			done <- err
		}()
		ch, ah, err := Client(ctx, a, psk, ClientHello{Name: "c"})
		if wrong {
			if err == nil {
				t.Fatal("handshake succeeded with mismatched PSK")
			}
			a.Close()
			<-done
			continue
		}
		if err != nil || ah.Direct[0] != "ws://lan" {
			t.Fatal(err)
		}
		ch.WriteMessage(ctx, []byte("hi"))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
