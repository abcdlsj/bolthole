// Package e2e implements connect codes, key derivation and the end-to-end
// encrypted channel between a client and an agent (spec §1 and §5).
package e2e

import (
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	// CodeSize is the length of a connect code in bytes.
	CodeSize = 32
	salt     = "bolthole/v1"
)

var b64 = base64.RawURLEncoding

// Code is the agent's only secret.
type Code [CodeSize]byte

// NewCode returns a fresh random code.
func NewCode() Code {
	var c Code
	if _, err := rand.Read(c[:]); err != nil {
		panic(err)
	}
	return c
}

// ParseCode decodes the base64url form produced by Code.String.
func ParseCode(s string) (Code, error) {
	var c Code
	b, err := b64.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != CodeSize {
		return c, errors.New("e2e: a connect code is 43 base64url characters")
	}
	copy(c[:], b)
	return c, nil
}

func (c Code) String() string { return b64.EncodeToString(c[:]) }

// Keys are everything derived from a code.
type Keys struct {
	RoutingToken string   // sent to the relay in client URLs
	TokenHash    string   // sent to the relay by the agent
	PSK          [32]byte // never leaves the endpoints
}

// Derive computes the routing token, token hash and PSK for a code.
func (c Code) Derive() Keys {
	prk, err := hkdf.Extract(sha256.New, c[:], []byte(salt))
	if err != nil {
		panic(err)
	}
	tok, err := hkdf.Expand(sha256.New, prk, "routing-token", 24)
	if err != nil {
		panic(err)
	}
	psk, err := hkdf.Expand(sha256.New, prk, "e2e-psk", 32)
	if err != nil {
		panic(err)
	}
	k := Keys{RoutingToken: b64.EncodeToString(tok)}
	k.TokenHash = HashToken(k.RoutingToken)
	copy(k.PSK[:], psk)
	return k
}

// HashToken is lowercase hex SHA-256 of the routing token.
func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// ValidToken reports whether s is a syntactically valid routing token.
func ValidToken(s string) bool {
	if len(s) < 16 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// ValidTokenHash reports whether s is 64 lowercase hex characters.
func ValidTokenHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// ConnString is a parsed connection string: the relay's base URL plus the code.
type ConnString struct {
	Relay *url.URL // scheme http(s), no fragment, path ends without "/"
	Code  Code
	Name  string // optional, from "&name="
}

// FormatConnString renders relay + code as "https://relay/#k=<code>".
func FormatConnString(relay string, code Code) (string, error) {
	u, err := url.Parse(relay)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("e2e: relay URL must be http(s), got %q", u.Scheme)
	}
	u.Fragment = ""
	u.RawFragment = ""
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String() + "#k=" + code.String(), nil
}

// ParseConnString parses "https://relay/#k=<code>[&name=…]".
func ParseConnString(s string) (ConnString, error) {
	var cs ConnString
	u, err := url.Parse(strings.TrimSpace(s))
	if err != nil {
		return cs, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return cs, fmt.Errorf("e2e: connection string must start with http(s)://")
	}
	frag, err := url.ParseQuery(u.Fragment)
	if err != nil {
		return cs, fmt.Errorf("e2e: bad fragment: %w", err)
	}
	if cs.Code, err = ParseCode(frag.Get("k")); err != nil {
		return cs, err
	}
	cs.Name = frag.Get("name")
	u.Fragment, u.RawFragment = "", ""
	u.Path = strings.TrimSuffix(u.Path, "/")
	cs.Relay = u
	return cs, nil
}
