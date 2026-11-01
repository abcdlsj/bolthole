package relay

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Authenticator decides whether an agent may register. It returns a
// subject (for logs) or an error whose text is sent to the agent.
//
// Registration control is about who may use your relay's capacity, not
// about security of the tunnels: a client can only reach an agent whose
// connect code it holds, whatever the relay allows.
type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request, h Hello) (subject string, err error)
}

// AuthFunc adapts a function to Authenticator.
type AuthFunc func(ctx context.Context, r *http.Request, h Hello) (string, error)

// Authenticate calls f.
func (f AuthFunc) Authenticate(ctx context.Context, r *http.Request, h Hello) (string, error) {
	return f(ctx, r, h)
}

// Open lets any agent register.
func Open() Authenticator {
	return AuthFunc(func(context.Context, *http.Request, Hello) (string, error) { return "", nil })
}

// SharedKey requires hello.auth to equal one of keys.
func SharedKey(keys ...string) Authenticator {
	return AuthFunc(func(_ context.Context, _ *http.Request, h Hello) (string, error) {
		for _, k := range keys {
			if k != "" && subtle.ConstantTimeCompare([]byte(h.Auth), []byte(k)) == 1 {
				return "key", nil
			}
		}
		return "", errors.New("registration key required")
	})
}

// JWTAudience is the required "aud" claim of agent tokens.
const JWTAudience = "bolthole"

// JWT accepts HS256 tokens signed with secret carrying aud "bolthole", a
// "sub" and an unexpired "exp". Rotate the secret to revoke them all.
func JWT(secret []byte) Authenticator {
	return AuthFunc(func(_ context.Context, _ *http.Request, h Hello) (string, error) {
		sub, err := VerifyJWT(secret, h.Auth, time.Now())
		if err != nil {
			return "", err
		}
		return sub, nil
	})
}

var errBadToken = errors.New("invalid relay token")

// VerifyJWT checks an HS256 agent token and returns its subject.
func VerifyJWT(secret []byte, token string, now time.Time) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errBadToken
	}
	b64 := base64.RawURLEncoding
	var hdr struct {
		Alg string `json:"alg"`
	}
	hb, err := b64.DecodeString(parts[0])
	if err != nil || json.Unmarshal(hb, &hdr) != nil || hdr.Alg != "HS256" {
		return "", errBadToken
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return "", errBadToken
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, m.Sum(nil)) {
		return "", errBadToken
	}
	var claims struct {
		Sub string          `json:"sub"`
		Exp int64           `json:"exp"`
		Aud json.RawMessage `json:"aud"`
	}
	cb, err := b64.DecodeString(parts[1])
	if err != nil || json.Unmarshal(cb, &claims) != nil {
		return "", errBadToken
	}
	if !audienceOK(claims.Aud) || claims.Sub == "" {
		return "", errBadToken
	}
	if claims.Exp == 0 || now.Unix() >= claims.Exp {
		return "", errors.New("relay token expired")
	}
	return claims.Sub, nil
}

func audienceOK(raw json.RawMessage) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one == JWTAudience
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, a := range many {
			if a == JWTAudience {
				return true
			}
		}
	}
	return false
}

// MintJWT creates an agent token for sub, valid for ttl.
func MintJWT(secret []byte, sub string, ttl time.Duration) string {
	b64 := base64.RawURLEncoding
	h := b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	c, _ := json.Marshal(map[string]any{"sub": sub, "aud": JWTAudience, "iat": time.Now().Unix(), "exp": time.Now().Add(ttl).Unix()})
	p := b64.EncodeToString(c)
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(h + "." + p))
	return h + "." + p + "." + b64.EncodeToString(m.Sum(nil))
}
