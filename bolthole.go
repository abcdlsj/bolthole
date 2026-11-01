// Package bolthole makes any service reachable from anywhere through a
// relay it dials out to, with end-to-end encryption the relay cannot read.
//
// Expose an http.Handler:
//
//	code := bolthole.LoadOrCreateCode("~/.config/myapp/bolthole.code")
//	a := bolthole.Expose("https://relay.example.com", code, myHandler)
//	fmt.Println("connect with:", must(a.ConnString()))
//	a.Run(ctx)
//
// Reach it from another program:
//
//	c, _ := bolthole.Dial(connString)
//	resp, _ := c.HTTPClient().Get("http://agent/api/status")
//
// The subpackages hold the pieces: agent, client, relay (the server),
// mux (multiplexed streams), e2e (codes and the encrypted channel).
package bolthole

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/abcdlsj/bolthole/agent"
	"github.com/abcdlsj/bolthole/client"
	"github.com/abcdlsj/bolthole/e2e"
)

// Code is a connect code: the one secret that grants access to an agent.
type Code = e2e.Code

// NewCode returns a fresh random code.
func NewCode() Code { return e2e.NewCode() }

// LoadOrCreateCode reads a code from path, creating it (mode 0600) if the
// file does not exist. A leading "~/" expands to the home directory.
func LoadOrCreateCode(path string) (Code, error) {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return Code{}, err
		}
		path = filepath.Join(home, path[2:])
	}
	b, err := os.ReadFile(path)
	if err == nil {
		return e2e.ParseCode(string(b))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Code{}, err
	}
	code := e2e.NewCode()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return Code{}, err
	}
	if err := os.WriteFile(path, []byte(code.String()+"\n"), 0o600); err != nil {
		return Code{}, err
	}
	return code, nil
}

// Expose returns an Agent serving h for `http` streams. For WebSocket, TCP
// or your own stream kinds, build an agent.Router instead.
func Expose(relayURL string, code Code, h http.Handler) *agent.Agent {
	return &agent.Agent{Relay: relayURL, Code: code, Handler: agent.NewRouter().HTTP(h)}
}

// Dial returns a client for a connection string. It connects lazily, picks
// the fastest path and reconnects on its own.
func Dial(connString string) (*client.Client, error) { return client.New(connString, nil) }
