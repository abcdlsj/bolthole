// Command bolthole is the relay server and a language-agnostic sidecar.
//
//	bolthole serve                                   run a relay
//	bolthole agent --relay URL --http :3000          expose a local service
//	bolthole connect CONN --listen 127.0.0.1:8080     reach it as a local port
//	bolthole tcp CONN [target]                        stdin/stdout byte pipe (ssh ProxyCommand)
//	bolthole code | token                             generate secrets
//
// Any program in any language can sit behind `agent` or in front of
// `connect`: they only ever see plain local HTTP, WebSocket or TCP.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmds := map[string]func(context.Context, []string) error{
		"serve":   cmdServe,
		"agent":   cmdAgent,
		"connect": cmdConnect,
		"tcp":     cmdTCP,
		"code":    cmdCode,
		"token":   cmdToken,
		"version": func(context.Context, []string) error { fmt.Println("bolthole", version); return nil },
	}
	fn, ok := cmds[os.Args[1]]
	if !ok {
		if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
			usage()
			return
		}
		fmt.Fprintf(os.Stderr, "bolthole: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err := fn(ctx, os.Args[2:]); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "bolthole:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, strings.TrimLeft(`
bolthole: reach any service from anywhere through a relay that can't read your traffic.

Commands:
  serve     run a relay server
  agent     expose local HTTP/WebSocket/TCP services through a relay
  connect   serve a remote agent's HTTP/WebSocket API on a local port
  tcp       pipe stdin/stdout to a remote agent's TCP target (ssh ProxyCommand)
  code      print a new random connect code
  token     mint a registration JWT for relays started with --jwt-secret
  version   print the version

Run "bolthole <command> -h" for flags.
`, "\n"))
}

func logger(verbose bool) *slog.Logger {
	lvl := slog.LevelInfo
	if verbose {
		lvl = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

// envOr returns the environment variable or def, so every flag can also
// be set from the environment (handy for containers).
func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// multiFlag collects repeated flags.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }
