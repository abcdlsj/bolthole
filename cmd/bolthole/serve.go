package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/abcdlsj/bolthole/e2e"
	"github.com/abcdlsj/bolthole/relay"
)

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", envOr("BOLTHOLE_ADDR", ":8787"), "listen address [BOLTHOLE_ADDR]")
	keys := fs.String("key", envOr("BOLTHOLE_KEYS", ""), "comma-separated registration keys agents must present [BOLTHOLE_KEYS]")
	jwtSecret := fs.String("jwt-secret", envOr("BOLTHOLE_JWT_SECRET", ""), "accept per-agent HS256 tokens signed with this secret [BOLTHOLE_JWT_SECRET]")
	maxBody := fs.Int64("max-body-mb", 128, "max tunneled request body in MiB")
	trustProxy := fs.Bool("trust-proxy", envOr("BOLTHOLE_TRUST_PROXY", "") == "1", "use X-Forwarded-For / CF-Connecting-IP for rate limits [BOLTHOLE_TRUST_PROXY=1]")
	tlsCert := fs.String("tls-cert", envOr("BOLTHOLE_TLS_CERT", ""), "TLS certificate file (otherwise put a TLS proxy in front)")
	tlsKey := fs.String("tls-key", envOr("BOLTHOLE_TLS_KEY", ""), "TLS key file")
	verbose := fs.Bool("v", false, "verbose logs")
	fs.Parse(args)

	log := logger(*verbose)
	cfg := relay.Config{MaxBodyBytes: *maxBody << 20, TrustProxy: *trustProxy, Logger: log}
	switch {
	case *jwtSecret != "":
		cfg.Auth = relay.JWT([]byte(*jwtSecret))
		log.Info("agents need a JWT (bolthole token)")
	case *keys != "":
		cfg.Auth = relay.SharedKey(strings.Split(*keys, ",")...)
		log.Info("agents need a registration key")
	default:
		log.Info("open relay: any agent may register (clients still need each agent's code)")
	}
	srv := relay.New(cfg)
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 15 * time.Second}
	errc := make(chan error, 1)
	go func() {
		log.Info("relay listening", "addr", *addr, "version", relay.Version)
		if *tlsCert != "" {
			errc <- hs.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			errc <- hs.ListenAndServe()
		}
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("draining: agents are told to reconnect")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Close the listener first so a replacement relay can bind the port
	// immediately; hijacked agent/client streams keep draining below.
	hsErr := make(chan error, 1)
	go func() { hsErr <- hs.Shutdown(sctx) }()
	_ = srv.Shutdown(sctx)
	if err := <-hsErr; err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func cmdCode(ctx context.Context, args []string) error {
	fmt.Println(e2e.NewCode())
	return nil
}

func cmdToken(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("token", flag.ExitOnError)
	secret := fs.String("secret", envOr("BOLTHOLE_JWT_SECRET", ""), "the relay's --jwt-secret [BOLTHOLE_JWT_SECRET]")
	sub := fs.String("sub", "", "who the token is for (shown in relay logs)")
	days := fs.Int("days", 365, "validity in days")
	fs.Parse(args)
	if *secret == "" || *sub == "" {
		return errors.New("token: --secret and --sub are required")
	}
	fmt.Println(relay.MintJWT([]byte(*secret), *sub, time.Duration(*days)*24*time.Hour))
	return nil
}
