// A terminal chat room hosted on your laptop, joinable from anywhere.
//
//	go run ./examples/chat host --relay https://relay.example.com
//	go run ./examples/chat join '<connection string>' --nick alice
//
// It shows the extension point every app gets for free: a custom stream
// kind ("example.chat"). Each member is one end-to-end encrypted stream; the
// relay only ever sees ciphertext.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/abcdlsj/bolthole"
	"github.com/abcdlsj/bolthole/agent"
	"github.com/abcdlsj/bolthole/client"
	"github.com/abcdlsj/bolthole/mux"
)

const kind = "example.chat"

type joinMeta struct {
	Nick string `json:"nick"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: chat host --relay URL | chat join CONN --nick NAME")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	var err error
	switch os.Args[1] {
	case "host":
		err = host(ctx, os.Args[2:])
	case "join":
		err = join(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "chat:", err)
		os.Exit(1)
	}
}

// room fans every line out to every member.
type room struct {
	mu      sync.Mutex
	members map[*mux.Stream]string
}

func (r *room) say(from, text string) {
	line := fmt.Sprintf("%s  %s: %s\n", time.Now().Format("15:04"), from, text)
	fmt.Print(line)
	r.mu.Lock()
	defer r.mu.Unlock()
	for st := range r.members {
		go st.Write([]byte(line)) // a slow member never blocks the room
	}
}

func host(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("host", flag.ExitOnError)
	relay := fs.String("relay", os.Getenv("BOLTHOLE_RELAY"), "relay URL")
	fs.Parse(args)
	code, err := bolthole.LoadOrCreateCode("~/.config/bolthole/chat.code")
	if err != nil {
		return err
	}
	r := &room{members: map[*mux.Stream]string{}}
	router := agent.NewRouter().HandleFunc(kind, func(ctx context.Context, st *mux.Stream, _ agent.Info) {
		var m joinMeta
		_ = st.DecodeMeta(&m)
		nick := strings.TrimSpace(m.Nick)
		if nick == "" || len(nick) > 24 {
			st.Reset(mux.CodeForbidden, "pick a nickname of 1-24 characters")
			return
		}
		r.mu.Lock()
		others := len(r.members)
		r.mu.Unlock()
		st.Accept(map[string]int{"members": others})
		r.mu.Lock()
		r.members[st] = nick
		r.mu.Unlock()
		r.say("*", nick+" joined")
		sc := bufio.NewScanner(st)
		for sc.Scan() {
			if t := strings.TrimSpace(sc.Text()); t != "" {
				r.say(nick, t)
			}
		}
		r.mu.Lock()
		delete(r.members, st)
		r.mu.Unlock()
		st.Close()
		r.say("*", nick+" left")
	})
	a := &agent.Agent{Relay: *relay, Code: code, Name: "chat", Handler: router}
	cs, err := a.ConnString()
	if err != nil {
		return err
	}
	fmt.Printf("Room is open. Others join with:\n\n  go run ./examples/chat join '%s' --nick NAME\n\nType to talk as host.\n\n", cs)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			if t := strings.TrimSpace(sc.Text()); t != "" {
				r.say("host", t)
			}
		}
	}()
	return a.Run(ctx)
}

func join(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("join needs the connection string")
	}
	cs := args[0]
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	nick := fs.String("nick", os.Getenv("USER"), "your nickname")
	fs.Parse(args[1:])
	c, err := client.New(cs, &client.Options{Name: *nick})
	if err != nil {
		return err
	}
	defer c.Close()
	conn, err := c.Conn(ctx)
	if err != nil {
		return err
	}
	st, raw, err := conn.Session.OpenAndWait(ctx, kind, joinMeta{Nick: *nick})
	if err != nil {
		return err
	}
	var info struct{ Members int }
	json.Unmarshal(raw, &info)
	fmt.Printf("joined via %s; %d other member(s) here.\n", c.Path(), info.Members)
	go func() {
		io.Copy(st, os.Stdin)
		st.CloseWrite()
	}()
	_, err = io.Copy(os.Stdout, st)
	return err
}
