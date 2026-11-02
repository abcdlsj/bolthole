package mux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func pair(t *testing.T, cfg Config) (*Session, *Session) {
	t.Helper()
	a, b := Pipe()
	ca, cb := cfg, cfg
	ca.Dialer, cb.Dialer = true, false
	sa, sb := NewSession(a, ca), NewSession(b, cb)
	t.Cleanup(func() { sa.Close(); sb.Close() })
	return sa, sb
}

func TestFrameRoundTrip(t *testing.T) {
	f := Frame{Type: TypeData, StreamID: 0xdeadbeef, Payload: []byte("hi")}
	g, err := Decode(f.Encode())
	if err != nil || g.Type != f.Type || g.StreamID != f.StreamID || string(g.Payload) != "hi" {
		t.Fatalf("got %+v %v", g, err)
	}
	if _, err := Decode([]byte{1, 2}); err == nil {
		t.Fatal("short frame decoded")
	}
}

// A large transfer must respect flow control in both directions and arrive intact.
func TestLargeEchoWithFlowControl(t *testing.T) {
	cli, srv := pair(t, Config{})
	go func() {
		st, err := srv.Accept(context.Background())
		if err != nil {
			return
		}
		st.Accept(map[string]string{"hello": "world"})
		io.Copy(st, st) // echo
		st.CloseWrite()
	}()
	ctx := context.Background()
	st, meta, err := cli.OpenAndWait(ctx, "echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(meta) != `{"hello":"world"}` {
		t.Fatalf("meta %s", meta)
	}
	data := make([]byte, 5<<20)
	rand.Read(data)
	go func() { st.Write(data); st.CloseWrite() }()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("echo mismatch")
	}
	select {
	case <-st.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("stream not finished after both FINs")
	}
	if n := cli.NumStreams(); n != 0 {
		t.Fatalf("%d streams leaked", n)
	}
}

func TestManyConcurrentStreams(t *testing.T) {
	cli, srv := pair(t, Config{})
	go func() {
		for {
			st, err := srv.Accept(context.Background())
			if err != nil {
				return
			}
			go func() { io.Copy(st, st); st.CloseWrite() }()
		}
	}()
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := cli.Open(context.Background(), "echo", nil)
			if err != nil {
				errs <- err
				return
			}
			msg := bytes.Repeat([]byte{byte(i)}, 70_000)
			go func() { st.Write(msg); st.CloseWrite() }()
			got, err := io.ReadAll(st)
			if err != nil || !bytes.Equal(got, msg) {
				errs <- errors.New("mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestResetPropagates(t *testing.T) {
	cli, srv := pair(t, Config{})
	go func() {
		st, _ := srv.Accept(context.Background())
		st.Reset(CodeNotFound, "nope")
	}()
	st, err := cli.Open(context.Background(), "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.WaitAccept(context.Background())
	var re *ResetError
	if !errors.As(err, &re) || re.Code != CodeNotFound {
		t.Fatalf("want not_found reset, got %v", err)
	}
}

func TestMaxStreamsRefused(t *testing.T) {
	cli, _ := pair(t, Config{MaxStreams: 2})
	var last error
	for i := 0; i < 3; i++ {
		st, err := cli.Open(context.Background(), "x", nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, last = st.WaitAccept(ctx)
		cancel()
	}
	var re *ResetError
	if !errors.As(last, &re) || re.Code != CodeRefused {
		t.Fatalf("third stream: want refused, got %v", last)
	}
}

func TestUnknownFrameIgnored(t *testing.T) {
	a, b := Pipe()
	s := NewSession(a, Config{Dialer: true})
	defer s.Close()
	b.WriteMessage(context.Background(), Frame{Type: 0x7f, StreamID: 9, Payload: []byte("future")}.Encode())
	b.WriteMessage(context.Background(), Frame{Type: TypePing, Payload: []byte("12345678")}.Encode())
	msg, err := b.ReadMessage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f, _ := Decode(msg)
	if f.Type != TypePong || string(f.Payload) != "12345678" {
		t.Fatalf("want pong, got %+v", f)
	}
}

func TestSilentPeerTimesOut(t *testing.T) {
	a, _ := Pipe() // the other end never reads or answers
	s := NewSession(a, Config{Dialer: true, Heartbeat: 40 * time.Millisecond})
	select {
	case <-s.Done():
		if !errors.Is(s.Err(), ErrTimeout) {
			t.Fatalf("want timeout, got %v", s.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not time out")
	}
}

func TestGoAwayBlocksNewStreams(t *testing.T) {
	cli, srv := pair(t, Config{})
	srv.GoAway(CodeGoingAway, "bye")
	select {
	case <-cli.RemoteGoAway():
	case <-time.After(time.Second):
		t.Fatal("no goaway")
	}
	if _, err := cli.Open(context.Background(), "x", nil); !errors.Is(err, ErrGoAway) {
		t.Fatalf("want ErrGoAway, got %v", err)
	}
}

func TestSessionCloseFailsStreams(t *testing.T) {
	cli, srv := pair(t, Config{})
	go srv.Accept(context.Background())
	st, _ := cli.Open(context.Background(), "x", nil)
	srv.Close()
	if _, err := st.Read(make([]byte, 1)); err == nil {
		t.Fatal("read succeeded on dead session")
	}
}

func TestReadDeadline(t *testing.T) {
	cli, srv := pair(t, Config{})
	go srv.Accept(context.Background())
	st, _ := cli.Open(context.Background(), "x", nil)
	st.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := st.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected deadline error")
	}
}

// Streams the peer doesn't read share one session window: writers stall once
// SessionWindow bytes are buffered, and resume once the peer reads.
func TestSessionWindowCapsBufferedData(t *testing.T) {
	cli, srv := pair(t, Config{})
	const streams = 24 // 24 × 4 MiB per-stream windows = 96 MiB > SessionWindow
	ctx := context.Background()
	outs := make([]*Stream, streams)
	for i := range outs {
		st, err := cli.Open(ctx, "sink", nil)
		if err != nil {
			t.Fatal(err)
		}
		outs[i] = st
	}
	ins := make([]*Stream, streams)
	for i := range ins {
		st, err := srv.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ins[i] = st
	}

	data := make([]byte, InitialWindow)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sent := make([]int, streams)
	total := 0
	for i, st := range outs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
			n, _ := st.Write(data)
			mu.Lock()
			sent[i] = n
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	if total != SessionWindow {
		t.Fatalf("peer buffered %d bytes before stalling, want %d", total, SessionWindow)
	}

	// Drain everything; the stalled writers must finish.
	errs := make(chan error, 2*streams)
	for i, st := range outs {
		go func() {
			st.SetWriteDeadline(time.Time{})
			_, err := st.Write(data[sent[i]:])
			if err == nil {
				err = st.CloseWrite()
			}
			errs <- err
		}()
	}
	for _, st := range ins {
		go func() {
			b, err := io.ReadAll(st)
			if err == nil && len(b) != len(data) {
				err = errors.New("short read")
			}
			errs <- err
		}()
	}
	timeout := time.After(10 * time.Second)
	for range 2 * streams {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatal("transfer stalled after the peer started reading")
		}
	}
}
