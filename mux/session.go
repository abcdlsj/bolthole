package mux

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Conn is a reliable, ordered message transport: a WebSocket, a `ws`
// stream, or an encrypted channel. Implementations must allow one concurrent
// reader and one concurrent writer.
type Conn interface {
	ReadMessage(ctx context.Context) ([]byte, error)
	WriteMessage(ctx context.Context, msg []byte) error
	Close() error
}

// Config tunes a Session. The zero value is usable except for Dialer.
type Config struct {
	// Dialer is true for the side that dialed the transport (odd stream ids).
	Dialer bool
	// Heartbeat is the PING interval. Zero means 20s; negative disables
	// heartbeats and the silence timeout.
	Heartbeat time.Duration
	// MaxStreams caps concurrent streams the peer may open. Zero means 256.
	MaxStreams int
	// MaxFrame is the largest frame accepted. Zero means DefaultMaxFrame.
	MaxFrame int
	// WriteTimeout bounds a single transport write. Zero means 30s.
	WriteTimeout time.Duration
	// Logger receives debug output. Nil discards it.
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Heartbeat == 0 {
		c.Heartbeat = 20 * time.Second
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = 256
	}
	if c.MaxFrame <= 0 {
		c.MaxFrame = DefaultMaxFrame
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

var (
	// ErrSessionClosed is returned by operations on a closed session.
	ErrSessionClosed = errors.New("mux: session closed")
	// ErrGoAway is returned by Open after the peer sent GOAWAY.
	ErrGoAway = errors.New("mux: peer is going away")
	// ErrTimeout is the session error after 3 silent heartbeat intervals.
	ErrTimeout = errors.New("mux: peer silent for too long")
)

// Session multiplexes streams over one Conn.
type Session struct {
	conn Conn
	cfg  Config
	log  *slog.Logger

	mu       sync.Mutex
	streams  map[uint32]*Stream
	nextID   uint32
	closeErr error

	acceptCh chan *Stream
	ctrl     chan Frame
	writeMu  sync.Mutex
	closed   chan struct{}
	once     sync.Once

	lastRecv atomic.Int64
	lastSend atomic.Int64

	localGoAway  atomic.Bool
	remoteGoAway chan struct{}
	goAwayOnce   sync.Once
	goAwayErr    atomic.Pointer[ResetError]
}

// NewSession starts a session over conn. It owns conn from now on.
func NewSession(conn Conn, cfg Config) *Session {
	cfg = cfg.withDefaults()
	s := &Session{
		conn:         conn,
		cfg:          cfg,
		log:          cfg.Logger,
		streams:      make(map[uint32]*Stream),
		acceptCh:     make(chan *Stream, cfg.MaxStreams),
		ctrl:         make(chan Frame, 4096),
		closed:       make(chan struct{}),
		remoteGoAway: make(chan struct{}),
	}
	if cfg.Dialer {
		s.nextID = 1
	} else {
		s.nextID = 2
	}
	now := time.Now().UnixNano()
	s.lastRecv.Store(now)
	s.lastSend.Store(now)
	go s.readLoop()
	go s.ctrlLoop()
	if cfg.Heartbeat > 0 {
		go s.heartbeatLoop()
	}
	return s
}

// Done is closed when the session ends.
func (s *Session) Done() <-chan struct{} { return s.closed }

// Err reports why the session ended (nil while it runs).
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// RemoteGoAway is closed when the peer sends GOAWAY.
func (s *Session) RemoteGoAway() <-chan struct{} { return s.remoteGoAway }

// NumStreams reports the number of live streams.
func (s *Session) NumStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

// Close tears the session down; every stream fails with ErrSessionClosed.
func (s *Session) Close() error { s.closeWith(ErrSessionClosed); return nil }

func (s *Session) closeWith(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.closeErr = err
		streams := s.streams
		s.streams = map[uint32]*Stream{}
		s.mu.Unlock()
		close(s.closed)
		_ = s.conn.Close()
		for _, st := range streams {
			st.terminate(err)
		}
		s.log.Debug("mux session closed", "err", err)
	})
}

// GoAway tells the peer to open no new streams; existing streams continue.
func (s *Session) GoAway(code, message string) {
	if s.localGoAway.Swap(true) {
		return
	}
	s.queueCtrl(Frame{Type: TypeGoAway, Payload: encodeJSON(ResetError{Code: code, Message: message})})
}

// Open starts a new stream. It returns without waiting for ACCEPT so the
// caller can pipeline a request body; use Stream.WaitAccept to wait.
func (s *Session) Open(ctx context.Context, kind string, meta any) (*Stream, error) {
	select {
	case <-s.remoteGoAway:
		return nil, ErrGoAway
	default:
	}
	var raw json.RawMessage
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("mux: encode meta: %w", err)
		}
		raw = b
	}
	s.mu.Lock()
	if s.closeErr != nil {
		s.mu.Unlock()
		return nil, s.closeErr
	}
	id := s.nextID
	s.nextID += 2
	st := newStream(s, id, kind, raw, true)
	s.streams[id] = st
	s.mu.Unlock()

	payload := encodeJSON(OpenPayload{Kind: kind, Meta: raw})
	if len(payload)+HeaderSize > s.cfg.MaxFrame {
		s.removeStream(id)
		return nil, fmt.Errorf("mux: open meta of %d bytes is too large", len(payload))
	}
	if err := s.writeFrame(ctx, Frame{Type: TypeOpen, StreamID: id, Payload: payload}); err != nil {
		s.removeStream(id)
		return nil, err
	}
	return st, nil
}

// OpenAndWait opens a stream and waits for the peer to accept it.
func (s *Session) OpenAndWait(ctx context.Context, kind string, meta any) (*Stream, json.RawMessage, error) {
	st, err := s.Open(ctx, kind, meta)
	if err != nil {
		return nil, nil, err
	}
	acc, err := st.WaitAccept(ctx)
	if err != nil {
		st.Reset(CodeCancel, "")
		return nil, nil, err
	}
	return st, acc, nil
}

// Accept returns the next stream the peer opened.
func (s *Session) Accept(ctx context.Context) (*Stream, error) {
	select {
	case st := <-s.acceptCh:
		return st, nil
	case <-s.closed:
		return nil, s.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Session) writeFrame(ctx context.Context, f Frame) error {
	select {
	case <-s.closed:
		return s.Err()
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.WriteTimeout)
	defer cancel()
	s.writeMu.Lock()
	err := s.conn.WriteMessage(ctx, f.Encode())
	s.writeMu.Unlock()
	if err != nil {
		// A failed or abandoned write may have left a partial message on the
		// transport, which is unrecoverable: close the whole session.
		s.closeWith(fmt.Errorf("mux: write: %w", err))
		return s.Err()
	}
	s.lastSend.Store(time.Now().UnixNano())
	return nil
}

// queueCtrl schedules a control frame from contexts that must not block
// (the read loop). A peer that floods us with frames needing replies faster
// than we can write them is cut off.
func (s *Session) queueCtrl(f Frame) {
	select {
	case s.ctrl <- f:
	case <-s.closed:
	default:
		s.closeWith(errors.New("mux: control queue overflow"))
	}
}

func (s *Session) ctrlLoop() {
	for {
		select {
		case f := <-s.ctrl:
			if err := s.writeFrame(context.Background(), f); err != nil {
				return
			}
		case <-s.closed:
			return
		}
	}
}

func (s *Session) heartbeatLoop() {
	hb := s.cfg.Heartbeat
	tick := time.NewTicker(hb / 4)
	defer tick.Stop()
	for {
		select {
		case <-s.closed:
			return
		case now := <-tick.C:
			if now.Sub(time.Unix(0, s.lastRecv.Load())) > 3*hb {
				s.closeWith(ErrTimeout)
				return
			}
			if now.Sub(time.Unix(0, s.lastSend.Load())) >= hb {
				var p [8]byte
				binary.BigEndian.PutUint64(p[:], uint64(now.UnixNano()))
				s.queueCtrl(Frame{Type: TypePing, Payload: p[:]})
			}
		}
	}
}

func (s *Session) readLoop() {
	for {
		msg, err := s.conn.ReadMessage(context.Background())
		if err != nil {
			s.closeWith(fmt.Errorf("mux: read: %w", err))
			return
		}
		s.lastRecv.Store(time.Now().UnixNano())
		if len(msg) > s.cfg.MaxFrame {
			s.closeWith(fmt.Errorf("mux: frame of %d bytes exceeds limit", len(msg)))
			return
		}
		f, err := Decode(msg)
		if err != nil {
			s.closeWith(err)
			return
		}
		if err := s.handle(f); err != nil {
			s.closeWith(err)
			return
		}
	}
}

func (s *Session) isPeerID(id uint32) bool {
	if id == 0 {
		return false
	}
	odd := id%2 == 1
	return odd != s.cfg.Dialer // the peer uses the other parity
}

func (s *Session) handle(f Frame) error {
	switch f.Type {
	case TypePing:
		if len(f.Payload) > 64 {
			return errors.New("mux: oversized ping")
		}
		s.queueCtrl(Frame{Type: TypePong, Payload: append([]byte(nil), f.Payload...)})
		return nil
	case TypePong:
		return nil
	case TypeGoAway:
		var ge ResetError
		_ = json.Unmarshal(f.Payload, &ge)
		s.goAwayErr.Store(&ge)
		s.goAwayOnce.Do(func() { close(s.remoteGoAway) })
		return nil
	case TypeOpen:
		return s.handleOpen(f)
	case TypeAccept, TypeData, TypeFin, TypeReset, TypeWindow:
		s.mu.Lock()
		st := s.streams[f.StreamID]
		s.mu.Unlock()
		if st == nil {
			if f.Type != TypeReset && f.Type != TypeWindow && f.Type != TypeFin {
				s.queueCtrl(resetFrame(f.StreamID, CodeUnknownStream, ""))
			}
			return nil
		}
		st.handleFrame(f)
		return nil
	default:
		return nil // unknown frame types are ignored (spec §3.1)
	}
}

func (s *Session) handleOpen(f Frame) error {
	if !s.isPeerID(f.StreamID) {
		return fmt.Errorf("mux: peer opened stream %d with our parity", f.StreamID)
	}
	var op OpenPayload
	if err := json.Unmarshal(f.Payload, &op); err != nil || op.Kind == "" {
		s.queueCtrl(resetFrame(f.StreamID, CodeProtocolError, "bad open payload"))
		return nil
	}
	s.mu.Lock()
	if _, exists := s.streams[f.StreamID]; exists {
		s.mu.Unlock()
		return fmt.Errorf("mux: stream %d opened twice", f.StreamID)
	}
	if s.localGoAway.Load() {
		s.mu.Unlock()
		s.queueCtrl(resetFrame(f.StreamID, CodeGoingAway, ""))
		return nil
	}
	if len(s.streams) >= s.cfg.MaxStreams {
		s.mu.Unlock()
		s.queueCtrl(resetFrame(f.StreamID, CodeRefused, "too many streams"))
		return nil
	}
	st := newStream(s, f.StreamID, op.Kind, op.Meta, false)
	s.streams[f.StreamID] = st
	s.mu.Unlock()
	select {
	case s.acceptCh <- st:
	default:
		s.removeStream(f.StreamID)
		s.queueCtrl(resetFrame(f.StreamID, CodeRefused, "accept backlog full"))
	}
	return nil
}

func (s *Session) removeStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

func resetFrame(id uint32, code, msg string) Frame {
	return Frame{Type: TypeReset, StreamID: id, Payload: encodeJSON(ResetError{Code: code, Message: msg})}
}
