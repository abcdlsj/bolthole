package mux

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// ErrStreamClosed is returned when writing after CloseWrite or Close.
var ErrStreamClosed = errors.New("mux: stream closed")

// Stream is one bidirectional byte stream. Read and Write may be called
// concurrently with each other; it implements io.ReadWriteCloser and the
// deadline methods of net.Conn.
type Stream struct {
	s        *Session
	id       uint32
	kind     string
	meta     json.RawMessage
	outbound bool

	mu         sync.Mutex
	signal     chan struct{} // closed and replaced on every state change
	recv       bytes.Buffer
	recvFin    bool
	recvCredit int64 // bytes the peer may still send
	unacked    int64 // consumed bytes not yet returned as WINDOW
	sendCredit int64
	localFin   bool
	accepted   bool // we sent ACCEPT (inbound) / got ACCEPT (outbound)
	acceptMeta json.RawMessage
	err        error // terminal error (reset or session closure)
	finished   bool
	done       chan struct{}

	readDeadline  time.Time
	writeDeadline time.Time
}

func newStream(s *Session, id uint32, kind string, meta json.RawMessage, outbound bool) *Stream {
	return &Stream{
		s: s, id: id, kind: kind, meta: meta, outbound: outbound,
		signal:     make(chan struct{}),
		recvCredit: InitialWindow,
		sendCredit: InitialWindow,
		done:       make(chan struct{}),
	}
}

// ID returns the stream id.
func (st *Stream) ID() uint32 { return st.id }

// Kind returns the stream kind from OPEN.
func (st *Stream) Kind() string { return st.kind }

// Meta returns the raw OPEN meta.
func (st *Stream) Meta() json.RawMessage { return st.meta }

// DecodeMeta unmarshals the OPEN meta into v.
func (st *Stream) DecodeMeta(v any) error {
	if len(st.meta) == 0 {
		return nil
	}
	return json.Unmarshal(st.meta, v)
}

// Session returns the session this stream belongs to.
func (st *Stream) Session() *Session { return st.s }

// Done is closed once the stream is fully finished or reset.
func (st *Stream) Done() <-chan struct{} { return st.done }

// Err returns the terminal error, if any.
func (st *Stream) Err() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.err
}

func (st *Stream) broadcast() {
	close(st.signal)
	st.signal = make(chan struct{})
}

// Accept answers an inbound OPEN. meta may be nil.
func (st *Stream) Accept(meta any) error {
	if st.outbound {
		return errors.New("mux: Accept on an outbound stream")
	}
	var raw []byte
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		raw = b
	}
	if len(raw)+HeaderSize > st.s.cfg.MaxFrame {
		st.Reset(CodeInternal, "accept meta too large")
		return errors.New("mux: accept meta too large")
	}
	st.mu.Lock()
	if st.accepted {
		st.mu.Unlock()
		return nil
	}
	if st.err != nil {
		err := st.err
		st.mu.Unlock()
		return err
	}
	st.accepted = true
	st.mu.Unlock()
	return st.s.writeFrame(context.Background(), Frame{Type: TypeAccept, StreamID: st.id, Payload: raw})
}

// WaitAccept blocks until the peer accepts (returning its meta) or resets.
func (st *Stream) WaitAccept(ctx context.Context) (json.RawMessage, error) {
	for {
		st.mu.Lock()
		if st.accepted {
			m := st.acceptMeta
			st.mu.Unlock()
			return m, nil
		}
		if st.err != nil {
			err := st.err
			st.mu.Unlock()
			return nil, err
		}
		sig := st.signal
		st.mu.Unlock()
		select {
		case <-sig:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Read reads stream data. It returns io.EOF after the peer's FIN.
func (st *Stream) Read(p []byte) (int, error) {
	for {
		st.mu.Lock()
		if st.recv.Len() > 0 {
			n, _ := st.recv.Read(p)
			st.unacked += int64(n)
			var credit int64
			if st.unacked >= InitialWindow/2 && !st.recvFin && st.err == nil {
				credit = st.unacked
				st.unacked = 0
				st.recvCredit += credit
			}
			st.mu.Unlock()
			if credit > 0 {
				var b [4]byte
				binary.BigEndian.PutUint32(b[:], uint32(credit))
				st.s.queueCtrl(Frame{Type: TypeWindow, StreamID: st.id, Payload: b[:]})
			}
			return n, nil
		}
		if st.err != nil {
			err := st.err
			st.mu.Unlock()
			return 0, err
		}
		if st.recvFin {
			st.mu.Unlock()
			return 0, io.EOF
		}
		sig, dl := st.signal, st.readDeadline
		st.mu.Unlock()
		if err := waitSignal(sig, dl); err != nil {
			return 0, err
		}
	}
}

// Write sends data, blocking while the peer's window is exhausted.
func (st *Stream) Write(p []byte) (int, error) {
	if !st.outbound {
		st.mu.Lock()
		acc := st.accepted
		st.mu.Unlock()
		if !acc {
			if err := st.Accept(nil); err != nil {
				return 0, err
			}
		}
	}
	written := 0
	for written < len(p) {
		st.mu.Lock()
		if st.err != nil {
			err := st.err
			st.mu.Unlock()
			return written, err
		}
		if st.localFin {
			st.mu.Unlock()
			return written, ErrStreamClosed
		}
		if st.sendCredit <= 0 {
			sig, dl := st.signal, st.writeDeadline
			st.mu.Unlock()
			if err := waitSignal(sig, dl); err != nil {
				return written, err
			}
			continue
		}
		n := len(p) - written
		if n > MaxChunk {
			n = MaxChunk
		}
		if int64(n) > st.sendCredit {
			n = int(st.sendCredit)
		}
		st.sendCredit -= int64(n)
		st.mu.Unlock()
		chunk := p[written : written+n]
		if err := st.s.writeFrame(context.Background(), Frame{Type: TypeData, StreamID: st.id, Payload: chunk}); err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

// CloseWrite sends FIN: no more data from this side.
func (st *Stream) CloseWrite() error {
	st.mu.Lock()
	if st.localFin || st.err != nil {
		st.mu.Unlock()
		return nil
	}
	if !st.outbound && !st.accepted {
		st.mu.Unlock()
		if err := st.Accept(nil); err != nil {
			return err
		}
		st.mu.Lock()
	}
	st.localFin = true
	st.broadcast()
	st.mu.Unlock()
	err := st.s.writeFrame(context.Background(), Frame{Type: TypeFin, StreamID: st.id})
	st.maybeFinish()
	return err
}

// Close finishes the stream. If the peer is still sending, the stream is
// reset with "cancel" so it stops.
func (st *Stream) Close() error {
	st.mu.Lock()
	remoteOpen := !st.recvFin && st.err == nil
	st.mu.Unlock()
	if remoteOpen {
		st.Reset(CodeCancel, "")
		return nil
	}
	return st.CloseWrite()
}

// Reset aborts the stream in both directions.
func (st *Stream) Reset(code, message string) {
	st.mu.Lock()
	if st.err != nil || st.finished {
		st.mu.Unlock()
		return
	}
	st.mu.Unlock()
	st.s.queueCtrl(resetFrame(st.id, code, message))
	st.terminate(&ResetError{Code: code, Message: message})
}

// terminate marks the stream dead with err and removes it from the session.
func (st *Stream) terminate(err error) {
	st.mu.Lock()
	if st.err == nil && !st.finished {
		st.err = err
	}
	alreadyDone := st.finished
	st.finished = true
	st.broadcast()
	st.mu.Unlock()
	if !alreadyDone {
		close(st.done)
	}
	st.s.removeStream(st.id)
}

func (st *Stream) maybeFinish() {
	st.mu.Lock()
	fin := st.localFin && st.recvFin && !st.finished
	if fin {
		st.finished = true
		st.broadcast()
	}
	st.mu.Unlock()
	if fin {
		close(st.done)
		st.s.removeStream(st.id)
	}
}

func (st *Stream) handleFrame(f Frame) {
	switch f.Type {
	case TypeAccept:
		st.mu.Lock()
		if st.outbound && !st.accepted {
			st.accepted = true
			st.acceptMeta = append(json.RawMessage(nil), f.Payload...)
			st.broadcast()
		}
		st.mu.Unlock()
	case TypeData:
		st.mu.Lock()
		if st.recvFin || st.err != nil {
			st.mu.Unlock()
			return
		}
		if int64(len(f.Payload)) > st.recvCredit {
			st.mu.Unlock()
			st.Reset(CodeFlowControl, "window exceeded")
			return
		}
		st.recvCredit -= int64(len(f.Payload))
		st.recv.Write(f.Payload)
		st.broadcast()
		st.mu.Unlock()
	case TypeFin:
		st.mu.Lock()
		st.recvFin = true
		st.broadcast()
		st.mu.Unlock()
		st.maybeFinish()
	case TypeReset:
		var re ResetError
		if err := json.Unmarshal(f.Payload, &re); err != nil || re.Code == "" {
			re = ResetError{Code: CodeInternal}
		}
		st.terminate(&re)
	case TypeWindow:
		if len(f.Payload) != 4 {
			st.Reset(CodeProtocolError, "bad window payload")
			return
		}
		st.mu.Lock()
		st.sendCredit += int64(binary.BigEndian.Uint32(f.Payload))
		if st.sendCredit > 1<<31 {
			st.mu.Unlock()
			st.Reset(CodeFlowControl, "window overflow")
			return
		}
		st.broadcast()
		st.mu.Unlock()
	}
}

// SetDeadline sets both read and write deadlines.
func (st *Stream) SetDeadline(t time.Time) error {
	st.mu.Lock()
	st.readDeadline, st.writeDeadline = t, t
	st.broadcast()
	st.mu.Unlock()
	return nil
}

// SetReadDeadline sets the read deadline.
func (st *Stream) SetReadDeadline(t time.Time) error {
	st.mu.Lock()
	st.readDeadline = t
	st.broadcast()
	st.mu.Unlock()
	return nil
}

// SetWriteDeadline sets the deadline for waiting on flow-control credit.
func (st *Stream) SetWriteDeadline(t time.Time) error {
	st.mu.Lock()
	st.writeDeadline = t
	st.broadcast()
	st.mu.Unlock()
	return nil
}

func waitSignal(sig <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		<-sig
		return nil
	}
	d := time.Until(deadline)
	if d <= 0 {
		return os.ErrDeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-sig:
		return nil
	case <-t.C:
		return os.ErrDeadlineExceeded
	}
}
