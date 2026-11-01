// Package httpkind implements the `http` stream kind (spec §4.1): one HTTP
// exchange per stream, bodies streamed both ways.
package httpkind

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/abcdlsj/bolthole/mux"
)

// Kind is the stream kind name.
const Kind = "http"

// RequestMeta is the OPEN meta of an http stream.
type RequestMeta struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers,omitempty"`
}

// ResponseMeta is the ACCEPT meta of an http stream.
type ResponseMeta struct {
	Status  int                 `json:"status"`
	Headers map[string][]string `json:"headers,omitempty"`
}

var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "transfer-encoding": true, "upgrade": true,
	"te": true, "trailer": true, "proxy-connection": true, "proxy-authenticate": true,
	"proxy-authorization": true, "host": true,
}

// EncodeHeaders lowercases names and drops hop-by-hop headers.
func EncodeHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, v := range h {
		lk := strings.ToLower(k)
		if hopByHop[lk] || strings.HasPrefix(lk, "sec-websocket-") {
			continue
		}
		out[lk] = append([]string(nil), v...)
	}
	return out
}

// DecodeHeaders turns meta headers back into an http.Header.
func DecodeHeaders(m map[string][]string) http.Header {
	h := make(http.Header, len(m))
	for k, v := range m {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, s := range v {
			h.Add(k, s)
		}
	}
	return h
}

func contentLength(h http.Header) int64 {
	var n int64 = -1
	if cl := h.Get("content-length"); cl != "" {
		if _, err := fmt.Sscan(cl, &n); err != nil || n < 0 {
			n = -1
		}
	}
	return n
}

// Serve answers an inbound http stream with h. It blocks until the handler
// returns and the response is flushed.
func Serve(h http.Handler, st *mux.Stream) {
	var meta RequestMeta
	if err := st.DecodeMeta(&meta); err != nil || meta.Method == "" || !strings.HasPrefix(meta.Path, "/") {
		st.Reset(mux.CodeProtocolError, "bad http meta")
		return
	}
	u, err := url.ParseRequestURI(meta.Path)
	if err != nil {
		st.Reset(mux.CodeProtocolError, "bad path")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-st.Done():
			if st.Err() != nil {
				cancel() // the peer went away: stop the handler
			}
		case <-ctx.Done():
		}
	}()
	req := (&http.Request{
		Method:     meta.Method,
		URL:        u,
		RequestURI: meta.Path,
		Proto:      "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:     DecodeHeaders(meta.Headers),
		Body:       readCloser{st},
		Host:       "relay",
		RemoteAddr: "relay",
	}).WithContext(ctx)
	req.ContentLength = contentLength(req.Header)
	req.Header.Del("content-length")
	w := &responseWriter{st: st, header: http.Header{}}
	defer func() {
		if r := recover(); r != nil {
			if r != http.ErrAbortHandler {
				st.Reset(mux.CodeInternal, "handler panic")
			} else {
				st.Reset(mux.CodeCancel, "")
			}
			return
		}
		if err := w.finish(); err != nil {
			st.Reset(mux.CodeInternal, "")
		}
	}()
	h.ServeHTTP(w, req)
}

type readCloser struct{ st *mux.Stream }

func (r readCloser) Read(p []byte) (int, error) { return r.st.Read(p) }
func (r readCloser) Close() error               { return nil }

type responseWriter struct {
	st          *mux.Stream
	header      http.Header
	wroteHeader bool
	err         error
}

func (w *responseWriter) Header() http.Header { return w.header }

func (w *responseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		return // informational responses are not forwarded
	}
	w.wroteHeader = true
	w.err = w.st.Accept(ResponseMeta{Status: status, Headers: EncodeHeaders(w.header)})
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		if w.header.Get("content-type") == "" {
			w.header.Set("content-type", http.DetectContentType(p))
		}
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.st.Write(p)
}

// Flush is a no-op: every Write is already on the wire.
func (w *responseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
}

func (w *responseWriter) finish() error {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return w.err
	}
	return w.st.CloseWrite()
}

// Opener opens streams; *mux.Session satisfies it.
type Opener interface {
	Open(ctx context.Context, kind string, meta any) (*mux.Stream, error)
}

// RoundTrip performs req over a new http stream.
func RoundTrip(o Opener, req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	headers := EncodeHeaders(req.Header)
	delete(headers, "content-length")
	if req.ContentLength > 0 {
		headers["content-length"] = []string{fmt.Sprint(req.ContentLength)}
	}
	meta := RequestMeta{Method: req.Method, Path: req.URL.RequestURI(), Headers: headers}
	if meta.Method == "" {
		meta.Method = http.MethodGet
	}
	st, err := o.Open(ctx, Kind, meta)
	if err != nil {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, err
	}
	bodyErr := make(chan error, 1)
	go func() {
		var err error
		if req.Body != nil {
			_, err = io.Copy(st, req.Body)
			req.Body.Close()
		}
		if err == nil {
			err = st.CloseWrite()
		}
		bodyErr <- err
	}()
	stop := context.AfterFunc(ctx, func() { st.Reset(mux.CodeCancel, "") })
	raw, err := st.WaitAccept(ctx)
	if err != nil {
		stop()
		st.Reset(mux.CodeCancel, "")
		return nil, err
	}
	var rm ResponseMeta
	if err := json.Unmarshal(raw, &rm); err != nil || rm.Status < 100 {
		stop()
		st.Reset(mux.CodeProtocolError, "bad response meta")
		return nil, errors.New("httpkind: bad response meta")
	}
	resp := &http.Response{
		Status:     fmt.Sprintf("%d %s", rm.Status, http.StatusText(rm.Status)),
		StatusCode: rm.Status,
		Proto:      "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:        DecodeHeaders(rm.Headers),
		Body:          &respBody{st: st, stop: stop},
		ContentLength: contentLength(DecodeHeaders(rm.Headers)),
		Request:       req,
	}
	return resp, nil
}

type respBody struct {
	st   *mux.Stream
	stop func() bool
	once sync.Once
}

func (b *respBody) Read(p []byte) (int, error) { return b.st.Read(p) }

func (b *respBody) Close() error {
	b.once.Do(func() {
		b.stop()
		b.st.Close()
	})
	return nil
}

// Transport is an http.RoundTripper that sends every request over a stream
// from Open. Use it as http.Client{Transport: …}.
type Transport struct {
	Opener func(ctx context.Context) (Opener, error)
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	o, err := t.Opener(req.Context())
	if err != nil {
		return nil, err
	}
	return RoundTrip(o, req)
}
