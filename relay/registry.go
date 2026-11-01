package relay

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/mux"
)

// AgentConn is one registered agent.
type AgentConn struct {
	Session   *mux.Session
	Name      string
	Subject   string // from the Authenticator
	TokenHash string
	ws        *websocket.Conn
	once      sync.Once
}

func (a *AgentConn) close(code websocket.StatusCode, reason string) {
	a.once.Do(func() {
		go func() {
			_ = a.ws.Close(code, reason)
			a.Session.Close()
		}()
	})
}

// Registry maps token hashes to live agents. The in-memory one fits a
// single process. To run several relay processes, route by token hash at
// the load balancer (consistent hashing on the `th` query parameter and on
// the hash of the /c/<token> segment) so each agent and its clients land on
// the same process; that is what the Cloudflare edition does with Durable
// Objects. Implement Registry if you need something else.
type Registry interface {
	// Register installs a and returns the agent it replaced, if any.
	Register(hash string, a *AgentConn) (replaced *AgentConn)
	// Unregister removes a if it is still the registered agent for hash.
	Unregister(hash string, a *AgentConn)
	// Lookup returns the agent for hash, waiting up to wait for one to
	// (re)register. It returns nil if none shows up.
	Lookup(ctx context.Context, hash string, wait time.Duration) *AgentConn
	All() []*AgentConn
	Len() int
}

// NewMemoryRegistry returns the default in-process Registry.
func NewMemoryRegistry() Registry {
	return &memRegistry{agents: map[string]*AgentConn{}, waiters: map[string]chan struct{}{}}
}

type memRegistry struct {
	mu      sync.Mutex
	agents  map[string]*AgentConn
	waiters map[string]chan struct{}
}

func (m *memRegistry) Register(hash string, a *AgentConn) *AgentConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.agents[hash]
	m.agents[hash] = a
	if ch, ok := m.waiters[hash]; ok {
		close(ch)
		delete(m.waiters, hash)
	}
	return old
}

func (m *memRegistry) Unregister(hash string, a *AgentConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.agents[hash] == a {
		delete(m.agents, hash)
	}
}

func (m *memRegistry) Lookup(ctx context.Context, hash string, wait time.Duration) *AgentConn {
	deadline := time.Now().Add(wait)
	for {
		m.mu.Lock()
		a := m.agents[hash]
		if a != nil && !isDone(a.Session) && !goingAway(a.Session) {
			m.mu.Unlock()
			return a
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			m.mu.Unlock()
			return nil
		}
		ch, ok := m.waiters[hash]
		if !ok {
			ch = make(chan struct{})
			m.waiters[hash] = ch
		}
		m.mu.Unlock()
		t := time.NewTimer(min(remaining, 250*time.Millisecond))
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil
		}
		t.Stop()
	}
}

func (m *memRegistry) All() []*AgentConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*AgentConn, 0, len(m.agents))
	for _, a := range m.agents {
		out = append(out, a)
	}
	return out
}

func (m *memRegistry) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.agents)
}

func isDone(s *mux.Session) bool {
	select {
	case <-s.Done():
		return true
	default:
		return false
	}
}

func goingAway(s *mux.Session) bool {
	// An agent that sent GOAWAY is shutting down; wait for its successor.
	select {
	case <-s.RemoteGoAway():
		return true
	default:
		return false
	}
}

// limiter is a per-key sliding one-minute counter.
type limiter struct {
	perMin int
	mu     sync.Mutex
	hits   map[string]*bucket
	last   time.Time
}

type bucket struct {
	tokens float64
	at     time.Time
}

func newLimiter(perMin int) *limiter { return &limiter{perMin: perMin, hits: map[string]*bucket{}} }

func (l *limiter) allow(key string) bool {
	if l.perMin < 0 {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.last) > time.Minute { // occasional cleanup
		for k, b := range l.hits {
			if now.Sub(b.at) > 2*time.Minute {
				delete(l.hits, k)
			}
		}
		l.last = now
	}
	b := l.hits[key]
	if b == nil {
		b = &bucket{tokens: float64(l.perMin), at: now}
		l.hits[key] = b
	}
	b.tokens = min(float64(l.perMin), b.tokens+now.Sub(b.at).Minutes()*float64(l.perMin))
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
