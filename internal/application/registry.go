package application

import (
	"sync"
	"time"

	"example.com/scheduler/internal/domain"
)

// Registry maps job types to handlers. Safe for concurrent use.
type Registry struct {
	mu sync.RWMutex
	m  map[string]domain.Handler
}

var _ domain.HandlerRegistry = (*Registry)(nil)

func NewRegistry() *Registry { return &Registry{m: map[string]domain.Handler{}} }

func (r *Registry) Register(jobType string, h domain.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[jobType] = h
}

func (r *Registry) Lookup(jobType string) (domain.Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.m[jobType]
	return h, ok
}

// SystemClock is the real clock; tests can inject a fake domain.Clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }
