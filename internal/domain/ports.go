package domain

import (
	"context"
	"time"
)

// JobRepository is the persistence port. The Redis adapter implements it.
type JobRepository interface {
	Save(ctx context.Context, j Job) error
	Get(ctx context.Context, id string) (Job, error)
	Cancel(ctx context.Context, id string) error
	RequeueDead(ctx context.Context, id string, at int64) error
	Stats(ctx context.Context, now int64) (Stats, error)

	// Worker-side operations.
	Claim(ctx context.Context, now int64, limit int, lease time.Duration, workerID string) ([]Lease, error)
	Ack(ctx context.Context, l Lease) (bool, error)
	Nack(ctx context.Context, l Lease, retryAt int64, maxAttempts int, now int64) (NackResult, error)
	Extend(ctx context.Context, l Lease, deadline int64) (bool, error)
	ReapExpired(ctx context.Context, now int64, limit, maxAttempts int) (int, error)

	Ping(ctx context.Context) error
}

// JobService is the use-case port consumed by the transport layer and
// wrapped by the logging / instrumenting decorators.
type JobService interface {
	Schedule(ctx context.Context, cmd ScheduleCommand) (Job, error)
	Get(ctx context.Context, id string) (Job, error)
	Cancel(ctx context.Context, id string) error
	Requeue(ctx context.Context, id string) error
	Stats(ctx context.Context) (Stats, error)
}

// LeaderLock picks one node at a time to fetch jobs (Redlock adapter).
// Acquire returns ErrNotLeader when another node holds the lock.
type LeaderLock interface {
	Acquire(ctx context.Context) (Leadership, error)
}

// Leadership is one leader term.
type Leadership interface {
	Token() string
	Valid() bool // false once the lock's TTL (Time To Live) may have run out
	Release(ctx context.Context) error
}

type IDGenerator interface {
	NewID() (string, error)
}

type Clock interface {
	Now() time.Time
}

// Handler executes one job type. Must be idempotent (at-least-once delivery).
type Handler interface {
	Handle(ctx context.Context, j Job) error
}

type HandlerFunc func(ctx context.Context, j Job) error

func (f HandlerFunc) Handle(ctx context.Context, j Job) error { return f(ctx, j) }

type HandlerRegistry interface {
	Lookup(jobType string) (Handler, bool)
}
