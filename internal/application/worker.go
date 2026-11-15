package application

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"example.com/scheduler/internal/domain"
)

// WorkerMetrics is implemented by the Prometheus adapter.
type WorkerMetrics interface {
	SetLeader(leading bool)
	TermFinished(jobs int, took time.Duration)
	LocalQueue(n int)
	JobFinished(jobType, outcome string, took time.Duration, lagMs int64)
	Reaped(n int)
	QueueDepth(s domain.Stats)
}

type WorkerConfig struct {
	NodeID         string
	BatchSize      int           // max jobs per Lua fetch
	BatchesPerTerm int           // fetches per leader term
	Concurrency    int           // goroutines executing jobs
	QueueSize      int           // local channel capacity
	ElectionDelay  time.Duration // retry interval when not leader
	IdleDelay      time.Duration // pause after a term with nothing due
	YieldDelay     time.Duration // pause after a busy term, lets other nodes win
	Lease          time.Duration // how long a claimed job stays ours without renewal
	MaxAttempts    int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
}

func (c *WorkerConfig) setDefaults() {
	or := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	orDur := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	or(&c.BatchSize, 100)
	or(&c.BatchesPerTerm, 1)
	or(&c.Concurrency, 16)
	or(&c.QueueSize, 2*c.Concurrency)
	or(&c.MaxAttempts, 5)
	orDur(&c.ElectionDelay, 200*time.Millisecond)
	orDur(&c.IdleDelay, 200*time.Millisecond)
	orDur(&c.YieldDelay, 20*time.Millisecond)
	orDur(&c.Lease, 30*time.Second)
	orDur(&c.BaseBackoff, time.Second)
	orDur(&c.MaxBackoff, 5*time.Minute)
}

// Worker runs on every node:
//
//	leader loop:  win lock → Lua fetch (pending → backup) → push to channel → release lock
//	worker pool:  read channel → run handler → ack (delete from backup) or nack
type Worker struct {
	repo     domain.JobRepository
	lock     domain.LeaderLock
	handlers domain.HandlerRegistry
	clock    domain.Clock
	metrics  WorkerMetrics
	log      *slog.Logger
	cfg      WorkerConfig
	jobs     chan domain.Lease
}

func NewWorker(repo domain.JobRepository, lock domain.LeaderLock, handlers domain.HandlerRegistry,
	clock domain.Clock, metrics WorkerMetrics, log *slog.Logger, cfg WorkerConfig) *Worker {
	cfg.setDefaults()
	return &Worker{
		repo: repo, lock: lock, handlers: handlers, clock: clock, metrics: metrics, cfg: cfg,
		log:  log.With("node_id", cfg.NodeID),
		jobs: make(chan domain.Lease, cfg.QueueSize),
	}
}

// Run blocks until ctx is cancelled, then finishes the jobs already queued.
func (w *Worker) Run(ctx context.Context) {
	var pool sync.WaitGroup
	for range w.cfg.Concurrency {
		pool.Add(1)
		go func() {
			defer pool.Done()
			for job := range w.jobs {
				w.metrics.LocalQueue(len(w.jobs))
				w.execute(job)
			}
		}()
	}

	w.leaderLoop(ctx)
	close(w.jobs)
	pool.Wait()
}

func (w *Worker) execute(job domain.Lease) {
	start := time.Now()
	lag := w.nowMs() - job.ScheduledAt
	log := w.log.With("job_id", job.ID, "type", job.Type, "attempt", job.Attempt)

	ctx, cancel := context.WithTimeout(context.Background(), 10*w.cfg.Lease)
	defer cancel()

	// The job waited in the channel; if its lease expired meanwhile, another
	// node may own it now.
	if !w.renewLease(ctx, job) {
		log.Warn("lease expired while queued, skipping")
		w.metrics.JobFinished(job.Type, "lease_lost", time.Since(start), lag)
		return
	}

	stopHeartbeat := w.heartbeat(ctx, cancel, job)
	err := w.runHandler(ctx, job.Job)
	stopHeartbeat()

	outcome := w.finish(job, err)
	if outcome != "success" {
		log.Warn("job failed", "outcome", outcome, "err", err)
	}
	w.metrics.JobFinished(job.Type, outcome, time.Since(start), lag)
}

// finish acks a successful job (deletes it from backup) or nacks a failed one.
func (w *Worker) finish(job domain.Lease, err error) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err == nil {
		if ok, ackErr := w.repo.Ack(ctx, job); !ok || ackErr != nil {
			w.log.Warn("ack failed, job may run again", "job_id", job.ID, "err", ackErr)
		}
		return "success"
	}

	retryAt := w.nowMs() + w.backoff(job.Attempt).Milliseconds()
	res, nackErr := w.repo.Nack(ctx, job, retryAt, w.cfg.MaxAttempts, w.nowMs())
	switch {
	case nackErr != nil:
		return "nack_error" // the reaper will recover it
	case res == domain.NackDeadLettered:
		return "dead"
	case res == domain.NackLeaseLost:
		return "lease_lost"
	default:
		return "retry"
	}
}

// heartbeat keeps extending the lease while the handler runs. If the lease is
// lost, the handler's context is cancelled. Call the returned func to stop.
func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, job domain.Lease) func() {
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(w.cfg.Lease / 3)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if !w.renewLease(ctx, job) {
					cancel()
					return
				}
			}
		}
	}()
	return func() { close(done) }
}

// renewLease returns false only when Redis says another owner has the job.
// A Redis error is not treated as lost: the ack's token check stays the guard.
func (w *Worker) renewLease(ctx context.Context, job domain.Lease) bool {
	ok, err := w.repo.Extend(ctx, job, w.nowMs()+w.cfg.Lease.Milliseconds())
	return ok || err != nil
}

func (w *Worker) runHandler(ctx context.Context, job domain.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	h, ok := w.handlers.Lookup(job.Type)
	if !ok {
		return fmt.Errorf("%w: %s", domain.ErrUnknownType, job.Type)
	}
	return h.Handle(ctx, job)
}

func (w *Worker) backoff(attempt int) time.Duration {
	return min(w.cfg.BaseBackoff<<min(max(attempt-1, 0), 20), w.cfg.MaxBackoff)
}

func (w *Worker) nowMs() int64 { return w.clock.Now().UnixMilli() }

// jitter returns d ± 25% so nodes don't retry in lockstep.
func jitter(d time.Duration) time.Duration {
	return d*3/4 + rand.N(d/2+1)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
