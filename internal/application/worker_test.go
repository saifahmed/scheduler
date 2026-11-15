package application

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/scheduler/internal/domain"
)

// Every job runs exactly once, only one node leads at a time, and the work
// spreads across nodes.
func TestJobsRunOnceAcrossNodes(t *testing.T) {
	c := newCluster(t, 3, 500, time.Millisecond)
	c.waitUntilDone()

	c.assertEachJobAckedOnce()
	c.assertSingleLeader()
	c.assertEveryNodeExecuted()
}

// With slow jobs a node's channel fills, it stops running for leader, and idle
// nodes take the next batches, so execution overlaps across nodes.
func TestBusyNodeHandsOverToIdleNodes(t *testing.T) {
	c := newCluster(t, 3, 300, 5*time.Millisecond)
	c.waitUntilDone()

	c.assertEachJobAckedOnce()
	c.assertSingleLeader()
	c.assertEveryNodeExecuted()
	if peak := c.peakRunning.Load(); peak <= 4 { // 4 = one node's concurrency
		t.Fatalf("jobs did not run in parallel on several nodes (peak %d)", peak)
	}
}

// ---- test cluster ----

type cluster struct {
	t           *testing.T
	repo        *memRepo
	lock        *memLock
	total       int
	perNode     []atomic.Int32
	running     atomic.Int32
	peakRunning atomic.Int32
	stop        func()
}

func newCluster(t *testing.T, nodes, jobs int, jobTime time.Duration) *cluster {
	c := &cluster{t: t, repo: newMemRepo(), lock: &memLock{}, total: jobs, perNode: make([]atomic.Int32, nodes)}
	for i := range jobs {
		c.repo.pending[fmt.Sprintf("job%06d", i)] = domain.Job{ID: fmt.Sprintf("job%06d", i), Type: "t"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for n := range nodes {
		handlers := NewRegistry()
		handlers.Register("t", domain.HandlerFunc(func(context.Context, domain.Job) error {
			c.peakRunning.Store(max(c.peakRunning.Load(), c.running.Add(1)))
			time.Sleep(jobTime)
			c.running.Add(-1)
			c.perNode[n].Add(1)
			return nil
		}))
		w := NewWorker(c.repo, c.lock, handlers, SystemClock{}, noMetrics{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
			WorkerConfig{NodeID: fmt.Sprint("node", n), BatchSize: 50, Concurrency: 4,
				ElectionDelay: time.Millisecond, IdleDelay: time.Millisecond, YieldDelay: time.Millisecond})
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(ctx) }()
	}
	c.stop = func() { cancel(); wg.Wait() }
	return c
}

func (c *cluster) waitUntilDone() {
	deadline := time.Now().Add(10 * time.Second)
	for c.repo.doneCount() < c.total {
		if time.Now().After(deadline) {
			c.t.Fatalf("timeout: %d of %d jobs done", c.repo.doneCount(), c.total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.stop()
}

func (c *cluster) assertEachJobAckedOnce() {
	for id, n := range c.repo.done {
		if n != 1 {
			c.t.Fatalf("job %s acked %d times", id, n)
		}
	}
	if len(c.repo.pending)+len(c.repo.backup) != 0 {
		c.t.Fatalf("left over: %d pending, %d in backup", len(c.repo.pending), len(c.repo.backup))
	}
}

func (c *cluster) assertSingleLeader() {
	if n := c.lock.maxConcurrent.Load(); n != 1 {
		c.t.Fatalf("%d nodes led at the same time", n)
	}
}

func (c *cluster) assertEveryNodeExecuted() {
	for i := range c.perNode {
		c.t.Logf("node%d ran %d jobs", i, c.perNode[i].Load())
		if c.perNode[i].Load() == 0 {
			c.t.Fatalf("node%d never ran a job", i)
		}
	}
}

// ---- in-memory fakes ----

type memLock struct {
	mu            sync.Mutex
	holder        string
	terms         int
	leaders       atomic.Int32
	maxConcurrent atomic.Int32
}

func (l *memLock) Acquire(context.Context) (domain.Leadership, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder != "" {
		return nil, domain.ErrNotLeader
	}
	l.terms++
	l.holder = fmt.Sprint("term", l.terms)
	l.maxConcurrent.Store(max(l.maxConcurrent.Load(), l.leaders.Add(1)))
	return &memLeadership{lock: l, token: l.holder}, nil
}

type memLeadership struct {
	lock  *memLock
	token string
}

func (m *memLeadership) Token() string { return m.token }
func (m *memLeadership) Valid() bool   { return true }
func (m *memLeadership) Release(context.Context) error {
	m.lock.mu.Lock()
	defer m.lock.mu.Unlock()
	m.lock.holder = ""
	m.lock.leaders.Add(-1)
	return nil
}

type memRepo struct {
	mu      sync.Mutex
	pending map[string]domain.Job
	backup  map[string]string // job ID → lease token
	done    map[string]int    // job ID → ack count
}

func newMemRepo() *memRepo {
	return &memRepo{pending: map[string]domain.Job{}, backup: map[string]string{}, done: map[string]int{}}
}

func (r *memRepo) doneCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.done)
}

func (r *memRepo) Claim(_ context.Context, _ int64, limit int, _ time.Duration, owner string) ([]domain.Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Lease
	for id, job := range r.pending {
		if len(out) == limit {
			break
		}
		delete(r.pending, id)
		r.backup[id] = owner + ":" + id
		job.Attempt = 1
		out = append(out, domain.Lease{Job: job, Token: r.backup[id]})
	}
	return out, nil
}

func (r *memRepo) Ack(_ context.Context, l domain.Lease) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.backup[l.ID] != l.Token {
		return false, nil
	}
	delete(r.backup, l.ID)
	r.done[l.ID]++
	return true, nil
}

func (r *memRepo) Extend(context.Context, domain.Lease, int64) (bool, error) { return true, nil }
func (r *memRepo) Nack(context.Context, domain.Lease, int64, int, int64) (domain.NackResult, error) {
	return domain.NackRetried, nil
}
func (r *memRepo) ReapExpired(context.Context, int64, int, int) (int, error) { return 0, nil }
func (r *memRepo) Stats(context.Context, int64) (domain.Stats, error)        { return domain.Stats{}, nil }
func (r *memRepo) Save(context.Context, domain.Job) error                    { return nil }
func (r *memRepo) Get(context.Context, string) (domain.Job, error)           { return domain.Job{}, nil }
func (r *memRepo) Cancel(context.Context, string) error                      { return nil }
func (r *memRepo) RequeueDead(context.Context, string, int64) error          { return nil }
func (r *memRepo) Ping(context.Context) error                                { return nil }

type noMetrics struct{}

func (noMetrics) SetLeader(bool)                                   {}
func (noMetrics) TermFinished(int, time.Duration)                  {}
func (noMetrics) LocalQueue(int)                                   {}
func (noMetrics) JobFinished(string, string, time.Duration, int64) {}
func (noMetrics) Reaped(int)                                       {}
func (noMetrics) QueueDepth(domain.Stats)                          {}
