package application

import (
	"context"
	"time"
)

// leaderLoop competes for the leader lock until ctx is cancelled. The lock is
// held only while fetching, so terms are short and every node gets turns.
func (w *Worker) leaderLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if w.freeSlots() == 0 { // busy node: let an idle one lead
			sleep(ctx, jitter(w.cfg.ElectionDelay))
			continue
		}

		lead, err := w.lock.Acquire(ctx)
		if err != nil {
			sleep(ctx, jitter(w.cfg.ElectionDelay))
			continue
		}

		w.metrics.SetLeader(true)
		fetched := w.leadTerm(ctx, lead.Valid, lead.Token())
		lead.Release(context.Background())
		w.metrics.SetLeader(false)

		if fetched == 0 {
			sleep(ctx, jitter(w.cfg.IdleDelay))
		} else {
			sleep(ctx, jitter(w.cfg.YieldDelay))
		}
	}
}

// leadTerm recovers expired leases, then fetches due jobs into the local
// channel. stillLeader guards each fetch against an expired lock.
func (w *Worker) leadTerm(ctx context.Context, stillLeader func() bool, token string) int {
	start := time.Now()

	if n, err := w.repo.ReapExpired(ctx, w.nowMs(), 1000, w.cfg.MaxAttempts); err != nil {
		w.log.Error("reap failed", "err", err)
	} else if n > 0 {
		w.metrics.Reaped(n)
	}
	if stats, err := w.repo.Stats(ctx, w.nowMs()); err == nil {
		w.metrics.QueueDepth(stats)
	}

	fetched := 0
	for range w.cfg.BatchesPerTerm {
		limit := min(w.cfg.BatchSize, w.freeSlots())
		if limit == 0 || !stillLeader() {
			break
		}

		// Lua: move up to `limit` due jobs from pending to backup.
		jobs, err := w.repo.Claim(ctx, w.nowMs(), limit, w.cfg.Lease, token)
		if err != nil {
			w.log.Error("fetch failed", "err", err)
			break
		}
		for _, job := range jobs {
			w.jobs <- job // never blocks: limit ≤ free slots, and only this loop sends
		}
		fetched += len(jobs)

		if len(jobs) < limit {
			break // nothing more is due
		}
	}

	w.metrics.LocalQueue(len(w.jobs))
	w.metrics.TermFinished(fetched, time.Since(start))
	return fetched
}

func (w *Worker) freeSlots() int { return cap(w.jobs) - len(w.jobs) }
