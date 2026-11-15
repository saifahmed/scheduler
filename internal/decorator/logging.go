// Package decorator wraps domain.JobService with cross-cutting concerns.
// Each decorator implements the same interface, so they stack:
// Instrumenting(Logging(core)).
package decorator

import (
	"context"
	"log/slog"
	"time"

	"example.com/scheduler/internal/domain"
	"example.com/scheduler/internal/reqctx"
)

type logging struct {
	next domain.JobService
	log  *slog.Logger
}

func Logging(log *slog.Logger) func(domain.JobService) domain.JobService {
	return func(next domain.JobService) domain.JobService {
		return &logging{next: next, log: log.With("component", "service")}
	}
}

func (l *logging) Schedule(ctx context.Context, cmd domain.ScheduleCommand) (domain.Job, error) {
	start := time.Now()
	job, err := l.next.Schedule(ctx, cmd)
	l.record(ctx, "Schedule", start, err, "job_id", job.ID, "type", cmd.Type)
	return job, err
}

func (l *logging) Get(ctx context.Context, id string) (domain.Job, error) {
	start := time.Now()
	job, err := l.next.Get(ctx, id)
	l.record(ctx, "Get", start, err, "job_id", id)
	return job, err
}

func (l *logging) Cancel(ctx context.Context, id string) error {
	start := time.Now()
	err := l.next.Cancel(ctx, id)
	l.record(ctx, "Cancel", start, err, "job_id", id)
	return err
}

func (l *logging) Requeue(ctx context.Context, id string) error {
	start := time.Now()
	err := l.next.Requeue(ctx, id)
	l.record(ctx, "Requeue", start, err, "job_id", id)
	return err
}

func (l *logging) Stats(ctx context.Context) (domain.Stats, error) {
	start := time.Now()
	stats, err := l.next.Stats(ctx)
	l.record(ctx, "Stats", start, err)
	return stats, err
}

func (l *logging) record(ctx context.Context, method string, start time.Time, err error, attrs ...any) {
	attrs = append(attrs, "method", method, "took", time.Since(start), "request_id", reqctx.RequestID(ctx))
	if err != nil {
		l.log.WarnContext(ctx, "service call failed", append(attrs, "err", err)...)
		return
	}
	l.log.DebugContext(ctx, "service call", attrs...)
}
