package decorator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"example.com/scheduler/internal/domain"
)

type instrumenting struct {
	next    domain.JobService
	calls   *prometheus.CounterVec   // labels: method, outcome
	latency *prometheus.HistogramVec // labels: method
}

func Instrumenting(calls *prometheus.CounterVec, latency *prometheus.HistogramVec) func(domain.JobService) domain.JobService {
	return func(next domain.JobService) domain.JobService {
		return &instrumenting{next: next, calls: calls, latency: latency}
	}
}

func (m *instrumenting) Schedule(ctx context.Context, cmd domain.ScheduleCommand) (domain.Job, error) {
	start := time.Now()
	job, err := m.next.Schedule(ctx, cmd)
	m.observe("Schedule", start, err)
	return job, err
}

func (m *instrumenting) Get(ctx context.Context, id string) (domain.Job, error) {
	start := time.Now()
	job, err := m.next.Get(ctx, id)
	m.observe("Get", start, err)
	return job, err
}

func (m *instrumenting) Cancel(ctx context.Context, id string) error {
	start := time.Now()
	err := m.next.Cancel(ctx, id)
	m.observe("Cancel", start, err)
	return err
}

func (m *instrumenting) Requeue(ctx context.Context, id string) error {
	start := time.Now()
	err := m.next.Requeue(ctx, id)
	m.observe("Requeue", start, err)
	return err
}

func (m *instrumenting) Stats(ctx context.Context) (domain.Stats, error) {
	start := time.Now()
	stats, err := m.next.Stats(ctx)
	m.observe("Stats", start, err)
	return stats, err
}

func (m *instrumenting) observe(method string, start time.Time, err error) {
	m.calls.WithLabelValues(method, outcome(err)).Inc()
	m.latency.WithLabelValues(method).Observe(time.Since(start).Seconds())
}

// outcome keeps label values few: ok, the caller's fault, or ours.
func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrDuplicate),
		errors.Is(err, domain.ErrInvalid), errors.Is(err, domain.ErrUnknownType),
		errors.Is(err, domain.ErrJobRunning), errors.Is(err, domain.ErrNotDead):
		return "client_error"
	default:
		return "error"
	}
}
