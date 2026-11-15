// Package redisrepo is the Redis adapter for domain.JobRepository.
package redisrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"example.com/scheduler/internal/domain"
)

type envelope struct {
	Type        string          `json:"type"`
	URL         string          `json:"url,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt int64           `json:"scheduled_at"`
	CreatedAt   int64           `json:"created_at"`
}

type Repository struct {
	rdb  redis.UniversalClient // standalone, Sentinel or Cluster
	keys []string
}

var _ domain.JobRepository = (*Repository)(nil)

func New(rdb redis.UniversalClient, namespace string) *Repository {
	p := "{" + namespace + "}:"
	return &Repository{rdb: rdb, keys: []string{
		p + "pending", p + "backup", p + "data", p + "lease", p + "attempt", p + "dead",
	}}
}

func (r *Repository) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

func (r *Repository) Save(ctx context.Context, j domain.Job) error {
	env, err := json.Marshal(envelope{j.Type, j.URL, j.Payload, j.ScheduledAt, j.CreatedAt})
	if err != nil {
		return err
	}
	n, err := scheduleScript.Run(ctx, r.rdb, r.keys, j.ID, j.ScheduledAt, env).Int()
	if err != nil {
		return fmt.Errorf("redis schedule: %w", err)
	}
	if n == 0 {
		return domain.ErrDuplicate
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, id string) (domain.Job, error) {
	res, err := getScript.Run(ctx, r.rdb, r.keys, id).Slice()
	if errors.Is(err, redis.Nil) {
		return domain.Job{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis get: %w", err)
	}
	j, err := decode(id, res[0])
	if err != nil {
		return domain.Job{}, err
	}
	j.Status = domain.Status(res[1].(string))
	j.Attempt = int(res[2].(int64))
	return j, nil
}

func (r *Repository) Cancel(ctx context.Context, id string) error {
	n, err := cancelScript.Run(ctx, r.rdb, r.keys, id).Int()
	switch {
	case err != nil:
		return fmt.Errorf("redis cancel: %w", err)
	case n == -1:
		return domain.ErrJobRunning
	case n == 0:
		return domain.ErrNotFound
	}
	return nil
}

func (r *Repository) RequeueDead(ctx context.Context, id string, at int64) error {
	n, err := requeueScript.Run(ctx, r.rdb, r.keys, id, at).Int()
	if err != nil {
		return fmt.Errorf("redis requeue: %w", err)
	}
	if n == 0 {
		return domain.ErrNotDead
	}
	return nil
}

func (r *Repository) Stats(ctx context.Context, now int64) (domain.Stats, error) {
	v, err := statsScript.Run(ctx, r.rdb, r.keys, now).Int64Slice()
	if err != nil {
		return domain.Stats{}, fmt.Errorf("redis stats: %w", err)
	}
	return domain.Stats{Pending: v[0], Due: v[1], Running: v[2], Dead: v[3]}, nil
}

func (r *Repository) Claim(ctx context.Context, now int64, limit int, lease time.Duration,
	workerID string) ([]domain.Lease, error) {
	res, err := fetchScript.Run(ctx, r.rdb, r.keys, now, limit, lease.Milliseconds(), workerID).Slice()
	if err != nil {
		return nil, fmt.Errorf("redis claim: %w", err)
	}
	out := make([]domain.Lease, 0, len(res)/4)
	for i := 0; i+3 < len(res); i += 4 {
		id := res[i].(string)
		j, err := decode(id, res[i+1])
		if err != nil {
			// Corrupt envelope: leave it in backup; reaper → DLQ after max attempts.
			continue
		}
		j.Attempt = int(res[i+3].(int64))
		j.Status = domain.StatusRunning
		out = append(out, domain.Lease{Job: j, Token: res[i+2].(string)})
	}
	return out, nil
}

func (r *Repository) Ack(ctx context.Context, l domain.Lease) (bool, error) {
	n, err := ackScript.Run(ctx, r.rdb, r.keys, l.ID, l.Token).Int()
	return n == 1, err
}

func (r *Repository) Nack(ctx context.Context, l domain.Lease, retryAt int64, maxAttempts int,
	now int64) (domain.NackResult, error) {
	n, err := nackScript.Run(ctx, r.rdb, r.keys, l.ID, l.Token, retryAt, maxAttempts, now).Int()
	return domain.NackResult(n), err
}

func (r *Repository) Extend(ctx context.Context, l domain.Lease, deadline int64) (bool, error) {
	n, err := extendScript.Run(ctx, r.rdb, r.keys, l.ID, l.Token, deadline).Int()
	return n == 1, err
}

func (r *Repository) ReapExpired(ctx context.Context, now int64, limit, maxAttempts int) (int, error) {
	return reapScript.Run(ctx, r.rdb, r.keys, now, limit, maxAttempts).Int()
}

func decode(id string, raw any) (domain.Job, error) {
	s, ok := raw.(string)
	if !ok {
		return domain.Job{}, fmt.Errorf("job %s: unexpected envelope type %T", id, raw)
	}
	var e envelope
	if err := json.Unmarshal([]byte(s), &e); err != nil {
		return domain.Job{}, fmt.Errorf("job %s: decode envelope: %w", id, err)
	}
	return domain.Job{ID: id, Type: e.Type, URL: e.URL, Payload: e.Payload,
		ScheduledAt: e.ScheduledAt, CreatedAt: e.CreatedAt}, nil
}
