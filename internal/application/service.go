// Package application holds the use cases. It depends only on domain ports.
package application

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"example.com/scheduler/internal/domain"
)

const (
	maxDelay       = 365 * 24 * time.Hour // reject jobs scheduled further ahead
	maxIDRetries   = 5                    // nanoid collisions are rare; retry a few times
	maxURLLength   = 2048
	defaultMaxBody = 64 << 10
)

type ServiceConfig struct {
	MaxPayloadSize int
}

type JobService struct {
	repo     domain.JobRepository
	ids      domain.IDGenerator
	clock    domain.Clock
	handlers domain.HandlerRegistry
	maxBody  int
}

func NewJobService(repo domain.JobRepository, ids domain.IDGenerator, clock domain.Clock,
	handlers domain.HandlerRegistry, cfg ServiceConfig) *JobService {
	if cfg.MaxPayloadSize <= 0 {
		cfg.MaxPayloadSize = defaultMaxBody
	}
	return &JobService{repo: repo, ids: ids, clock: clock, handlers: handlers, maxBody: cfg.MaxPayloadSize}
}

func (s *JobService) Schedule(ctx context.Context, cmd domain.ScheduleCommand) (domain.Job, error) {
	now := s.clock.Now()
	if cmd.RunAt.IsZero() {
		cmd.RunAt = now
	}
	if cmd.Type == "" && cmd.URL != "" {
		cmd.Type = domain.TypeHTTP
	}
	if err := s.validate(cmd, now); err != nil {
		return domain.Job{}, err
	}

	job := domain.Job{
		ID:          cmd.ID,
		Type:        cmd.Type,
		URL:         cmd.URL,
		ScheduledAt: cmd.RunAt.UnixMilli(),
		CreatedAt:   now.UnixMilli(),
		Payload:     cmd.Payload,
		Status:      domain.StatusPending,
	}

	// A caller-supplied ID makes the create idempotent: a repeat returns ErrDuplicate.
	if job.ID != "" {
		return job, s.repo.Save(ctx, job)
	}
	return s.saveWithNewID(ctx, job)
}

func (s *JobService) validate(cmd domain.ScheduleCommand, now time.Time) error {
	switch {
	case cmd.Type == "":
		return fmt.Errorf("%w: type or url is required", domain.ErrInvalid)
	case cmd.Type == domain.TypeHTTP && cmd.URL == "":
		return fmt.Errorf("%w: url is required for type %q", domain.ErrInvalid, domain.TypeHTTP)
	case cmd.URL != "" && !validURL(cmd.URL):
		return fmt.Errorf("%w: url must be an absolute http(s) URL of at most %d chars", domain.ErrInvalid, maxURLLength)
	case cmd.ID != "" && !domain.ValidID(cmd.ID):
		return fmt.Errorf("%w: id must be %d chars of [A-Za-z0-9_-]", domain.ErrInvalid, domain.IDLen)
	case len(cmd.Payload) > s.maxBody:
		return fmt.Errorf("%w: payload exceeds %d bytes", domain.ErrInvalid, s.maxBody)
	case cmd.RunAt.Sub(now) > maxDelay:
		return fmt.Errorf("%w: scheduled_at is more than a year ahead", domain.ErrInvalid)
	}
	if _, ok := s.handlers.Lookup(cmd.Type); !ok {
		return fmt.Errorf("%w: %q", domain.ErrUnknownType, cmd.Type)
	}
	return nil
}

func validURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && len(raw) <= maxURLLength && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func (s *JobService) saveWithNewID(ctx context.Context, job domain.Job) (domain.Job, error) {
	for range maxIDRetries {
		id, err := s.ids.NewID()
		if err != nil {
			return domain.Job{}, err
		}
		job.ID = id
		if err := s.repo.Save(ctx, job); !errors.Is(err, domain.ErrDuplicate) {
			return job, err
		}
	}
	return domain.Job{}, domain.ErrDuplicate
}

func (s *JobService) Get(ctx context.Context, id string) (domain.Job, error) {
	if !domain.ValidID(id) {
		return domain.Job{}, domain.ErrNotFound
	}
	return s.repo.Get(ctx, id)
}

func (s *JobService) Cancel(ctx context.Context, id string) error {
	if !domain.ValidID(id) {
		return domain.ErrNotFound
	}
	return s.repo.Cancel(ctx, id)
}

func (s *JobService) Requeue(ctx context.Context, id string) error {
	if !domain.ValidID(id) {
		return domain.ErrNotFound
	}
	return s.repo.RequeueDead(ctx, id, s.clock.Now().UnixMilli())
}

func (s *JobService) Stats(ctx context.Context) (domain.Stats, error) {
	return s.repo.Stats(ctx, s.clock.Now().UnixMilli())
}
