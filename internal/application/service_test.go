package application

import (
	"context"
	"errors"
	"testing"

	"example.com/scheduler/internal/domain"
)

func TestScheduleValidatesURLAndType(t *testing.T) {
	handlers := NewRegistry()
	handlers.Register(domain.TypeHTTP, domain.HandlerFunc(func(context.Context, domain.Job) error { return nil }))
	svc := NewJobService(newMemRepo(), fixedID("V1StGXR8_"), SystemClock{}, handlers, ServiceConfig{})

	tests := []struct {
		name     string
		cmd      domain.ScheduleCommand
		wantType string
		wantErr  error
	}{
		{"url alone defaults to http", domain.ScheduleCommand{URL: "https://api.example.com/hook"}, domain.TypeHTTP, nil},
		{"neither type nor url", domain.ScheduleCommand{}, "", domain.ErrInvalid},
		{"http type without url", domain.ScheduleCommand{Type: domain.TypeHTTP}, "", domain.ErrInvalid},
		{"relative url", domain.ScheduleCommand{URL: "/hook"}, "", domain.ErrInvalid},
		{"non-http scheme", domain.ScheduleCommand{URL: "ftp://example.com/x"}, "", domain.ErrInvalid},
		{"unregistered type", domain.ScheduleCommand{Type: "email"}, "", domain.ErrUnknownType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job, err := svc.Schedule(context.Background(), tc.cmd)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if err == nil && (job.Type != tc.wantType || job.URL != tc.cmd.URL || job.ID != "V1StGXR8_") {
				t.Fatalf("got job %+v", job)
			}
		})
	}
}

type fixedID string

func (f fixedID) NewID() (string, error) { return string(f), nil }
