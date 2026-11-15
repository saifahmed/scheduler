package http

import (
	"encoding/json"

	"example.com/scheduler/internal/domain"
)

// CreateJobRequest needs a url (payload is POSTed there) or a registered type.
// Give scheduled_at (epoch ms) or delay_ms; neither means run now.
type CreateJobRequest struct {
	ID          string          `json:"id,omitempty"`
	URL         string          `json:"url,omitempty" binding:"omitempty,max=2048"`
	Type        string          `json:"type,omitempty" binding:"omitempty,max=64"`
	ScheduledAt *int64          `json:"scheduled_at,omitempty" binding:"omitempty,gt=0"`
	DelayMs     *int64          `json:"delay_ms,omitempty" binding:"omitempty,gte=0"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

type JobResponse struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	URL         string          `json:"url,omitempty"`
	Status      domain.Status   `json:"status"`
	ScheduledAt int64           `json:"scheduled_at"`
	CreatedAt   int64           `json:"created_at"`
	Attempt     int             `json:"attempt"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

type ErrorResponse struct {
	Error     string `json:"error"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

func toResponse(j domain.Job) JobResponse {
	return JobResponse{ID: j.ID, Type: j.Type, URL: j.URL, Status: j.Status, ScheduledAt: j.ScheduledAt,
		CreatedAt: j.CreatedAt, Attempt: j.Attempt, Payload: j.Payload}
}
