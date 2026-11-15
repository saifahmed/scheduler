// Package domain is the innermost onion layer: entities, errors and ports
// (interfaces). It depends on nothing but the standard library.
package domain

import (
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDead    Status = "dead"
)

// Job is the core entity. ID is a 9-char nanoid; ScheduledAt is epoch millis.
type Job struct {
	ID          string
	Type        string
	URL         string // for TypeHTTP: where the payload is POSTed
	ScheduledAt int64
	CreatedAt   int64
	Payload     json.RawMessage
	Attempt     int
	Status      Status
}

// Lease is a claimed job plus the owner token that proves this worker
// still owns the current attempt.
type Lease struct {
	Job
	Token string
}

type Stats struct {
	Pending int64 `json:"pending"`
	Due     int64 `json:"due"` // pending and already past scheduled time (backlog)
	Running int64 `json:"running"`
	Dead    int64 `json:"dead"`
}

type NackResult int

const (
	NackLeaseLost NackResult = iota
	NackRetried
	NackDeadLettered
)

// TypeHTTP is the built-in job type: POST the payload to the job's URL.
const TypeHTTP = "http"

type ScheduleCommand struct {
	ID      string // optional; generated when empty
	Type    string // optional when URL is set (defaults to TypeHTTP)
	URL     string
	RunAt   time.Time
	Payload json.RawMessage
}

var (
	ErrNotFound    = errors.New("job not found")
	ErrDuplicate   = errors.New("job id already exists")
	ErrJobRunning  = errors.New("job is running")
	ErrNotDead     = errors.New("job is not in dead letter queue")
	ErrInvalid     = errors.New("invalid argument")
	ErrUnknownType = errors.New("unknown job type")
	ErrNotLeader   = errors.New("leader lock held by another node")
)

const IDLen = 9

var idRe = regexp.MustCompile(`^[A-Za-z0-9_-]{9}$`)

func ValidID(id string) bool { return idRe.MatchString(id) }
