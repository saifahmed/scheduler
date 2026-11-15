// Package http is the outermost delivery layer: Gin handlers that translate
// HTTP ⇄ domain commands. It depends on domain.JobService only (an interface).
package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"example.com/scheduler/internal/domain"
	"example.com/scheduler/internal/reqctx"
)

// HealthChecker lets readiness probe dependencies (Redis) without knowing about them.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

type Handler struct {
	svc    domain.JobService
	health HealthChecker
	now    func() time.Time
}

func NewHandler(svc domain.JobService, health HealthChecker) *Handler {
	return &Handler{svc: svc, health: health, now: time.Now}
}

// POST /v1/jobs
func (h *Handler) CreateJob(c *gin.Context) {
	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.ScheduledAt != nil && req.DelayMs != nil {
		h.fail(c, http.StatusBadRequest, "invalid_request", "use either scheduled_at or delay_ms")
		return
	}
	var runAt time.Time
	switch {
	case req.ScheduledAt != nil:
		runAt = time.UnixMilli(*req.ScheduledAt)
	case req.DelayMs != nil:
		runAt = h.now().Add(time.Duration(*req.DelayMs) * time.Millisecond)
	}

	job, err := h.svc.Schedule(c.Request.Context(), domain.ScheduleCommand{
		ID: req.ID, Type: req.Type, URL: req.URL, RunAt: runAt, Payload: req.Payload,
	})
	if err != nil {
		h.domainError(c, err)
		return
	}
	c.Header("Location", "/v1/jobs/"+job.ID)
	c.JSON(http.StatusCreated, toResponse(job))
}

// GET /v1/jobs/:id
func (h *Handler) GetJob(c *gin.Context) {
	job, err := h.svc.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.domainError(c, err)
		return
	}
	c.JSON(http.StatusOK, toResponse(job))
}

// DELETE /v1/jobs/:id
func (h *Handler) CancelJob(c *gin.Context) {
	if err := h.svc.Cancel(c.Request.Context(), c.Param("id")); err != nil {
		h.domainError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// POST /v1/jobs/:id/requeue  (moves a job from DLQ back to pending)
func (h *Handler) RequeueJob(c *gin.Context) {
	if err := h.svc.Requeue(c.Request.Context(), c.Param("id")); err != nil {
		h.domainError(c, err)
		return
	}
	c.Status(http.StatusAccepted)
}

// GET /v1/stats
func (h *Handler) Stats(c *gin.Context) {
	st, err := h.svc.Stats(c.Request.Context())
	if err != nil {
		h.domainError(c, err)
		return
	}
	c.JSON(http.StatusOK, st)
}

// GET /healthz — liveness: process is up.
func (h *Handler) Live(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) }

// GET /readyz — readiness: Redis reachable.
func (h *Handler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
	defer cancel()
	if err := h.health.Ping(ctx); err != nil {
		h.fail(c, http.StatusServiceUnavailable, "not_ready", "redis unavailable")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// domainError is the single place mapping domain errors → HTTP status codes.
func (h *Handler) domainError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		h.fail(c, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, domain.ErrDuplicate):
		h.fail(c, http.StatusConflict, "duplicate", err.Error())
	case errors.Is(err, domain.ErrJobRunning):
		h.fail(c, http.StatusConflict, "job_running", err.Error())
	case errors.Is(err, domain.ErrNotDead):
		h.fail(c, http.StatusConflict, "not_dead", err.Error())
	case errors.Is(err, domain.ErrInvalid), errors.Is(err, domain.ErrUnknownType):
		h.fail(c, http.StatusUnprocessableEntity, "invalid_job", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		_ = c.Error(err)
		h.fail(c, http.StatusGatewayTimeout, "timeout", "upstream timeout")
	default:
		_ = c.Error(err) // shows up in the access log, not in the response
		h.fail(c, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h *Handler) fail(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, ErrorResponse{
		Error: code, Message: msg, RequestID: reqctx.RequestID(c.Request.Context()),
	})
}
