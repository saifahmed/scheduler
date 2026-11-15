// Package middleware contains Gin HTTP middleware: request ID, structured
// access logging, Prometheus instrumenting, panic recovery and body limits.
package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	"example.com/scheduler/internal/reqctx"
)

const HeaderRequestID = "X-Request-ID"

// RequestID propagates an incoming X-Request-ID or generates one, and stores it
// in the request context so service-layer logs can correlate.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(HeaderRequestID)
		if id == "" || len(id) > 64 {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		c.Header(HeaderRequestID, id)
		c.Request = c.Request.WithContext(reqctx.WithRequestID(c.Request.Context(), id))
		c.Next()
	}
}

// Logging writes one structured access log line per request.
func Logging(log *slog.Logger) gin.HandlerFunc {
	log = log.With("component", "http")
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		attrs := []any{
			"method", c.Request.Method,
			"route", routeOf(c),
			"path", c.Request.URL.Path,
			"status", status,
			"took", time.Since(start),
			"bytes", c.Writer.Size(),
			"client_ip", c.ClientIP(),
			"request_id", reqctx.RequestID(c.Request.Context()),
		}
		if len(c.Errors) > 0 {
			attrs = append(attrs, "errors", c.Errors.String())
		}
		switch {
		case status >= 500:
			log.Error("request", attrs...)
		case status >= 400:
			log.Warn("request", attrs...)
		default:
			log.Info("request", attrs...)
		}
	}
}

// Metrics records RED (Rate, Errors, Duration) metrics per route template.
// Using c.FullPath() (e.g. /v1/jobs/:id) keeps label cardinality bounded.
func Metrics(requests *prometheus.CounterVec, latency *prometheus.HistogramVec, inFlight prometheus.Gauge) gin.HandlerFunc {
	return func(c *gin.Context) {
		inFlight.Inc()
		start := time.Now()
		c.Next()
		inFlight.Dec()

		route := routeOf(c)
		requests.WithLabelValues(c.Request.Method, route, strconv.Itoa(c.Writer.Status())).Inc()
		latency.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
	}
}

// Recovery converts panics into a 500 JSON response and logs the stack.
func Recovery(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic recovered", "panic", r, "stack", string(debug.Stack()),
					"request_id", reqctx.RequestID(c.Request.Context()))
				c.AbortWithStatusJSON(http.StatusInternalServerError,
					gin.H{"error": "internal_error", "message": "internal server error"})
			}
		}()
		c.Next()
	}
}

// BodyLimit caps request body size to protect Redis and memory.
func BodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

func routeOf(c *gin.Context) string {
	if r := c.FullPath(); r != "" {
		return r
	}
	return "unmatched"
}
