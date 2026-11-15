package http

import (
	"log/slog"
	"net/http"
	"net/http/pprof"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"example.com/scheduler/internal/infrastructure/metrics"
	"example.com/scheduler/internal/transport/http/middleware"
)

// NewAPIRouter builds the public API (Application Programming Interface) router.
// Middleware order: recovery → request ID → metrics → logging → body limit → handler.
func NewAPIRouter(h *Handler, m *metrics.Metrics, log *slog.Logger, maxBody int64) *gin.Engine {
	r := gin.New()
	r.Use(
		middleware.Recovery(log),
		middleware.RequestID(),
		middleware.Metrics(m.HTTPRequests, m.HTTPLatency, m.HTTPInFlight),
		middleware.Logging(log),
		middleware.BodyLimit(maxBody),
	)
	r.HandleMethodNotAllowed = true
	r.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "not_found", Message: "route not found"})
	})

	r.GET("/healthz", h.Live)
	r.GET("/readyz", h.Ready)

	v1 := r.Group("/v1")
	{
		v1.POST("/jobs", h.CreateJob)
		v1.GET("/jobs/:id", h.GetJob)
		v1.DELETE("/jobs/:id", h.CancelJob)
		v1.POST("/jobs/:id/requeue", h.RequeueJob)
		v1.GET("/stats", h.Stats)
	}
	return r
}

// NewAdminRouter serves /metrics and /debug/pprof/* on a separate, internal-only
// port so profiling endpoints are never exposed through the public load balancer.
func NewAdminRouter(m *metrics.Metrics, log *slog.Logger) *gin.Engine {
	r := gin.New()
	r.Use(middleware.Recovery(log))

	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})))

	pp := r.Group("/debug/pprof")
	{
		pp.GET("/", gin.WrapF(pprof.Index))
		pp.GET("/cmdline", gin.WrapF(pprof.Cmdline))
		pp.GET("/profile", gin.WrapF(pprof.Profile)) // CPU (Central Processing Unit): ?seconds=30
		pp.GET("/symbol", gin.WrapF(pprof.Symbol))
		pp.POST("/symbol", gin.WrapF(pprof.Symbol))
		pp.GET("/trace", gin.WrapF(pprof.Trace))
		for _, name := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
			pp.GET("/"+name, gin.WrapH(pprof.Handler(name)))
		}
	}
	return r
}
