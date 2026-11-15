// Composition root: the only place that knows every concrete type.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"example.com/scheduler/internal/application"
	"example.com/scheduler/internal/config"
	"example.com/scheduler/internal/decorator"
	"example.com/scheduler/internal/domain"
	"example.com/scheduler/internal/infrastructure/idgen"
	"example.com/scheduler/internal/infrastructure/metrics"
	"example.com/scheduler/internal/infrastructure/redisrepo"
	"example.com/scheduler/internal/infrastructure/redlock"
	"example.com/scheduler/internal/infrastructure/webhook"
	transport "example.com/scheduler/internal/transport/http"
)

func main() {
	cfg := config.Load()
	log := newLogger(cfg.LogLevel)
	gin.SetMode(gin.ReleaseMode)
	runtime.SetBlockProfileRate(cfg.BlockProfile)
	runtime.SetMutexProfileFraction(cfg.MutexProfile)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: cfg.RedisAddrs, Password: cfg.RedisPassword})
	defer rdb.Close()

	repo := redisrepo.New(rdb, cfg.Namespace)
	m := metrics.New()
	clock := application.SystemClock{}
	handlers := registerHandlers(cfg, log)

	// Use cases, wrapped as Instrumenting(Logging(core)).
	var svc domain.JobService = application.NewJobService(repo, idgen.NewNanoid(), clock, handlers,
		application.ServiceConfig{MaxPayloadSize: int(cfg.MaxBodyBytes)})
	svc = decorator.Logging(log)(svc)
	svc = decorator.Instrumenting(m.ServiceCalls, m.ServiceLatency)(svc)

	h := transport.NewHandler(svc, repo)
	api := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           transport.NewAPIRouter(h, m, log, cfg.MaxBodyBytes),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	admin := &http.Server{ // no WriteTimeout: pprof profiles stream for ?seconds=N
		Addr:              cfg.AdminAddr,
		Handler:           transport.NewAdminRouter(m, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	serve(&wg, api, log, stop)
	serve(&wg, admin, log, stop)

	if cfg.EnableWorker {
		lock := newLeaderLock(cfg)
		worker := application.NewWorker(repo, lock, handlers, clock, m, log, application.WorkerConfig{
			NodeID:         cfg.NodeID,
			BatchSize:      cfg.BatchSize,
			BatchesPerTerm: cfg.BatchesPerTerm,
			Concurrency:    cfg.Concurrency,
			QueueSize:      cfg.QueueSize,
			ElectionDelay:  cfg.ElectionDelay,
			YieldDelay:     cfg.YieldDelay,
			Lease:          cfg.Lease,
			MaxAttempts:    cfg.MaxAttempts,
		})
		wg.Add(1)
		go func() { defer wg.Done(); worker.Run(ctx) }()
	}

	<-ctx.Done()
	shutdown(&wg, log, cfg.ShutdownTimeout, api, admin)
}

// registerHandlers is where job types are added. "http" (url jobs) is built in.
func registerHandlers(cfg config.Config, log *slog.Logger) *application.Registry {
	r := application.NewRegistry()
	r.Register(domain.TypeHTTP, webhook.New(cfg.WebhookTimeout, cfg.BlockPrivate))
	r.Register("echo", domain.HandlerFunc(func(ctx context.Context, j domain.Job) error {
		log.Info("echo", "job_id", j.ID, "attempt", j.Attempt, "payload", string(j.Payload))
		return nil
	}))
	return r
}

// newLeaderLock connects to each independent Redlock master.
func newLeaderLock(cfg config.Config) *redlock.Redlock {
	clients := make([]redis.UniversalClient, len(cfg.RedlockAddrs))
	for i, addr := range cfg.RedlockAddrs {
		clients[i] = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{addr}, Password: cfg.RedisPassword})
	}
	return redlock.New(clients, "{"+cfg.Namespace+"}:leader", cfg.NodeID, cfg.LockTTL)
}

func serve(wg *sync.WaitGroup, s *http.Server, log *slog.Logger, onFail func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("listening", "addr", s.Addr)
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "addr", s.Addr, "err", err)
			onFail()
		}
	}()
}

// shutdown stops the HTTP servers, then waits for the worker to drain its channel.
func shutdown(wg *sync.WaitGroup, log *slog.Logger, timeout time.Duration, servers ...*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(ctx)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Info("shutdown complete")
	case <-ctx.Done():
		log.Warn("shutdown timed out; unfinished jobs will be reaped after their lease")
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
	slog.SetDefault(log)
	return log
}
