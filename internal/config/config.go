// Package config loads settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	AdminAddr       string // /metrics + /debug/pprof (keep internal)
	RedisAddrs      []string
	RedisPassword   string
	Namespace       string
	NodeID          string
	EnableWorker    bool     // false = API-only node (never runs for leader)
	RedlockAddrs    []string // independent masters for the leader lock (3 or 5 in prod)
	LockTTL         time.Duration
	ElectionDelay   time.Duration // follower retry interval
	YieldDelay      time.Duration // leader pause after releasing, lets others win
	BatchSize       int           // jobs per Lua fetch
	BatchesPerTerm  int           // batches a leader runs before releasing the lock
	Concurrency     int           // worker goroutines reading the local channel
	QueueSize       int           // local channel capacity (0 = 2 × Concurrency)
	Lease           time.Duration
	MaxAttempts     int
	WebhookTimeout  time.Duration // per-request timeout for url jobs
	BlockPrivate    bool          // refuse url jobs that resolve to internal addresses
	MaxBodyBytes    int64
	ShutdownTimeout time.Duration
	LogLevel        string
	BlockProfile    int // runtime.SetBlockProfileRate; 0 = off
	MutexProfile    int // runtime.SetMutexProfileFraction; 0 = off
}

func Load() Config {
	host, _ := os.Hostname()
	return Config{
		HTTPAddr:        str("HTTP_ADDR", ":8080"),
		AdminAddr:       str("ADMIN_ADDR", ":6060"),
		RedisAddrs:      strings.Split(str("REDIS_ADDRS", "localhost:6379"), ","),
		RedisPassword:   str("REDIS_PASSWORD", ""),
		Namespace:       str("NAMESPACE", "sched"),
		NodeID:          str("NODE_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		EnableWorker:    boolean("ENABLE_WORKER", true),
		RedlockAddrs:    strings.Split(str("REDLOCK_ADDRS", str("REDIS_ADDRS", "localhost:6379")), ","),
		LockTTL:         dur("LOCK_TTL", 10*time.Second),
		ElectionDelay:   dur("ELECTION_DELAY", 200*time.Millisecond),
		YieldDelay:      dur("YIELD_DELAY", 20*time.Millisecond),
		BatchSize:       integer("BATCH_SIZE", 100),
		BatchesPerTerm:  integer("BATCHES_PER_TERM", 1),
		Concurrency:     integer("WORKER_CONCURRENCY", 16),
		QueueSize:       integer("LOCAL_QUEUE_SIZE", 0), // 0 = 2 × WORKER_CONCURRENCY
		Lease:           dur("LEASE", 30*time.Second),
		MaxAttempts:     integer("MAX_ATTEMPTS", 5),
		WebhookTimeout:  dur("WEBHOOK_TIMEOUT", 10*time.Second),
		BlockPrivate:    boolean("BLOCK_PRIVATE_URLS", true),
		MaxBodyBytes:    int64(integer("MAX_BODY_BYTES", 1<<20)),
		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 30*time.Second),
		LogLevel:        str("LOG_LEVEL", "info"),
		BlockProfile:    integer("PPROF_BLOCK_RATE", 0),
		MutexProfile:    integer("PPROF_MUTEX_FRACTION", 0),
	}
}

func str(k, d string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return d
}

func integer(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func boolean(k string, d bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func dur(k string, d time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return d
}
