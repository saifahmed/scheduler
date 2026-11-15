// Package redlock implements domain.LeaderLock with the Redlock algorithm:
// a node holds the lock when it sets the key on a majority of N independent
// Redis masters before the lock's TTL runs out.
//
// Use 1 instance in development and 3 or 5 independent masters in production.
package redlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"example.com/scheduler/internal/domain"
)

// Deletes the key only if we still own it.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

const (
	nodeTimeout = 50 * time.Millisecond // per-instance call timeout
	driftFactor = 0.01                  // clock-drift allowance from the Redlock paper
)

type Redlock struct {
	clients []redis.UniversalClient
	key     string
	nodeID  string
	ttl     time.Duration
}

func New(clients []redis.UniversalClient, key, nodeID string, ttl time.Duration) *Redlock {
	return &Redlock{clients: clients, key: key, nodeID: nodeID, ttl: ttl}
}

func (r *Redlock) Acquire(ctx context.Context) (domain.Leadership, error) {
	token, err := r.newToken()
	if err != nil {
		return nil, err
	}

	start := time.Now()
	acquired := r.onEachInstance(ctx, func(ctx context.Context, c redis.UniversalClient) bool {
		ok, err := c.SetNX(ctx, r.key, token, r.ttl).Result()
		return err == nil && ok
	})

	drift := time.Duration(float64(r.ttl)*driftFactor) + 2*time.Millisecond
	validUntil := start.Add(r.ttl - drift)

	if acquired < len(r.clients)/2+1 || time.Now().After(validUntil) {
		r.release(token) // undo partial locks so the next election isn't blocked
		return nil, domain.ErrNotLeader
	}
	return &leadership{lock: r, token: token, validUntil: validUntil}, nil
}

// onEachInstance runs fn on every instance in parallel and counts successes.
func (r *Redlock) onEachInstance(ctx context.Context, fn func(context.Context, redis.UniversalClient) bool) int {
	var ok atomic.Int32
	var wg sync.WaitGroup
	for _, c := range r.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, nodeTimeout)
			defer cancel()
			if fn(ctx, c) {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(ok.Load())
}

func (r *Redlock) release(token string) {
	r.onEachInstance(context.Background(), func(ctx context.Context, c redis.UniversalClient) bool {
		return releaseScript.Run(ctx, c, []string{r.key}, token).Err() == nil
	})
}

func (r *Redlock) newToken() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return r.nodeID + ":" + hex.EncodeToString(b), nil
}

type leadership struct {
	lock       *Redlock
	token      string
	validUntil time.Time
}

func (l *leadership) Token() string { return l.token }

func (l *leadership) Valid() bool { return time.Now().Before(l.validUntil) }

func (l *leadership) Release(context.Context) error {
	l.lock.release(l.token)
	return nil
}
