// Package webhook is the built-in job handler: it POSTs the job's payload to
// the job's URL. A 2xx response is success; anything else is retried.
package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"

	"example.com/scheduler/internal/domain"
)

var ErrPrivateAddress = errors.New("webhook: private or loopback address not allowed")

type Poster struct {
	client *http.Client
}

// New returns a Poster. With blockPrivate set, URLs that resolve to loopback,
// private or link-local addresses are refused at connect time, so a user-supplied
// URL cannot reach internal services (SSRF, Server-Side Request Forgery).
func New(timeout time.Duration, blockPrivate bool) *Poster {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if blockPrivate {
		dialer.Control = refusePrivate
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext

	return &Poster{client: &http.Client{
		Timeout:   timeout,
		Transport: transport,
		// Don't follow redirects: a 3xx could point at an internal address.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (p *Poster) Handle(ctx context.Context, job domain.Job) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.URL, bytes.NewReader(job.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", job.ID) // same key on every retry
	req.Header.Set("X-Job-Id", job.ID)
	req.Header.Set("X-Job-Attempt", strconv.Itoa(job.Attempt))
	req.Header.Set("X-Job-Scheduled-At", strconv.FormatInt(job.ScheduledAt, 10))

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // lets the connection be reused

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook: %s returned %d", job.URL, resp.StatusCode)
	}
	return nil
}

// refusePrivate runs after DNS resolution, so it also catches hostnames that
// resolve to internal addresses.
func refusePrivate(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return ErrPrivateAddress
	}
	return nil
}
