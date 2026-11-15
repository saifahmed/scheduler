package webhook

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"example.com/scheduler/internal/domain"
)

func TestPostsPayloadWithJobHeaders(t *testing.T) {
	var got *http.Request
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got, body = r, string(b)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	job := domain.Job{ID: "V1StGXR8_", URL: srv.URL + "/hook", Payload: []byte(`{"a":1}`), Attempt: 2, ScheduledAt: 1000}
	if err := New(time.Second, false).Handle(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	if got.Method != http.MethodPost || got.URL.Path != "/hook" || body != `{"a":1}` {
		t.Fatalf("got %s %s %q", got.Method, got.URL.Path, body)
	}
	for header, want := range map[string]string{
		"Content-Type": "application/json", "Idempotency-Key": "V1StGXR8_",
		"X-Job-Attempt": "2", "X-Job-Scheduled-At": "1000",
	} {
		if got.Header.Get(header) != want {
			t.Errorf("%s = %q, want %q", header, got.Header.Get(header), want)
		}
	}
}

func TestNon2xxIsAnError(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusBadRequest, http.StatusServiceUnavailable} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "http://example.com")
			w.WriteHeader(status)
		}))
		err := New(time.Second, false).Handle(context.Background(), domain.Job{URL: srv.URL})
		srv.Close()
		if err == nil {
			t.Errorf("status %d: expected an error", status)
		}
	}
}

func TestBlocksPrivateAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("request reached a loopback server")
	}))
	defer srv.Close()

	err := New(time.Second, true).Handle(context.Background(), domain.Job{URL: srv.URL})
	if !errors.Is(err, ErrPrivateAddress) {
		t.Fatalf("got %v, want ErrPrivateAddress", err)
	}
}
