package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rainier/internal/cli"
)

func TestSummarizeReportsCenterSpreadAndInterpolatedTail(t *testing.T) {
	values := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		100 * time.Millisecond,
	}
	original := append([]time.Duration(nil), values...)

	got := summarize("terminal_rtt", values, 75*time.Millisecond)
	if got.Metric != "terminal_rtt" || got.N != 5 {
		t.Fatalf("summary identity = %+v, want terminal_rtt n=5", got)
	}
	checks := map[string]struct {
		got, want float64
	}{
		"mean": {got.MeanMS, 40},
		"p25":  {got.P25MS, 20},
		"p50":  {got.P50MS, 30},
		"p75":  {got.P75MS, 40},
		"p95":  {got.P95MS, 88}, // R-7: 40 + 0.8*(100-40)
		"min":  {got.MinMS, 10},
		"max":  {got.MaxMS, 100},
	}
	for name, check := range checks {
		if math.Abs(check.got-check.want) > 0.001 {
			t.Errorf("%s = %.3fms, want %.3fms", name, check.got, check.want)
		}
	}
	if got.TargetP95MS != 75 || got.Pass == nil || *got.Pass {
		t.Fatalf("target result = target %.1f pass %v, want 75 false", got.TargetP95MS, got.Pass)
	}

	// summarize must not mutate observations; callers reuse their chronological
	// order when writing raw records before the summaries.
	for i, want := range original {
		if values[i] != want {
			t.Fatalf("values mutated at %d: got %s want %s", i, values[i], want)
		}
	}
}

func TestSummarizeRequiresP95StrictlyBelowTarget(t *testing.T) {
	got := summarize("terminal_rtt", []time.Duration{75 * time.Millisecond}, 75*time.Millisecond)
	if got.Pass == nil || *got.Pass {
		t.Fatalf("pass = %v at equality, want false because the target is strictly under 75ms", got.Pass)
	}
}

func TestWaitRunningCancelsAStalledPoll(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer ts.Close()

	started := time.Now()
	_, err := waitRunning(context.Background(), &cli.Client{Base: ts.URL}, "sess_synthetic", 50*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitRunning error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("waitRunning returned after %s, want prompt cancellation", elapsed)
	}
}

func TestPublicFailureDoesNotExposeSessionOrServerDetails(t *testing.T) {
	err := errors.New(`waiting for running state: Get "https://private.invalid/v1/sessions/sess_secret": context deadline exceeded`)
	got := publicFailure(err)
	for _, secret := range []string{"private.invalid", "/v1/sessions", "sess_secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("publicFailure = %q, leaked %q", got, secret)
		}
	}
	if got != "waiting for running state" {
		t.Fatalf("publicFailure = %q, want the allowlisted safe operation", got)
	}
}

func TestPublicFailureHidesUnknownErrors(t *testing.T) {
	got := publicFailure(errors.New(`/private/internal/path: request failed for sess_secret`))
	if got != "operation failed" {
		t.Fatalf("publicFailure = %q, want generic fallback for an unknown error", got)
	}
}

func TestPublicFailureReportsOnlyAllowlistedAPICode(t *testing.T) {
	err := fmt.Errorf("preflighting synthetic session name: %w", &cli.APIError{Code: "invalid_request", Message: "mentions sess_secret at private.invalid"})
	got := publicFailure(err)
	if got != "preflighting synthetic session name (API invalid_request)" {
		t.Fatalf("publicFailure = %q, want safe operation and code", got)
	}
	if strings.Contains(got, "sess_secret") || strings.Contains(got, "private.invalid") {
		t.Fatalf("publicFailure leaked API message: %q", got)
	}
}

func TestMeasureSampleFallsBackToNameCleanupBeforeCreateAck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"sessions":[]}`)
	}))
	defer ts.Close()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	binary := filepath.Join(dir, "synthetic-rainier")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$RAINIER_TEST_LOG\"\nif [ \"$1\" = \"new\" ]; then case \" $* \" in *\" --detach \"*) printf 'sess_recovered\\n';; esac; fi\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RAINIER_TEST_LOG", logPath)

	err := measureSample(context.Background(), options{Rainier: binary, Image: "synthetic.invalid/image", Timeout: time.Second}, &cli.Client{Base: ts.URL}, 1, func(string, int, time.Duration) error { return nil })
	if err == nil {
		t.Fatal("measureSample: want missing create acknowledgement error")
	}
	data, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("read cleanup log: %v", readErr)
	}
	got := string(data)
	if !strings.Contains(got, "new --detach") || !strings.Contains(got, "rm sess_recovered") {
		t.Fatalf("cleanup calls = %q, want idempotent create recovery followed by exact-id rm", got)
	}
	if strings.Contains(got, "rm latency-test-") {
		t.Fatalf("cleanup calls = %q, must never delete by name", got)
	}
}

func TestEnsureNameAvailableFiltersAndPaginatesClientSide(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "page2" {
			io.WriteString(w, `{"sessions":[{"id":"sess_collision","name":"latency-test-collision"}]}`)
			return
		}
		// Model an older server that ignores the exact-name filter and returns
		// unrelated rows; the client must not mistake these for a collision.
		io.WriteString(w, `{"sessions":[{"id":"sess_unrelated","name":"other-synthetic"}],"next_cursor":"page2"}`)
	}))
	defer ts.Close()
	c := &cli.Client{Base: ts.URL}
	if err := ensureNameAvailable(context.Background(), c, "latency-test-free", time.Second); err != nil {
		t.Fatalf("unrelated paginated rows: %v", err)
	}
	if err := ensureNameAvailable(context.Background(), c, "latency-test-collision", time.Second); err == nil {
		t.Fatal("ensureNameAvailable: want exact collision from page 2")
	}
}

func TestSummarizeEmptyMetric(t *testing.T) {
	got := summarize("cold_resume_to_usable", nil, 0)
	if got.Metric != "cold_resume_to_usable" || got.N != 0 || got.Pass != nil {
		t.Fatalf("empty summary = %+v, want named n=0 non-passing summary", got)
	}
}
