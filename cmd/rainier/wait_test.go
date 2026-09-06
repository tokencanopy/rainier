package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
)

func TestInitialAttachExhaustionExplainsReadiness(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason, runners, want string
		status                             int
	}{
		{name: "queued no runners", state: "queued", runners: `{"runners":[]}`, want: "no runners reported"},
		{name: "queued disconnected", state: "queued", runners: `{"runners":[{"connected":false,"capacity_used":0,"capacity_total":1}]}`, want: "no connected runners"},
		{name: "queued full", state: "queued", runners: `{"runners":[{"connected":true,"capacity_used":1,"capacity_total":1}]}`, want: "no free capacity"},
		{name: "queue reason", state: "queued", reason: "waiting for runner example\u001b[31m\n", want: "server queue reason: waiting for runner example"},
		{name: "queue credentials", state: "queued", reason: "tok_example https://user:password_example@example.invalid/path?token=query_example", want: "[redacted]"},
		{name: "running 503", state: "running", runners: `{"runners":[]}`, want: "cause is not established"},
		{name: "unknown", status: 404, want: "compatibility not established"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutations, observations := 0, 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					mutations++
					t.Errorf("mutation %s", r.Method)
				}
				switch r.URL.Path {
				case "/v0/sessions/sess_example/attach":
					w.WriteHeader(503)
					fmt.Fprint(w, "upstream secret tok_example")
					return
				case "/v0/sessions/sess_example":
					observations++
					if tc.status != 0 {
						w.WriteHeader(tc.status)
						return
					}
					json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_example", State: tc.state, QueueReason: tc.reason}})
				case "/v0/runners":
					observations++
					fmt.Fprint(w, tc.runners)
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer ts.Close()
			cfg := cli.Config{ServerURL: ts.URL, Token: "tok_example"}
			err := attachWithRetryBudget(cfg, "sess_example", 17, func(time.Duration) {}, 0)
			if err == nil {
				t.Fatal("expected readiness error")
			}
			out := err.Error()
			if !strings.Contains(out, tc.want) || !strings.Contains(out, "rainier attach sess_example --since 17") || !strings.Contains(out, "rainier doctor") {
				t.Fatalf("%s", out)
			}
			if strings.Contains(out, "\x1b") || strings.Contains(out, "tok_example") || strings.Contains(out, "password_example") || strings.Contains(out, "query_example") || strings.Contains(out, "upstream secret") {
				t.Fatalf("unsafe output %q", out)
			}
			if mutations != 0 || observations > 2 {
				t.Fatalf("mutations=%d observations=%d", mutations, observations)
			}
			if tc.state == "running" && strings.Contains(out, "no runners") {
				t.Fatalf("unproven capacity inference: %s", out)
			}
		})
	}
}

func TestInitialAttachPreservesAuthErrors(t *testing.T) {
	for _, status := range []int{401, 403, 404, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(status) }))
			defer ts.Close()
			err := attachWithRetryBudget(cli.Config{ServerURL: ts.URL, Token: "tok_example"}, "sess_example", 0, func(time.Duration) {}, 0)
			if err == nil || strings.Contains(err.Error(), "rainier doctor") || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestWaitGuidanceBoundsDiagnosticsAndUnsafeIDs(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; <-r.Context().Done() }))
	defer ts.Close()
	cfg := cli.Config{ServerURL: ts.URL, Token: "tok_example"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := initialAttachGuidance(ctx, cfg, "sess_example", 0)
	if time.Since(start) > time.Second || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("%v", err)
	}
	before := calls
	err = initialAttachGuidance(context.Background(), cfg, "sess_bad; touch /tmp/example", 0)
	if calls != before || strings.Contains(err.Error(), "touch") || !strings.Contains(err.Error(), "rainier ls") {
		t.Fatalf("unsafe id guidance: %v", err)
	}
}
