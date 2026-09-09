package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/tokencanopy/rainier/internal/cli"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCompletionExplicitEnvironmentPinsCatalogID(t *testing.T) {
	f := &newFixture{launchers: `{"catalog_version":"1","agents":[{"id":"claude","argv":["claude"]}]}`}
	hostedConfig(t, f.server(t).URL)
	if _, err := captureStdout(t, func() error { return runNew([]string{"--env", "default", "--agent", "claude", "--detach"}) }); err != nil {
		t.Fatal(err)
	}
	if f.created.Environment != "env_1" {
		t.Fatalf("create environment=%q, want catalog ID env_1", f.created.Environment)
	}
}

func TestCompletionWarmStopDoesNotClaimReleasedCapacity(t *testing.T) {
	f := &stopFixture{states: []string{"running"}, suspendTo: "suspended_warm"}
	var out bytes.Buffer
	err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", false, &out)
	if err == nil && strings.Contains(out.String(), "resources released") {
		t.Fatalf("false release claim: %s", out.String())
	}
	if !strings.Contains(out.String(), "capacity") && (err == nil || !strings.Contains(err.Error(), "capacity")) {
		t.Fatalf("missing capacity guidance: out=%q err=%v", out.String(), err)
	}
}

func TestCompletionVerboseRedactsDiagnosticURL(t *testing.T) {
	var out bytes.Buffer
	cfg := cli.Config{}
	cfg.SetContext("test", cli.Context{Server: "https://edge.example.test", Token: "token_synthetic_secret"})
	printSessions(&out, cfg, []session{{ID: "sess_example", State: "queued", QueueReason: "waiting https://example.test/private/token-synthetic token_synthetic_secret"}}, true)
	if strings.Contains(out.String(), "private/token-synthetic") || strings.Contains(out.String(), "token_synthetic_secret") {
		t.Fatalf("diagnostic leaked: %s", out.String())
	}
}

func TestCompletionDiagnosticAttachResolvesTerminalName(t *testing.T) {
	for _, state := range []string{"dead", "canceled", "destroyed"} {
		t.Run(state, func(t *testing.T) {
			lookedUp := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/v0/sessions" {
					fmt.Fprintf(w, `{"sessions":[{"id":"sess_old","name":"old","state":%q}]}`, state)
					return
				}
				if r.URL.Path == "/v0/sessions/sess_old" {
					lookedUp = true
					w.WriteHeader(404)
					fmt.Fprint(w, `{"error":{"code":"not_found"}}`)
					return
				}
				t.Errorf("unexpected request %s", r.URL.Path)
				w.WriteHeader(404)
			}))
			defer srv.Close()
			hostedConfig(t, srv.URL)
			_ = runAttach([]string{"old", "--since", "0"})
			if !lookedUp {
				t.Fatal("diagnostic replay filtered terminal name before authoritative lookup")
			}
		})
	}
}

func TestCompletionWarmStopPathsPreserveCapacityWarning(t *testing.T) {
	for _, tc := range []struct {
		name     string
		states   []string
		conflict string
	}{
		{"already warm", []string{"suspended_warm"}, ""},
		{"concurrent warm", []string{"running", "suspended_warm"}, `{"error":{"code":"conflict"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &stopFixture{states: tc.states, suspendErr: tc.conflict}
			var out bytes.Buffer
			if err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", true, &out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "capacity remains reserved") || strings.Contains(out.String(), "resources released") {
				t.Fatalf("misleading output: %s", out.String())
			}
		})
	}
}
