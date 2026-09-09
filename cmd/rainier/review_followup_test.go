package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/tokencanopy/rainier/internal/cli"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReviewGoneSessionNameReachesLifecycleDiagnostic(t *testing.T) {
	for _, state := range []string{"dead", "canceled", "destroyed"} {
		t.Run(state, func(t *testing.T) {
			row := session{ID: "sess_history", Name: "history", OwnerID: "usr_example", State: state}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v0/sessions" {
					json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{row}})
					return
				}
				json.NewEncoder(w).Encode(sessionEnvelope{Session: row})
			}))
			defer ts.Close()
			hostedConfig(t, ts.URL)
			for _, run := range []func([]string) error{runAttach, runStop} {
				err := run([]string{"history"})
				if err == nil || strings.Contains(err.Error(), "no session named") {
					t.Fatalf("missing lifecycle diagnostic: %v", err)
				}
			}
		})
	}
}
func TestReviewSelfHostedDoesNotRequireDefaultEnvironment(t *testing.T) {
	for _, envs := range []string{`{"environments":[]}`, `{"environments":[{"id":"env_a","name":"a"},{"id":"env_b","name":"b"}]}`} {
		f := &readinessFixture{envs: envs}
		cfg := cli.Config{}
		cfg.SetContext("local", cli.Context{Server: f.server(t).URL, Token: "token_synthetic_example", Workspace: "ws_example"})
		rows, ready := collectStatus(context.Background(), cfg)
		if !ready {
			t.Fatalf("self hosted scratch/explicit-env blocked: %+v", rows)
		}
	}
}
func TestReviewSessionJSONRedactsKnownCredentials(t *testing.T) {
	cfg := cli.Config{Token: "token_synthetic_example"}
	var out bytes.Buffer
	err := writeSessionJSON(&out, cfg, session{ID: "sess_example", State: "running", Name: cfg.Token, Environment: cfg.Token, Runner: cfg.Token})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), cfg.Token) {
		t.Fatal("known credential leaked")
	}
}
func TestReviewDefinitiveCreateRefusalHasNoAmbiguousRetry(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":{"code":"invalid_request","message":"invalid"}}`))
	}))
	defer ts.Close()
	_, err := createSession(&cli.Client{Base: ts.URL}, createSessionRequest{}, "retry_example")
	if err == nil || strings.Contains(err.Error(), "not confirmed") {
		t.Fatalf("definitive refusal was hidden: %v", err)
	}
}
func TestReviewNewIDCannotRewriteTerminal(t *testing.T) {
	f := &newFixture{}
	upstream := f.server(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/v0/sessions" {
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_\x1b[2KFORGED", State: "running"}})
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	defer ts.Close()
	hostedConfig(t, ts.URL)
	out, _, _ := captureBoth(t, func() error { return runNew([]string{"--detach"}) })
	if strings.Contains(out, "\x1b") {
		t.Fatalf("terminal escape leaked: %q", out)
	}
}
