package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/tokencanopy/rainier/internal/cli"
)

func TestRequiredReadinessFactsCannotBeInvented(t *testing.T) {
	for _, envs := range []string{`{"environments":[]}`, `{"environments":[{"id":"env_a","name":"a"},{"id":"env_b","name":"b"}]}`, `{"environments":[{"id":"env_a","default":true},{"id":"env_b","default":true}]}`} {
		f := &readinessFixture{compute: readyCompute, envs: envs}
		cfg := hostedConfig(t, f.server(t).URL)
		if _, ready := collectStatus(context.Background(), cfg); ready {
			t.Fatal("missing or ambiguous default was reported ready")
		}
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer ts.Close()
	row := workspaceRow(context.Background(), &cli.Client{Base: ts.URL}, cli.Context{Kind: cli.KindHosted, Workspace: "ws_example"})
	if row.Ready {
		t.Fatal("failed workspace lookup was reported ready")
	}
}

func TestStatusJSONPreservesComputeFacts(t *testing.T) {
	var out bytes.Buffer
	row := computeRow(computeState{Published: true, Status: "ready", Health: "unavailable"}, nil, onboardingDestinations{})
	if err := writeStatusJSON(&out, cli.Config{}, []statusRow{row}, false); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Checks []struct {
			Facts map[string]string `json:"facts"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Checks[0].Facts["status"] != "ready" || doc.Checks[0].Facts["health"] != "unavailable" {
		t.Fatal(out.String())
	}
}

func TestOpaqueSelectorsCannotChangeTheEndpoint(t *testing.T) {
	for _, ref := range []string{"sess_a/../sess_b", "sess_a?force=true", "sess_a%2fb", "sess_a#fragment"} {
		if _, err := resolveSessionID(nil, "", ref); exitCodeFor(err) != 2 {
			t.Fatalf("selector %q accepted", ref)
		}
	}
}

func TestRepeatedSelectorPageFailsWithoutMutation(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet {
			t.Error("unexpected mutation")
		}
		fmt.Fprint(w, `{"sessions":[],"next_cursor":"same"}`)
	}))
	defer ts.Close()
	_, err := resolveSessionID(&cli.Client{Base: ts.URL}, "", "missing")
	if err == nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestSessionFailureDoesNotExposeUnknownSecrets(t *testing.T) {
	const private = "synthetic-provider-secret-not-in-config"
	row := session{ID: "sess_example", State: "failed", Error: private, LastEventAt: "2026-09-01T00:00:00Z"}
	var human, machine bytes.Buffer
	printInfo(&human, cli.Config{}, row)
	if err := writeSessionJSON(&machine, cli.Config{}, row); err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{human.String(), machine.String()} {
		if strings.Contains(out, private) {
			t.Fatal("raw failure leaked")
		}
		if !strings.Contains(out, row.LastEventAt) {
			t.Fatal("recent activity missing")
		}
	}
	var diagnostic bytes.Buffer
	reportError(cli.Config{}, &diagnostic, &cli.APIError{Code: "unavailable", Status: 503, Message: private, RequestID: "req_example"})
	if strings.Contains(diagnostic.String(), private) || !strings.Contains(diagnostic.String(), "req_example") {
		t.Fatal(diagnostic.String())
	}
}

func TestCurrentRemembersOriginalContext(t *testing.T) {
	cfg := hostedConfig(t, "https://example.test")
	if err := cli.UpdateConfig(func(latest *cli.Config) error {
		latest.SetContext("other", cli.Context{Server: "https://other.test", Token: "synthetic_token"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := rememberCurrentSession("sess_example", cfg); err != nil {
		t.Fatal(err)
	}
	after, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	if after.Contexts["edge"].CurrentSession != "sess_example" || after.Contexts["other"].CurrentSession != "" || after.ActiveName() != "other" {
		t.Fatal("current retargeted to another context")
	}
}

func TestCreateRecoveryRetainsKeyAndCause(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Idempotency-Key") != "retry_example" {
			t.Error("retry key changed")
		}
		w.WriteHeader(503)
		fmt.Fprint(w, `{"error":{"code":"unavailable","message":"synthetic-private-provider-response"}}`)
	}))
	defer ts.Close()
	_, err := createSession(&cli.Client{Base: ts.URL}, createSessionRequest{}, "retry_example")
	var api *cli.APIError
	if calls != 1 || !errors.As(err, &api) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	var out bytes.Buffer
	reportError(cli.Config{}, &out, err)
	if !strings.Contains(out.String(), "--idempotency-key retry_example") || strings.Contains(out.String(), "synthetic-private") {
		t.Fatal(out.String())
	}
}

// Exercise the actual executable, streams, parser, and exit status against
// synthetic HTTP fixtures. Log only commands and exit codes.
func TestCompiledV0Fixtures(t *testing.T) {
	bin := buildCLI(t, "")
	f := &readinessFixture{compute: `{"status":"ready","health":"unavailable"}`}
	hostedConfig(t, f.server(t).URL)
	config := os.Getenv("RAINIER_CONFIG")
	for _, tc := range []struct {
		args []string
		code int
		json bool
	}{
		{[]string{"--help"}, 0, false},
		{[]string{"status"}, 1, false},
		{[]string{"status", "--json"}, 1, true},
		{[]string{"agent", "status", "--json"}, 0, true},
		{[]string{"new", "--agent", "claude", "--detach", "--json"}, 1, false},
		{[]string{"delete", "sess_example"}, 2, false},
		{[]string{"info", "sess_a/../sess_b", "--json"}, 2, false},
		{[]string{"diff"}, 2, false},
	} {
		out, _, code := runCLISplit(t, bin, config, tc.args...)
		t.Logf("rainier %s => exit %d", strings.Join(tc.args, " "), code)
		if code != tc.code {
			t.Fatalf("unexpected exit %d, want %d", code, tc.code)
		}
		if tc.json && !json.Valid([]byte(out)) {
			t.Fatal("stdout was not JSON")
		}
		if tc.code != 0 && !tc.json && tc.args[0] != "status" && out != "" {
			t.Fatal("failure polluted stdout")
		}
	}
}

func TestCompiledSessionFixtures(t *testing.T) {
	bin := buildCLI(t, "")
	row := session{ID: "sess_example", Name: "fixture", State: "running", Reachable: false, ChildExitCode: intPtr(0), LastEventAt: "2026-09-01T00:00:00Z"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			row.State = "destroyed"
			w.WriteHeader(http.StatusNoContent)
			return
		case strings.HasSuffix(r.URL.Path, "/suspend"):
			row.State = "suspended_cold"
		case strings.HasSuffix(r.URL.Path, "/resume"):
			row.State = "running"
		case r.URL.Path == "/v0/sessions":
			json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{row}})
			return
		}
		json.NewEncoder(w).Encode(sessionEnvelope{Session: row})
	}))
	defer ts.Close()
	hostedConfig(t, ts.URL)
	config := os.Getenv("RAINIER_CONFIG")
	for _, args := range [][]string{
		{"ls"}, {"ls", "--json"}, {"info", "fixture", "--json"},
		{"stop", "sess_example", "--json"}, {"resume", "sess_example", "--json"},
		{"delete", "sess_example", "--yes", "--json"},
	} {
		out, _, code := runCLISplit(t, bin, config, args...)
		t.Logf("rainier %s => exit %d", strings.Join(args, " "), code)
		if code != 0 {
			t.Fatal("fixture invocation failed")
		}
		if args[len(args)-1] == "--json" && !json.Valid([]byte(out)) {
			t.Fatal("stdout was not JSON")
		}
		if len(args) == 1 && (!strings.Contains(out, "Exited (0)") || !strings.Contains(out, "Unavailable") || !strings.Contains(out, "Running")) {
			t.Fatal("session facts were conflated")
		}
	}
}

func TestFailedAttachDoesNotChangeCurrent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/attach") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_refused", State: "running"}})
	}))
	defer ts.Close()
	cfg := hostedConfig(t, ts.URL)
	if err := rememberCurrentSession("sess_previous", cfg); err != nil {
		t.Fatal(err)
	}
	if err := runAttach([]string{"sess_refused"}); err == nil {
		t.Fatal("attach should fail")
	}
	after, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	if after.Contexts["edge"].CurrentSession != "sess_previous" {
		t.Fatal("failed attachment changed current")
	}
}

func TestNullDeviceIsNotAConfirmationTerminal(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if charDevice(f) {
		t.Fatal("null device was treated as an interactive terminal")
	}
}

func TestTerminalCloseReasonIsNotPrinted(t *testing.T) {
	var out bytes.Buffer
	reportError(cli.Config{}, &out, websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "synthetic-private-terminal-content"})
	if strings.Contains(out.String(), "synthetic-private") || !strings.Contains(out.String(), "1008") {
		t.Fatal(out.String())
	}
}

func TestUnknownAgentIsAUsageError(t *testing.T) {
	_, err := agentProviderNamed("unknown-synthetic")
	if exitCodeFor(err) != 2 {
		t.Fatalf("unknown provider exit = %d", exitCodeFor(err))
	}
}
