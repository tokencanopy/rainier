package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
)

// The compatibility aliases (docs/cli-v0-contract.md §2.1) exist for the
// scripts and runbooks that already call them. Each one must still work, must
// say that it is an alias, and must reach the command that replaced it —
// which is why each is implemented as a call into that command rather than as
// a second copy of it.
func TestCompatibilityAliases(t *testing.T) {
	var seen []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/suspend"):
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_x", Name: "box", State: "suspended_cold"}})
		case r.URL.Path == "/v0/sessions/sess_x":
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_x", Name: "box", State: "running"}})
		case r.URL.Path == "/v0/sessions":
			json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{ID: "sess_x", Name: "box", State: "running"}}})
		case r.URL.Path == "/v0/agents":
			fmt.Fprint(w, `{"agents":[{"provider":"claude","status":"none"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such route"}}`)
		}
	}))
	t.Cleanup(ts.Close)
	hostedConfig(t, ts.URL)

	t.Run("suspend reaches stop, and always cold", func(t *testing.T) {
		seen = nil
		out, diag, err := captureBoth(t, func() error { return runSuspend([]string{"sess_x"}) })
		if err != nil {
			t.Fatalf("suspend: %v", err)
		}
		if !strings.Contains(diag, "compatibility alias") {
			t.Errorf("the alias does not announce itself: %q", diag)
		}
		if !strings.Contains(out, "resources released") {
			t.Errorf("suspend did not run stop: %q", out)
		}
		// `suspend --cold` is what a script that already wanted the safe stop
		// says, and it must keep working rather than becoming an unknown flag.
		if _, _, err := captureBoth(t, func() error { return runSuspend([]string{"sess_x", "--cold"}) }); err != nil {
			t.Errorf("suspend --cold: %v", err)
		}
	})

	t.Run("rm keeps its no-prompt behavior", func(t *testing.T) {
		seen = nil
		// isInteractive is false here, which is exactly the situation in which
		// `delete` refuses. `rm` must not: scripts have always called it
		// without --yes, and breaking them buys nothing.
		out, diag, err := captureBoth(t, func() error { return runRm([]string{"sess_x"}) })
		if err != nil {
			t.Fatalf("rm: %v", err)
		}
		if !strings.Contains(out, "removed sess_x") {
			t.Errorf("rm output = %q", out)
		}
		if !strings.Contains(diag, "compatibility alias") {
			t.Errorf("the alias does not announce itself: %q", diag)
		}
		found := false
		for _, r := range seen {
			if r == "DELETE /v0/sessions/sess_x" {
				found = true
			}
		}
		if !found {
			t.Errorf("rm did not delete: %v", seen)
		}
	})

	t.Run("agent ls reaches agent status", func(t *testing.T) {
		out, diag, err := captureBoth(t, func() error { return runAgent([]string{"ls"}) })
		if err != nil {
			t.Fatalf("agent ls: %v", err)
		}
		if !strings.Contains(out, "AGENT") || !strings.Contains(out, agentNotConfigured) {
			t.Errorf("agent ls did not run agent status: %q", out)
		}
		if !strings.Contains(diag, "compatibility alias") {
			t.Errorf("the alias does not announce itself: %q", diag)
		}
	})
}

// `ls --json` is a stable document, and its session states are the five
// user-facing ones rather than the control plane's (contract §6.2).
func TestLsJSONIsStable(t *testing.T) {
	created := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{
			{ID: "sess_a", Name: "box", State: "running", Environment: "default", CreatedAt: created},
			{ID: "sess_b", Name: "done", State: "running", ChildExitCode: intPtr(0), CreatedAt: created},
		}})
	}))
	t.Cleanup(ts.Close)
	cfg := hostedConfig(t, ts.URL)

	rows, err := listSessions(cli.NewClient(cfg), false)
	if err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	if err := writeSessionsJSON(&buf, cfg, rows); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Schema   string `json:"schema"`
		Version  int    `json:"version"`
		Sessions []struct {
			ID       string `json:"id"`
			State    string `json:"state"`
			Process  string `json:"process"`
			ExitCode *int   `json:"child_exit_code"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal([]byte(buf.String()), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	if doc.Schema != schemaSessions || doc.Version != jsonSchemaVersion || len(doc.Sessions) != 2 {
		t.Fatalf("document = %+v", doc)
	}
	// Server ordering is preserved: nothing here re-sorts pages.
	if doc.Sessions[0].ID != "sess_a" || doc.Sessions[1].ID != "sess_b" {
		t.Errorf("ls reordered the server's pages: %+v", doc.Sessions)
	}
	// Both sessions carry the RAW API state. The one whose child exited is
	// still `running` — that is what the server said, and the exit is a
	// separate fact in a separate field.
	for i, want := range []string{"running", "running"} {
		if doc.Sessions[i].State != want {
			t.Errorf("session %d state = %q, want the raw %q", i, doc.Sessions[i].State, want)
		}
	}
	if doc.Sessions[0].Process != processRunning || doc.Sessions[1].Process != processExited {
		t.Errorf("process = %q/%q, want running/exited", doc.Sessions[0].Process, doc.Sessions[1].Process)
	}
	if doc.Sessions[1].ExitCode == nil || *doc.Sessions[1].ExitCode != 0 {
		t.Errorf("child_exit_code = %v, want 0", doc.Sessions[1].ExitCode)
	}
	if doc.Sessions[0].ExitCode != nil {
		t.Errorf("child_exit_code = %v for a live child, want null", doc.Sessions[0].ExitCode)
	}
}

// `agent status --json` never describes a stored credential as verified
// provider access (contract §4.4).
func TestAgentStatusJSONDoesNotClaimVerification(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"agents":[{"provider":"claude","status":"logged_in","since":"2026-01-02T03:04:05Z","version":3}]}`)
	}))
	t.Cleanup(ts.Close)
	hostedConfig(t, ts.URL)

	out, diag, err := captureBoth(t, func() error { return runAgentStatus([]string{"--json"}) })
	if err != nil {
		t.Fatalf("agent status --json: %v", err)
	}
	var doc struct {
		Schema string `json:"schema"`
		Agents []struct {
			Provider string `json:"provider"`
			Status   string `json:"status"`
			Verified bool   `json:"credential_verified"`
		} `json:"agents"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if doc.Schema != schemaAgentStatus || len(doc.Agents) != 1 {
		t.Fatalf("document = %+v", doc)
	}
	if doc.Agents[0].Status != agentReady || doc.Agents[0].Verified {
		t.Errorf("agent = %+v, want ready and explicitly unverified", doc.Agents[0])
	}
	// The JSON document must be the only thing on stdout.
	if strings.Contains(out, "note:") {
		t.Errorf("--json output carries a human note:\n%s", out)
	}
	if !strings.Contains(diag, "does not verify it with the provider") {
		t.Errorf("the caveat is missing from stderr: %q", diag)
	}
}
