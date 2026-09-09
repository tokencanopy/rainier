package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/cli"
)

// hostileName is what a teammate can call a session. GET /v0/sessions is
// team-visible and the create route puts no character restriction on a name,
// so every table that renders one is rendering somebody else's input. The
// escape here clears the screen and homes the cursor: printed raw, it rewrites
// lines the reader has already seen.
const hostileName = "box\x1b[2J\x1b[1;1Hforged"

// No human output path may hand a server-supplied string to the terminal
// unchanged (docs/cli-v0-contract.md §6.3).
func TestServerStringsNeverReachTheTerminalRaw(t *testing.T) {
	row := session{
		ID: "sess_a\x1b[K", Name: hostileName, State: "queued",
		Environment: "env\x1b[2K", Runner: "runner\x07",
		QueueReason: "waiting\x1b[1;1H", CreatedAt: "2026-09-07T20:00:00Z",
		UpdatedAt: "2026-09-07T20:00:00Z",
	}

	var plain, verbose, info bytes.Buffer
	printSessions(&plain, cli.Config{}, []session{row}, false)
	printSessions(&verbose, cli.Config{}, []session{row}, true)
	printInfo(&info, cli.Config{}, row)

	statusRows := []statusRow{
		{Label: agentLabel("cla\x1b[2Jude"), Key: "agent_x", Value: "ready\x1b[2J"},
		{Label: "Compute", Key: "compute", Value: "setup required", Continue: "https://example.test/\x1b[2J"},
	}
	var status bytes.Buffer
	printStatus(&status, statusRows)

	for name, out := range map[string]string{
		"ls":           plain.String(),
		"ls --verbose": verbose.String(),
		"info":         info.String(),
		"status":       status.String(),
	} {
		if strings.ContainsRune(out, 0x1b) || strings.ContainsRune(out, 0x07) {
			t.Errorf("%s let a terminal escape through:\n%q", name, out)
		}
	}
	// Sanitizing must not swallow the value: the readable part survives.
	if !strings.Contains(plain.String(), "forged") {
		t.Errorf("ls dropped the name entirely:\n%s", plain.String())
	}
}

// The error footer carries two server-controlled strings — the envelope's code
// and the response's request id — and both used to reach the terminal without
// passing the sanitizer the message above them does.
func TestErrorFooterIsSanitized(t *testing.T) {
	cfg := cli.Config{}
	cfg.SetContext("edge", cli.Context{Server: "https://example.test", Token: "tok_secret_value"})

	var out bytes.Buffer
	reportError(cfg, &out, &cli.APIError{
		Status:    500,
		Code:      "inter\x1b[2Jnal",
		Message:   "failed while holding tok_secret_value",
		RequestID: "req\x1b[1;1H_abc",
	})
	if strings.ContainsRune(out.String(), 0x1b) {
		t.Errorf("the error footer let an escape through:\n%q", out.String())
	}
	if strings.Contains(out.String(), "tok_secret_value") {
		t.Errorf("the error leaked a credential:\n%s", out.String())
	}
	for _, want := range []string{"code ", "request "} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the footer dropped %q:\n%s", want, out.String())
		}
	}
}

// `--json` promises one document on stdout and nothing else. `new --detach`
// used to print the bare session id first, which made the stream unparseable.
func TestNewDetachJSONEmitsOneDocument(t *testing.T) {
	f := &newFixture{}
	hostedConfig(t, f.server(t).URL)

	out, _, err := captureBoth(t, func() error { return runNew([]string{"--detach", "--json", "--name", "box"}) })
	if err != nil {
		t.Fatalf("new --detach --json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%q", err, out)
	}
	if doc["schema"] != schemaMutation || doc["action"] != "new" || doc["session"] != "sess_created" {
		t.Errorf("document = %v", doc)
	}
	// Without --json the id remains the output, because a script that reads
	// it that way predates this flag.
	plain, _, err := captureBoth(t, func() error { return runNew([]string{"--detach"}) })
	if err != nil {
		t.Fatalf("new --detach: %v", err)
	}
	if strings.TrimSpace(plain) != "sess_created" {
		t.Errorf("plain --detach output = %q, want the bare id", plain)
	}
}

// A hosted context written before the Kind field existed says "hosted" only by
// carrying a refresh token — and logout is the operation that deletes it. It
// must record how the context authenticates before removing the evidence, or a
// hosted user who upgrades, signs out and signs back in is told there is no
// server configured.
func TestLogoutPreservesHostedIdentityForALegacyContext(t *testing.T) {
	// Exactly what origin/main wrote: no "kind".
	cfg := cli.Config{}
	cfg.SetContext("edge", cli.Context{
		Server: "https://edge.example.test", Token: "tok_access_example",
		RefreshToken: "tok_refresh_example", Workspace: "ws_example",
	})
	writeConfigForTest(t, cfg)

	if _, _, err := captureBoth(t, func() error { return runLogout(nil) }); err != nil {
		t.Fatalf("logout: %v", err)
	}
	after := mustLoad(t)
	ctx := after.Contexts["edge"]
	if ctx.Token != "" || ctx.RefreshToken != "" {
		t.Fatalf("logout left credentials behind: %+v", ctx)
	}
	if !ctx.Hosted() {
		t.Fatalf("logout forgot the context was hosted: %+v", ctx)
	}
	server, name, ok := reauthenticationTarget("")
	if !ok || server != "https://edge.example.test" || name != "edge" {
		t.Fatalf("a bare login after logout has no target: (%q, %q, %t)", server, name, ok)
	}
}

// A build with a hosted default must not hijack a machine whose only context
// is a self-hosted controld: "log me in again" means that server, not a
// different one.
func TestHostedDefaultDoesNotHijackASelfHostedMachine(t *testing.T) {
	cfg := cli.Config{}
	cfg.SetContext("default", cli.Context{Server: "https://controld.example.test", Token: "gh_token_value"})
	writeConfigForTest(t, cfg)
	t.Setenv("RAINIER_SERVER", "https://hosted.example.test")

	if !hasConfiguredServer("") {
		t.Fatal("a context naming a self-hosted server was not seen as configured")
	}
	// And with nothing configured at all, the default is exactly what should
	// be used.
	writeConfigForTest(t, cli.Config{})
	if hasConfiguredServer("") {
		t.Fatal("an empty config reported a configured server")
	}
}

// A stale `current` must fail clearly for every session command — and `delete`
// especially must not read the resulting 404 as "already gone" and exit 0,
// which would report a successful deletion of a session it never targeted.
func TestStaleCurrentFailsForEverySessionCommand(t *testing.T) {
	var deletes int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			deletes++
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such session"}}`)
	}))
	t.Cleanup(ts.Close)
	hostedConfig(t, ts.URL)
	if err := rememberCurrentSession("sess_gone"); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"info", func() error { return runInfo([]string{currentSelector}) }},
		{"stop", func() error { return runStop([]string{currentSelector}) }},
		{"delete", func() error { return runDelete([]string{currentSelector, "--yes"}) }},
		{"attach", func() error { return runAttach([]string{currentSelector}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, _, err := captureBoth(t, tc.run)
			if err == nil {
				t.Fatalf("%s accepted a stale current: %q", tc.name, out)
			}
			if !strings.Contains(err.Error(), "no longer exists") {
				t.Errorf("%s error = %v, want the stale-current explanation", tc.name, err)
			}
		})
	}
	if deletes != 0 {
		t.Errorf("delete sent %d requests for a session `current` no longer names", deletes)
	}
}

// A server that repeats a pagination cursor must be an error, not a loop that
// allocates until the machine gives up.
func TestListSessionsRefusesARepeatedCursor(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessionsEnvelope{
			Sessions:   []session{{ID: "sess_a", Name: "box", State: "running"}},
			NextCursor: "always-the-same",
		})
	}))
	t.Cleanup(ts.Close)

	_, err := listSessions(&cli.Client{Base: ts.URL}, false)
	if err == nil || !strings.Contains(err.Error(), "repeated a pagination cursor") {
		t.Fatalf("listSessions = %v, want a repeated-cursor error", err)
	}
}

// A provider name whose first character is multi-byte must not render as a
// replacement glyph.
func TestAgentLabelDecodesARune(t *testing.T) {
	if got := agentLabel("étoile"); got != "Étoile" {
		t.Errorf("agentLabel = %q, want %q", got, "Étoile")
	}
	if got := agentLabel("claude"); got != "Claude" {
		t.Errorf("agentLabel = %q, want Claude", got)
	}
	if got := agentLabel(""); got != "Agent" {
		t.Errorf("agentLabel(\"\") = %q, want Agent", got)
	}
}

// `stop --warm` is wrong about what stop does; saying "usage: stop <session>"
// would send the reader looking for a missing argument instead.
func TestStopWarmIsRefusedBeforeTheSelector(t *testing.T) {
	err := runStop([]string{"--warm"})
	if err == nil || !strings.Contains(err.Error(), "no warm stop") {
		t.Fatalf("error = %v, want the warm-stop refusal", err)
	}
	if exitCodeFor(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeFor(err))
	}
}
