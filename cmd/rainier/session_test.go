package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/cli"
)

// ---------------------------------------------------------------------------
// rainier new
// ---------------------------------------------------------------------------

// newFixture is a control plane for the create paths: it records every
// request and serves whatever environment catalog, launch catalog, compute
// enrollment and onboarding map a case needs.
type newFixture struct {
	// envs is GET /v0/environments. launchers is
	// GET /v0/environments/{id}/agents — "" means the route 404s, which is
	// what every deployment does today.
	envs       string
	launchers  string
	agents     string
	compute    string
	onboarding string
	createErr  string
	createCode int

	requests []recordedRequest
	created  createSessionRequest
}

type recordedRequest struct {
	method, path string
}

func (f *newFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests = append(f.requests, recordedRequest{r.Method, r.URL.Path})
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/v0/environments/") && strings.HasSuffix(r.URL.Path, "/agents"):
			if f.launchers == "" {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such route"}}`)
				return
			}
			fmt.Fprint(w, f.launchers)
		case strings.HasPrefix(r.URL.Path, "/v0/workspaces/") && strings.HasSuffix(r.URL.Path, "/compute"):
			if f.compute == "" {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such route"}}`)
				return
			}
			fmt.Fprint(w, f.compute)
		case r.URL.Path == onboardingPath:
			if f.onboarding == "" {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such route"}}`)
				return
			}
			fmt.Fprint(w, f.onboarding)
		case r.URL.Path == "/v0/environments":
			body := f.envs
			if body == "" {
				body = `{"environments":[{"id":"env_1","name":"default","image":"example:test"}]}`
			}
			fmt.Fprint(w, body)
		case r.URL.Path == "/v0/agents":
			body := f.agents
			if body == "" {
				body = `{"agents":[{"provider":"claude","status":"logged_in"},{"provider":"codex","status":"none"}]}`
			}
			fmt.Fprint(w, body)
		case r.Method == http.MethodPost && r.URL.Path == "/v0/sessions":
			if f.createErr != "" {
				code := f.createCode
				if code == 0 {
					code = http.StatusConflict
				}
				w.WriteHeader(code)
				fmt.Fprint(w, f.createErr)
				return
			}
			json.NewDecoder(r.Body).Decode(&f.created)
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{
				ID: "sess_created", Name: f.created.Name, State: "queued",
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such route"}}`)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// With no --env, the session starts from the workspace's default environment.
// Which one that is comes from the server: an environment it MARKS default,
// or the only one in its catalog (docs/cli-v0-contract.md §5.3).
func TestNewUsesTheDefaultEnvironment(t *testing.T) {
	t.Run("the environment the server marks default", func(t *testing.T) {
		f := &newFixture{envs: `{"environments":[
			{"id":"env_1","name":"scratch","image":"x"},
			{"id":"env_2","name":"hosted-default","image":"y","default":true}]}`}
		hostedConfig(t, f.server(t).URL)

		if _, err := captureStdout(t, func() error { return runNew([]string{"--detach", "--name", "box"}) }); err != nil {
			t.Fatalf("new: %v", err)
		}
		if f.created.Environment != "env_2" {
			t.Errorf("created with environment %q, want the server's default", f.created.Environment)
		}
	})

	t.Run("the single environment in the catalog", func(t *testing.T) {
		f := &newFixture{}
		hostedConfig(t, f.server(t).URL)

		if _, err := captureStdout(t, func() error { return runNew([]string{"--detach"}) }); err != nil {
			t.Fatalf("new: %v", err)
		}
		if f.created.Environment != "env_1" {
			t.Errorf("created with environment %q, want the catalog's only one", f.created.Environment)
		}
	})

	t.Run("an explicit --image opts out of the default", func(t *testing.T) {
		f := &newFixture{}
		hostedConfig(t, f.server(t).URL)

		if _, err := captureStdout(t, func() error { return runNew([]string{"--detach", "--image", "node:22"}) }); err != nil {
			t.Fatalf("new: %v", err)
		}
		if f.created.Environment != "" || f.created.Image != "node:22" {
			t.Errorf("created %+v; an explicit image must not inherit an environment", f.created)
		}
	})

	t.Run("a scratch session when nothing names one", func(t *testing.T) {
		// Two environments and no published default is genuinely ambiguous.
		// Rather than pick, `new` does what it always did before this flag
		// existed — which keeps every self-hosted deployment working.
		f := &newFixture{envs: `{"environments":[
			{"id":"env_1","name":"a","image":"x"},{"id":"env_2","name":"b","image":"y"}]}`}
		hostedConfig(t, f.server(t).URL)

		if _, err := captureStdout(t, func() error { return runNew([]string{"--detach"}) }); err != nil {
			t.Fatalf("new: %v", err)
		}
		if f.created.Environment != "" {
			t.Errorf("created with environment %q, want a scratch session", f.created.Environment)
		}
	})
}

// --agent and an explicit command both answer "what does this session run".
// Two answers is a mistake, not a precedence puzzle, and it costs one retype
// to say which (contract §3.3).
func TestNewRejectsAgentWithAnExplicitCommand(t *testing.T) {
	f := &newFixture{}
	hostedConfig(t, f.server(t).URL)

	_, err := captureStdout(t, func() error {
		return runNew([]string{"--agent", "claude", "--", "bash", "-lc", "echo hi"})
	})
	if err == nil {
		t.Fatal("new accepted --agent alongside an explicit command")
	}
	if exitCodeFor(err) != 2 {
		t.Errorf("exit code = %d, want 2 for an invalid invocation", exitCodeFor(err))
	}
	for _, request := range f.requests {
		if request.method == http.MethodPost {
			t.Errorf("a refused invocation still created something: %+v", request)
		}
	}
}

// --agent is resolved only from the server's launch catalog. Until that
// contract ships, the refusal names the exact dependency and the way round it
// — and this CLI never carries its own `claude` or `codex` command line
// (contract §5.3).
func TestNewAgentNeedsTheServerLaunchCatalog(t *testing.T) {
	f := &newFixture{}
	hostedConfig(t, f.server(t).URL)

	_, err := captureStdout(t, func() error { return runNew([]string{"--agent", "claude", "--detach"}) })
	if err == nil {
		t.Fatal("new --agent invented a launch command")
	}
	for _, want := range []string{"/v0/environments/{id}/agents", "rainier new", "-- claude"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q: %v", want, err)
		}
	}
	for _, request := range f.requests {
		if request.method == http.MethodPost {
			t.Errorf("a refused --agent still created a session: %+v", request)
		}
	}
}

// A provider the server does not know is the caller's typo, and it is named
// as one — a different failure from the missing contract above.
func TestNewAgentRejectsAnUnknownProvider(t *testing.T) {
	f := &newFixture{launchers: `{"catalog_version":"1","agents":[
		{"id":"claude","display_name":"Claude Code","argv":["claude"],"requires_login":true}]}`}
	hostedConfig(t, f.server(t).URL)

	_, err := captureStdout(t, func() error { return runNew([]string{"--agent", "clod", "--detach"}) })
	if err == nil || !strings.Contains(err.Error(), "cannot start clod") {
		t.Fatalf("error = %v, want a refusal naming the agent", err)
	}
	if !strings.Contains(err.Error(), "claude") {
		t.Errorf("the refusal does not name what the environment carries: %v", err)
	}
}

// An environment whose image carries nothing Rainier can start is a readiness
// problem with the workspace, and is reported as one rather than as a typo.
func TestNewAgentReportsAnEnvironmentThatCarriesNoAgent(t *testing.T) {
	f := &newFixture{launchers: `{"catalog_version":"1","agents":[]}`}
	hostedConfig(t, f.server(t).URL)

	_, err := captureStdout(t, func() error { return runNew([]string{"--agent", "claude", "--detach"}) })
	if err == nil || !strings.Contains(err.Error(), "carries no coding agent") {
		t.Fatalf("error = %v, want a readiness refusal", err)
	}
	for _, request := range f.requests {
		if request.method == http.MethodPost {
			t.Errorf("a refused --agent still created a session: %+v", request)
		}
	}
}

// When the launch catalog IS published, --agent uses it verbatim.
func TestNewAgentUsesTheServerLaunchCommand(t *testing.T) {
	f := &newFixture{launchers: `{"catalog_version":"1","agents":[
		{"id":"claude","display_name":"Claude Code","argv":["claude","--print-mode","stream"],"requires_login":true}]}`}
	hostedConfig(t, f.server(t).URL)

	if _, err := captureStdout(t, func() error { return runNew([]string{"--agent", "claude", "--detach"}) }); err != nil {
		t.Fatalf("new --agent: %v", err)
	}
	// Structured argv, passed through verbatim: the CLI adds nothing to it
	// and never re-splits it.
	want := []string{"claude", "--print-mode", "stream"}
	if strings.Join(f.created.Cmd, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("created with cmd %v, want the server's launch argv %v", f.created.Cmd, want)
	}
	// The launch catalog is asked of the ENVIRONMENT, never of /v0/agents:
	// custody and launch capability are different resources.
	var askedEnvironment bool
	for _, request := range f.requests {
		if strings.HasSuffix(request.path, "/agents") && strings.HasPrefix(request.path, "/v0/environments/") {
			askedEnvironment = true
		}
	}
	if !askedEnvironment {
		t.Errorf("new --agent did not read the environment's launch catalog: %+v", f.requests)
	}
}

// A workspace with no compute creates nothing, and says so in one message
// carrying the server's own destination (contract §3.3).
func TestNewWorkspaceNotReadyCreatesNothing(t *testing.T) {
	const dest = "https://example.test/app/onboarding/compute"
	f := &newFixture{
		compute:    `{"status":"needs_plan","health":"unknown","revision":"1"}`,
		onboarding: `{"console_url":"https://example.test","destinations":{"needs_plan":"` + dest + `"}}`,
		createCode: http.StatusConflict,
		createErr:  `{"error":{"code":"workspace_not_ready","message":"this workspace has no compute plan"}}`,
	}
	hostedConfig(t, f.server(t).URL)

	out, err := captureStdout(t, func() error { return runNew([]string{"--detach", "--name", "box"}) })
	if err == nil {
		t.Fatal("new reported success against a workspace with no compute")
	}
	if !strings.Contains(err.Error(), "no session was created") {
		t.Errorf("the message does not say nothing was created: %v", err)
	}
	if !strings.Contains(err.Error(), dest) {
		t.Errorf("the message does not carry the server's destination: %v", err)
	}
	if strings.Contains(out, "sess_") {
		t.Errorf("new printed a session id for a create that was refused: %q", out)
	}
}

// ---------------------------------------------------------------------------
// selectors
// ---------------------------------------------------------------------------

// `current` is a keyword resolved from the id this context remembered, and it
// is remembered by both of the commands the contract names: create and attach
// (contract §3.2).
func TestCurrentSelector(t *testing.T) {
	f := &newFixture{}
	hostedConfig(t, f.server(t).URL)

	if _, err := currentSessionID(mustLoad(t)); err == nil {
		t.Fatal("current resolved before anything created a session")
	}

	if _, err := captureStdout(t, func() error { return runNew([]string{"--detach"}) }); err != nil {
		t.Fatalf("new: %v", err)
	}
	id, err := currentSessionID(mustLoad(t))
	if err != nil || id != "sess_created" {
		t.Fatalf("current = %q, %v; want sess_created after a create", id, err)
	}

	// The ID is what is persisted, never the name: a name freed by a delete
	// and reused by the next `new` must not silently retarget `current`.
	stored := mustLoad(t)
	ctx, _ := stored.Active()
	if ctx.CurrentSession != "sess_created" {
		t.Errorf("config stored %q, want the opaque id", ctx.CurrentSession)
	}

	// A different server context has its own memory, and does not inherit one
	// whose ids mean nothing to it.
	if err := cli.UpdateConfig(func(c *cli.Config) error {
		c.SetContext("other", cli.Context{Server: "https://other.test", Token: "t", Kind: cli.KindHosted, RefreshToken: "r"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if id, err := currentSessionID(mustLoad(t)); err == nil {
		t.Errorf("current leaked %q across contexts", id)
	}
}

// A `current` whose session has been deleted says exactly that. A bare "no
// session sess_abc" for an id nobody typed reads as a bug in the CLI.
func TestStaleCurrentSessionExplainsItself(t *testing.T) {
	err := staleCurrentSession("sess_gone", &cli.APIError{Status: 404, Code: "not_found", Message: "no such session"})
	if err == nil || !strings.Contains(err.Error(), "no longer exists") || !strings.Contains(err.Error(), "sess_gone") {
		t.Fatalf("error = %v, want a stale-current explanation", err)
	}
	// Anything that is not a missing session is somebody else's problem and
	// is passed through unchanged.
	other := &cli.APIError{Status: 503, Code: "unavailable", Message: "try later"}
	if got := staleCurrentSession("sess_gone", other); got != error(other) {
		t.Errorf("staleCurrentSession rewrote an unrelated error: %v", got)
	}
}

// writeConfigForTest points this process at a fresh config file holding cfg.
func writeConfigForTest(t *testing.T, cfg cli.Config) {
	t.Helper()
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
}

func mustLoad(t *testing.T) cli.Config {
	t.Helper()
	cfg, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// rainier stop
// ---------------------------------------------------------------------------

// stopFixture answers GET and the suspend POST with a scripted sequence of
// states, so a test can pin what stop does about a server that has not caught
// up, one that refuses, and one that never gets there.
type stopFixture struct {
	states     []string // consumed by successive GETs
	suspendErr string
	suspendTo  string
	suspends   int
}

func (f *stopFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	gets := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet:
			state := f.states[min(gets, len(f.states)-1)]
			gets++
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_x", Name: "box", State: state}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/suspend"):
			f.suspends++
			var body suspendRequest
			json.NewDecoder(r.Body).Decode(&body)
			if body.Warm == nil || *body.Warm {
				t.Errorf("stop sent warm=%v; the safe stop is always cold", body.Warm)
			}
			if f.suspendErr != "" {
				w.WriteHeader(http.StatusConflict)
				fmt.Fprint(w, f.suspendErr)
				return
			}
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_x", Name: "box", State: f.suspendTo}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestStopIsIdempotentAndVerified(t *testing.T) {
	t.Run("stops a running session and confirms it", func(t *testing.T) {
		f := &stopFixture{states: []string{"running"}, suspendTo: "suspended_cold"}
		var out bytes.Buffer
		err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", false, &out)
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
		if !strings.Contains(out.String(), "resources released") {
			t.Errorf("output = %q", out.String())
		}
	})

	t.Run("an already-stopped session is success and sends nothing", func(t *testing.T) {
		f := &stopFixture{states: []string{"suspended_cold"}}
		var out bytes.Buffer
		if err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", false, &out); err != nil {
			t.Fatalf("stop: %v", err)
		}
		if f.suspends != 0 {
			t.Errorf("suspend requests = %d, want none for a stopped session", f.suspends)
		}
		if !strings.Contains(out.String(), "already stopped") {
			t.Errorf("output = %q", out.String())
		}
	})

	t.Run("another client winning the stop is not a failure", func(t *testing.T) {
		f := &stopFixture{
			states:     []string{"running", "suspended_cold"},
			suspendErr: `{"error":{"code":"conflict","message":"session is not running"}}`,
		}
		var out bytes.Buffer
		if err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", false, &out); err != nil {
			t.Fatalf("stop: %v", err)
		}
		if !strings.Contains(out.String(), "already stopped") {
			t.Errorf("output = %q", out.String())
		}
	})

	// The one thing stop must never do. An unpersisted stop is lost work, so
	// a session that did not reach a stopped state is a failure however the
	// suspend call itself went (contract §3.7).
	t.Run("a persistence failure is never reported as success", func(t *testing.T) {
		f := &stopFixture{states: []string{"running", "failed", "failed"}, suspendTo: "failed"}
		var out bytes.Buffer
		err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", false, &out)
		if err == nil {
			t.Fatalf("stop reported success for a session that did not stop: %q", out.String())
		}
		if !strings.Contains(err.Error(), "did not complete") || !strings.Contains(err.Error(), "not been released") {
			t.Errorf("error = %v, want it to say the state was not released", err)
		}
	})

	t.Run("--json is a stable mutation document", func(t *testing.T) {
		f := &stopFixture{states: []string{"running"}, suspendTo: "suspended_cold"}
		var out bytes.Buffer
		if err := stopSession(context.Background(), &cli.Client{Base: f.server(t).URL}, "sess_x", true, &out); err != nil {
			t.Fatalf("stop: %v", err)
		}
		var doc map[string]any
		if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out.String())
		}
		if doc["schema"] != schemaMutation || doc["action"] != "stop" || doc["state"] != "suspended_cold" {
			t.Errorf("document = %v", doc)
		}
	})
}

// ---------------------------------------------------------------------------
// rainier delete
// ---------------------------------------------------------------------------

// Deletion is irreversible, so consent is the design. A terminal is asked; a
// script without --yes is REFUSED rather than prompted, because a prompt
// written into a pipe is a program that hangs forever (contract §3.8).
func TestDeleteRequiresConsent(t *testing.T) {
	deletes := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			deletes++
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{ID: "sess_x", Name: "box", State: "running"}}})
	}))
	t.Cleanup(ts.Close)
	hostedConfig(t, ts.URL)

	t.Run("no terminal and no --yes refuses without hanging", func(t *testing.T) {
		saved := isInteractive
		isInteractive = func() bool { return false }
		t.Cleanup(func() { isInteractive = saved })

		var out bytes.Buffer
		err := deleteSession("sess_x", false, false, &out)
		if err == nil {
			t.Fatal("delete proceeded with no consent and no terminal")
		}
		if exitCodeFor(err) != 2 {
			t.Errorf("exit code = %d, want 2", exitCodeFor(err))
		}
		if !strings.Contains(err.Error(), "--yes") || !strings.Contains(err.Error(), "cannot be undone") {
			t.Errorf("error = %v, want it to name --yes and the irreversibility", err)
		}
		if deletes != 0 {
			t.Errorf("delete requests = %d, want none", deletes)
		}
	})

	t.Run("--yes proceeds", func(t *testing.T) {
		deletes = 0
		var out bytes.Buffer
		if err := deleteSession("sess_x", true, false, &out); err != nil {
			t.Fatalf("delete --yes: %v", err)
		}
		if deletes != 1 || !strings.Contains(out.String(), "deleted permanently") {
			t.Errorf("deletes=%d output=%q", deletes, out.String())
		}
	})

	t.Run("a terminal is asked, and no means no", func(t *testing.T) {
		deletes = 0
		answerPrompt(t, "n\n")
		var out bytes.Buffer
		// The warning and the prompt are an interaction, not a result, so they
		// go to stderr. That is what lets `delete --json` promise the document
		// on stdout is the only thing there (contract §6.1, §6.2).
		stdout, prompt, err := captureBoth(t, func() error { return deleteSession("sess_x", false, false, &out) })
		if exitCodeFor(err) != 1 {
			t.Fatalf("delete: %v", err)
		}
		if !strings.Contains(prompt, "cannot be recovered") || !strings.Contains(prompt, "continue? [y/N]") {
			t.Errorf("prompt = %q", prompt)
		}
		if stdout != "" {
			t.Errorf("the prompt polluted stdout: %q", stdout)
		}
		if deletes != 0 {
			t.Errorf("delete requests = %d after a refusal, want none", deletes)
		}
	})
}

// An asynchronous acceptance is said out loud: "deleted" would claim
// something the server has not finished doing.
func TestDeleteReportsAsynchronousAcceptance(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{ID: "sess_x", Name: "box", State: "running"}}})
	}))
	t.Cleanup(ts.Close)
	hostedConfig(t, ts.URL)

	var out bytes.Buffer
	if err := deleteSession("sess_x", true, true, &out); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if doc["async"] != true || !strings.Contains(doc["detail"].(string), "accepted") {
		t.Errorf("document = %v, want an explicit asynchronous acceptance", doc)
	}
}

// A session that is already gone is the outcome that was asked for.
func TestDeleteOfAnAlreadyRemovedSessionSucceeds(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such session"}}`)
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{ID: "sess_x", Name: "box", State: "failed"}}})
	}))
	t.Cleanup(ts.Close)
	hostedConfig(t, ts.URL)

	var out bytes.Buffer
	if err := deleteSession("sess_x", true, false, &out); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !strings.Contains(out.String(), "already gone") {
		t.Errorf("output = %q", out.String())
	}
}

// ---------------------------------------------------------------------------
// rainier info
// ---------------------------------------------------------------------------

// info is one authoritative view, and what it will not show is as deliberate
// as what it will: a failed session's reason is sanitized, and nothing that
// looks like terminal control reaches the screen.
func TestInfoRendersOneAuthoritativeView(t *testing.T) {
	cfg := cli.Config{}
	cfg.SetContext("edge", cli.Context{Server: "https://example.test", Token: "tok_secret_example"})
	row := session{
		ID: "sess_x", Name: "box", State: "failed", Environment: "default",
		CreatedAt: "2026-09-01T00:00:00Z", UpdatedAt: "2026-09-01T00:05:00Z",
		Error: "setup failed\x1b[2J using tok_secret_example",
	}

	var out bytes.Buffer
	printInfo(&out, cfg, row)
	for _, want := range []string{"sess_x", "box", "Session:      Failed", "default", "Attach:       no", "Delete:       yes"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("info is missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "tok_secret_example") || strings.Contains(out.String(), "\x1b") {
		t.Errorf("info leaked a credential or a terminal escape:\n%q", out.String())
	}

	var jsonOut bytes.Buffer
	if err := writeSessionJSON(&jsonOut, cfg, row); err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(jsonOut.Bytes(), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, jsonOut.String())
	}
	actions, _ := doc["actions"].(map[string]any)
	if doc["schema"] != schemaSession || doc["state"] != "failed" || doc["lifecycle"] != lifecycleFailed ||
		actions["attach"] != eligibleNo {
		t.Errorf("document = %v", doc)
	}
	if strings.Contains(jsonOut.String(), "tok_secret_example") {
		t.Errorf("info --json leaked a credential:\n%s", jsonOut.String())
	}
}

// ---------------------------------------------------------------------------
// rainier logout
// ---------------------------------------------------------------------------

// logout removes the credentials for one server and nothing else, is safe to
// repeat, and leaves the context behind so a bare `rainier login` knows where
// to sign back in (contract §4.2).
func TestLogoutIsIdempotentAndNarrow(t *testing.T) {
	hostedConfig(t, "https://edge.example.test")

	out, err := captureStdout(t, func() error { return runLogout(nil) })
	if err != nil {
		t.Fatalf("logout: %v", err)
	}
	if !strings.Contains(out, "signed out of edge") {
		t.Errorf("output = %q, want it to name the context", out)
	}
	for _, secret := range []string{"tok_access_example", "tok_refresh_example"} {
		if strings.Contains(out, secret) {
			t.Errorf("logout printed a credential: %q", out)
		}
	}

	after := mustLoad(t)
	ctx, ok := after.Contexts["edge"]
	if !ok {
		t.Fatal("logout deleted the context; a later bare login has nowhere to sign in")
	}
	if ctx.Token != "" || ctx.RefreshToken != "" || ctx.AccessExpiresAt != "" {
		t.Errorf("logout left credentials behind: %+v", ctx)
	}
	// Everything that is not a credential survives, including the marker that
	// says how to sign back in.
	if ctx.Server == "" || ctx.Workspace != "ws_example" || !ctx.Hosted() {
		t.Errorf("logout discarded context identity: %+v", ctx)
	}

	// Idempotent: doing it again is satisfied, not an error.
	out, err = captureStdout(t, func() error { return runLogout(nil) })
	if err != nil {
		t.Fatalf("second logout: %v", err)
	}
	if !strings.Contains(out, "already signed out") {
		t.Errorf("second logout output = %q", out)
	}

	// And with no context at all.
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "empty.json"))
	if out, err := captureStdout(t, func() error { return runLogout(nil) }); err != nil || !strings.Contains(out, "not signed in") {
		t.Fatalf("logout with no config: %v %q", err, out)
	}
}

// A bare `rainier login` re-authenticates against the server this machine is
// already using — including after a logout, which is the case that needs a
// durable marker rather than the (now deleted) refresh token.
func TestBareLoginTargetsTheExistingContext(t *testing.T) {
	hostedConfig(t, "https://edge.example.test")
	if _, err := captureStdout(t, func() error { return runLogout(nil) }); err != nil {
		t.Fatal(err)
	}

	server, name, ok := reauthenticationTarget("")
	if !ok || server != "https://edge.example.test" || name != "edge" {
		t.Fatalf("reauthenticationTarget = (%q, %q, %t), want the logged-out hosted context", server, name, ok)
	}

	// A self-hosted context has no browser flow to rerun, so a bare login
	// there falls through to the usage that names the flag which supplies a
	// credential.
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "selfhosted.json"))
	cfg := cli.Config{}
	cfg.SetContext("default", cli.Context{Server: "https://controld.example.test", Token: "gh_token"})
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := reauthenticationTarget(""); ok {
		t.Error("a bare login tried to run the browser flow against a self-hosted context")
	}
	// And it says the true thing about why. "No server configured" would be
	// false here — one is — and would send the reader looking for the wrong
	// problem; what a bare login cannot do is obtain a GitHub token.
	err := bareLoginHasNoTarget("")
	if err == nil || exitCodeFor(err) != 2 {
		t.Fatalf("bareLoginHasNoTarget = %v (exit %d), want an invalid invocation", err, exitCodeFor(err))
	}
	if !strings.Contains(err.Error(), "self-hosted server") || !strings.Contains(err.Error(), "--from-gh") {
		t.Errorf("error = %v, want it to name the self-hosted context and how to supply a token", err)
	}
	if strings.Contains(err.Error(), "no server configured") {
		t.Errorf("the self-hosted case claims no server is configured: %v", err)
	}

	// With no context at all, there genuinely is nothing to sign in to, and
	// this build compiles in no hosted default.
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "empty.json"))
	err = bareLoginHasNoTarget("")
	if err == nil || !strings.Contains(err.Error(), "no server configured") {
		t.Fatalf("error = %v, want the no-context explanation", err)
	}
	if !strings.Contains(err.Error(), "--cloud EDGE_URL") {
		t.Errorf("the refusal does not say how to name a server: %v", err)
	}
}

// The hosted default is a build seam and is empty in source builds: no
// unconfirmed production hostname is compiled in (contract §4.1).
func TestHostedDefaultServerIsABuildSeam(t *testing.T) {
	t.Setenv("RAINIER_SERVER", "")
	if got := hostedDefaultServer(); got != "" {
		t.Errorf("a source build names a hosted default %q; it must name none", got)
	}
	t.Setenv("RAINIER_SERVER", "https://staging.example.test/")
	if got := hostedDefaultServer(); got != "https://staging.example.test" {
		t.Errorf("hostedDefaultServer = %q, want the override without its trailing slash", got)
	}
}
