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

// readinessFixture is a hosted control plane for `rainier status`. compute
// and onboarding are the two routes the Compute row depends on; "" means the
// route 404s, which is what a cell selling no compute and every deployment
// without a published console do respectively.
type readinessFixture struct {
	compute    string
	onboarding string
	runners    string
	envs       string
	agents     string
	me         string
	seen       []string
}

func (f *readinessFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.seen = append(f.seen, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		notFound := func() {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":"not_found","message":"no such route"}}`)
		}
		body := ""
		switch {
		case strings.HasPrefix(r.URL.Path, "/v0/workspaces/") && strings.HasSuffix(r.URL.Path, "/compute"):
			if f.compute == "" {
				notFound()
				return
			}
			body = f.compute
		case r.URL.Path == onboardingPath:
			if f.onboarding == "" {
				notFound()
				return
			}
			body = f.onboarding
		case r.URL.Path == "/v0/me":
			body = f.me
			if body == "" {
				body = `{"user":{"id":"usr_example","login":"developer","role":"member"}}`
			}
		case r.URL.Path == "/v0/runners":
			body = f.runners
			if body == "" {
				body = availableRunner
			}
		case r.URL.Path == "/v0/environments":
			body = f.envs
			if body == "" {
				body = `{"environments":[{"id":"env_example","name":"default","image":"example:test","default":true}]}`
			}
		case r.URL.Path == "/v0/agents":
			body = f.agents
			if body == "" {
				body = `{"agents":[{"provider":"claude","status":"logged_in"},{"provider":"codex","status":"logged_in"}]}`
			}
		case r.URL.Path == "/v0/credentials":
			body = `{"credentials":[{"provider":"github","status":"valid"}]}`
		case r.URL.Path == "/v0/connections":
			body = `{"connections":[{"provider":"github","login":"developer","access_mode":"selected","workspaces":["ws_example"]}]}`
		case r.URL.Path == "/v0/workspaces":
			body = `{"workspaces":[{"id":"ws_example","name":"Personal","role":"owner"}]}`
		default:
			notFound()
			return
		}
		fmt.Fprint(w, body)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// readyCompute and consoleMap are the fixtures a healthy hosted workspace
// answers with.
const readyCompute = `{"status":"ready","health":"available","revision":"7"}`
const consoleMap = `{"console_url":"https://example.test","destinations":{
	"needs_plan":"https://example.test/app/onboarding/compute",
	"awaiting_payment":"https://example.test/app/onboarding/compute",
	"provisioning":"https://example.test/app/onboarding/provisioning",
	"ready":"https://example.test/app/settings/compute"}}`

// hostedConfig writes a signed-in hosted context pointing at ts.
func hostedConfig(t *testing.T, serverURL string) cli.Config {
	t.Helper()
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	cfg := cli.Config{}
	cfg.SetContext("edge", cli.Context{
		Server: serverURL, Token: "tok_access_example", RefreshToken: "tok_refresh_example",
		Kind: cli.KindHosted, Workspace: "ws_example", OwnerID: "usr_example",
	})
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

// A ready workspace renders the seven lines the contract specifies, with no
// Continue line — there is nothing outstanding to continue.
func TestStatusReady(t *testing.T) {
	f := &readinessFixture{compute: readyCompute, onboarding: consoleMap}
	cfg := hostedConfig(t, f.server(t).URL)

	var out bytes.Buffer
	rows, ready := collectStatus(context.Background(), cfg)
	printStatus(&out, rows)
	if !ready {
		t.Fatalf("workspace reported not ready:\n%s", out.String())
	}
	for _, want := range []string{
		"Signed in: yes", "Workspace: Personal", "Compute: ready",
		"Default environment: ready", "GitHub: connected", "Claude: ready", "Codex: ready",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status is missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "Continue:") {
		t.Errorf("a ready workspace printed a Continue line:\n%s", out.String())
	}
}

// Ready capacity that cannot be reached is NOT ready. `status` + `health` are
// two facts exactly as a session's lifecycle and connection are, and reporting
// this pairing as ready would send somebody to `rainier new` to be refused.
func TestStatusReadyButUnavailableIsNotReady(t *testing.T) {
	f := &readinessFixture{
		compute:    `{"status":"ready","health":"unavailable","revision":"7"}`,
		onboarding: consoleMap,
	}
	cfg := hostedConfig(t, f.server(t).URL)

	var out bytes.Buffer
	rows, ready := collectStatus(context.Background(), cfg)
	printStatus(&out, rows)
	if ready {
		t.Fatalf("ready+unavailable compute reported ready:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "not reachable right now") {
		t.Errorf("status does not say the capacity is unreachable:\n%s", out.String())
	}
	// And it must not be confused with needing a plan: the entitlement is
	// intact and the recovery is completely different.
	if strings.Contains(out.String(), "setup required") {
		t.Errorf("unreachable capacity was reported as needing setup:\n%s", out.String())
	}
}

// The whole compute vocabulary, verbatim from the cloud's own package.
func TestStatusComputeVocabulary(t *testing.T) {
	for _, tc := range []struct {
		status, health, want string
		ready                bool
	}{
		{computeNeedsPlan, healthUnknown, "setup required (no plan selected)", false},
		{computeAwaitingPayment, healthUnknown, "setup required (awaiting payment)", false},
		{computeProvisioning, healthUnknown, "provisioning", false},
		{computeReady, healthAvailable, "ready", true},
		{computeFailed, healthUnknown, "failed", false},
		{computeCancelling, healthUnknown, "cancelling", false},
		{computeCancelled, healthUnknown, "cancelled", false},
		{"teleporting", healthUnknown, "unknown (server reported state teleporting)", false},
	} {
		t.Run(tc.status, func(t *testing.T) {
			state := computeState{Published: true, Status: tc.status, Health: tc.health}
			var onboarding onboardingDestinations
			row := computeRow(state, nil, onboarding)
			if row.Value != tc.want {
				t.Errorf("value = %q, want %q", row.Value, tc.want)
			}
			if row.Ready != tc.ready {
				t.Errorf("ready = %t, want %t", row.Ready, tc.ready)
			}
		})
	}
}

// Before compute is ready, the server's own destination is printed — and only
// the server's. The CLI performs a lookup in a map the server supplied,
// keyed by a status the server supplied; it composes nothing.
func TestStatusOnboardingRequired(t *testing.T) {
	f := &readinessFixture{
		compute:    `{"status":"needs_plan","health":"unknown","revision":"1"}`,
		onboarding: consoleMap,
	}
	cfg := hostedConfig(t, f.server(t).URL)

	var out bytes.Buffer
	rows, ready := collectStatus(context.Background(), cfg)
	printStatus(&out, rows)
	if ready {
		t.Fatalf("a workspace without compute reported ready:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Compute: setup required") {
		t.Errorf("status does not say compute needs setup:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Continue: https://example.test/app/onboarding/compute") {
		t.Errorf("status does not print the server's destination:\n%s", out.String())
	}
}

// A destination the server did not publish is not invented, and one that is
// not an absolute https URL is dropped rather than printed: the browser's own
// bootstrap serves a RELATIVE path today, and a relative path in a "go here"
// message is worse than no message.
func TestStatusRefusesADestinationItCannotVouchFor(t *testing.T) {
	for _, tc := range []struct{ name, onboarding string }{
		{"no route at all", ""},
		{"a relative path", `{"console_url":"","destinations":{"needs_plan":"/app/onboarding/compute"}}`},
		{"a non-https scheme", `{"console_url":"","destinations":{"needs_plan":"javascript:alert(1)"}}`},
		{"a status nobody mapped", `{"console_url":"https://example.test","destinations":{"ready":"https://example.test/x"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &readinessFixture{
				compute:    `{"status":"needs_plan","health":"unknown","revision":"1"}`,
				onboarding: tc.onboarding,
			}
			cfg := hostedConfig(t, f.server(t).URL)
			var out bytes.Buffer
			rows, _ := collectStatus(context.Background(), cfg)
			printStatus(&out, rows)
			if strings.Contains(out.String(), "Continue:") {
				t.Errorf("status printed a destination it could not vouch for:\n%s", out.String())
			}
		})
	}
}

// A cell that publishes no compute route is a deployable state — an older
// cell, a self-hosted controld, a cell that sells no compute — and it must not
// read as a broken workspace.
func TestStatusWithoutTheComputeRoute(t *testing.T) {
	f := &readinessFixture{}
	cfg := hostedConfig(t, f.server(t).URL)

	var out bytes.Buffer
	rows, ready := collectStatus(context.Background(), cfg)
	printStatus(&out, rows)
	if ready {
		t.Fatalf("a hosted server with no compute route reported ready:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "unknown") {
		t.Errorf("status does not say the server sells no compute:\n%s", out.String())
	}
	if strings.Contains(out.String(), "Continue:") {
		t.Errorf("status invented a destination:\n%s", out.String())
	}
}

// Signed out is one line and a stop. Seven unknown rows would bury the only
// one that matters.
func TestStatusUnauthenticated(t *testing.T) {
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	var out bytes.Buffer
	rows, ready := collectStatus(context.Background(), cli.Config{})
	printStatus(&out, rows)
	if ready || len(rows) != 1 || !strings.Contains(out.String(), "Signed in: no") {
		t.Fatalf("ready=%t rows=%d\n%s", ready, len(rows), out.String())
	}
}

// --json is a stable document, and no credential reaches it on any path.
func TestStatusJSONIsStableAndRedacted(t *testing.T) {
	f := &readinessFixture{
		compute:    `{"status":"needs_plan","health":"unknown","revision":"1"}`,
		onboarding: consoleMap,
	}
	cfg := hostedConfig(t, f.server(t).URL)

	var out bytes.Buffer
	rows, ready := collectStatus(context.Background(), cfg)
	if err := writeStatusJSON(&out, cfg, rows, ready); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Schema  string `json:"schema"`
		Version int    `json:"version"`
		Ready   bool   `json:"ready"`
		Checks  []struct {
			Name     string `json:"name"`
			Value    string `json:"value"`
			Ready    bool   `json:"ready"`
			Required bool   `json:"required"`
			Continue string `json:"continue"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("status --json is not valid JSON: %v\n%s", err, out.String())
	}
	if doc.Schema != schemaStatus || doc.Version != jsonSchemaVersion {
		t.Errorf("schema/version = %q/%d", doc.Schema, doc.Version)
	}
	if doc.Ready {
		t.Error("--json reported ready for a workspace that is not")
	}
	byName := map[string]bool{}
	for _, c := range doc.Checks {
		byName[c.Name] = true
		if c.Ready && c.Continue != "" {
			t.Errorf("check %q is ready but carries a continue address %q", c.Name, c.Continue)
		}
		if c.Name == "compute" && c.Continue == "" {
			t.Errorf("the compute check is not ready and names no destination: %+v", c)
		}
	}
	for _, want := range []string{"signed_in", "workspace", "compute", "default_environment", "github", "agent_claude", "agent_codex"} {
		if !byName[want] {
			t.Errorf("--json has no %q check:\n%s", want, out.String())
		}
	}
	for _, secret := range []string{"tok_access_example", "tok_refresh_example"} {
		if strings.Contains(out.String(), secret) {
			t.Errorf("--json leaked %q:\n%s", secret, out.String())
		}
	}
}

// Every credential this machine holds is replaced in any text that reaches a
// user, including server prose the CLI merely relays.
func TestRedactSecrets(t *testing.T) {
	cfg := cli.Config{}
	cfg.SetContext("edge", cli.Context{Server: "https://example.test", Token: "tok_short", RefreshToken: "tok_short_and_longer"})

	got := redactSecrets(cfg, "the server said tok_short_and_longer and also tok_short\x1b[2J")
	if strings.Contains(got, "tok_short") {
		t.Errorf("redaction left a credential behind: %q", got)
	}
	if strings.Contains(got, "\x1b") {
		t.Errorf("redaction left a terminal escape behind: %q", got)
	}
	// The URL survives: several of these messages exist only to carry one.
	if got := redactSecrets(cfg, "Continue: https://example.test/app/onboarding/compute"); !strings.Contains(got, "https://example.test/app/onboarding/compute") {
		t.Errorf("redaction destroyed the onboarding destination: %q", got)
	}
	// So do the line breaks. Several errors are deliberately two or three
	// lines — one of them the address or the command to run next — and
	// running them together makes the actionable half unreadable.
	multiline := "no session was created: no compute\nContinue: https://example.test/app/onboarding/compute"
	if got := redactSecrets(cfg, multiline); got != multiline {
		t.Errorf("redaction rewrote a multi-line message:\n got %q\nwant %q", got, multiline)
	}
}

// TestRedactAll covers the two ways a redactor can destroy the message it was
// supposed to make safe. Both were real: a config whose token was one
// character turned every occurrence of that letter into a nested pile of
// markers, and the message became unreadable while protecting nothing.
func TestRedactAll(t *testing.T) {
	// One pass, not a loop of ReplaceAll: the marker contains letters, so a
	// second secret matching inside a marker the first one wrote produces
	// nested markers rather than a redaction.
	got := redactAll("run rainier attach box", []string{"aaaaaaaa", "ted]rrrr"})
	if strings.Contains(got, "[redac") {
		t.Errorf("a secret matched inside the redactor's own marker: %q", got)
	}
	if got != "run rainier attach box" {
		t.Errorf("redactAll = %q, want the text unchanged", got)
	}

	// A value too short to be any credential these servers issue is left
	// alone. Substituting it would replace every occurrence of a common
	// letter and shred the message.
	if got := redactAll("cannot attach to this session", []string{"t", "a"}); got != "cannot attach to this session" {
		t.Errorf("a one-character value shredded the message: %q", got)
	}

	// Real credentials are still removed, longest match first, so a short one
	// that is a prefix of a long one never leaves the rest legible.
	got = redactAll("saw tok_secret_value_long and tok_secret", []string{"tok_secret", "tok_secret_value_long"})
	if strings.Contains(got, "tok_secret") {
		t.Errorf("a credential survived: %q", got)
	}
	if got != "saw [redacted] and [redacted]" {
		t.Errorf("redactAll = %q", got)
	}
}
