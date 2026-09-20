package runner_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// TestBootstrapFieldsAreAdditive is the compatibility promise of the microVM
// bootstrap design, at the hop where it is made: controld rolls before
// runners, so every create a control plane that KNOWS about the token
// dispatches to a runner that does not must be byte-identical to the one that
// runner already reads.
//
// The golden create is TestPublicRunnerWireShapes'. It is repeated here
// literally rather than referenced, because the point of a golden is that a
// change has to edit the bytes and say so.
func TestBootstrapFieldsAreAdditive(t *testing.T) {
	create := runner.ToRunner{
		Type: "create", ReqID: 7, Session: "sess_example",
		Spec: &runner.Spec{Image: "example.invalid/agent@sha256:0000", Cmd: []string{"bash"}},
	}
	got, err := json.Marshal(create)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"type":"create","req_id":7,"session":"sess_example",` +
		`"spec":{"image":"example.invalid/agent@sha256:0000","cmd":["bash"]}}`
	if string(got) != want {
		t.Fatalf("a create with no bootstrap = %s\nwant %s", got, want)
	}
	for _, tag := range []string{"bootstrap_token", "secret_names"} {
		if strings.Contains(string(got), `"`+tag+`"`) {
			t.Fatalf("a create with no bootstrap leaked %q: %s", tag, got)
		}
	}

	// And a Docker create carrying secret values keeps carrying them: the
	// withholding is keyed on the capability, not on the field existing.
	docker, err := json.Marshal(runner.Spec{Image: "img", Env: map[string]string{"TOKEN": "value_example"}})
	if err != nil {
		t.Fatal(err)
	}
	if string(docker) != `{"image":"img","env":{"TOKEN":"value_example"}}` {
		t.Fatalf("a docker create = %s", docker)
	}
}

// TestBootstrapSpecRoundTrip pins the two tags a microVM create adds and
// proves they survive the hop — an omitempty field that never carried its
// value would fence nobody.
func TestBootstrapSpecRoundTrip(t *testing.T) {
	in := runner.ToRunner{Type: "create", ReqID: 21, Session: "sess_example", Spec: &runner.Spec{
		Image:          "img",
		BootstrapToken: "dG9rZW5fZXhhbXBsZQ",
		SecretNames:    []string{"DEPLOY_KEY", "NPM_TOKEN"},
	}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"bootstrap_token":"dG9rZW5fZXhhbXBsZQ"`,
		`"secret_names":["DEPLOY_KEY","NPM_TOKEN"]`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("bootstrap tags wrong on the wire: %s", b)
		}
	}
	// A microVM create carries the NAMES and no values at all.
	if strings.Contains(string(b), `"env"`) {
		t.Fatalf("a withheld create still carried an env block: %s", b)
	}
	var out runner.ToRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Spec == nil || out.Spec.BootstrapToken != in.Spec.BootstrapToken ||
		len(out.Spec.SecretNames) != 2 || out.Spec.SecretNames[1] != "NPM_TOKEN" {
		t.Fatalf("round trip mangled the bootstrap: %+v", out.Spec)
	}
}

// TestBootstrapVocabulary pins the three wire words. They are read by a
// control plane, a runner and a sandbox that are three separately released
// binaries, so a typo in one of them would be a silently unanswered method
// rather than a build failure.
func TestBootstrapVocabulary(t *testing.T) {
	if runner.MethodFetchSessionSecrets != "fetch_session_secrets" {
		t.Fatalf("fetch method = %q", runner.MethodFetchSessionSecrets)
	}
	if runner.MethodMintSessionBootstrap != "mint_session_bootstrap" {
		t.Fatalf("mint method = %q", runner.MethodMintSessionBootstrap)
	}
	if runner.CapabilityMicrovmV1 != "microvm.v1" {
		t.Fatalf("microvm capability = %q", runner.CapabilityMicrovmV1)
	}
	if runner.SessionBootstrapProtocolVersion != 1 {
		t.Fatalf("bootstrap protocol = %d", runner.SessionBootstrapProtocolVersion)
	}
}

// TestBootConfigWireShape pins the bytes a host sends a guest as its first
// control frame. sessiond ships inside the session image and a session keeps
// the one it booted with for life, so these tags are a contract between
// builds that may be months apart.
func TestBootConfigWireShape(t *testing.T) {
	minimal, err := json.Marshal(runner.BootConfig{
		Protocol:  runner.SessionBootstrapProtocolVersion,
		SessionID: "sess_example",
		Cmd:       []string{"bash"},
	})
	if err != nil {
		t.Fatal(err)
	}
	const wantMinimal = `{"protocol":1,"session_id":"sess_example","cmd":["bash"]}`
	if string(minimal) != wantMinimal {
		t.Fatalf("a minimal boot config = %s\nwant %s", minimal, wantMinimal)
	}

	full := runner.BootConfig{
		Protocol:        runner.SessionBootstrapProtocolVersion,
		SessionID:       "sess_example",
		Cmd:             []string{"bash", "-l"},
		ProxyURL:        "http://sess_example@proxy.invalid:3128",
		NoProxy:         "127.0.0.1,localhost",
		EgressAllow:     []string{"registry.npmjs.org"},
		Setup:           "apt-get install -y jq\n",
		SetupTimeoutSec: 900,
		Init:            "make dev\n",
		InitTimeoutSec:  300,
		Repos: []runner.RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main",
			SessionBranch: "rainier/work", Dir: "app"}},
		GitAuthorName:  "example",
		GitAuthorEmail: "42+example@users.noreply.github.com",
		Env:            map[string]string{"CLAUDE_CONFIG_DIR": "/rainier/agents/claude"},
		SecretNames:    []string{"DEPLOY_KEY"},
		BootstrapToken: "dG9rZW5fZXhhbXBsZQ",
	}
	b, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var back runner.BootConfig
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.SessionID != full.SessionID || back.NoProxy != full.NoProxy ||
		back.SetupTimeoutSec != 900 || back.InitTimeoutSec != 300 ||
		len(back.Repos) != 1 || back.Repos[0].Dir != "app" ||
		back.Env["CLAUDE_CONFIG_DIR"] != "/rainier/agents/claude" ||
		len(back.SecretNames) != 1 || back.BootstrapToken != full.BootstrapToken {
		t.Fatalf("boot config round trip mangled: %+v", back)
	}
}
