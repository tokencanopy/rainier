package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The microVM boot, over a net.Pipe standing in for the vsock conn.
//
// There is no VM and no KVM here, and that is the point of the seam: the
// transport is a func returning a relay.Conn, so everything above it — the
// boot configuration, the exchange, what lands in this process's environment
// — is exercised end to end against a fake host. What only a real host can
// answer is that Firecracker forwards <uds_path>_1024 to the socket and that
// the guest kernel has /dev/vsock; see the PR body.

// fakeHost is the runner's end of a guest's control channel: it writes the
// boot configuration as the first frame and answers (or refuses) the one
// exchange the guest makes.
type fakeHost struct {
	conn relay.Conn
	t    *testing.T
}

func (h *fakeHost) sendBootConfig(cfg runner.BootConfig) {
	h.t.Helper()
	body, err := json.Marshal(cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	h.sendControl(relay.ControlEvent{Kind: relay.KindBootConfig, Payload: body})
}

func (h *fakeHost) sendControl(ev relay.ControlEvent) {
	h.t.Helper()
	payload, err := json.Marshal(ev)
	if err != nil {
		h.t.Fatal(err)
	}
	frame, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.conn.Write(ctx, frame); err != nil {
		h.t.Fatalf("write to the guest: %v", err)
	}
}

// nextRequest reads the next control REQUEST the guest sent, failing the
// test if none arrives.
func (h *fakeHost) nextRequest() relay.ControlEvent {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		raw, err := h.conn.Read(ctx)
		if err != nil {
			h.t.Fatalf("read from the guest: %v", err)
		}
		f, err := relay.Decode(raw)
		if err != nil || f.Type != relay.FrameControl {
			continue
		}
		var ev relay.ControlEvent
		if json.Unmarshal(f.Payload, &ev) != nil {
			continue
		}
		if strings.HasPrefix(ev.Kind, "req:") {
			return ev
		}
	}
}

// nothingMore asserts the guest sent no further request within a short
// window. It is how "exactly once" is checked.
func (h *fakeHost) nothingMore(within time.Duration) bool {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	for {
		raw, err := h.conn.Read(ctx)
		if err != nil {
			return true // the window closed with nothing on it
		}
		f, derr := relay.Decode(raw)
		if derr != nil || f.Type != relay.FrameControl {
			continue
		}
		var ev relay.ControlEvent
		if json.Unmarshal(f.Payload, &ev) != nil {
			continue
		}
		if strings.HasPrefix(ev.Kind, "req:") {
			return false
		}
	}
}

func (h *fakeHost) answer(id uint64, env map[string]string) {
	h.t.Helper()
	body, err := json.Marshal(struct {
		Env map[string]string `json:"env"`
	}{env})
	if err != nil {
		h.t.Fatal(err)
	}
	h.sendControl(relay.ControlEvent{Kind: "resp", ID: id, OK: true, Payload: body})
}

func (h *fakeHost) refuse(id uint64, reason string) {
	h.t.Helper()
	body, err := json.Marshal(struct {
		Error string `json:"error"`
	}{reason})
	if err != nil {
		h.t.Fatal(err)
	}
	h.sendControl(relay.ControlEvent{Kind: "resp", ID: id, Payload: body})
}

// fakeTransport returns a dialer that hands out one pipe per dial and the
// host ends of those pipes, in order.
func fakeTransport(t *testing.T) (dialSession, <-chan *fakeHost) {
	hosts := make(chan *fakeHost, 4)
	return func(context.Context) (relay.Conn, error) {
		guest, host := net.Pipe()
		t.Cleanup(func() { _ = guest.Close(); _ = host.Close() })
		hosts <- &fakeHost{conn: relay.NetConn(host), t: t}
		return relay.NetConn(guest), nil
	}, hosts
}

// cleanEnv unsets every variable the boot might set, before and after, so a
// case reads what THIS boot applied and not what the last one left.
func cleanEnv(t *testing.T) {
	t.Helper()
	names := []string{
		"RAINIER_SESSION", "RAINIER_DIAL",
		"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy",
		"RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT", "RAINIER_INIT_B64", "RAINIER_INIT_TIMEOUT",
		"RAINIER_REPOS_B64", "RAINIER_GIT_AUTHOR_NAME", "RAINIER_GIT_AUTHOR_EMAIL",
		"CLAUDE_CONFIG_DIR", "RAINIER_AGENTS_B64", "DEPLOY_KEY", "NPM_TOKEN",
	}
	unset := func() {
		for _, n := range names {
			_ = os.Unsetenv(n)
		}
	}
	unset()
	t.Cleanup(unset)
}

// TestBootOverVsockAppliesTheConfigurationAndTheSecrets is the whole boot in
// one case: the guest reads a configuration off its first frame, exchanges
// its token exactly once, and ends up with an environment block in the shape
// the boot chain already reads — which is what makes everything downstream
// (prepareBoot, the agent sync, the exec runner, the agent itself) identical
// to the Docker path.
func TestBootOverVsockAppliesTheConfigurationAndTheSecrets(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	boots := &bootstrapper{}

	cfg := runner.BootConfig{
		Protocol:        runner.SessionBootstrapProtocolVersion,
		SessionID:       "sess_example",
		Cmd:             []string{"claude"},
		ProxyURL:        "http://sess_example:rainier@proxy.invalid:3128",
		NoProxy:         "127.0.0.1,localhost",
		Setup:           "npm ci\n",
		SetupTimeoutSec: 600,
		Init:            "make dev\n",
		InitTimeoutSec:  180,
		Repos: []runner.RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main",
			SessionBranch: "rainier/work", Dir: "app"}},
		GitAuthorName:  "example",
		GitAuthorEmail: "42+example@users.noreply.github.com",
		Env:            map[string]string{"CLAUDE_CONFIG_DIR": "/rainier/agents/claude"},
		SecretNames:    []string{"DEPLOY_KEY", "NPM_TOKEN"},
		BootstrapToken: "token_example",
	}

	done := make(chan struct{})
	var (
		conn    relay.Conn
		failure *bootFailure
		bootErr error
	)
	go func() {
		defer close(done)
		conn, _, failure, bootErr = bootOverVsock(context.Background(), dial, boots)
	}()

	host := <-hosts
	host.sendBootConfig(cfg)
	req := host.nextRequest()
	if req.Kind != "req:"+runner.MethodFetchSessionSecrets {
		t.Fatalf("the guest asked %q, want the secret fetch", req.Kind)
	}
	var body struct {
		Protocol int    `json:"protocol"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(req.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.Protocol != runner.SessionBootstrapProtocolVersion || body.Token != "token_example" {
		t.Fatalf("the request carried %+v", body)
	}
	host.answer(req.ID, map[string]string{"DEPLOY_KEY": "value_example", "NPM_TOKEN": "other_value_example"})

	<-done
	if bootErr != nil {
		t.Fatalf("boot: %v", bootErr)
	}
	if failure != nil {
		t.Fatalf("the boot reported a failure: %v", failure)
	}
	if conn == nil {
		t.Fatal("the boot returned no connection to serve")
	}

	// The configuration, in the shape bootEnvFromOS reads.
	env := bootEnvFromOS()
	if env.SetupB64 != base64.StdEncoding.EncodeToString([]byte("npm ci\n")) || env.SetupTimeout != "600" {
		t.Errorf("setup = %q/%q", env.SetupB64, env.SetupTimeout)
	}
	if env.InitB64 != base64.StdEncoding.EncodeToString([]byte("make dev\n")) || env.InitTimeout != "180" {
		t.Errorf("init = %q/%q", env.InitB64, env.InitTimeout)
	}
	if env.GitAuthorName != "example" || env.GitAuthorEmail != "42+example@users.noreply.github.com" {
		t.Errorf("git identity = %q <%q>", env.GitAuthorName, env.GitAuthorEmail)
	}
	repos, err := decodeRepos(env.ReposB64)
	if err != nil || len(repos) != 1 || repos[0].Dir != "app" {
		t.Errorf("repos = %+v (%v)", repos, err)
	}
	if os.Getenv("RAINIER_SESSION") != "sess_example" {
		t.Errorf("RAINIER_SESSION = %q", os.Getenv("RAINIER_SESSION"))
	}
	if os.Getenv("CLAUDE_CONFIG_DIR") != "/rainier/agents/claude" {
		t.Errorf("the agent home configuration was not applied")
	}
	for _, k := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"} {
		if os.Getenv(k) != cfg.ProxyURL {
			t.Errorf("%s = %q, want the proxy", k, os.Getenv(k))
		}
	}
	if os.Getenv("NO_PROXY") != "127.0.0.1,localhost" {
		t.Errorf("NO_PROXY = %q", os.Getenv("NO_PROXY"))
	}
	// There is nothing to dial, and the guest must not think there is.
	if v := os.Getenv("RAINIER_DIAL"); v != "" {
		t.Errorf("RAINIER_DIAL = %q on a session with no URL to dial", v)
	}
	// And the secrets, which arrived over the exchange and from nowhere else.
	if os.Getenv("DEPLOY_KEY") != "value_example" || os.Getenv("NPM_TOKEN") != "other_value_example" {
		t.Error("the delivered secrets did not reach this process's environment")
	}

	// Exactly once: the token is single-use, so a second exchange on the same
	// token would be refused and would fail a boot that had already worked.
	if !host.nothingMore(200 * time.Millisecond) {
		t.Fatal("the guest made a second request on one boot")
	}
}

// TestBootOverVsockWithNoSecretsDeclaredIsACleanBoot pins the difference
// between "none declared" and "declared and never arrived": the first asks
// for nothing at all.
func TestBootOverVsockWithNoSecretsDeclaredIsACleanBoot(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	boots := &bootstrapper{}

	done := make(chan struct{})
	var failure *bootFailure
	var bootErr error
	go func() {
		defer close(done)
		_, _, failure, bootErr = bootOverVsock(context.Background(), dial, boots)
	}()

	host := <-hosts
	host.sendBootConfig(runner.BootConfig{
		Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example",
		BootstrapToken: "token_example",
	})
	<-done
	if bootErr != nil || failure != nil {
		t.Fatalf("a session declaring no secrets did not boot cleanly: %v / %v", bootErr, failure)
	}
	if !host.nothingMore(200 * time.Millisecond) {
		t.Fatal("a session declaring no secrets asked for some anyway")
	}
}

// TestARefusedExchangeFailsTheBootChain is the compatibility table's last
// row, and the one that decides what a user sees. A refusal must not boot an
// agent into an environment that promised a credential it does not have; it
// must fail the boot chain, as the existing stage_failed, naming the COUNT
// of undelivered names and none of their values.
func TestARefusedExchangeFailsTheBootChain(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	boots := &bootstrapper{}

	done := make(chan struct{})
	var (
		conn    relay.Conn
		failure *bootFailure
		bootErr error
	)
	go func() {
		defer close(done)
		conn, _, failure, bootErr = bootOverVsock(context.Background(), dial, boots)
	}()

	cfg := runner.BootConfig{
		Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example",
		SecretNames: []string{"DEPLOY_KEY", "NPM_TOKEN"}, BootstrapToken: "token_example",
	}
	host := <-hosts
	host.sendBootConfig(cfg)
	req := host.nextRequest()
	host.refuse(req.ID, "this session's bootstrap token has already been exchanged")

	<-done
	if bootErr != nil {
		t.Fatalf("a refusal was reported as a boot error rather than a failed chain: %v", bootErr)
	}
	if failure == nil {
		t.Fatal("a refused exchange did not fail the boot")
	}
	if conn != nil {
		t.Fatal("a failed boot handed back a connection it may already have broken")
	}
	// The count, the plane's own condition, and no value or name.
	if !strings.Contains(failure.Error(), "2 secret(s)") {
		t.Errorf("the failure does not name the count: %q", failure)
	}
	if !strings.Contains(failure.Error(), "already been exchanged") {
		t.Errorf("the failure drops the control plane's reason: %q", failure)
	}
	for _, leaked := range []string{"DEPLOY_KEY", "NPM_TOKEN", "token_example"} {
		if strings.Contains(failure.Error(), leaked) {
			t.Errorf("the failure carries %q: %q", leaked, failure)
		}
	}

	// And it becomes a STAGE: the first one, which only fails, so the agent
	// is never exec'd and the watcher reports stage_failed with this tail.
	dir := t.TempDir()
	stages, _, err := prepareBoot(dir, "/workspace", bootEnv{SecretsFailure: failure.Error(), SetupB64: "ZWNobyBoaQo="})
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 2 || stages[0].Name != stageSecrets {
		t.Fatalf("stages = %v, want the secrets stage first", stageNames(stages))
	}
	script, err := os.ReadFile(stages[0].ScriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "exit 1") {
		t.Fatalf("the secrets stage does not fail:\n%s", script)
	}

	// And the session still comes up. This is the half that decides whether
	// a user can see WHY: the boot chain has failed and said so, and the
	// session now has to register so that the failure, the terminal and an
	// attach are reachable at all.
	//
	// The token is spent — the control plane consumes it before it resolves
	// anything — so the connection that carries the session must not
	// re-present it. A sessiond that retried would be refused on every
	// redial, `connect` would drop every connection, and a session that was
	// merely missing its secrets would be off the air completely.
	redial, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	redialHost := <-hosts
	redialDone := make(chan error, 1)
	go func() { redialDone <- reBootstrap(context.Background(), redial, boots) }()
	redialHost.sendBootConfig(cfg)
	if err := <-redialDone; err != nil {
		t.Fatalf("the connection after a failed boot was refused: %v", err)
	}
	if !redialHost.nothingMore(200 * time.Millisecond) {
		t.Fatal("the redial re-presented a token the control plane had already spent")
	}
}

// TestTheBootExchangeIsNumberedOutOfTheDispatchersSpace pins the id the boot
// exchange uses, which is the one request this process makes before its RPC
// dispatcher exists.
//
// It cannot be 1. The dispatcher counts from 1, so a boot exchange that
// timed out and was answered late would have its `{"env": …}` matched to the
// dispatcher's first call — an agent credential fetch, typically — which
// would decode it as an empty credential set and report a shape it cannot
// read for a request that was never answered.
//
// And it cannot carry the high bit, which runnerd reserves for its own
// requests and refuses a sandbox for using.
func TestTheBootExchangeIsNumberedOutOfTheDispatchersSpace(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	boots := &bootstrapper{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = bootOverVsock(context.Background(), dial, boots)
	}()
	host := <-hosts
	host.sendBootConfig(runner.BootConfig{
		Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example",
		SecretNames: []string{"DEPLOY_KEY"}, BootstrapToken: "token_example",
	})
	req := host.nextRequest()
	host.answer(req.ID, map[string]string{"DEPLOY_KEY": "value_example"})
	<-done

	if req.ID == 0 {
		t.Fatal("the boot exchange carried no id")
	}
	// The dispatcher's own first id, which this must not collide with.
	d := newRPCDispatcher()
	d.online(&recordingSender{})
	firstDispatcherID := d.seq.Add(1)
	if req.ID == firstDispatcherID {
		t.Fatalf("the boot exchange uses id %d, which is the dispatcher's first", req.ID)
	}
	if req.ID&(1<<63) != 0 {
		t.Fatalf("the boot exchange uses id %d, which is in the runner's reserved space", req.ID)
	}
}

// TestAMissingTokenWithDeclaredSecretsFailsTheBootChain is the same row from
// the other direction: an older control plane that declared nothing and
// withheld nothing sends no token, and a guest whose configuration names
// secrets it was given no way to fetch must not start either.
func TestAMissingTokenWithDeclaredSecretsFailsTheBootChain(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	boots := &bootstrapper{}

	done := make(chan struct{})
	var failure *bootFailure
	go func() {
		defer close(done)
		_, _, failure, _ = bootOverVsock(context.Background(), dial, boots)
	}()

	host := <-hosts
	host.sendBootConfig(runner.BootConfig{
		Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example",
		SecretNames: []string{"DEPLOY_KEY"},
	})
	<-done
	if failure == nil {
		t.Fatal("a configuration declaring secrets with no token booted anyway")
	}
	if !strings.Contains(failure.Error(), "1 secret(s)") || !strings.Contains(failure.Error(), "no bootstrap token") {
		t.Errorf("the failure = %q", failure)
	}
	if !host.nothingMore(200 * time.Millisecond) {
		t.Fatal("the guest asked for its secrets with no token to ask with")
	}
}

// TestAResumeReExchangesAndARedialDoesNot is the rule that makes single use
// workable.
//
// A bootstrap token is spent by its first exchange. A sessiond that
// re-exchanged on every connection would be refused on the first redial
// inside a live VM and stay refused forever; one that never re-exchanged
// would come back from a cold resume with no secrets. What tells the two
// apart is the token itself: a resume brings a new one, a redial brings the
// one already spent.
func TestAResumeReExchangesAndARedialDoesNot(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	boots := &bootstrapper{}

	// Boot.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = bootOverVsock(context.Background(), dial, boots)
	}()
	host := <-hosts
	cfg := runner.BootConfig{
		Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example",
		SecretNames: []string{"DEPLOY_KEY"}, BootstrapToken: "token_from_the_create",
	}
	host.sendBootConfig(cfg)
	first := host.nextRequest()
	host.answer(first.ID, map[string]string{"DEPLOY_KEY": "value_example"})
	<-done

	// A REDIAL inside the same VM: the same configuration, the same token.
	// Nothing is asked for, because the token has been spent and the values
	// are already in this process.
	redialConn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	redialHost := <-hosts
	redialDone := make(chan error, 1)
	go func() { redialDone <- reBootstrap(context.Background(), redialConn, boots) }()
	redialHost.sendBootConfig(cfg)
	if err := <-redialDone; err != nil {
		t.Fatalf("a redial failed: %v", err)
	}
	if !redialHost.nothingMore(200 * time.Millisecond) {
		t.Fatal("a redial re-exchanged a spent token, which would be refused forever")
	}

	// A RESUME: a new VM, a new socket, a new token. The secrets are fetched
	// again, because the process on this side may be a completely new one
	// and the values it holds may be from a boot that no longer exists.
	_ = os.Unsetenv("DEPLOY_KEY")
	resumeConn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resumeHost := <-hosts
	resumeDone := make(chan error, 1)
	go func() { resumeDone <- reBootstrap(context.Background(), resumeConn, boots) }()
	resumed := cfg
	resumed.BootstrapToken = "token_from_the_resume"
	resumeHost.sendBootConfig(resumed)
	req := resumeHost.nextRequest()
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(req.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.Token != "token_from_the_resume" {
		t.Fatalf("the resume exchanged %q, want the freshly minted token", body.Token)
	}
	resumeHost.answer(req.ID, map[string]string{"DEPLOY_KEY": "value_after_the_resume"})
	if err := <-resumeDone; err != nil {
		t.Fatalf("a resume failed: %v", err)
	}
	if os.Getenv("DEPLOY_KEY") != "value_after_the_resume" {
		t.Errorf("the resumed session's secret = %q", os.Getenv("DEPLOY_KEY"))
	}
}

// TestForgetUnsetsEveryDeliveredSecret is the cold suspend's third act. The
// VM is about to end with no memory image anywhere, so this is belt and
// braces — but it is the half this process can make, and a sessiond that
// was frozen instead of terminated has nothing left in its environment.
func TestForgetUnsetsEveryDeliveredSecret(t *testing.T) {
	cleanEnv(t)
	boots := &bootstrapper{}
	cfg := runner.BootConfig{Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example"}
	if err := applyBootConfig(cfg, map[string]string{"DEPLOY_KEY": "value_example", "NPM_TOKEN": "other"}); err != nil {
		t.Fatal(err)
	}
	boots.remember([]string{"DEPLOY_KEY", "NPM_TOKEN"})
	if os.Getenv("DEPLOY_KEY") == "" {
		t.Fatal("the fixture did not apply anything, so forgetting it proves nothing")
	}
	if n := boots.forget(); n != 2 {
		t.Fatalf("forgot %d secret(s), want 2", n)
	}
	for _, k := range []string{"DEPLOY_KEY", "NPM_TOKEN"} {
		if v, set := os.LookupEnv(k); set {
			t.Errorf("%s survived the cold suspend as %q", k, v)
		}
	}
	// And the session id, which is configuration and not a secret, is still
	// there: forgetting is by name and not a blanket wipe.
	if os.Getenv("RAINIER_SESSION") != "sess_example" {
		t.Error("forgetting the secrets also dropped the session's own configuration")
	}
}

// TestAConfigurationWinsOverADeliveredSecret pins the precedence the control
// plane already applies on the Docker path: the agent-home keys are launch
// invariants, and a delivered value spelled like one of them must not
// redirect credential custody. The plane also drops such a name from
// SecretNames, so this is the second of two fences.
func TestAConfigurationWinsOverADeliveredSecret(t *testing.T) {
	cleanEnv(t)
	cfg := runner.BootConfig{
		Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example",
		Env: map[string]string{"CLAUDE_CONFIG_DIR": "/rainier/agents/claude"},
	}
	if err := applyBootConfig(cfg, map[string]string{"CLAUDE_CONFIG_DIR": "/workspace/attacker"}); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("CLAUDE_CONFIG_DIR"); got != "/rainier/agents/claude" {
		t.Fatalf("CLAUDE_CONFIG_DIR = %q; a delivered value replaced the agent home", got)
	}
}

// TestAMisversionedBootConfigIsRefused pins that a guest booted from an
// image older than the host's configuration protocol says so rather than
// running on a configuration it has half understood.
func TestAMisversionedBootConfigIsRefused(t *testing.T) {
	cleanEnv(t)
	dial, hosts := fakeTransport(t)
	conn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	host := <-hosts
	go host.sendBootConfig(runner.BootConfig{Protocol: 99, SessionID: "sess_example"})

	if _, err := readBootConfig(context.Background(), conn); err == nil {
		t.Fatal("a configuration from an unknown protocol was accepted")
	} else if !strings.Contains(err.Error(), "must be replaced") {
		t.Errorf("error = %q, want it to say which side has to change", err)
	}
}
