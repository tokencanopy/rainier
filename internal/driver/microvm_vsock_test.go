// internal/driver/microvm_vsock_test.go
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// dialGuest connects to the host end of a session's vsock control channel the
// way Firecracker's forwarding does: an AF_UNIX connection to
// "<uds_path>_1024". No VM and no KVM are involved — the socket is the part
// of this that runs anywhere, and the part a host harness has to prove is
// that Firecracker forwards to it.
func dialGuest(t *testing.T, m *Microvm, instanceID string, boot int) relay.Conn {
	t.Helper()
	_, listenPath, err := m.vsockPaths(instanceID, boot)
	if err != nil {
		t.Fatalf("vsock paths: %v", err)
	}
	// The driver opens the listener before it launches, so it is there by the
	// time Create returns; a short retry covers nothing but scheduling.
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("unix", listenPath)
		if err == nil {
			t.Cleanup(func() { _ = c.Close() })
			return relay.NetConn(c)
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial the guest control socket %s: %v", listenPath, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readBootConfig reads the first frame off a guest connection and requires it
// to be the boot configuration.
func readBootConfig(t *testing.T, conn relay.Conn) runner.BootConfig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read the first frame: %v", err)
	}
	frame, err := relay.Decode(raw)
	if err != nil {
		t.Fatalf("decode the first frame: %v", err)
	}
	if frame.Type != relay.FrameControl || frame.AttachID != 0 {
		t.Fatalf("the first frame is %+v, want a control frame on attachment 0", frame)
	}
	var ev relay.ControlEvent
	if err := json.Unmarshal(frame.Payload, &ev); err != nil {
		t.Fatalf("decode the control event: %v", err)
	}
	if ev.Kind != relay.KindBootConfig {
		t.Fatalf("the first control frame is %q, want %q", ev.Kind, relay.KindBootConfig)
	}
	var cfg runner.BootConfig
	if err := json.Unmarshal(ev.Payload, &cfg); err != nil {
		t.Fatalf("decode the boot configuration: %v", err)
	}
	return cfg
}

// TestMicrovmSendsTheBootConfigFirst is the whole configuration path in one
// test: a guest connects to the socket Firecracker forwards port 1024 to, and
// the first thing it reads is everything it needs to be a session.
//
// The assertions are on what the GUEST sees, not on what the driver stored,
// because the guest is the only consumer and a field it does not receive is a
// session that boots wrong however neatly the host recorded it.
func TestMicrovmSendsTheBootConfigFirst(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	host := &stubMicrovmHost{}
	m.SetHost(host)
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID:       "sess-boot",
		ProxyURL:        "http://proxy.example.invalid:3128",
		EgressAllow:     []string{"registry.npmjs.org"},
		Setup:           "npm ci\n",
		SetupTimeoutSec: 600,
		Init:            "make dev\n",
		InitTimeoutSec:  180,
		Repos:           []RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"}},
		GitAuthorName:   "example",
		GitAuthorEmail:  "42+example@users.noreply.github.com",
		Cmd:             []string{"claude"},
		Env:             map[string]string{"CLAUDE_CONFIG_DIR": "/rainier/agents/claude"},
		SecretNames:     []string{"DEPLOY_KEY"},
		BootstrapToken:  "token_example",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	cfg := readBootConfig(t, dialGuest(t, m, h.ID, 1))

	if cfg.Protocol != runner.SessionBootstrapProtocolVersion {
		t.Errorf("protocol = %d, want %d", cfg.Protocol, runner.SessionBootstrapProtocolVersion)
	}
	// The session id is the HOST's statement, taken from the socket path the
	// connection arrived on, and not something the guest asserted.
	if cfg.SessionID != "sess-boot" {
		t.Errorf("session id = %q, want sess-boot", cfg.SessionID)
	}
	if len(cfg.Cmd) != 1 || cfg.Cmd[0] != "claude" {
		t.Errorf("cmd = %v", cfg.Cmd)
	}
	if cfg.Setup != "npm ci\n" || cfg.SetupTimeoutSec != 600 || cfg.Init != "make dev\n" || cfg.InitTimeoutSec != 180 {
		t.Errorf("the boot chain did not survive: %+v", cfg)
	}
	if len(cfg.Repos) != 1 || cfg.Repos[0].Dir != "app" || cfg.Repos[0].SessionBranch != "rainier/work" {
		t.Errorf("repos = %+v", cfg.Repos)
	}
	if cfg.GitAuthorName != "example" || cfg.GitAuthorEmail != "42+example@users.noreply.github.com" {
		t.Errorf("git identity = %q <%q>", cfg.GitAuthorName, cfg.GitAuthorEmail)
	}
	if cfg.Env["CLAUDE_CONFIG_DIR"] != "/rainier/agents/claude" {
		t.Errorf("the agent-home configuration did not reach the guest: %v", cfg.Env)
	}
	if len(cfg.SecretNames) != 1 || cfg.SecretNames[0] != "DEPLOY_KEY" || cfg.BootstrapToken != "token_example" {
		t.Errorf("the bootstrap pair did not reach the guest: names=%v", cfg.SecretNames)
	}
	// The proxy carries the session's identity as URL userinfo, exactly as
	// the Docker driver composes it, so egressd reads one thing on both
	// paths. And NO_PROXY has no dial host to exempt, because there is no
	// dial: the conn this rode in on IS the channel.
	if !strings.Contains(cfg.ProxyURL, "sess-boot:") {
		t.Errorf("proxy url = %q, want the session's identity as userinfo", cfg.ProxyURL)
	}
	if cfg.NoProxy == "" || strings.Contains(cfg.NoProxy, "runner") {
		t.Errorf("no_proxy = %q, want the base list with no dial host in it", cfg.NoProxy)
	}

	// And the connection was handed to the runner above the driver, which is
	// what turns it into the session's relay hub.
	waitFor(t, func() bool {
		for _, s := range host.connected() {
			if s == "sess-boot" {
				return true
			}
		}
		return false
	}, "the guest connection was never handed to the runner")
}

// TestMicrovmGuestChannelIsPrivateToItsSession pins the mode on the one file
// this path creates. The socket is a session's control channel on a host
// shared with other tenants, and anything that can connect to it can be that
// session's sandbox.
func TestMicrovmGuestChannelIsPrivateToItsSession(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-mode"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	_, listenPath, err := m.vsockPaths(h.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(listenPath)
	if err != nil {
		t.Fatalf("stat the guest control socket: %v", err)
	}
	if info.Mode().Perm() != microvmFileMode {
		t.Errorf("the guest control socket has mode %04o, want %04o", info.Mode().Perm(), microvmFileMode)
	}
}

// TestMicrovmColdResumeMintsAFreshTokenAndSocket is §4.4's resume, in the two
// things that make it safe: the token the previous boot was handed is
// single-use and fenced, so a new VM gets a new one; and Firecracker's own
// documentation warns that one uds_path cannot be multiplexed across VMs, so
// a new VM gets a new socket.
func TestMicrovmColdResumeMintsAFreshTokenAndSocket(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	host := &stubMicrovmHost{}
	m.SetHost(host)
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-resume", BootstrapToken: "token_from_the_create"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)
	if got := readBootConfig(t, dialGuest(t, m, h.ID, 1)).BootstrapToken; got != "token_from_the_create" {
		t.Fatalf("the first boot got token %q, want the create's", got)
	}

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	// The cold-parked VM's socket goes with it: a path nothing serves is one
	// more file on a shared host describing a session that is not running.
	_, firstPath, _ := m.vsockPaths(h.ID, 1)
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Errorf("the cold-parked session left its control socket behind: %v", err)
	}

	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatalf("cold resume: %v", err)
	}
	if !restarted {
		t.Error("a cold resume reported no restart")
	}
	if host.mintCount() != 1 {
		t.Fatalf("the cold resume made %d mint(s), want exactly 1", host.mintCount())
	}

	// A new socket, and the new token on it.
	cfg := readBootConfig(t, dialGuest(t, m, h.ID, 2))
	if cfg.BootstrapToken == "token_from_the_create" {
		t.Fatal("the resumed guest was handed the previous boot's token, which is spent and fenced")
	}
	if cfg.BootstrapToken == "" {
		t.Fatal("the resumed guest was handed no token at all")
	}
	if cfg.SessionID != "sess-resume" {
		t.Errorf("the resumed guest's session id = %q", cfg.SessionID)
	}
}

// TestMicrovmColdResumeFailsWhenTheTokenCannotBeMinted is the fail-closed
// half: a resume that cannot get a token reports that, rather than booting a
// guest that will ask for its secrets and be refused.
func TestMicrovmColdResumeFailsWhenTheTokenCannotBeMinted(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	host := &stubMicrovmHost{mintErr: errors.New("this runner has no controld connection")}
	m.SetHost(host)
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-nomint", BootstrapToken: "token_example"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Resume(ctx, h.ID); err == nil {
		t.Fatal("a cold resume with no token succeeded")
	} else if !strings.Contains(err.Error(), "no controld connection") {
		t.Errorf("error = %q, want the mint's own reason", err)
	}
	// And the session is still parked, so a later resume can try again.
	if st := listedState(t, m, "sess-nomint"); st != StateSuspended {
		t.Errorf("a refused resume changed the session's state to %q", st)
	}
}

// TestMicrovmRefusesSecretValuesWithNoToken is the driver's own half of the
// compatibility table's "old controld + new runner" row. The shared contract
// asserts the same refusal for any driver announcing microvm.v1; this one
// asserts the message says what an operator has to change.
func TestMicrovmRefusesSecretValuesWithNoToken(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	_, err := m.Create(ctx, Spec{
		SessionID: "sess-unwithheld",
		Env:       map[string]string{"DEPLOY_KEY": "value_must_not_be_accepted"},
	})
	if err == nil {
		t.Fatal("a create carrying secret values with no token was accepted")
	}
	for _, want := range []string{"bootstrap token", "microvm.v1", "sess-unwithheld"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal = %q, want it to name %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "value_must_not_be_accepted") {
		t.Errorf("the refusal quotes the value it refused: %q", err)
	}
	// Nothing was reserved, created, or left behind: the check runs before
	// the slot is taken.
	if used, _, _ := m.Capacity(ctx); used != 0 {
		t.Errorf("a refused create left %d slot(s) occupied", used)
	}
	// And an empty env with no token is a perfectly ordinary create: the
	// refusal is about VALUES, not about the field existing.
	if _, err := m.Create(ctx, Spec{SessionID: "sess-plain"}); err != nil {
		t.Fatalf("a create with no env and no token was refused: %v", err)
	}
}

// TestMicrovmVsockPathRefusesAnOverlongStateDir pins the one configuration
// mistake that would otherwise surface as "invalid argument" from bind(2).
func TestMicrovmVsockPathRefusesAnOverlongStateDir(t *testing.T) {
	long := "/" + strings.Repeat("d", 120)
	m := &Microvm{opts: MicrovmOpts{StateDir: long}}
	if _, _, err := m.vsockPaths("mvm-1", 1); err == nil {
		t.Fatal("an overlong state directory produced a socket path")
	} else if !strings.Contains(err.Error(), "--microvm-state-dir") {
		t.Errorf("error = %q, want it to name the flag an operator can change", err)
	}
}

// waitFor polls cond until it holds or the test gives up. It exists for the
// one assertion here that crosses a goroutine — the driver hands a guest
// connection to the runner from its accept loop.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
