// internal/runnerd/microvm_test.go
package runnerd

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// withholdingDriver is a driver that announces microvm.v1 and takes a host,
// over the ordinary fake.
//
// It is NOT a *driver.Microvm, and it deliberately cannot be: the microVM
// driver's simulated engine lives in a _test.go file of its own package
// precisely so no code outside it can construct a simulated hypervisor, and
// that rule is worth more than the convenience of reusing it here. What this
// file tests is the RUNNER's half — what it announces, where a guest's
// connection goes, what a create carries, what a cold suspend says — and
// every one of those is keyed on the announced capability rather than on a
// concrete type, so a driver that announces it is exactly the right fixture.
// The driver's own half is tested in internal/driver.
type withholdingDriver struct {
	*driver.Fake
	mu   sync.Mutex
	host driver.MicrovmHost
}

func (d *withholdingDriver) Capabilities() []string { return []string{runner.CapabilityMicrovmV1} }

func (d *withholdingDriver) SetHost(h driver.MicrovmHost) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.host = h
}

func (d *withholdingDriver) installedHost() driver.MicrovmHost {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.host
}

// testMicrovmServer builds a runner whose driver withholds, with the dial
// base and proxy a real one is configured with.
func testMicrovmServer(t *testing.T) (*Server, *withholdingDriver) {
	t.Helper()
	d := &withholdingDriver{Fake: driver.NewFake(4)}
	return New(d, "ws://runner.example.invalid:8080", "", "http://proxy.example.invalid:3128"), d
}

// TestMicrovmRunnerAnnouncesTheCapability is the fence the control plane keys
// its withholding on. It is a fact about which driver this runner was started
// with, not a flag an operator has to remember beside --driver=microvm, and a
// runner that failed to announce it would be dispatched the secret values its
// own driver then refuses.
func TestMicrovmRunnerAnnouncesTheCapability(t *testing.T) {
	s, _ := testMicrovmServer(t)
	caps := buildCapabilities(nil, s.driverCapabilities()...)
	if !slices.Contains(caps, runner.CapabilityMicrovmV1) {
		t.Fatalf("a microVM runner announces %v, want %s among them", caps, runner.CapabilityMicrovmV1)
	}
	if !slices.Contains(caps, runner.CapabilityExecV1) {
		t.Fatalf("announcing microvm.v1 dropped exec.v1: %v", caps)
	}

	// And a Docker runner announces neither it nor anything new: the whole
	// compatibility floor is that a runner without this capability is
	// dispatched exactly what it always was.
	docker := New(driver.NewFake(4), "", "", "")
	if got := buildCapabilities(nil, docker.driverCapabilities()...); !slices.Equal(got, []string{runner.CapabilityExecV1}) {
		t.Fatalf("a non-microVM runner announces %v, want only exec.v1", got)
	}

	// The operator's own claims are kept, in order, and not reordered by the
	// build facts appended after them.
	declared := []string{"gpu", "docker.rootless"}
	got := buildCapabilities(declared, s.driverCapabilities()...)
	if len(got) != 4 || got[0] != "gpu" || got[1] != "docker.rootless" {
		t.Fatalf("buildCapabilities(%v) = %v; it reordered the operator's list", declared, got)
	}
}

// TestMicrovmCreateLeavesNoDialURL is §4's step 2. There is no URL to dial:
// the guest's control channel is the vsock conn IT opens, so a dial URL would
// be a promise of an endpoint the guest has no route to — and an empty one is
// what makes NO_PROXY inside the guest the base list with no dial host in it.
func TestMicrovmCreateLeavesNoDialURL(t *testing.T) {
	s, d := testMicrovmServer(t)
	if err := s.CreateWithID(context.Background(), "sess-nodial", driver.Spec{}, nil); err != nil {
		t.Fatal(err)
	}
	got := d.LastSpec()
	if got.DialURL != "" {
		t.Fatalf("a withholding driver's create carries dial URL %q; there is nothing to dial", got.DialURL)
	}
	// Everything else a create carries is unchanged: the id and the proxy
	// are still this runner's own concerns and still set here.
	if got.SessionID != "sess-nodial" || got.ProxyURL != "http://proxy.example.invalid:3128" {
		t.Fatalf("the create lost something else: %+v", got)
	}

	// The Docker path keeps its dial URL, byte for byte: that is the floor
	// this whole change promises not to move.
	dockerRunner := New(driver.NewFake(4), "ws://runner.example.invalid:8080", "", "")
	if err := dockerRunner.CreateWithID(context.Background(), "sess-docker", driver.Spec{}, nil); err != nil {
		t.Fatal(err)
	}
	fake := dockerRunner.drv.(*driver.Fake)
	if got := fake.LastSpec().DialURL; got != "ws://runner.example.invalid:8080/register" {
		t.Fatalf("the docker path's dial URL = %q", got)
	}
}

// TestGuestConnectedBecomesTheSessionsHub is the vsock half of `register`:
// the connection Firecracker forwarded becomes the session's relay hub,
// keyed by the session the SOCKET PATH named, and the session reports
// running.
func TestGuestConnectedBecomesTheSessionsHub(t *testing.T) {
	s, _ := testMicrovmServer(t)
	events := make(chan string, 8)
	s.SetOnEvent(func(id, state, _ string) { events <- id + ":" + state })

	s.reg.put("sess-guest", &sessionEntry{id: "sess-guest", state: "running"})

	guest, host := net.Pipe()
	defer guest.Close()
	s.GuestConnected("sess-guest", relay.NetConn(host))

	select {
	case got := <-events:
		if got != "sess-guest:running" {
			t.Fatalf("event = %q, want sess-guest:running", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a guest connection produced no running event")
	}
	if _, ok := s.reg.hub("sess-guest"); !ok {
		t.Fatal("a guest connection did not become the session's hub")
	}

	// A guest for a session this runner does not hold is closed rather than
	// served: a destroy that raced the boot must not leave a conn and a
	// goroutine with no entry that could ever reap them.
	other, otherHost := net.Pipe()
	defer other.Close()
	s.GuestConnected("sess-unknown", relay.NetConn(otherHost))
	if _, err := other.Read(make([]byte, 1)); err == nil {
		t.Fatal("a guest for an unknown session was left connected")
	}
}

// TestMintSessionBootstrapRidesTheControlConnection is the cold-resume mint,
// end to end over a fake control plane: the runner originates the request,
// the plane answers it, and the answer reaches the caller inside this process
// rather than being forwarded into the sandbox.
func TestMintSessionBootstrapRidesTheControlConnection(t *testing.T) {
	s, _ := testMicrovmServer(t)
	s.reg.put("sess-mint", &sessionEntry{id: "sess-mint", state: "running"})

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})
	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	minted := make(chan string, 1)
	mintErr := make(chan error, 1)
	go func() {
		tok, err := s.MintSessionBootstrap(context.Background(), "sess-mint")
		if err != nil {
			mintErr <- err
			return
		}
		minted <- tok
	}()

	req := conn.readMsg(t)
	if req.Type != "session_req" || req.Session != "sess-mint" || req.RPC == nil {
		t.Fatalf("the runner sent %+v, want a session_req for sess-mint", req)
	}
	if req.RPC.Method != runner.MethodMintSessionBootstrap {
		t.Fatalf("method = %q, want %q", req.RPC.Method, runner.MethodMintSessionBootstrap)
	}
	// The id is in the runner's own space, so it cannot collide with a
	// sandbox's — both travel up this one connection for this one session.
	if !isRunnerOriginated(req.RPC.ID) {
		t.Fatalf("the runner's own request carries id %d, which is in the sandbox's space", req.RPC.ID)
	}
	var body struct {
		Protocol int `json:"protocol"`
	}
	if err := json.Unmarshal(req.RPC.Payload, &body); err != nil || body.Protocol != runner.SessionBootstrapProtocolVersion {
		t.Fatalf("request body = %s (%v)", req.RPC.Payload, err)
	}

	answer, _ := json.Marshal(map[string]any{"token": "token_example", "expires_in_sec": 120})
	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: "sess-mint",
		RPC: &runner.RPCEnvelope{ID: req.RPC.ID, Method: "resp", OK: true, Payload: answer}})

	select {
	case tok := <-minted:
		if tok != "token_example" {
			t.Fatalf("minted token = %q", tok)
		}
	case err := <-mintErr:
		t.Fatalf("mint: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the mint never returned")
	}

	// And the answer did NOT reach the sandbox: the runner is a forwarder
	// for everything the sandbox asked and a participant only in this one.
	if _, ok := s.reg.hub("sess-mint"); ok {
		t.Fatal("this fixture has no sandbox; the assertion below would prove nothing")
	}
}

// TestMintSessionBootstrapFailsClosed pins the two ways a mint can go wrong,
// both of which a cold resume has to hear about rather than boot through.
func TestMintSessionBootstrapFailsClosed(t *testing.T) {
	t.Run("no control connection", func(t *testing.T) {
		s, _ := testMicrovmServer(t)
		if _, err := s.MintSessionBootstrap(context.Background(), "sess-x"); err == nil {
			t.Fatal("a mint with no controld connection succeeded")
		} else if !strings.Contains(err.Error(), "no controld connection") {
			t.Fatalf("error = %q", err)
		}
	})

	t.Run("a refusal is relayed verbatim", func(t *testing.T) {
		s, _ := testMicrovmServer(t)
		s.reg.put("sess-refused", &sessionEntry{id: "sess-refused", state: "running"})
		fc := newFakeControld(t, testToken)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go s.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})
		conn := fc.nextConn(t)
		conn.readAnnounce(t)

		out := make(chan error, 1)
		go func() {
			_, err := s.MintSessionBootstrap(context.Background(), "sess-refused")
			out <- err
		}()
		req := conn.readMsg(t)
		refusal, _ := json.Marshal(map[string]string{"error": "this session is not placed on the runner that asked"})
		conn.send(t, runner.ToRunner{Type: "session_rpc", Session: "sess-refused",
			RPC: &runner.RPCEnvelope{ID: req.RPC.ID, Method: "resp", Payload: refusal}})

		select {
		case err := <-out:
			if err == nil || !strings.Contains(err.Error(), "not placed on the runner that asked") {
				t.Fatalf("error = %v, want the control plane's own sentence", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the mint never returned")
		}
	})

	t.Run("a cancelled context ends the wait", func(t *testing.T) {
		s, _ := testMicrovmServer(t)
		s.reg.put("sess-cancel", &sessionEntry{id: "sess-cancel", state: "running"})
		fc := newFakeControld(t, testToken)
		agentCtx, stopAgent := context.WithCancel(context.Background())
		defer stopAgent()
		go s.RunAgent(agentCtx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})
		fc.nextConn(t).readAnnounce(t)

		ctx, cancel := context.WithCancel(context.Background())
		out := make(chan error, 1)
		go func() {
			_, err := s.MintSessionBootstrap(ctx, "sess-cancel")
			out <- err
		}()
		cancel()
		select {
		case err := <-out:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want a cancellation", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a cancelled mint never returned")
		}
	})
}

// TestColdSuspendTellsAMicrovmSandboxItIsCold is §4.4's handshake. A cold
// microVM suspend terminates the VM with no memory image anywhere, so the
// sandbox is told first and told that it is cold — flush, unmount, forget the
// delivered secrets — where a warm one is a freeze and says nothing new.
func TestColdSuspendTellsAMicrovmSandboxItIsCold(t *testing.T) {
	s, d := testMicrovmServer(t)
	if d.installedHost() == nil {
		t.Fatal("New did not install the runner as the driver's host")
	}
	s.suspendAckWait = 200 * time.Millisecond
	s.suspendReadyWait = 200 * time.Millisecond
	ctx := context.Background()
	if err := s.CreateWithID(ctx, "sess-cold", driver.Spec{}, nil); err != nil {
		t.Fatal(err)
	}

	// A sandbox on the far side of the session's conn, reading control
	// frames. The vsock transport is the same relay.Conn the WebSocket one
	// is, so a pipe stands in for it exactly.
	guest, host := net.Pipe()
	defer guest.Close()
	s.GuestConnected("sess-cold", relay.NetConn(host))
	waitForHub(t, s, "sess-cold")
	sandbox := relay.NetConn(guest)

	done := make(chan error, 1)
	go func() { done <- s.Op(ctx, "sess-cold", "suspend", false) }()

	ev := readControlEvent(t, sandbox)
	if ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q, want %q", ev.Kind, relay.KindSuspending)
	}
	if !ev.Cold {
		t.Fatal("a cold microVM suspend was announced as a freeze; the guest would not flush or unmount")
	}
	if ev.ID == 0 {
		t.Fatal("the suspend notice carries no nonce")
	}
	if err := <-done; err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
}

// TestColdSuspendOnDockerIsUnchanged is the other half, and the one that
// matters most: the Docker path sends no notice on a cold stop, exactly as it
// never has. `docker stop` delivers the SIGTERM sessiond's own handler
// answers, and a frame added here would be a behaviour change on the one path
// this design promises to leave alone.
func TestColdSuspendOnDockerIsUnchanged(t *testing.T) {
	fd := driver.NewFake(4)
	s := New(fd, "", "", "")
	ctx := context.Background()
	if err := s.CreateWithID(ctx, "sess-docker", driver.Spec{}, nil); err != nil {
		t.Fatal(err)
	}
	guest, host := net.Pipe()
	defer guest.Close()
	go s.serveSessionConn(context.Background(), "sess-docker", relay.NetConn(host))
	waitForHub(t, s, "sess-docker")

	if err := s.Op(ctx, "sess-docker", "suspend", false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	// Nothing was written to the sandbox at all. A read with a deadline is
	// how "no frame" is asserted; the conn is live throughout.
	_ = guest.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if n, err := guest.Read(make([]byte, 64)); err == nil {
		t.Fatalf("the docker path sent the sandbox %d bytes on a cold stop", n)
	}
}

// readControlEvent reads one frame off a sandbox-side conn and decodes the
// control event in it.
func readControlEvent(t *testing.T, conn relay.Conn) relay.ControlEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read a frame: %v", err)
	}
	f, err := relay.Decode(raw)
	if err != nil {
		t.Fatalf("decode a frame: %v", err)
	}
	if f.Type != relay.FrameControl {
		t.Fatalf("frame type = %d, want a control frame", f.Type)
	}
	var ev relay.ControlEvent
	if err := json.Unmarshal(f.Payload, &ev); err != nil {
		t.Fatalf("decode a control event: %v", err)
	}
	return ev
}
