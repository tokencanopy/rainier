// internal/driver/microvm_vsock.go
//
// virtio-vsock: the single host-to-guest control channel a microVM session
// has, and the whole of how one is configured (ADR-0003 §2.7 item 2, and
// docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md §4).
//
// It replaces two things the driver spike had and this driver deliberately
// does not: MMDS, which answers unauthenticated HTTP at the one address the
// host firewall exists to drop, and a host-side session.json, which put a
// tenant's configuration on a shared host's disk. Nothing this file writes to
// disk is a value: the only file it creates is a unix socket.
//
// This is the one place the driver layer depends on protocol/runner, and it
// is deliberate rather than a leak of the control-plane vocabulary this
// package otherwise keeps out (see driver.Spec and driver.RepoSpec, which
// have their own twins for exactly that reason). runner.BootConfig is not a
// runner-plane message: it is the host-to-GUEST schema, and the design note
// pins it beside runner.Spec because every field of it is a field of the
// create it came from. Somebody has to depend on it, and a second spelling
// of it here would be a session configured with something the control plane
// never dispatched.
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const (
	// guestCID is the guest's context id. 3 is the value Firecracker's own
	// documentation uses; the host is always HOST_CID, which is 2.
	guestCID = 3
	// guestControlPort is the ONE port this design uses, and there is not a
	// second one. internal/relay already multiplexes configuration, the
	// terminal stream, the session RPC and the lifecycle handshake over a
	// single conn — that is what the package is for — so three ports would be
	// three sockets carrying what one already carries, with three teardowns
	// to get wrong.
	//
	// A guest connection to host port N is forwarded by Firecracker to an
	// AF_UNIX socket at "<uds_path>_N", which is why the host listens on a
	// path and never calls connect: the host never sends CONNECT.
	guestControlPort = 1024
	// unixPathMax is the bound on a sockaddr_un path: Linux's sun_path is 108
	// bytes including its NUL terminator. It is checked here rather than left
	// to bind(2) because the path is composed from an operator's
	// --microvm-state-dir, and "invalid argument" on a socket is not an error
	// anybody can act on, where naming the limit and the path is.
	unixPathMax = 107
)

// MicrovmHost is what the microVM driver needs from the runner above it.
//
// There are exactly two things, and both are things only the runner can do.
// A guest's connection has to become a relay Hub keyed by the session — which
// means the registry, the boot epoch, the control routing and the event
// callbacks, all of which live in runnerd and none of which belong in a
// driver. And a cold resume needs a fresh bootstrap token, which means a
// session RPC upward to the control plane on the runner's own connection.
//
// nil is a real state and not an unconfigured one: the local dev surface and
// the driver contract suite have no runner above them, and a driver with no
// host simply has no guest channel and refuses a cold resume that would need
// one, rather than inventing either.
type MicrovmHost interface {
	// GuestConnected hands over one guest's control connection, already
	// carrying its boot configuration as the first frame it will read. The
	// implementation builds the session's relay hub over it exactly as the
	// WebSocket register path does.
	//
	// It must not block: the driver calls it from its accept loop.
	GuestConnected(sessionID string, conn relay.Conn)
	// MintSessionBootstrap asks the control plane for a fresh single-use
	// token for sessionID, which is what a cold resume boots with. The
	// session id is the runner's own — the control plane answers from the row
	// its placement guard read, never from anything in the request.
	MintSessionBootstrap(ctx context.Context, sessionID string) (string, error)
}

var (
	_ CapabilityDriver = (*Microvm)(nil)
	_ HostedDriver     = (*Microvm)(nil)
)

// Capabilities is what the runner announces on this driver's behalf, and it
// is the fence rather than a hint: the control plane withholds an
// environment's decrypted secret values from a placement whose runner
// announced microvm.v1, and this driver refuses a create that carries them
// anyway. The claim and the refusal are two halves of one promise, so they
// are read from one place.
func (m *Microvm) Capabilities() []string { return []string{runner.CapabilityMicrovmV1} }

// SetHost installs the runner above this driver. It is a setter rather than a
// field on MicrovmOpts because the runner is composed OVER the driver
// (runnerd.New takes one), so the two cannot both be constructed first.
//
// Called once, before the driver serves anything.
func (m *Microvm) SetHost(h MicrovmHost) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.host = h
}

func (m *Microvm) currentHost() MicrovmHost {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.host
}

// vsockPaths returns the Firecracker uds_path for one boot of an instance and
// the socket the guest's port-1024 connections are forwarded to.
//
// The path is per BOOT, not per instance: Firecracker's own documentation
// warns that one uds_path cannot be multiplexed across VMs, and a cold resume
// is a new VM. boot is the instance's launch counter.
func (m *Microvm) vsockPaths(id string, boot int) (udsPath, listenPath string, err error) {
	if err := checkPathSegment("instance id", id); err != nil {
		return "", "", err
	}
	udsPath = filepath.Join(m.instanceDir(id), "v"+strconv.Itoa(boot)+".sock")
	listenPath = udsPath + "_" + strconv.Itoa(guestControlPort)
	if len(listenPath) > unixPathMax {
		return "", "", fmt.Errorf(
			"microvm: the vsock socket path %q is %d bytes, over the %d-byte kernel limit; "+
				"a shorter --microvm-state-dir is the fix, because a truncated path is a guest that boots and is never configured",
			listenPath, len(listenPath), unixPathMax)
	}
	return udsPath, listenPath, nil
}

// GuestSocketPath returns the host path a session's guest control channel is
// currently listening on, and whether there is one — a cold-parked session
// has none, because its VM is gone and so is its socket.
//
// It is exported for the same reason SnapshotEnvKeys and Volumes are: it is
// how a test above this package can act like a guest, which is the only way
// to assert what a guest actually receives without a VM. Nothing in
// production reads it.
func (m *Microvm) GuestSocketPath(sessionID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, inst := range m.instances {
		if inst.SessionID == sessionID && inst.channel != nil {
			return inst.channel.path, true
		}
	}
	return "", false
}

// guestChannel is one instance's host end of the vsock control channel: the
// listener Firecracker forwards the guest's port-1024 connections to, and the
// boot configuration whoever connects is handed as their first frame.
//
// The configuration is held HERE, in memory, and nowhere else. It carries the
// bootstrap token, and §2.7 item 1's whole point is that no part of a
// session's configuration reaches a shared host's disk — which is why this
// struct has no persisted twin and why a runnerd restart loses it (see
// (*Microvm).Resume).
type guestChannel struct {
	listener net.Listener
	path     string

	mu   sync.Mutex
	boot runner.BootConfig
	// closed makes the accept loop's exit quiet: a listener closed by
	// teardown reports an error like any other, and logging that as a
	// failure would put a line in an operator's log for every session that
	// ends normally.
	closed bool
}

// setBoot replaces the configuration the next guest connection is handed. A
// cold resume mints a new token, and the guest that comes up must get THAT
// one — the retired token no longer works.
func (g *guestChannel) setBoot(cfg runner.BootConfig) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.boot = cfg
}

func (g *guestChannel) currentBoot() runner.BootConfig {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.boot
}

func (g *guestChannel) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	if g.listener != nil {
		_ = g.listener.Close()
	}
	// The socket file is the listener's own and goes with it. An absent one
	// is not an error: a teardown that runs twice is ordinary.
	_ = os.Remove(g.path)
}

func (g *guestChannel) isClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

// openGuestChannel creates the host end of the channel before the VM starts.
//
// Before, deliberately: the guest dials port 1024 as soon as its sessiond is
// up, and Firecracker refuses a guest connection whose "<uds_path>_1024" does
// not exist. A listener created after InstanceStart is a race whose loser is
// a session that boots and is never configured.
func (m *Microvm) openGuestChannel(sessionID, listenPath string, cfg runner.BootConfig) (*guestChannel, error) {
	if err := os.MkdirAll(filepath.Dir(listenPath), microvmDirMode); err != nil {
		return nil, fmt.Errorf("create the guest channel directory: %w", err)
	}
	// A socket left behind by a VM that is gone would make Listen fail with
	// "address already in use" for a path nothing is listening on.
	_ = os.Remove(listenPath)
	ln, err := net.Listen("unix", listenPath)
	if err != nil {
		return nil, fmt.Errorf("listen on the guest control socket %s: %w", listenPath, err)
	}
	// 0600: the socket is one tenant's control channel on a shared host, and
	// anything that can connect to it can be that session's sandbox. Listen
	// creates it with the process umask, which is not a policy this driver
	// gets to inherit.
	if err := os.Chmod(listenPath, microvmFileMode); err != nil {
		_ = ln.Close()
		_ = os.Remove(listenPath)
		return nil, fmt.Errorf("restrict the guest control socket %s: %w", listenPath, err)
	}
	g := &guestChannel{listener: ln, path: listenPath, boot: cfg}
	go m.acceptGuests(sessionID, g)
	return g, nil
}

// acceptGuests serves the channel for the life of the instance.
//
// It loops rather than accepting once because a sandbox redials: sessiond
// survives losing its connection and comes back (cmd/sessiond's dial loop),
// and the guest that reconnects needs its configuration again — it is a new
// process as often as not, on the far side of a cold resume.
func (m *Microvm) acceptGuests(sessionID string, g *guestChannel) {
	for {
		c, err := g.listener.Accept()
		if err != nil {
			if !g.isClosed() {
				log.Printf("microvm: session %s: the guest control socket stopped accepting: %v", sessionID, err)
			}
			return
		}
		m.serveGuest(sessionID, g, c)
	}
}

// serveGuest hands one guest connection its configuration and then hands the
// connection to the runner.
//
// The boot configuration is the FIRST frame on the conn, written here before
// anything else can write to it, which is what makes "nothing has to predate
// the stream" true: the guest reads its whole configuration off the same
// connection it will then serve its terminal and its RPC over.
//
// A host that has not been installed closes the connection rather than
// holding it. There is nothing to attach it to, and a guest that is dropped
// redials, where a guest held by a channel nobody serves waits forever.
func (m *Microvm) serveGuest(sessionID string, g *guestChannel, c net.Conn) {
	conn := relay.NetConn(c)
	if err := writeBootConfig(conn, g.currentBoot()); err != nil {
		log.Printf("microvm: session %s: sending the boot configuration: %v", sessionID, err)
		_ = conn.Close()
		return
	}
	host := m.currentHost()
	if host == nil {
		log.Printf("microvm: session %s: a guest connected but this driver has no runner above it to serve it", sessionID)
		_ = conn.Close()
		return
	}
	host.GuestConnected(sessionID, conn)
}

// writeBootConfig sends one boot configuration as an ordinary FrameControl —
// the same frame shape every event, request and response on this channel
// uses, so the guest needs no special reader for it.
//
// The context is Background and not the caller's: this is the one write whose
// failure means the guest is unconfigurable, and inheriting a create's
// context would make a create that has just returned cancel the configuration
// of the guest it started.
func writeBootConfig(conn relay.Conn, cfg runner.BootConfig) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		// Unreachable — every field is a string, an int, or a slice of them —
		// and logged without the error, because a json message quotes what it
		// choked on and this value carries the bootstrap token.
		return errors.New("the boot configuration could not be encoded")
	}
	payload, err := json.Marshal(relay.ControlEvent{Kind: relay.KindBootConfig, Payload: body})
	if err != nil {
		return errors.New("the boot configuration frame could not be encoded")
	}
	frame, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	if err != nil {
		return errors.New("the boot configuration frame could not be encoded")
	}
	return conn.Write(context.Background(), frame)
}

// bootConfigFor composes what a guest is told about itself.
//
// Every field is one the create resolved, carried across unchanged. There are
// two things deliberately NOT in it: any environment secret value (they are
// exactly what the token buys, and they never reach this host), and a dial
// URL (there is nothing to dial — the connection this rides on IS the
// channel, which is why createWithID leaves Spec.DialURL empty on this
// driver).
//
// The proxy URL carries the session's identity as URL userinfo, composed here
// with the same helper the Docker driver uses, so egressd reads one thing
// whichever driver the session is on.
func bootConfigFor(spec Spec) runner.BootConfig {
	cfg := runner.BootConfig{
		Protocol:        runner.SessionBootstrapProtocolVersion,
		SessionID:       spec.SessionID,
		Cmd:             slices.Clone(spec.Cmd),
		EgressAllow:     slices.Clone(spec.EgressAllow),
		Setup:           spec.Setup,
		SetupTimeoutSec: spec.SetupTimeoutSec,
		Init:            spec.Init,
		InitTimeoutSec:  spec.InitTimeoutSec,
		GitAuthorName:   spec.GitAuthorName,
		GitAuthorEmail:  spec.GitAuthorEmail,
		SecretNames:     slices.Clone(spec.SecretNames),
		BootstrapToken:  spec.BootstrapToken,
	}
	if len(cfg.Cmd) == 0 {
		cfg.Cmd = []string{"/bin/bash"}
	}
	if spec.ProxyURL != "" {
		cfg.ProxyURL = withSessionUserinfo(spec.ProxyURL, spec.SessionID)
		// No dial host to exempt, because there is no dial: NO_PROXY is the
		// base list and nothing else.
		cfg.NoProxy = noProxyFor("")
	}
	for _, r := range spec.Repos {
		cfg.Repos = append(cfg.Repos, runner.RepoSpec{
			Owner: r.Owner, Name: r.Name, BaseBranch: r.BaseBranch,
			SessionBranch: r.SessionBranch, Dir: r.Dir,
		})
	}
	// Env is the NON-SECRET configuration block, which for a microVM create
	// is the agent-home path variables and the agent manifest — paths and
	// names. Create has already refused any create whose Env carries values
	// with no token, and a create WITH a token is one the control plane
	// withheld the values from, so what is left here is configuration by
	// construction.
	if len(spec.Env) > 0 {
		cfg.Env = make(map[string]string, len(spec.Env))
		for k, v := range spec.Env {
			cfg.Env[k] = v
		}
	}
	return cfg
}

// refuseUnwithheldEnv is the microVM driver's half of the compatibility
// table's "old controld + new runner" row.
//
// A control plane that predates the bootstrap token dispatches an
// environment's decrypted secrets in Spec.Env and no token. On the Docker
// path that is fine and always has been — the values reach a `docker run`
// argv and no file. On this one it is not: this driver has no argv to hand
// them to, and every channel it does have ends in something that outlives the
// session on a host shared with other tenants.
//
// So it refuses, loudly, naming what the control plane has to be. A refusal
// costs one session; the alternative — writing them somewhere and booting
// anyway — is the exposure §2.7 was written against, and it would be silent.
func refuseUnwithheldEnv(spec Spec) error {
	if len(spec.Env) == 0 || spec.BootstrapToken != "" {
		return nil
	}
	return fmt.Errorf(
		"microvm: refusing to create session %s: its create carries %d environment value(s) and no bootstrap token, "+
			"which means a control plane older than the session bootstrap exchange "+
			"(docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md). A microVM host has no argv to hand a "+
			"value to and will not write one to disk, so this session cannot be booted here until the control plane "+
			"announces it withholds them for a runner announcing %q",
		spec.SessionID, len(spec.Env), runner.CapabilityMicrovmV1)
}
