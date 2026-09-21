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
	"time"

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
	// bootConfigWriteTimeout bounds the ONE write this driver makes onto a
	// guest connection, and it exists because the thing written is large and
	// the peer is untrusted.
	//
	// A boot configuration carries two scripts of up to MaxSetupBytes each
	// (driver.go), so it can reach well over a megabyte — far past any socket
	// buffer. A guest that connects and never reads would park an unbounded
	// write forever, which is a session's control channel held open by
	// whoever dialled it first. Ten seconds is orders of magnitude more than
	// a guest that IS reading needs, and it is finite.
	//
	// relay.NetConn answers an expired write context by CLOSING the conn,
	// which is exactly the right answer here: a guest that will not take its
	// own configuration has nothing further to say on this connection.
	bootConfigWriteTimeout = 10 * time.Second
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

// vsockSocketName is the file Firecracker binds for one boot, and
// vsockGuestPath is that same file named from INSIDE the VM's chroot, which
// is what the VMM is told (see VMMConfig.VsockUDSPath).
func vsockSocketName(boot int) string { return "v" + strconv.Itoa(boot) + ".sock" }
func vsockGuestPath(boot int) string  { return "/" + vsockSocketName(boot) }

// vsockPaths returns, for one boot of an instance, the two HOST paths the
// control channel lives at: the one Firecracker binds, and the one the
// guest's port-1024 connections are forwarded to and this driver listens on.
//
// They are inside the VM's jail, and they have to be: a chrooted Firecracker
// can neither create a socket outside its root nor connect to one. That is
// also why the chroot base is one letter — every byte of the jail path is on
// this socket's budget, and sun_path is 108 bytes.
//
// The path is per BOOT, not per instance: Firecracker's own documentation
// warns that one uds_path cannot be multiplexed across VMs, and a cold resume
// is a new VM. boot is the instance's launch counter.
func (m *Microvm) vsockPaths(id string, boot int) (udsPath, listenPath string, err error) {
	if err := checkPathSegment("instance id", id); err != nil {
		return "", "", err
	}
	udsPath = filepath.Join(jailRootDir(m.opts.StateDir, id), vsockSocketName(boot))
	listenPath = udsPath + "_" + strconv.Itoa(guestControlPort)
	if len(listenPath) > unixPathMax {
		return "", "", fmt.Errorf(
			"microvm: the vsock socket path %q is %d bytes, over the %d-byte kernel limit; "+
				"a shorter --microvm-state-dir is the fix, because a truncated path is a guest that boots and is never configured",
			listenPath, len(listenPath), unixPathMax)
	}
	return udsPath, listenPath, nil
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
	// listenPath is "<udsPath>_1024", the socket this end serves; udsPath is
	// the one FIRECRACKER binds when it is told about the device. Both are
	// held because both have to be removed: the second is not this process's
	// to create, but a VM that died leaves it behind, and a later boot that
	// found it would fail its PUT /vsock with "address already in use" for a
	// path nothing is serving.
	listenPath string
	udsPath    string
	// boot is the configuration whoever connects is handed. It is written
	// once, at construction, and never again: a cold resume mints a new
	// token and gets a new CHANNEL, because the socket path is per-boot too.
	boot runner.BootConfig

	mu sync.Mutex
	// closed makes the accept loop's exit quiet: a listener closed by
	// teardown reports an error like any other, and logging that as a
	// failure would put a line in an operator's log for every session that
	// ends normally.
	closed bool
	// served records that this boot generation's ONE guest connection has
	// been taken. /dev/vsock is world-accessible inside an ordinary guest,
	// so every process in the sandbox can dial (2, 1024) — and what the
	// first frame carries is the session's whole configuration and a LIVE
	// bootstrap token. Serving every connection would hand that to whoever
	// asked, as many times as they asked, and let the last one become the
	// session's hub.
	//
	// So the model is the design note's: one guest-initiated connection per
	// boot (§4). The first is sessiond; every later one is refused and closed
	// having received nothing at all. A sessiond that crashed and came back
	// is a NEW boot generation — a new VM, a new socket, a new mint (open
	// question 2) — and not something to re-serve this token to.
	served bool
	// conns are the connections this channel is still responsible for. It
	// holds at most the one served guest, and it exists so close() can end
	// it: a Destroy that closed only the listener would leave a wedged
	// boot-config write (a guest that never drains) holding a goroutine and
	// a socket for as long as its deadline, with nothing able to interrupt
	// it.
	conns map[net.Conn]struct{}
}

// claim reserves this boot generation's one guest connection for c and takes
// responsibility for closing it. It reports false for a second connection and
// for one that arrived after teardown — in both cases the caller closes c
// without writing a byte to it.
func (g *guestChannel) claim(c net.Conn) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.served {
		return false
	}
	g.served = true
	if g.conns == nil {
		g.conns = map[net.Conn]struct{}{}
	}
	g.conns[c] = struct{}{}
	return true
}

// drop closes a claimed connection this channel will not be handing on, and
// stops tracking it. The claim is NOT released: a boot generation gets one
// attempt, and a guest that could not be configured needs a fresh boot and a
// fresh token rather than a second go at a spent one.
func (g *guestChannel) drop(c net.Conn) {
	g.mu.Lock()
	delete(g.conns, c)
	g.mu.Unlock()
	_ = c.Close()
}

func (g *guestChannel) close() {
	g.mu.Lock()
	g.closed = true
	conns := make([]net.Conn, 0, len(g.conns))
	for c := range g.conns {
		conns = append(conns, c)
	}
	g.conns = nil
	g.mu.Unlock()
	if g.listener != nil {
		_ = g.listener.Close()
	}
	// The accepted connection goes too, and it is what makes Destroy
	// unblockable: a boot-config write into a guest that is not draining is
	// only interrupted by the conn dying.
	for _, c := range conns {
		_ = c.Close()
	}
	// Both paths, and an absent one is not an error: a teardown that runs
	// twice is ordinary, and a VM that never started leaves only one of them.
	_ = os.Remove(g.listenPath)
	_ = os.Remove(g.udsPath)
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
func (m *Microvm) openGuestChannel(sessionID, udsPath, listenPath string, cfg runner.BootConfig) (*guestChannel, error) {
	if err := os.MkdirAll(filepath.Dir(listenPath), microvmDirMode); err != nil {
		return nil, fmt.Errorf("create the guest channel directory: %w", err)
	}
	// Sockets left behind by a VM that is gone would make this Listen, or
	// Firecracker's own bind at PUT /vsock, fail with "address already in
	// use" for paths nothing is serving. A launch that failed after the
	// device was configured leaves exactly that. A resume no longer reuses
	// the failed attempt's number (Resume advances the counter under the
	// driver mutex, so no two boots of one instance share a path) — but a
	// create is always boot 1, and a record recovered after a runnerd
	// restart counts from whatever it was, so neither path gets to assume
	// the path it was handed is unused.
	_ = os.Remove(listenPath)
	_ = os.Remove(udsPath)
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
	g := &guestChannel{listener: ln, listenPath: listenPath, udsPath: udsPath, boot: cfg}
	go m.acceptGuests(sessionID, g)
	return g, nil
}

// acceptGuests keeps accepting for the life of the instance.
//
// It keeps accepting even though exactly one connection is ever SERVED,
// because the alternative is worse in both directions: an accept loop that
// stopped would leave later dials queued in the kernel with nobody to refuse
// them, and a loop that served inline would be wedged for good by the first
// guest that connected and did not read (the boot config can exceed a
// megabyte — see bootConfigWriteTimeout). So each connection gets a goroutine
// of its own, and all but the first are refused in it.
func (m *Microvm) acceptGuests(sessionID string, g *guestChannel) {
	for {
		c, err := g.listener.Accept()
		if err != nil {
			if !g.isClosed() {
				log.Printf("microvm: session %s: the guest control socket stopped accepting: %v", sessionID, err)
			}
			return
		}
		go m.serveGuest(sessionID, g, c)
	}
}

// serveGuest hands ONE guest connection its configuration and then hands the
// connection to the runner.
//
// The boot configuration is the FIRST frame on the conn, written here before
// anything else can write to it, which is what makes "nothing has to predate
// the stream" true: the guest reads its whole configuration off the same
// connection it will then serve its terminal and its RPC over.
//
// Every connection after the first is closed having received nothing — not
// the configuration, not the token, not a byte. See guestChannel.served: the
// socket is reachable by every process in the sandbox, and this is the one
// place that decides the first dial is the session's and no other is.
//
// A host that has not been installed closes the connection rather than
// holding it. There is nothing to attach it to, and the claim stays spent:
// this boot has had its one connection.
func (m *Microvm) serveGuest(sessionID string, g *guestChannel, c net.Conn) {
	if !g.claim(c) {
		// Not an error the operator can act on and not a rarity worth a
		// line per occurrence — but it IS somebody in the guest dialling a
		// socket that is not theirs, so it is said once per attempt and
		// names nothing about the session but its id.
		log.Printf("microvm: session %s: refusing a second connection on a control channel that serves one guest per boot", sessionID)
		_ = c.Close()
		return
	}
	conn := relay.NetConn(c)
	ctx, cancel := context.WithTimeout(context.Background(), bootConfigWriteTimeout)
	defer cancel()
	if err := writeBootConfig(ctx, conn, g.boot); err != nil {
		log.Printf("microvm: session %s: sending the boot configuration: %v", sessionID, err)
		g.drop(c)
		return
	}
	host := m.currentHost()
	if host == nil {
		log.Printf("microvm: session %s: a guest connected but this driver has no runner above it to serve it", sessionID)
		g.drop(c)
		return
	}
	host.GuestConnected(sessionID, conn)
}

// writeBootConfig sends one boot configuration as an ordinary FrameControl —
// the same frame shape every event, request and response on this channel
// uses, so the guest needs no special reader for it.
//
// The context is the caller's own bounded one rather than a create's: this is
// the one write whose failure means the guest is unconfigurable, so
// inheriting a create's context would let a create that has just returned
// cancel the configuration of the guest it started — but leaving it unbounded
// would let a guest that never reads hold the write open forever. See
// bootConfigWriteTimeout.
func writeBootConfig(ctx context.Context, conn relay.Conn, cfg runner.BootConfig) error {
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
	return conn.Write(ctx, frame)
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
			"(docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md). Not all of those values are necessarily "+
			"secret — a session with a creator carries its agent-home configuration in the same block — but this host "+
			"cannot tell them apart, has no argv to hand any of them to, and will not write one to disk. Upgrade the "+
			"control plane to one that withholds them for a runner announcing %q",
		spec.SessionID, len(spec.Env), runner.CapabilityMicrovmV1)
}
