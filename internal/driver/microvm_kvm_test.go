//go:build linux

// internal/driver/microvm_kvm_test.go
//
// The Firecracker-backed harness: this driver's contract and one whole session
// lifecycle, against a REAL hypervisor on a real KVM host.
//
// ADR-0003 §8 acceptance criterion 1 is "MicrovmDriver passes RunContract",
// and until this file the only thing that ran that contract against this
// driver was TestMicrovmSatisfiesContract, which injects the simulated engine,
// the fake network host and a no-op formatter (microvm_sim_test.go). That test
// is worth having — it is the whole of this driver's own bookkeeping, on every
// developer machine and in CI — but it evidences nothing about Firecracker,
// the jailer, a TAP device, an nftables ruleset or a guest. The Phase A
// runbook's experiment (a) records the gap in as many words: "a Firecracker-
// backed harness does not exist yet".
//
// # It never runs by accident
//
// Three gates, and all three have to be open:
//
//  1. the build tag above, so no non-Linux machine compiles it at all;
//  2. RAINIER_MICROVM_KVM_TEST=1, an explicit opt-in that nothing sets for
//     you — not CI, not `make test`, not `go test ./...`;
//  3. every host precondition below, each checked by name, so a skip says
//     which one was missing rather than "not a microVM host".
//
// A machine with /dev/kvm and no opt-in skips. A machine with the opt-in and
// no /dev/kvm skips. `GOOS=linux go vet` and `GOOS=linux go build` still
// compile it, which is the point of the gate being an env var rather than a
// second build tag: a harness nobody can compile is a harness that rots.
//
// # What the base image under test must contain
//
// RAINIER_MICROVM_TEST_IMAGES names a directory holding two files, both of
// them the operator's, both built by the Phase A runbook's Step 2:
//
//	vmlinux       an uncompressed ELF guest kernel (Firecracker's CI kernel is
//	              fine) with CONFIG_VIRTIO_VSOCKETS and CONFIG_VIRTIO_BLK, and
//	              world-readable (o+r): it is hard-linked into every jail and
//	              a jailed VMM is neither its owner nor in its group.
//	rootfs.ext4   an ext4 image of `environments/default` whose /init:
//	                - mounts /proc, /sys, /dev, and a tmpfs over the writable
//	                  paths a read-only... (the root here is NOT read-only: it
//	                  is this session's own copy-on-write copy, so /init may
//	                  write to it directly);
//	                - formats and mounts /dev/vdb on /workspace, and /dev/vdc
//	                  on /rainier/agents when it is present;
//	                - brings lo and eth0 up;
//	                - execs `sessiond --transport=vsock` (or with
//	                  RAINIER_TRANSPORT=vsock in its environment).
//
// That last line is the one the runbook's draft /init gets wrong today: it
// execs sessiond with no flags, which takes the WebSocket path, dials nothing
// over vsock and never reads a boot configuration. A guest that does that
// fails this harness at the first wait, with a message saying so.
//
// The file names are overridable (RAINIER_MICROVM_TEST_KERNEL,
// RAINIER_MICROVM_TEST_ROOTFS), absolute or relative to the images directory.
//
// # It is built the production way
//
// newKVMMicrovm passes NO Engine, Net or Format. That is the whole point: the
// three are one seam that MicrovmOpts requires whole or not at all, and
// passing none of them is what makes NewMicrovm check this host and then build
// the real FirecrackerEngine under the jailer, the real netslot.LinuxHost, and
// mkfs.ext4. The only things injected are deployment configuration an operator
// passes too — an image source, a state directory, the slot ranges — and the
// runner above the driver, which on this host is a stand-in for the control
// plane (see kvmRunner).
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const (
	// kvmOptInEnv is the opt-in, and it is deliberately not a build tag: a
	// harness that only a tag can compile is one `go vet` never sees.
	kvmOptInEnv = "RAINIER_MICROVM_KVM_TEST"
	// kvmImagesEnv names the directory holding the guest kernel and the base
	// ext4. It is also handed to the driver as its DirImageSource, so a ref
	// published there by digest resolves the way it would on a real runner.
	kvmImagesEnv = "RAINIER_MICROVM_TEST_IMAGES"
	kvmKernelEnv = "RAINIER_MICROVM_TEST_KERNEL"
	kvmRootfsEnv = "RAINIER_MICROVM_TEST_ROOTFS"
	// kvmStateParentEnv is where the per-run state directory is made. It
	// exists so a Phase A operator can put state on the Local SSD with a
	// filesystem that reflinks (ADR-0003 §2.7 item 3) rather than on whatever
	// TMPDIR points at.
	kvmStateParentEnv = "RAINIER_MICROVM_TEST_STATE_DIR"
	// kvmConnectEnv bounds the wait for a guest to boot and dial (2, 1024).
	kvmConnectEnv = "RAINIER_MICROVM_TEST_CONNECT_TIMEOUT"
	// kvmProxyEnv is the one host-side destination a slot's firewall allows,
	// as ip:port. Empty is legal and means no exception at all.
	kvmProxyEnv = "RAINIER_MICROVM_TEST_PROXY"
	// kvmTimingsEnv, when set, is a file the harness writes its measurements
	// to as JSON, for the runbook's evidence table to cite.
	kvmTimingsEnv = "RAINIER_MICROVM_TEST_TIMINGS"

	kvmDefaultKernel = "vmlinux"
	kvmDefaultRootfs = "rootfs.ext4"
	// kvmConnectWait is generous on purpose: it bounds a guest that is never
	// going to answer, not one that is merely booting on a cold page cache.
	kvmConnectWait = 90 * time.Second
	// kvmSlots is the harness's admission envelope. RunContract never holds
	// more than two sessions at once; four leaves room without asking a
	// feasibility host for sixteen 8 GiB guests' worth of address space.
	kvmSlots = 4
	// kvmSlotPrefix, kvmGuestCIDR and kvmUplinkCIDR are deliberately NOT the
	// production defaults. A Phase A host may have a runnerd on it (Step 3 of
	// the runbook starts one), and a harness that used `rnr` names and the
	// 10.201/10.202 ranges would reclaim that runner's namespaces at startup
	// and hand its slots out again.
	kvmSlotPrefix  = "rkvm"
	kvmGuestCIDR   = "10.211.0.0/16"
	kvmUplinkCIDR  = "10.212.0.0/16"
	kvmCgroupChild = "rainier-kvmtest"
)

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

// kvmFixture is the host, resolved, once every precondition has held.
type kvmFixture struct {
	images      string
	kernel      string
	rootfs      string
	stateParent string
	connectWait time.Duration
	proxyAddr   string
	proxyPort   int
}

// requireKVMHost skips unless this machine can actually boot a microVM, and
// names the precondition that failed.
//
// The order is the operator's: the opt-in first (so a machine that simply was
// not asked says so), then the machine facts they cannot change, then the
// artefacts they staged, then the binaries, then the privileges. Each skip
// names one thing and what it is for; a skip that said "preconditions not met"
// would be a skip nobody can act on.
func requireKVMHost(t *testing.T) kvmFixture {
	t.Helper()

	if v := os.Getenv(kvmOptInEnv); v != "1" {
		t.Skipf("%s is %q, not \"1\": this harness boots real Firecracker microVMs on this machine and never runs unless it is asked to. Run it with scripts/microvm-kvm-test.sh (make microvm-kvm-test)", kvmOptInEnv, v)
	}

	// /dev/kvm, opened the way the driver opens it, so a skip here and a
	// NewMicrovm failure cannot disagree.
	if f, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0); err != nil {
		t.Skipf("/dev/kvm is not usable by this process (%v): a microVM session is a hardware-isolated VM and there is no software fallback. On a GCE host this usually means the instance was created without --enable-nested-virtualization, or this user is not in the `kvm` group", err)
	} else {
		_ = f.Close()
	}

	images := os.Getenv(kvmImagesEnv)
	if images == "" {
		t.Skipf("%s is not set: it must name a directory holding the guest kernel and the base ext4 rootfs this harness boots (see this file's header for what the rootfs must contain)", kvmImagesEnv)
	}
	if fi, err := os.Stat(images); err != nil || !fi.IsDir() {
		t.Skipf("%s=%q is not a directory (%v): it must hold the guest kernel and the base ext4 rootfs", kvmImagesEnv, images, err)
	}
	kernel := kvmImagePath(images, os.Getenv(kvmKernelEnv), kvmDefaultKernel)
	rootfs := kvmImagePath(images, os.Getenv(kvmRootfsEnv), kvmDefaultRootfs)
	if err := readableFile("the guest kernel", kernel); err != nil {
		t.Skipf("%v. Stage it as %s/%s, or name it with %s (Phase A runbook, Step 2)", err, images, kvmDefaultKernel, kvmKernelEnv)
	}
	// The kernel is hard-linked into every jail and opened by a VMM running as
	// neither its owner nor a member of its group. Checked here rather than
	// left to the first create, where the message is about a jail path the
	// operator never named.
	if err := checkSharedImageReadable(kernel); err != nil {
		t.Skipf("%v", err)
	}
	if err := readableFile("the base rootfs image", rootfs); err != nil {
		t.Skipf("%v. Stage it as %s/%s, or name it with %s (Phase A runbook, Step 2)", err, images, kvmDefaultRootfs, kvmRootfsEnv)
	}

	for _, bin := range []struct{ name, why string }{
		{"firecracker", "the VMM every microVM session is"},
		{"jailer", "the per-VM uid, cgroup, netns, chroot and seccomp envelope ADR-0003 §4.5 requires; there is no unjailed launch path"},
		{"ip", "each session's network namespace, veth pair and TAP device"},
		{"nft", "each session's per-slot firewall (ADR-0003 §4.3)"},
		{"sysctl", "forwarding inside each slot's own namespace"},
		{"mkfs.ext4", "formatting a session's workspace and agent-home disk images"},
	} {
		if _, err := exec.LookPath(bin.name); err != nil {
			t.Skipf("%q is not on PATH, and this harness needs it for %s: %v", bin.name, bin.why, err)
		}
	}

	// The capability list, read from the same table the driver's own startup
	// gate reads, so the skip names exactly what NewMicrovm would have
	// refused on.
	effective, err := effectiveCapabilities()
	if err != nil {
		t.Skipf("this process's capabilities cannot be read (%v), so the harness cannot tell whether it may build a session's network or jail", err)
	}
	for _, c := range microvmCapabilities {
		if effective&(uint64(1)<<c.Bit) == 0 {
			t.Skipf("this process does not hold %s, which the microVM driver needs to %s.\n\n%s", c.Name, c.Why, MicrovmHostRequirements())
		}
	}
	if _, err := os.Stat(filepath.Join(defaultCgroupRoot, "cgroup.controllers")); err != nil {
		t.Skipf("%s does not look like a cgroup v2 mount (no cgroup.controllers): the jailer is asked for cgroup v2 and ADR-0003 §4.6 meters each VM from cpu.stat and memory.current under it: %v", defaultCgroupRoot, err)
	}
	if err := checkHostForwarding(); err != nil {
		t.Skipf("%v", err)
	}

	fx := kvmFixture{
		images:      images,
		kernel:      kernel,
		rootfs:      rootfs,
		stateParent: os.Getenv(kvmStateParentEnv),
		connectWait: kvmConnectWait,
	}
	if raw := os.Getenv(kvmConnectEnv); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("%s=%q is not a duration: %v", kvmConnectEnv, raw, err)
		}
		fx.connectWait = d
	}
	if raw := os.Getenv(kvmProxyEnv); raw != "" {
		addr, portStr, ok := strings.Cut(raw, ":")
		if !ok {
			t.Fatalf("%s=%q is not ip:port", kvmProxyEnv, raw)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatalf("%s=%q has a port that is not a number: %v", kvmProxyEnv, raw, err)
		}
		fx.proxyAddr, fx.proxyPort = addr, port
	}
	return fx
}

// kvmImagePath resolves one artefact name against the images directory. An
// absolute name is taken as given, so an operator can point at a kernel that
// lives somewhere else without moving it.
func kvmImagePath(dir, named, fallback string) string {
	if named == "" {
		named = fallback
	}
	if filepath.IsAbs(named) {
		return named
	}
	return filepath.Join(dir, named)
}

// ---------------------------------------------------------------------------
// The driver, built the production way
// ---------------------------------------------------------------------------

// kvmStateDir makes this run's state directory and reports whether its
// filesystem can share extents.
//
// The name is short for the reason shortTempDir is: the guest control socket
// lands at "<dir>/j/firecracker/<id>/root/v1.sock_1024" and sun_path is 108
// bytes. A directory named after the test would be over the limit before the
// driver had done anything wrong.
//
// The reflink probe is a NOTE and never a failure. ADR-0003 §2.7 item 3 puts
// the host state directory on XFS with reflinks so a create is a copy-on-write
// copy rather than a copy of an environment image, and §9 asks Phase 1 to
// measure what that costs — but a sparse copy is correct, merely slow, and a
// harness that refused to run on ext4 would refuse on most feasibility hosts.
func kvmStateDir(t *testing.T, parent string) (dir string, clone CloneMethod) {
	t.Helper()
	dir, err := os.MkdirTemp(parent, "mvmk")
	if err != nil {
		t.Fatalf("state directory under %q: %v", parent, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	// 0700 is what the driver writes with; MkdirTemp already gives it, and
	// this says so rather than relying on it.
	if err := os.Chmod(dir, microvmDirMode); err != nil {
		t.Fatalf("restrict the state directory: %v", err)
	}

	src := filepath.Join(dir, ".reflink-probe")
	if err := os.WriteFile(src, []byte("probe"), microvmFileMode); err != nil {
		t.Fatalf("reflink probe: %v", err)
	}
	dst := src + ".copy"
	clone, err = NewFileCloner().Clone(src, dst)
	_ = os.Remove(src)
	_ = os.Remove(dst)
	if err != nil {
		t.Fatalf("reflink probe: %v", err)
	}
	if clone != CloneReflink {
		t.Logf("NOTE: %s is on a filesystem that does not share extents (%s), so every create copies the whole environment image. "+
			"ADR-0003 §2.7 item 3 puts the host state directory on XFS with reflinks; a sparse copy is correct but is seconds of I/O per create "+
			"that the design budgeted at microseconds. Point %s at a reflinking filesystem to measure the real number.", dir, clone, kvmStateParentEnv)
	}
	return dir, clone
}

// newKVMMicrovm builds the driver exactly as cmd/runnerd does — no test seam
// injected — and installs a runner above it.
func newKVMMicrovm(t *testing.T, fx kvmFixture) (*Microvm, *kvmRunner, CloneMethod) {
	t.Helper()
	stateDir, clone := kvmStateDir(t, fx.stateParent)

	m, err := NewMicrovm(MicrovmOpts{
		KernelPath: fx.kernel,
		BaseRootfs: fx.rootfs,
		StateDir:   stateDir,
		TotalSlots: kvmSlots,
		// Left at ADR-0003 §5.1's per-session floor, which is what
		// NewMicrovm defaults them to. Named here rather than omitted so a
		// reader of a timing sees the shape it was taken on.
		VCPU:      defaultMicrovmVCPU,
		MemoryMiB: defaultMicrovmMemoryMiB,

		ImageSource: DirImageSource{Dir: fx.images},

		SlotGuestCIDR:  kvmGuestCIDR,
		SlotUplinkCIDR: kvmUplinkCIDR,
		SlotNamePrefix: kvmSlotPrefix,

		EgressProxyAddr: fx.proxyAddr,
		EgressProxyPort: fx.proxyPort,

		Jail: JailOpts{CgroupParent: kvmCgroupChild},
	})
	if err != nil {
		// A Fatal and not a Skip: every precondition this constructor checks
		// has already been checked by name above, so a failure here is a real
		// disagreement between the gate and the driver and is worth seeing.
		t.Fatalf("NewMicrovm on a host that passed every precondition: %v", err)
	}
	runnerAbove := newKVMRunner(fx.connectWait)
	m.SetHost(runnerAbove)
	t.Cleanup(runnerAbove.close)
	return m, runnerAbove, clone
}

// kvmTeardown destroys whatever a driver still holds, so the next subtest's
// reclaim has nothing of ours to find and the host is left as it was.
func kvmTeardown(t *testing.T, m *Microvm) {
	t.Helper()
	ctx := context.Background()
	listed, err := m.List(ctx)
	if err != nil {
		t.Logf("teardown: List: %v", err)
		return
	}
	for _, l := range listed {
		if err := m.Destroy(ctx, l.Handle.ID); err != nil {
			t.Logf("teardown: destroying %s: %v", l.Handle.ID, err)
		}
	}
}

// ---------------------------------------------------------------------------
// The runner above the driver
// ---------------------------------------------------------------------------

// kvmRunner is what runnerd is to this driver in production, reduced to the
// three things driver.MicrovmHost names and no more.
//
// Two of the three are real here and one is a stand-in, and the difference is
// worth stating rather than buried:
//
//   - GuestConnected is real: it takes the conn the driver hands over and
//     reads it, which is how this harness can assert that a guest booted, that
//     it read its boot configuration, and that it can be asked to flush.
//   - FlushGuest is real: it sends relay.KindFlush on that conn and waits for
//     the guest's KindFlushed, exactly as (*runnerd.Server).FlushGuest does.
//   - MintSessionBootstrap is a STAND-IN. In production the token comes from
//     the control plane over the runner's own connection, and a feasibility
//     host has no control plane. It mints a synthetic one, and every
//     assertion that turns on it says what it is evidencing: that the guest
//     was handed THIS token in its boot configuration and echoed it back, not
//     that any control plane issued it.
type kvmRunner struct {
	connectWait time.Duration

	mu      sync.Mutex
	closed  bool
	guests  map[string]*kvmGuest
	mints   map[string]int
	tokens  map[string]string
	secrets map[string]string
	nonce   uint64
}

// kvmGuest is one boot's control connection and what has been observed on it.
type kvmGuest struct {
	conn relay.Conn
	// at is when the driver handed this connection over, which is the instant
	// a boot became a session: the driver writes the boot configuration onto
	// the conn and only then calls GuestConnected (see serveGuest), so this
	// timestamp is "booted, dialled, and configured".
	at time.Time

	mu sync.Mutex
	// exchangedToken is the bootstrap token this guest sent back in its
	// secrets exchange. It is the strongest evidence a host-side test can have
	// that the guest read its boot configuration: the token is in no other
	// channel, is never on this host's disk, and the guest could not have
	// invented it.
	exchangedToken string
	exchanged      chan struct{}
	flushes        map[uint64]chan struct{}
	dead           chan struct{}
}

func newKVMRunner(connectWait time.Duration) *kvmRunner {
	return &kvmRunner{
		connectWait: connectWait,
		guests:      map[string]*kvmGuest{},
		mints:       map[string]int{},
		tokens:      map[string]string{},
		secrets:     map[string]string{},
	}
}

// SetSecret is what the control plane would answer a session's exchange with.
// The value never reaches this host's disk — it lives here and goes to the
// guest over the conn — which is the property ADR-0003 §2.7 item 1 is about.
func (r *kvmRunner) SetSecret(name, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secrets[name] = value
}

func (r *kvmRunner) GuestConnected(sessionID string, conn relay.Conn) {
	g := &kvmGuest{
		conn:      conn,
		at:        time.Now(),
		exchanged: make(chan struct{}),
		flushes:   map[uint64]chan struct{}{},
		dead:      make(chan struct{}),
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = conn.Close()
		return
	}
	// A cold resume is a new boot on a new socket, and the record is replaced:
	// what a later assertion wants is the CURRENT boot's guest, and keeping
	// the parked one would make "the resumed guest connected" answerable by
	// the connection that was there before the suspend.
	r.guests[sessionID] = g
	r.mu.Unlock()
	go r.readGuest(sessionID, g)
}

func (r *kvmRunner) MintSessionBootstrap(_ context.Context, sessionID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mints[sessionID]++
	token := fmt.Sprintf("kvm-harness-token-%s-%d", sessionID, r.mints[sessionID])
	r.tokens[sessionID] = token
	return token, nil
}

// FlushGuest asks a session's guest to sync its block devices and waits for it
// to say it has, exactly as the runner does.
//
// It WAITS for the guest to connect first, which (*runnerd.Server).FlushGuest
// does not, and the reason is RunContract rather than production: the contract
// creates a session and snapshots it in the next statement, and a real microVM
// is still booting then. In production a snapshot arrives from a control plane
// for a session that has been running for minutes, so a runner that refused a
// guest that had not registered yet is refusing the right thing. Here it would
// only be refusing the clock.
func (r *kvmRunner) FlushGuest(ctx context.Context, sessionID string) error {
	g, err := r.awaitGuest(ctx, sessionID, r.connectWait)
	if err != nil {
		return fmt.Errorf("session %s has no sandbox connection to flush; nothing may be published from its filesystem: %w", sessionID, err)
	}

	r.mu.Lock()
	r.nonce++
	nonce := r.nonce
	r.mu.Unlock()

	done := make(chan struct{})
	g.mu.Lock()
	g.flushes[nonce] = done
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.flushes, nonce)
		g.mu.Unlock()
	}()

	if err := g.send(relay.ControlEvent{Kind: relay.KindFlush, ID: nonce}); err != nil {
		return fmt.Errorf("asking session %s to flush: %w", sessionID, err)
	}
	// The wait is bounded by a timer and NOT by a context on the write or the
	// read: relay.NetConn answers a cancelled context by closing the conn, so
	// bounding the I/O would take the session's control channel down with the
	// flush. runnerd's own FlushGuest is shaped the same way.
	timer := time.NewTimer(r.connectWait)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-g.dead:
		return fmt.Errorf("session %s lost its sandbox connection before it had flushed", sessionID)
	case <-ctx.Done():
		return fmt.Errorf("waiting for session %s to flush: %w", sessionID, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("session %s did not report a flush within %s: its sessiond either predates relay.KindFlush or is not running", sessionID, r.connectWait)
	}
}

// awaitGuest returns sessionID's current guest, waiting up to wait for one to
// connect. The poll is a poll because a guest arrives on the driver's accept
// loop and the thing being waited for is a boot, not an event this end
// controls; a 25ms tick against a boot measured in seconds costs nothing.
func (r *kvmRunner) awaitGuest(ctx context.Context, sessionID string, wait time.Duration) (*kvmGuest, error) {
	deadline := time.Now().Add(wait)
	for {
		r.mu.Lock()
		g, ok := r.guests[sessionID]
		r.mu.Unlock()
		if ok {
			return g, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no guest dialled (2, %d) for session %s within %s", guestControlPort, sessionID, wait)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// forget drops a session's guest, so a later wait is for the NEXT boot's
// connection rather than satisfied by the one that has just been terminated.
func (r *kvmRunner) forget(sessionID string) {
	r.mu.Lock()
	delete(r.guests, sessionID)
	r.mu.Unlock()
}

func (r *kvmRunner) mintCount(sessionID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mints[sessionID]
}

func (r *kvmRunner) close() {
	r.mu.Lock()
	r.closed = true
	guests := make([]*kvmGuest, 0, len(r.guests))
	for _, g := range r.guests {
		guests = append(guests, g)
	}
	r.guests = map[string]*kvmGuest{}
	r.mu.Unlock()
	for _, g := range guests {
		_ = g.conn.Close()
	}
}

// readGuest drains one guest's control connection for the life of the conn.
//
// It answers exactly one thing — the boot-time secrets exchange — and records
// one other, the flush acknowledgement. Everything else a real sessiond sends
// up this channel (stage events, exec counts, its own session RPC) is dropped:
// this is a harness for the DRIVER, and a session hub is runnerd's job.
func (r *kvmRunner) readGuest(sessionID string, g *kvmGuest) {
	defer close(g.dead)
	for {
		// context.Background and not a bounded one: relay.NetConn closes the
		// conn when a read context expires, so a deadline here would be a
		// harness that hangs up on the guest it is observing.
		raw, err := g.conn.Read(context.Background())
		if err != nil {
			return
		}
		f, err := relay.Decode(raw)
		if err != nil || f.Type != relay.FrameControl {
			continue
		}
		var ev relay.ControlEvent
		if json.Unmarshal(f.Payload, &ev) != nil {
			continue
		}
		switch {
		case ev.Kind == "req:"+runner.MethodFetchSessionSecrets:
			r.answerSecrets(sessionID, g, ev)
		case ev.Kind == relay.KindFlushed:
			g.mu.Lock()
			done, ok := g.flushes[ev.ID]
			delete(g.flushes, ev.ID)
			g.mu.Unlock()
			if ok {
				close(done)
			}
		}
	}
}

// answerSecrets plays the control plane's half of the bootstrap exchange.
//
// The token the guest sends is recorded BEFORE the answer, and it is recorded
// whether the answer succeeds or not: what a later assertion wants to know is
// which token this guest was configured with, and that is a fact about the
// request.
func (r *kvmRunner) answerSecrets(sessionID string, g *kvmGuest, ev relay.ControlEvent) {
	var req struct {
		Protocol int    `json:"protocol"`
		Token    string `json:"token"`
	}
	_ = json.Unmarshal(ev.Payload, &req)

	g.mu.Lock()
	first := g.exchangedToken == ""
	g.exchangedToken = req.Token
	g.mu.Unlock()
	if first {
		close(g.exchanged)
	}

	r.mu.Lock()
	want, values := r.tokens[sessionID], map[string]string{}
	for k, v := range r.secrets {
		values[k] = v
	}
	r.mu.Unlock()

	answer := relay.ControlEvent{Kind: "resp", ID: ev.ID}
	switch {
	case req.Protocol != runner.SessionBootstrapProtocolVersion:
		answer.Payload = kvmErrorPayload("this harness speaks session-bootstrap protocol " +
			strconv.Itoa(runner.SessionBootstrapProtocolVersion) + " and the guest speaks " + strconv.Itoa(req.Protocol))
	case want != "" && req.Token != want:
		// The refusal never quotes either token: one of them is live.
		answer.Payload = kvmErrorPayload("this bootstrap token is not the one this session was created with")
	default:
		body, err := json.Marshal(struct {
			Env map[string]string `json:"env"`
		}{values})
		if err != nil {
			answer.Payload = kvmErrorPayload("the secret set could not be encoded")
			break
		}
		answer.OK, answer.Payload = true, body
	}
	_ = g.send(answer)
}

func kvmErrorPayload(msg string) json.RawMessage {
	b, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	return b
}

// send writes one control event onto a guest's conn.
func (g *kvmGuest) send(ev relay.ControlEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	frame, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	if err != nil {
		return err
	}
	return g.conn.Write(context.Background(), frame)
}

// token is what this guest echoed back in its exchange, and whether it has.
func (g *kvmGuest) token() (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.exchangedToken, g.exchangedToken != ""
}

// awaitExchange waits for this guest to have performed its bootstrap
// exchange, which is what proves it READ the configuration rather than merely
// opened the socket.
func (g *kvmGuest) awaitExchange(wait time.Duration) error {
	select {
	case <-g.exchanged:
		return nil
	case <-g.dead:
		return errors.New("the guest's control connection ended before it exchanged its bootstrap token")
	case <-time.After(wait):
		return fmt.Errorf("the guest did not exchange its bootstrap token within %s", wait)
	}
}

// ---------------------------------------------------------------------------
// The contract, against Firecracker
// ---------------------------------------------------------------------------

// TestMicrovmSatisfiesContractOnKVM is ADR-0003 §8 acceptance criterion 1, and
// the only place it can be evidenced: the same RunContract that every driver
// in this package is held to, against a real FirecrackerEngine under the
// jailer, a real netslot.LinuxHost and mkfs.ext4.
//
// Each subtest gets its own driver over its own state directory, which is what
// the contract's newDriver signature is for, and the cleanup destroys whatever
// the subtest left so the next one's startup reclaim finds nothing of ours.
//
// It is slow, and knowing why is the difference between waiting and debugging:
// every Create boots a real guest (seconds), and every Snapshot subtest copies
// and digests a whole environment image. The contract performs four snapshots.
// A base rootfs of a few gigabytes keeps the run inside the 30-minute timeout
// scripts/microvm-kvm-test.sh passes; a twelve-gigabyte one may not.
func TestMicrovmSatisfiesContractOnKVM(t *testing.T) {
	fx := requireKVMHost(t)
	RunContract(t, func(t *testing.T) (Driver, func()) {
		m, _, _ := newKVMMicrovm(t, fx)
		return m, func() { kvmTeardown(t, m) }
	})
}
