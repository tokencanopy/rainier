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
//	                - mounts /proc, /sys and /dev, and a tmpfs on /tmp and
//	                  /run. The ROOT is writable — it is this session's own
//	                  copy-on-write copy of this image (ADR-0003 §2.7 item 3),
//	                  not the image itself — so /init needs no overlay above it;
//	                - formats-if-empty and mounts /dev/vdb on /workspace, and
//	                  /dev/vdc on /rainier/agents when that device is present;
//	                - brings lo and eth0 up;
//	                - execs `sessiond --transport=vsock` (or with
//	                  RAINIER_TRANSPORT=vsock in its environment).
//
//	              The drive letters follow the order the driver PUTs them:
//	              rootfs is /dev/vda, the workspace /dev/vdb, the agent home
//	              /dev/vdc. The boot smoke below creates a session with no
//	              agent home, so /dev/vdc is absent for it and /init must
//	              tolerate that.
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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver/netslot"
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
	// suspends is the cold-suspend handshakes in flight on this guest, keyed by
	// nonce — which is also the id of the workspace stream that answers each
	// one. At most one is ever live; the map is what keeps a late answer from
	// a suspend that gave up out of the next one's, exactly as the runner's own
	// nonce matching does.
	suspends map[uint64]*kvmSuspend
	dead     chan struct{}
}

// kvmSuspend is one cold suspend's three answers on this harness: the
// acknowledgement, the workspace stream, and the end marker. The runner's own
// version of this is internal/runnerd/workspacestream.go; this one is the same
// shape with the budgets the harness already uses.
type kvmSuspend struct {
	dst   io.Writer
	n     int64
	werr  error
	ack   chan struct{}
	end   chan relay.ControlEvent
	ready chan struct{}
	once  sync.Once
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
		suspends:  map[uint64]*kvmSuspend{},
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
		if err != nil {
			continue
		}
		if f.Type == relay.FrameStream {
			// One chunk of a workspace. Written straight through, the way the
			// runner writes it into the checkpoint writer's pipe: a harness
			// that buffered the tree would be measuring something production
			// does not do.
			g.mu.Lock()
			s := g.suspends[f.AttachID]
			g.mu.Unlock()
			if s != nil {
				n, werr := s.dst.Write(f.Payload)
				g.mu.Lock()
				s.n += int64(n)
				if werr != nil && s.werr == nil {
					s.werr = werr
				}
				g.mu.Unlock()
			}
			continue
		}
		if f.Type != relay.FrameControl {
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
		case ev.Kind == relay.KindSuspendAck, ev.Kind == relay.KindWorkspaceEnd, ev.Kind == relay.KindSuspendReady:
			g.mu.Lock()
			s := g.suspends[ev.ID]
			g.mu.Unlock()
			if s == nil {
				continue
			}
			switch ev.Kind {
			case relay.KindSuspendAck:
				s.once.Do(func() { close(s.ack) })
			case relay.KindWorkspaceEnd:
				select {
				case s.end <- ev:
				default:
				}
			case relay.KindSuspendReady:
				close(s.ready)
			}
		}
	}
}

// StreamWorkspace is the harness's cold-suspend handshake: tell the guest, read
// the workspace it streams back into dst, and return when its end marker says
// the tree is complete.
//
// It is the same shape as (*runnerd.Server).StreamWorkspace, with this
// harness's single patience budget in place of the runner's three. It exists so
// that a run on a real KVM host exercises the whole barrier — a real guest, a
// real workspace, a real ext4 underneath it — which is the one thing no fake
// on a developer machine can evidence.
func (r *kvmRunner) StreamWorkspace(ctx context.Context, sessionID string, dst io.Writer) (WorkspaceStream, error) {
	var none WorkspaceStream
	g, err := r.awaitGuest(ctx, sessionID, r.connectWait)
	if err != nil {
		return none, fmt.Errorf("session %s has no sandbox connection to stream its workspace over: %w", sessionID, err)
	}

	r.mu.Lock()
	r.nonce++
	nonce := r.nonce
	r.mu.Unlock()

	s := &kvmSuspend{
		dst:   dst,
		ack:   make(chan struct{}),
		end:   make(chan relay.ControlEvent, 1),
		ready: make(chan struct{}),
	}
	g.mu.Lock()
	g.suspends[nonce] = s
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.suspends, nonce)
		g.mu.Unlock()
	}()

	if err := g.send(relay.ControlEvent{Kind: relay.KindSuspending, ID: nonce, Cold: true}); err != nil {
		return none, fmt.Errorf("asking session %s to cold suspend: %w", sessionID, err)
	}

	timer := time.NewTimer(r.connectWait)
	defer timer.Stop()
	var end relay.ControlEvent
	select {
	case end = <-s.end:
	case <-g.dead:
		return none, fmt.Errorf("session %s lost its sandbox connection part way through its workspace stream", sessionID)
	case <-ctx.Done():
		return none, fmt.Errorf("streaming session %s's workspace: %w", sessionID, ctx.Err())
	case <-timer.C:
		return none, fmt.Errorf("session %s did not finish streaming its workspace within %s", sessionID, r.connectWait)
	}

	g.mu.Lock()
	received, werr := s.n, s.werr
	g.mu.Unlock()
	switch {
	case werr != nil:
		return none, fmt.Errorf("session %s: the workspace stream could not be taken: %w", sessionID, werr)
	case !end.OK:
		return none, fmt.Errorf("session %s could not stream its workspace (stage %s, %d entries and %d bytes in): %s",
			sessionID, end.Stage, end.Entries, end.Bytes, end.Tail)
	case end.Bytes != received:
		return none, fmt.Errorf("session %s streamed %d bytes of workspace and %d arrived", sessionID, end.Bytes, received)
	}

	ready := time.NewTimer(r.connectWait)
	defer ready.Stop()
	select {
	case <-s.ready:
	case <-g.dead:
	case <-ready.C:
	}
	return WorkspaceStream{Entries: end.Entries, Bytes: received, Nonce: nonce}, nil
}

// CheckpointCommitted is the harness's half of the "your work is durable"
// event. The guest logs it and answers nothing, so there is nothing for this to
// wait on.
func (r *kvmRunner) CheckpointCommitted(sessionID string, nonce uint64) {
	r.mu.Lock()
	g := r.guests[sessionID]
	r.mu.Unlock()
	if g == nil {
		return
	}
	_ = g.send(relay.ControlEvent{Kind: relay.KindCheckpointCommitted, ID: nonce})
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
// Host observations
// ---------------------------------------------------------------------------

// kvmLiveRecord copies what an assertion needs out of a live instance record,
// under the driver mutex. The harness is in this package precisely so it can
// read the driver's own view of a session rather than re-derive it: a check
// written against re-derived names would pass on a driver that had stopped
// recording where it put things.
func kvmLiveRecord(t *testing.T, m *Microvm, id string) (VMMConfig, *netslot.Slot) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[id]
	if !ok {
		t.Fatalf("the driver has no live record of %s", id)
	}
	if inst.slot == nil {
		t.Fatalf("%s holds no network slot, so it has neither a namespace nor a uid of its own (ADR-0003 §4.5)", id)
	}
	return inst.Cfg, inst.slot
}

// kvmProcUID is the real uid of a running process, from /proc.
func kvmProcUID(t *testing.T, pid int) int {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatalf("read /proc/%d/status: %v", pid, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		rest, ok := strings.CutPrefix(line, "Uid:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			break
		}
		uid, err := strconv.Atoi(fields[0])
		if err != nil {
			t.Fatalf("parse Uid %q from /proc/%d/status: %v", fields[0], pid, err)
		}
		return uid
	}
	t.Fatalf("/proc/%d/status has no Uid line", pid)
	return -1
}

// kvmNetnsLink is the "net:[<inode>]" a process's network namespace reads as,
// and the same string for a NAMED namespace on this host. Comparing the two is
// what turns "the jailer was passed --netns" into "the VMM is in it".
func kvmNetnsLink(t *testing.T, pid int) string {
	t.Helper()
	link, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil {
		t.Fatalf("readlink /proc/%d/ns/net: %v", pid, err)
	}
	return link
}

func kvmNamedNetnsLink(t *testing.T, name string) string {
	t.Helper()
	fi, err := os.Stat(filepath.Join(netslot.DefaultNetnsDir, name))
	if err != nil {
		t.Fatalf("stat the named network namespace %s: %v", name, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: no inode available", name)
	}
	return fmt.Sprintf("net:[%d]", st.Ino)
}

// kvmInode identifies a file by identity rather than by path, which is what
// "the workspace survived" and "the rootfs is a fresh clone" both need: the
// two live at fixed paths, so a path check cannot tell a kept file from a
// recreated one.
func kvmInode(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: no inode available", path)
	}
	return st.Ino
}

func kvmAbsent(t *testing.T, what, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s survived the teardown at %s (stat err = %v)", what, path, err)
	}
}

// kvmScanHostFiles reads every file the DRIVER wrote under the state directory
// and reports the first one containing needle.
//
// Three directories are deliberately skipped, and the reason is the same for
// all three: they are the guest's own block devices, not this host's writing.
// A workspace disk, an agent home, and a session's root filesystem are a
// tenant's filesystem — what a guest chooses to put in its own files is the
// guest's business, and scanning them would be asserting something about
// sessiond rather than about the driver. What ADR-0003 §2.2 and §2.7 item 1
// forbid is the HOST writing a session's configuration down, and that is
// everything else under here: the instance records, the jail, the image store
// and its manifests.
func kvmScanHostFiles(t *testing.T, stateDir, needle, what string) {
	t.Helper()
	if needle == "" {
		t.Fatal("kvmScanHostFiles: an empty needle matches everything")
	}
	skip := map[string]bool{"workspaces": true, "homes": true, "rootfs": true}
	err := filepath.WalkDir(stateDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if rel, rerr := filepath.Rel(stateDir, path); rerr == nil && skip[rel] {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		if strings.Contains(string(data), needle) {
			t.Fatalf("ADR-0003 §2.7 item 1: %s was written to this host's disk, at %s", what, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", stateDir, err)
	}
}

// kvmNoMemoryImage is ADR-0003 §2.2 and §8 item 4: a cold suspension writes no
// guest memory image anywhere, because an authenticated session's RAM is a
// secret-bearing artefact.
//
// The names are the ones Firecracker's own snapshot API produces, which is the
// only way such a file could appear at all: this driver's MicrovmEngine has no
// Snapshot method, so there is nothing that could ask for one. That is what
// makes this check cheap AND worth keeping — it is a regression test for an
// interface, phrased as a fact about the host.
func kvmNoMemoryImage(t *testing.T, stateDir string) {
	t.Helper()
	suspicious := []string{"memfile", "mem_file", "mem.snapshot", "snapshot.json", "vmstate"}
	err := filepath.WalkDir(stateDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := strings.ToLower(d.Name())
		for _, bad := range suspicious {
			if strings.Contains(name, bad) {
				t.Fatalf("ADR-0003 §2.2: a cold suspension left what looks like a guest memory image at %s", path)
			}
		}
		if strings.HasSuffix(name, ".snap") {
			t.Fatalf("ADR-0003 §2.2: a cold suspension left what looks like a guest memory image at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", stateDir, err)
	}
}

// ---------------------------------------------------------------------------
// The boot smoke
// ---------------------------------------------------------------------------

// kvmTimings is what this harness measures, for the Phase A runbook's evidence
// table. It is written to the file RAINIER_MICROVM_TEST_TIMINGS names, and
// carries nothing but durations and the shape they were taken on: no session
// id, no path, no token, nothing correlated (Bakeoff §12).
type kvmTimings struct {
	Schema              string `json:"schema"`
	BootToConnectedMS   int64  `json:"boot_to_connected_ms"`
	ColdSuspendMS       int64  `json:"cold_suspend_ms"`
	ResumeToConnectedMS int64  `json:"resume_to_connected_ms"`
	DestroyMS           int64  `json:"destroy_ms"`
	CloneMethod         string `json:"clone_method"`
	VCPU                int    `json:"vcpu"`
	MemoryMiB           int    `json:"memory_mib"`
	Samples             int    `json:"samples"`
}

const kvmTimingsSchema = "rainier.microvm.kvm-test/v1"

// writeKVMTimings records the run's measurements where the operator's script
// can pick them up, and logs them either way.
//
// One sample each, and the field says so: five samples is not a p95 and one is
// not a p50. The runbook's §7 table is the place that decides what a single
// number may be written as, and it already says "anything measured once, say
// so".
func writeKVMTimings(t *testing.T, tm kvmTimings) {
	t.Helper()
	tm.Schema = kvmTimingsSchema
	tm.Samples = 1
	blob, err := json.MarshalIndent(tm, "", "  ")
	if err != nil {
		t.Fatalf("encode timings: %v", err)
	}
	t.Logf("MICROVM_KVM_TIMINGS %s", strings.ReplaceAll(string(blob), "\n", " "))
	path := os.Getenv(kvmTimingsEnv)
	if path == "" {
		return
	}
	if err := os.WriteFile(path, append(blob, '\n'), 0o644); err != nil {
		t.Fatalf("write %s=%q: %v", kvmTimingsEnv, path, err)
	}
}

// TestMicrovmBootSmokeOnKVM is one whole session lifecycle on real hardware,
// and it runs BEFORE the contract suite below it (source order is run order
// within a file) for a practical reason: if a guest cannot boot and dial on
// this host, that is one failure with one message, rather than fourteen
// contract subtests each failing somewhere in the middle.
//
// Every assertion names the ADR section it evidences. The sections are not
// decoration: this is the only test in the repository that can evidence any of
// them, and a failure here is a Phase A finding rather than a broken unit.
func TestMicrovmBootSmokeOnKVM(t *testing.T) {
	fx := requireKVMHost(t)
	m, above, clone := newKVMMicrovm(t, fx)
	defer kvmTeardown(t, m)
	ctx := context.Background()
	stateDir := m.opts.StateDir

	const (
		sessionID  = "kvmsmoke1"
		secretName = "RAINIER_KVM_SMOKE_SECRET"
		// Synthetic, and it never leaves this process except down one
		// session's vsock conn — which is the property being asserted.
		secretValue = "synthetic-value-not-a-credential"
	)
	above.SetSecret(secretName, secretValue)

	// The control plane's half of the create: the token it mints and the
	// names it declares, with the VALUES withheld (ADR-0003 §2.7 item 1).
	token, err := above.MintSessionBootstrap(ctx, sessionID)
	if err != nil {
		t.Fatalf("mint a bootstrap token: %v", err)
	}

	start := time.Now()
	h, err := m.Create(ctx, Spec{
		Name:           "kvm-smoke",
		SessionID:      sessionID,
		BootstrapToken: token,
		SecretNames:    []string{secretName},
	})
	if err != nil {
		t.Fatalf("Create on a real KVM host: %v", err)
	}

	// --- ADR-0003 §4.1 (Create row) and §2.7 item 2: the guest boots and
	// dials (2, 1024), and the driver hands it its whole configuration as the
	// first frame on that conn.
	guest, err := above.awaitGuest(ctx, sessionID, fx.connectWait)
	if err != nil {
		t.Fatalf("ADR-0003 §4.1: no guest connected over vsock within %s: %v\n\n"+
			"This is the base image under test, not the driver. The rootfs at %s must carry an /init that mounts the\n"+
			"pseudo-filesystems, mounts /dev/vdb on /workspace, brings the link up, and execs `sessiond --transport=vsock`\n"+
			"(or sets RAINIER_TRANSPORT=vsock). A sessiond started with no flags takes the WebSocket path and never dials\n"+
			"vsock at all. The guest kernel must have CONFIG_VIRTIO_VSOCKETS and expose /dev/vsock.\n\n"+
			"There is nowhere to read the guest's console from, and that is a Phase A finding in its own right: the boot\n"+
			"args carry console=ttyS0, this engine configures no Firecracker logger, and it starts the VMM with no Stdout,\n"+
			"so the serial console goes to /dev/null. Reproducing a boot failure today means running the jailer by hand\n"+
			"against the jail this create left at %s.",
			fx.connectWait, err, fx.rootfs, jailInstanceDir(stateDir, h.ID))
	}
	bootToConnected := guest.at.Sub(start)
	t.Logf("boot to connected: %s (%d vCPU, %d MiB, rootfs clone by %s)", bootToConnected.Round(time.Millisecond), defaultMicrovmVCPU, defaultMicrovmMemoryMiB, clone)

	// The guest received boot_config, proven by the one thing it could not
	// have invented: it echoed the exact bootstrap token back in its secrets
	// exchange. The token is in no other channel and on no disk.
	if err := guest.awaitExchange(fx.connectWait); err != nil {
		t.Fatalf("ADR-0003 §2.7 item 2: %v. The guest opened the control channel but never acted on its boot configuration", err)
	}
	if got, _ := guest.token(); got != token {
		t.Fatalf("ADR-0003 §2.7 item 2: the guest exchanged a bootstrap token that is not the one this create carried")
	}

	cfg, slot := kvmLiveRecord(t, m, h.ID)
	pid := m.engine.PID(h.ID)
	if pid <= 0 {
		t.Fatalf("the driver has no pid for %s, so nothing about the VMM process can be evidenced", h.ID)
	}

	// --- ADR-0003 §4.3: the per-slot firewall is applied, read back from the
	// host rather than from the renderer that produced it.
	rules, err := m.slots.Firewall(ctx, slot)
	if err != nil {
		t.Fatalf("ADR-0003 §4.3: slot %d's firewall could not be read back from the host: %v", slot.Index, err)
	}
	for _, want := range []string{
		// This chain has an opinion about exactly one interface: this
		// session's TAP.
		`iifname != "` + slot.Tap + `"`,
		// The named metadata control (Hosted Tenancy §10.2 and §16).
		"169.254.169.254",
		// IPv6 from the guest, dropped before anything is accepted.
		"meta nfproto ipv6 drop",
		// And the deny set, which contains 10.0.0.0/8 and therefore every
		// neighbouring slot's /30 on this host — the harness's guest and
		// uplink ranges are both inside it, so the set dedupes to the
		// broader range and that is what a reader sees.
		"10.0.0.0/8",
	} {
		if !strings.Contains(rules, want) {
			t.Fatalf("ADR-0003 §4.3: the ruleset slot %d is actually behind does not contain %q:\n%s", slot.Index, want, rules)
		}
	}

	// --- ADR-0003 §4.5: the VM runs under its own uid, in its own network
	// namespace, in its own cgroup.
	uids, err := newUIDRange(0, 0)
	if err != nil {
		t.Fatalf("uid range: %v", err)
	}
	wantUID, err := uids.forSlot(slot.Index)
	if err != nil {
		t.Fatalf("uid for slot %d: %v", slot.Index, err)
	}
	if got := kvmProcUID(t, pid); got != wantUID {
		t.Fatalf("ADR-0003 §4.5: the VMM runs as uid %d, want %d (the slot index above the per-VM range). "+
			"A VMM sharing a uid with another session shares every file-permission decision the kernel makes about them", got, wantUID)
	}
	if got, want := kvmNetnsLink(t, pid), kvmNamedNetnsLink(t, slot.Netns); got != want {
		t.Fatalf("ADR-0003 §4.5: the VMM's network namespace is %s, want %s (%s)", got, want, slot.Netns)
	}
	if kvmNetnsLink(t, pid) == kvmNetnsLink(t, os.Getpid()) {
		t.Fatalf("ADR-0003 §4.5: the VMM is in this process's own network namespace, not a slot's")
	}
	if _, err := os.Stat(cfg.CgroupPath); err != nil {
		t.Fatalf("ADR-0003 §4.5 and §4.6: the VM's cgroup %s is not there, so nothing meters it: %v", cfg.CgroupPath, err)
	}
	procCgroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		t.Fatalf("read /proc/%d/cgroup: %v", pid, err)
	}
	if want := "/" + kvmCgroupChild + "/" + h.ID; !strings.Contains(string(procCgroup), want) {
		t.Fatalf("ADR-0003 §4.6: the VMM is not in its own cgroup (%q is not in %q)", want, strings.TrimSpace(string(procCgroup)))
	}

	workspaceInode := kvmInode(t, cfg.WorkspaceDiskPath)
	rootfsInode := kvmInode(t, cfg.RootfsPath)
	jailDir := jailInstanceDir(stateDir, h.ID)
	if _, err := os.Stat(jailDir); err != nil {
		t.Fatalf("ADR-0003 §4.5: the VM's jail directory %s is not there: %v", jailDir, err)
	}

	// --- ADR-0003 §2.2: cold suspension terminates the VM and writes no
	// memory image and no secret to this host's disk.
	above.forget(sessionID)
	coldStart := time.Now()
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	coldSuspend := time.Since(coldStart)
	t.Logf("cold suspend: %s", coldSuspend.Round(time.Millisecond))

	kvmNoMemoryImage(t, stateDir)
	kvmScanHostFiles(t, stateDir, token, "this session's bootstrap token")
	kvmScanHostFiles(t, stateDir, secretValue, "an environment secret value")
	// The rootfs copy goes with the VM (§4.1's Suspend row); the workspace
	// stays (§2.3).
	kvmAbsent(t, "the cold-parked session's root filesystem", cfg.RootfsPath)
	if _, err := os.Stat(cfg.WorkspaceDiskPath); err != nil {
		t.Fatalf("ADR-0003 §2.3: a cold suspension took the workspace disk with it: %v", err)
	}

	// --- ADR-0003 §2.6: a resume is a FRESH BOOT with preserved files.
	resumeStart := time.Now()
	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatalf("cold resume: %v", err)
	}
	if !restarted {
		t.Fatal("ADR-0003 §2.6: a cold resume reported no restart; a resumed session is a fresh boot and runnerd decides whether the agent is a new process from this bit")
	}
	resumed, err := above.awaitGuest(ctx, sessionID, fx.connectWait)
	if err != nil {
		t.Fatalf("ADR-0003 §2.6: the resumed session's guest did not connect within %s: %v", fx.connectWait, err)
	}
	resumeToConnected := resumed.at.Sub(resumeStart)
	t.Logf("resume to connected: %s", resumeToConnected.Round(time.Millisecond))

	if err := resumed.awaitExchange(fx.connectWait); err != nil {
		t.Fatalf("ADR-0003 §2.6: %v", err)
	}
	resumedToken, _ := resumed.token()
	if resumedToken == token {
		t.Fatal("ADR-0003 §2.7 item 1: the resumed guest was handed the parked session's token; a bootstrap token is single-use and a new VM gets a new one")
	}
	if got := above.mintCount(sessionID); got != 2 {
		t.Fatalf("the runner minted %d bootstrap token(s) for this session, want 2 (one per boot)", got)
	}

	cfg2, slot2 := kvmLiveRecord(t, m, h.ID)
	if got := kvmInode(t, cfg2.WorkspaceDiskPath); got != workspaceInode {
		t.Fatalf("ADR-0003 §2.6: the workspace disk is a different file after the resume (inode %d, was %d); the session came back to a blank workspace", got, workspaceInode)
	}
	if got := kvmInode(t, cfg2.RootfsPath); got == rootfsInode {
		t.Fatalf("ADR-0003 §4.1 (Suspend row): the resumed session reused the parked boot's root filesystem (inode %d); a cold resume clones a fresh one from the environment image", got)
	}
	if got, _ := m.Inspect(ctx, h.ID); got.State != StateRunning {
		t.Fatalf("after a cold resume the session is %s, want %s", got.State, StateRunning)
	}

	// --- Teardown: every resource this session held is gone.
	destroyStart := time.Now()
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	destroy := time.Since(destroyStart)

	if got, _ := m.Inspect(ctx, h.ID); got.State != StateGone {
		t.Fatalf("after Destroy the session is %s, want %s", got.State, StateGone)
	}
	kvmAbsent(t, "the session's root filesystem clone", cfg2.RootfsPath)
	kvmAbsent(t, "the session's workspace disk", cfg2.WorkspaceDiskPath)
	kvmAbsent(t, "the VM's jail directory", jailInstanceDir(stateDir, h.ID))
	kvmAbsent(t, "the VM's cgroup", cfg2.CgroupPath)
	// The TAP lives inside the slot's namespace, so the namespace going is
	// what takes it; the host end of the uplink veth is the half this
	// namespace can still be seen from.
	kvmAbsent(t, "the slot's uplink veth", filepath.Join("/sys/class/net", slot2.VethHost))

	observer, err := netslot.NewLinuxHost("")
	if err != nil {
		t.Fatalf("a host to observe namespaces with: %v", err)
	}
	live, err := observer.ListNetns(ctx)
	if err != nil {
		t.Fatalf("list network namespaces: %v", err)
	}
	for _, ns := range []string{slot.Netns, slot2.Netns} {
		if slices.Contains(live, ns) {
			t.Fatalf("the network namespace %s survived the teardown; its slot is off the pool for good", ns)
		}
	}
	// Either sentinel is the answer here: the namespace is asserted gone just
	// above, and a read in a namespace that no longer exists reports
	// ErrNoNetns rather than ErrNoNftTable. What this asserts is that no
	// ruleset can still be read back for the slot — anything else, including a
	// table found in a namespace that outlived the teardown, is the failure.
	if _, err := m.slots.Firewall(ctx, slot2); !errors.Is(err, netslot.ErrNoNftTable) && !errors.Is(err, netslot.ErrNoNetns) {
		t.Fatalf("ADR-0003 §4.3: slot %d's firewall table survived its release (err = %v)", slot2.Index, err)
	}
	if used, _, _ := m.Capacity(ctx); used != 0 {
		t.Fatalf("after the teardown this host reports %d slot(s) in use, want 0", used)
	}

	writeKVMTimings(t, kvmTimings{
		BootToConnectedMS:   bootToConnected.Milliseconds(),
		ColdSuspendMS:       coldSuspend.Milliseconds(),
		ResumeToConnectedMS: resumeToConnected.Milliseconds(),
		DestroyMS:           destroy.Milliseconds(),
		CloneMethod:         string(clone),
		VCPU:                defaultMicrovmVCPU,
		MemoryMiB:           defaultMicrovmMemoryMiB,
	})
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
