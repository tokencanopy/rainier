// internal/driver/microvm_jailer_test.go
package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// fakeStarter is the exec seam: it records the argv a host would be asked to
// run and hands back a process that never was.
type fakeStarter struct {
	mu      sync.Mutex
	calls   [][]string
	names   []string
	err     error
	started []*fakeProcess
}

func (s *fakeStarter) Start(name string, args []string) (vmmProcess, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names = append(s.names, name)
	s.calls = append(s.calls, slices.Clone(args))
	if s.err != nil {
		return nil, s.err
	}
	p := &fakeProcess{}
	s.started = append(s.started, p)
	return p, nil
}

func (s *fakeStarter) lastArgs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return nil
	}
	return s.calls[len(s.calls)-1]
}

type fakeProcess struct {
	mu     sync.Mutex
	killed bool
	waited bool
}

func (p *fakeProcess) Pid() int { return 0 }

func (p *fakeProcess) Wait() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waited = true
	return nil
}

func (p *fakeProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.killed = true
	return nil
}

func (p *fakeProcess) state() (killed, waited bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.killed, p.waited
}

// jailTestEngine builds an engine whose jail is one this test process can
// actually create: the per-VM uid is our own, so the chowns are the
// unprivileged kind, and the starter is fake.
func jailTestEngine(t *testing.T, starter processStarter) (*FirecrackerEngine, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "jail")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	fc := NewFirecrackerEngine(FirecrackerOpts{
		VMMPath:  "/opt/rainier/firecracker",
		StateDir: dir,
		NetnsDir: "/var/run/netns",
		Jail: JailOpts{
			JailerPath: "/opt/rainier/jailer",
			UIDFirst:   os.Getuid(),
			UIDCount:   1,
			RunnerGID:  os.Getgid(),
		},
		Starter: starter,
	})
	// The binaries are not on this machine and neither is /dev/kvm, so the
	// engine would refuse every launch. Every test here is about what the
	// engine WOULD run and what it puts on disk first, so the two host facts
	// are supplied explicitly rather than by arranging for a machine that has
	// them. Nothing in production replaces either.
	fc.initErr = nil
	fc.kvm = func() bool { return true }
	return fc, dir
}

// TestJailerArgvIsExactlyWhatADRRequires reads back the command line a host
// would be asked to run. Every flag here is an ADR-0003 §4.5 control, so the
// argv is the control: a missing --netns is a VM on the host's network, and a
// stray --no-seccomp is a VMM with no syscall filter.
func TestJailerArgvIsExactlyWhatADRRequires(t *testing.T) {
	spec := jailSpec{
		ID:        "mvm-7",
		ExecFile:  "/opt/rainier/firecracker",
		Base:      "/var/lib/rainier/j",
		Root:      "/var/lib/rainier/j/firecracker/mvm-7/root",
		UID:       200003,
		GID:       200003,
		RunnerGID: 998,
		Netns:     "/var/run/netns/rnr-ns-3",
		Cgroup:    "rainier",
		Seccomp:   true,
	}
	want := []string{
		"--id", "mvm-7",
		"--exec-file", "/opt/rainier/firecracker",
		"--uid", "200003",
		"--gid", "200003",
		"--chroot-base-dir", "/var/lib/rainier/j",
		"--cgroup-version", "2",
		"--parent-cgroup", "rainier",
		"--netns", "/var/run/netns/rnr-ns-3",
		"--", "--api-sock", "/run/firecracker.socket",
	}
	if got := jailerArgs(spec); !reflect.DeepEqual(got, want) {
		t.Fatalf("jailer argv =\n  %v\nwant\n  %v", got, want)
	}

	// Seccomp is on by omission. There is no configuration mistake that can
	// leave the filter off; turning it off takes an explicit flag, and then
	// the argv says so.
	spec.Seccomp = false
	got := jailerArgs(spec)
	if !slices.Contains(got, "--no-seccomp") {
		t.Fatalf("a seccomp-off jail did not pass --no-seccomp: %v", got)
	}
	if slices.Index(got, "--no-seccomp") > slices.Index(got, "--") {
		t.Fatalf("--no-seccomp landed after the separator, where it is Firecracker's argument and not the jailer's: %v", got)
	}

	// And neither of the two flags that would put the VMM somewhere this
	// engine can no longer wait on it.
	for _, forbidden := range []string{"--daemonize", "--new-pid-ns"} {
		if slices.Contains(got, forbidden) {
			t.Fatalf("the jailer argv carries %s, which detaches the VMM from the process this engine tracks: %v", forbidden, got)
		}
	}
}

// TestJailerArgvOmitsNetnsWhenThereIsNone: a slotless configuration must not
// produce an empty --netns, which the jailer would read as a path.
func TestJailerArgvOmitsNetnsWhenThereIsNone(t *testing.T) {
	got := jailerArgs(jailSpec{ID: "mvm-1", Seccomp: true})
	if slices.Contains(got, "--netns") {
		t.Fatalf("a jail with no network namespace passed --netns: %v", got)
	}
}

// TestLaunchBuildsTheChrootLayout: everything the VM needs is inside its
// root, because after the chroot there is no outside.
func TestLaunchBuildsTheChrootLayout(t *testing.T) {
	starter := &fakeStarter{}
	fc, stateDir := jailTestEngine(t, starter)

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	workspace := writeJailImage(t, stateDir, "ws.ext4", 0o600)
	home := writeJailImage(t, stateDir, "home.ext4", 0o600)

	cfg := VMMConfig{
		ID:                "mvm-1",
		SessionID:         "sess-jail",
		KernelPath:        kernel,
		RootfsPath:        rootfs,
		WorkspaceDiskPath: workspace,
		HomeDiskPath:      home,
		Netns:             "rnr-ns-1",
		TapDevice:         "rnr-tap-1",
		VsockUDSPath:      vsockGuestPath(1),
	}

	// The launch cannot finish: nothing opens the API socket. What it must
	// have done BEFORE that is the subject here, so the wait is cut short
	// rather than served out.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fc.Launch(ctx, cfg)
	if err == nil {
		t.Fatal("Launch succeeded with no VMM behind it")
	}

	// The argv named this VM's namespace and its own chroot.
	args := starter.lastArgs()
	if !slices.Contains(args, "/var/run/netns/rnr-ns-1") {
		t.Fatalf("the jailer was not told the session's network namespace: %v", args)
	}
	if !slices.Contains(args, jailBaseDir(stateDir)) {
		t.Fatalf("the jailer was not given the chroot base %s: %v", jailBaseDir(stateDir), args)
	}

	// The layout, as it stood before the cleanup. The cleanup removed it, so
	// this is asserted from what the links pointed at: every per-session
	// image still exists on the host, which is the property that matters —
	// removing a jail must never remove a tenant's workspace.
	for _, img := range []string{kernel, rootfs, workspace, home} {
		if _, err := os.Stat(img); err != nil {
			t.Fatalf("the jail teardown removed %s, which is the tenant's file and not the link to it: %v", img, err)
		}
	}
}

// TestPrepareJailPutsEveryImageInsideTheRoot inspects the layout directly,
// without a launch to tear it down again.
func TestPrepareJailPutsEveryImageInsideTheRoot(t *testing.T) {
	fc, stateDir := jailTestEngine(t, &fakeStarter{})

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	workspace := writeJailImage(t, stateDir, "ws.ext4", 0o600)
	cfg := VMMConfig{
		ID:                "mvm-2",
		KernelPath:        kernel,
		RootfsPath:        rootfs,
		WorkspaceDiskPath: workspace,
	}
	spec, err := fc.jailSpecFor(cfg)
	if err != nil {
		t.Fatalf("jailSpecFor: %v", err)
	}
	if err := fc.prepareJail(spec, cfg); err != nil {
		t.Fatalf("prepareJail: %v", err)
	}

	root := jailRootDir(stateDir, "mvm-2")
	for _, rel := range []string{jailKernelPath, jailRootfsPath, jailWorkspacePath, "/run"} {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Errorf("the jail is missing %s: %v", rel, err)
		}
	}
	// No home drive was configured, so none was linked in: a jail must carry
	// exactly what its session was given.
	if _, err := os.Stat(filepath.Join(root, jailHomePath)); err == nil {
		t.Error("the jail carries an agent home image for a session that has none")
	}

	// The links are links: the workspace inside the jail and the workspace on
	// the host are one file, which is what makes a 10 GiB image cost an
	// inode.
	inJail, err := os.Stat(filepath.Join(root, jailWorkspacePath))
	if err != nil {
		t.Fatalf("stat the jailed workspace: %v", err)
	}
	onHost, err := os.Stat(workspace)
	if err != nil {
		t.Fatalf("stat the host workspace: %v", err)
	}
	if !os.SameFile(inJail, onHost) {
		t.Error("the jail holds a COPY of the workspace; a per-boot copy of a 10 GiB image is not a boot")
	}

	// Nothing in the jail is readable by "other". The VM next door is
	// neither this VM's uid nor in the runner's group, and that is the whole
	// of what keeps it out.
	for _, rel := range []string{"", "/run", jailWorkspacePath} {
		info, err := os.Stat(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("stat %s: %v", rel, err)
		}
		if info.Mode().Perm()&0o007 != 0 {
			t.Errorf("%s is mode %#o: the jail is readable by every other VM on this host", rel, info.Mode().Perm())
		}
	}

	// And the teardown takes the whole per-instance directory, not just its
	// root, so a host does not accumulate an empty directory per session it
	// has ever run.
	if err := fc.removeJail("mvm-2"); err != nil {
		t.Fatalf("removeJail: %v", err)
	}
	if _, err := os.Stat(jailInstanceDir(stateDir, "mvm-2")); !os.IsNotExist(err) {
		t.Errorf("the jail directory survived its teardown: %v", err)
	}
	if _, err := os.Stat(workspace); err != nil {
		t.Errorf("removing the jail removed the tenant's workspace: %v", err)
	}
}

// TestPrepareJailRefusesAnUnreadableSharedImage. The kernel and the base
// rootfs are hard-linked into every jail, so their ownership cannot be
// changed for one VM; a jailed VM can only read them through the "other" bit.
// Saying so beats a guest that boots to a kernel it cannot open.
func TestPrepareJailRefusesAnUnreadableSharedImage(t *testing.T) {
	fc, stateDir := jailTestEngine(t, &fakeStarter{})
	kernel := writeJailImage(t, stateDir, "private-vmlinux", 0o600)
	cfg := VMMConfig{ID: "mvm-3", KernelPath: kernel}
	spec, err := fc.jailSpecFor(cfg)
	if err != nil {
		t.Fatalf("jailSpecFor: %v", err)
	}
	err = fc.prepareJail(spec, cfg)
	if err == nil {
		t.Fatal("prepareJail accepted a kernel image the jailed VM could not read")
	}
	if !strings.Contains(err.Error(), "world-readable") {
		t.Fatalf("error = %q, want it to name the fix", err)
	}
}

// TestLaunchCleansUpAfterAFailedStart is the leak: a jail full of hard links
// and a uid nobody else may use, for a VM that never started.
func TestLaunchCleansUpAfterAFailedStart(t *testing.T) {
	starter := &fakeStarter{err: errors.New("jailer: Permission denied")}
	fc, stateDir := jailTestEngine(t, starter)

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	cfg := VMMConfig{ID: "mvm-4", KernelPath: kernel, RootfsPath: rootfs}

	if err := fc.Launch(context.Background(), cfg); err == nil {
		t.Fatal("Launch succeeded with a starter that refuses")
	}
	if _, err := os.Stat(jailInstanceDir(stateDir, "mvm-4")); !os.IsNotExist(err) {
		t.Errorf("a failed start left the jail behind: %v", err)
	}
	// The uid came back: this host has exactly one, and a second launch must
	// be able to have it.
	if _, err := fc.uids.claim("mvm-5"); err != nil {
		t.Errorf("the uid was not released after a failed start: %v", err)
	}
}

// TestLaunchCleansUpAfterAVMMThatNeverAnswers: the process started, the API
// socket never appeared, and everything the launch made has to go — process
// first, because removing a chroot under a live Firecracker is how a VMM ends
// up writing into unlinked files.
func TestLaunchCleansUpAfterAVMMThatNeverAnswers(t *testing.T) {
	starter := &fakeStarter{}
	fc, stateDir := jailTestEngine(t, starter)

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	cfg := VMMConfig{ID: "mvm-6", KernelPath: kernel, RootfsPath: rootfs}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no waiting for a socket that will never open
	if err := fc.Launch(ctx, cfg); err == nil {
		t.Fatal("Launch succeeded with no VMM behind it")
	}

	if len(starter.started) != 1 {
		t.Fatalf("started %d processes, want 1", len(starter.started))
	}
	killed, waited := starter.started[0].state()
	if !killed {
		t.Error("the VMM that never answered was left running")
	}
	if !waited {
		t.Error("the VMM that never answered was never reaped; one zombie per failed boot")
	}
	if _, err := os.Stat(jailInstanceDir(stateDir, "mvm-6")); !os.IsNotExist(err) {
		t.Errorf("a failed boot left the jail behind: %v", err)
	}
	fc.mu.Lock()
	tracked := len(fc.procs)
	fc.mu.Unlock()
	if tracked != 0 {
		t.Errorf("a failed boot left %d process(es) in the engine's map", tracked)
	}
}

// TestUIDAllocatorGivesEachVMItsOwn. Two sessions sharing a uid share every
// file-permission decision the kernel makes about them.
func TestUIDAllocatorGivesEachVMItsOwn(t *testing.T) {
	a, err := newUIDAllocator(200000, 3)
	if err != nil {
		t.Fatalf("newUIDAllocator: %v", err)
	}
	one, err := a.claim("mvm-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	two, err := a.claim("mvm-2")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if one == two {
		t.Fatalf("two VMs were given uid %d", one)
	}
	// Idempotent: a relaunch of the same instance keeps the ownership its
	// jail already carries.
	if again, _ := a.claim("mvm-1"); again != one {
		t.Fatalf("a second claim for mvm-1 returned %d, want the held %d", again, one)
	}
	if _, err := a.claim("mvm-3"); err != nil {
		t.Fatalf("claim of the last uid: %v", err)
	}
	if _, err := a.claim("mvm-4"); err == nil {
		t.Fatal("the allocator handed out a fourth uid from a range of three")
	}
	a.release("mvm-2")
	if recycled, err := a.claim("mvm-4"); err != nil || recycled != two {
		t.Fatalf("after a release the next claim was (%d, %v), want the recycled %d", recycled, err, two)
	}
}

// TestUIDAllocatorRefusesTheSystemRange. A jailed VM must never run as a real
// account on this host.
func TestUIDAllocatorRefusesTheSystemRange(t *testing.T) {
	if _, err := newUIDAllocator(33, 10); err == nil {
		t.Fatal("the allocator accepted a range inside the system uids")
	}
}

// TestEngineRefusesWithoutAJailer is ADR-0003 §4.5 as a startup condition:
// there is no unjailed launch path and no flag that produces one.
func TestEngineRefusesWithoutAJailer(t *testing.T) {
	fc := NewFirecrackerEngine(FirecrackerOpts{
		VMMPath:  "/bin/sh", // present, so the refusal can only be about the jailer
		StateDir: t.TempDir(),
		Jail:     JailOpts{JailerPath: "/nonexistent/bin/jailer"},
	})
	err := fc.Launch(context.Background(), VMMConfig{ID: "mvm-1"})
	if err == nil {
		t.Fatal("an engine with no jailer launched a VM")
	}
	if !strings.Contains(err.Error(), "jailer") {
		t.Fatalf("error = %q, want it to name the jailer", err)
	}
}

// TestKillProcessTreeOnlySignalsAGroupItLeads. A pid recovered from a pid
// file after a runnerd restart belongs to a group this process knows nothing
// about, and signalling that group could reach anything on the host.
func TestKillProcessTreeOnlySignalsAGroupItLeads(t *testing.T) {
	// This process is not a process-group leader under `go test` (the test
	// binary is started by the go tool), so a group signal from here would
	// reach the tool. killProcessTree must therefore signal this pid alone —
	// and signal 0 is the observation, since it changes nothing.
	if err := killProcessTree(os.Getpid(), syscall.Signal(0)); err != nil {
		t.Fatalf("killProcessTree with signal 0: %v", err)
	}
	// A pid that is not there is not an error: teardown is retried, and a
	// retry has to be able to finish.
	if err := killProcessTree(99999999, syscall.SIGTERM); err != nil {
		t.Fatalf("killProcessTree of an absent pid: %v", err)
	}
	if err := killProcessTree(0, syscall.SIGKILL); err != nil {
		t.Fatalf("killProcessTree of pid 0 must be a no-op, not a signal to this process group: %v", err)
	}
}

// TestVsockPathsLiveInsideTheJail: a chrooted Firecracker can neither create
// a socket outside its root nor connect to one, so both ends of the control
// channel have to be in there.
func TestVsockPathsLiveInsideTheJail(t *testing.T) {
	m, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2})
	hostUDS, listenPath, err := m.vsockPaths("mvm-1", 3)
	if err != nil {
		t.Fatalf("vsockPaths: %v", err)
	}
	root := jailRootDir(m.opts.StateDir, "mvm-1")
	if !strings.HasPrefix(hostUDS, root+"/") || !strings.HasPrefix(listenPath, root+"/") {
		t.Fatalf("the control channel is outside the jail:\n  %s\n  %s\nroot %s", hostUDS, listenPath, root)
	}
	if got := vsockGuestPath(3); got != "/v3.sock" {
		t.Fatalf("the path handed to the VMM is %q, want the in-chroot /v3.sock", got)
	}
	if filepath.Base(hostUDS) != "v3.sock" {
		t.Fatalf("the host path %q does not name the same file as %q", hostUDS, vsockGuestPath(3))
	}
}

// writeJailImage puts a file where an image would be, with a given mode.
func writeJailImage(t *testing.T, dir, name string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("not a real image"), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	// WriteFile applies the process umask, which is not what this test is
	// asking for.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	return path
}
