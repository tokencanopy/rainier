// internal/driver/microvm_jailer_test.go
package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
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
	nextPid int
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
	// A pid the engine can write down. It is deliberately not this process's
	// and not a live one: what turns on it here is the pid FILE, which
	// outlives the jail because it is not in it.
	if s.nextPid == 0 {
		s.nextPid = 4_000_000
	}
	s.nextPid++
	p := &fakeProcess{pid: s.nextPid}
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
	pid    int
	killed bool
	waited bool
}

func (p *fakeProcess) Pid() int { return p.pid }

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

// jailFakeFS is the jail's two filesystem seams: giving a file to another uid
// (which needs CAP_CHOWN) and landing an image on another filesystem (which
// needs two of them). Neither is something a test process on a developer
// machine can do or arrange, and both are what the jail's isolation rests on,
// so they are recorded rather than skipped.
type jailFakeFS struct {
	mu    sync.Mutex
	owner map[string][2]int // path -> {uid, gid}
	exdev bool
}

func newJailFakeFS() *jailFakeFS {
	return &jailFakeFS{owner: make(map[string][2]int)}
}

// chown records the ownership the host would have been given, after checking
// that the path is really there — a chown of a file the jail does not have is
// a bug this fake must not hide.
func (f *jailFakeFS) chown(path string, uid, gid int) error {
	if _, err := os.Lstat(path); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.owner[path] = [2]int{uid, gid}
	return nil
}

// link is os.Link, or EXDEV when the test is asking for the copy fallback: a
// base image on a different filesystem from the state directory.
func (f *jailFakeFS) link(oldname, newname string) error {
	f.mu.Lock()
	exdev := f.exdev
	f.mu.Unlock()
	if exdev {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: syscall.EXDEV}
	}
	return os.Link(oldname, newname)
}

func (f *jailFakeFS) acrossFilesystems() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exdev = true
}

// ownerOf is the (uid, gid) this path was given, and whether it was given one
// at all.
func (f *jailFakeFS) ownerOf(path string) ([2]int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.owner[path]
	return o, ok
}

// jailTestEngine builds an engine over a fresh state directory.
//
// The uid range is a REAL one — well above the system range, and nothing to
// do with whoever is running the tests — because the uid is what keeps two
// tenants apart, and a fixture that quietly used this process's own uid would
// be exercising a jail nobody ships. The chown such a range needs is the
// injected seam, which is also how these tests can see who a jail was given
// to.
func jailTestEngine(t *testing.T, starter processStarter) (*FirecrackerEngine, string, *jailFakeFS) {
	t.Helper()
	dir, err := os.MkdirTemp("", "jail")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	fs := newJailFakeFS()
	return jailTestEngineIn(t, dir, fs, starter), dir, fs
}

// jailTestEngineIn is jailTestEngine over an EXISTING state directory and an
// existing filesystem fake, which is what a restarted runnerd is: a new
// engine with no memory of anything, the same host underneath it.
//
// Note what it does not do: it never clears initErr. An engine that refuses
// to construct has to refuse in a test too, or the tests are exercising a
// half-built engine no host would produce — which is why the binaries named
// here are ones that exist rather than the paths a real deployment uses.
func jailTestEngineIn(t *testing.T, dir string, fs *jailFakeFS, starter processStarter) *FirecrackerEngine {
	t.Helper()
	fc := NewFirecrackerEngine(FirecrackerOpts{
		VMMPath:  "/bin/sh",
		StateDir: dir,
		NetnsDir: "/var/run/netns",
		Jail: JailOpts{
			JailerPath: "/bin/sh",
			UIDFirst:   200000,
			UIDCount:   4096,
			RunnerGID:  os.Getgid(),
		},
		Starter: starter,
		Chown:   fs.chown,
		Link:    fs.link,
	})
	if fc.initErr != nil {
		t.Fatalf("the test engine refused to construct: %v", fc.initErr)
	}
	// /dev/kvm is the one host fact left that no seam covers and no developer
	// machine has. Every test here is about what the engine WOULD run and
	// what it puts on disk first; production never replaces this.
	fc.kvm = func() bool { return true }
	return fc
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
	fc, stateDir, _ := jailTestEngine(t, starter)

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
		SlotIndex:         1,
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

// TestLaunchRefusesAVMWithNoNetworkSlot. The per-VM uid is derived from the
// slot index (see uidRange), so a VM with no slot has neither a namespace of
// its own nor a uid of its own — and two of them would share every
// file-permission decision the kernel makes about either.
func TestLaunchRefusesAVMWithNoNetworkSlot(t *testing.T) {
	fc, stateDir, _ := jailTestEngine(t, &fakeStarter{})
	cfg := VMMConfig{
		ID:         "mvm-1",
		KernelPath: writeJailImage(t, stateDir, "vmlinux.bin", 0o644),
		RootfsPath: writeJailImage(t, stateDir, "base.ext4", 0o644),
	}
	err := fc.Launch(context.Background(), cfg)
	if err == nil {
		t.Fatal("a VM with no network slot was launched, and would have shared a uid with the next one")
	}
	if !strings.Contains(err.Error(), "network slot") {
		t.Fatalf("error = %q, want it to name the missing slot", err)
	}
}

// TestPrepareJailPutsEveryImageInsideTheRoot inspects the layout directly,
// without a launch to tear it down again.
func TestPrepareJailPutsEveryImageInsideTheRoot(t *testing.T) {
	fc, stateDir, fs := jailTestEngine(t, &fakeStarter{})

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	workspace := writeJailImage(t, stateDir, "ws.ext4", 0o600)
	cfg := VMMConfig{
		ID:                "mvm-2",
		SlotIndex:         2,
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

	// Everything the VM has to be able to open was given to the VM's own uid,
	// with the runner's group beside it — the ROOT filesystem included, since
	// it is this session's own copy of an environment image and the VM writes
	// to it.
	for _, rel := range []string{"", "/run", jailRootfsPath, jailWorkspacePath} {
		path := filepath.Join(root, rel)
		owner, ok := fs.ownerOf(path)
		if !ok {
			t.Errorf("%s was never given to the jailed VM's uid", path)
			continue
		}
		if owner != [2]int{spec.UID, spec.RunnerGID} {
			t.Errorf("%s was given to %v, want the VM's uid %d and the runner's gid %d", path, owner, spec.UID, spec.RunnerGID)
		}
	}

	// Nothing in the jail is readable by "other". The VM next door is
	// neither this VM's uid nor in the runner's group, and that is the whole
	// of what keeps it out.
	for _, rel := range []string{"", "/run", jailRootfsPath, jailWorkspacePath} {
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

// TestPrepareJailRefusesAnUnreadableSharedImage. The guest kernel is
// hard-linked into every jail, so its ownership cannot be changed for one VM;
// a jailed VM can only read it through the "other" bit. Saying so beats a
// guest that boots to a kernel it cannot open.
//
// It is the only shared image left: a session's ROOT filesystem is its own
// copy-on-write copy of an environment image, and is chowned to the VM like
// the workspace.
func TestPrepareJailRefusesAnUnreadableSharedImage(t *testing.T) {
	fc, stateDir, _ := jailTestEngine(t, &fakeStarter{})
	kernel := writeJailImage(t, stateDir, "private-vmlinux", 0o600)
	cfg := VMMConfig{ID: "mvm-3", SlotIndex: 3, KernelPath: kernel}
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

// TestACopiedJailImageIsGivenToTheVM is the EXDEV fallback, which produces a
// different FILE and not another name for the same one.
//
// A base rootfs on another filesystem from --microvm-state-dir cannot be hard
// linked, so prepareJail copies it — and the copy is a fresh file owned by
// runnerd at 0600, which a jailed VM is neither the owner of nor in the group
// of. Chowning the ORIGINAL (the host path) leaves the VM with an image it
// cannot open, and checking the original's mode instead of the copy's cannot
// see the problem at all.
func TestACopiedJailImageIsGivenToTheVM(t *testing.T) {
	fc, stateDir, fs := jailTestEngine(t, &fakeStarter{})
	fs.acrossFilesystems()

	// World-readable on the host, which is what the hard-link path needs and
	// what the copy path must not be made to depend on.
	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	workspace := writeJailImage(t, stateDir, "ws.ext4", 0o600)
	cfg := VMMConfig{
		ID:                "mvm-9",
		SlotIndex:         9,
		KernelPath:        kernel,
		WorkspaceDiskPath: workspace,
	}
	spec, err := fc.jailSpecFor(cfg)
	if err != nil {
		t.Fatalf("jailSpecFor: %v", err)
	}
	if err := fc.prepareJail(spec, cfg); err != nil {
		t.Fatalf("prepareJail across filesystems: %v", err)
	}

	root := jailRootDir(stateDir, "mvm-9")
	for _, rel := range []string{jailKernelPath, jailWorkspacePath} {
		inJail := filepath.Join(root, rel)
		info, err := os.Stat(inJail)
		if err != nil {
			t.Fatalf("the copied image is not in the jail: %v", err)
		}
		if rel == jailKernelPath {
			onHost, err := os.Stat(kernel)
			if err != nil {
				t.Fatalf("stat the host image: %v", err)
			}
			if os.SameFile(info, onHost) {
				t.Fatal("the EXDEV branch produced a hard link, so this test is not testing the copy")
			}
		}
		owner, ok := fs.ownerOf(inJail)
		if !ok {
			t.Fatalf("%s is a COPY the VM has to open, and it was never given to the VM's uid: the chown went to the host's own file instead", inJail)
		}
		if owner != [2]int{spec.UID, spec.RunnerGID} {
			t.Fatalf("%s was given to %v, want the VM's uid %d and the runner's gid %d", inJail, owner, spec.UID, spec.RunnerGID)
		}
		if info.Mode().Perm()&0o007 != 0 {
			t.Fatalf("%s is mode %#o: the copy is readable by every other VM on this host", inJail, info.Mode().Perm())
		}
	}

	// And the host's own images were left alone: the copy is the VM's, and
	// chowning the original would hand one tenant a file that is not theirs.
	if _, ok := fs.ownerOf(kernel); ok {
		t.Error("the host's kernel image was chowned for a VM that holds a copy of it")
	}
	if _, ok := fs.ownerOf(workspace); ok {
		t.Error("the host's workspace image was chowned for a VM that holds a copy of it")
	}
}

// TestLaunchCleansUpAfterAFailedStart is the leak: a jail full of hard links
// for a VM that never started.
func TestLaunchCleansUpAfterAFailedStart(t *testing.T) {
	starter := &fakeStarter{err: errors.New("jailer: Permission denied")}
	fc, stateDir, _ := jailTestEngine(t, starter)

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	cfg := VMMConfig{ID: "mvm-4", SlotIndex: 1, KernelPath: kernel, RootfsPath: rootfs}

	if err := fc.Launch(context.Background(), cfg); err == nil {
		t.Fatal("Launch succeeded with a starter that refuses")
	}
	if _, err := os.Stat(jailInstanceDir(stateDir, "mvm-4")); !os.IsNotExist(err) {
		t.Errorf("a failed start left the jail behind: %v", err)
	}
	// The uid is the slot's and the slot is the caller's to give back, so a
	// failed start consumes neither: the next VM on that slot gets the same
	// uid, and no allocator anywhere is one short.
	spec, err := fc.jailSpecFor(VMMConfig{ID: "mvm-5", SlotIndex: 1})
	if err != nil {
		t.Fatalf("the slot could not be used again after a failed start: %v", err)
	}
	if want := 200000 + 1; spec.UID != want {
		t.Errorf("the uid for slot 1 is %d, want the derived %d", spec.UID, want)
	}
}

// TestLaunchCleansUpAfterAVMMThatNeverAnswers: the process started, the API
// socket never appeared, and everything the launch made has to go — process
// first, because removing a chroot under a live Firecracker is how a VMM ends
// up writing into unlinked files.
func TestLaunchCleansUpAfterAVMMThatNeverAnswers(t *testing.T) {
	starter := &fakeStarter{}
	fc, stateDir, _ := jailTestEngine(t, starter)

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	rootfs := writeJailImage(t, stateDir, "base.ext4", 0o644)
	cfg := VMMConfig{ID: "mvm-6", SlotIndex: 6, KernelPath: kernel, RootfsPath: rootfs}

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
	// And the pid file this launch wrote. It lives OUTSIDE the jail, so
	// removing the jail does not take it, and a stale one is what State and
	// PID read on the next boot — a pid that by then names whatever the host
	// has recycled it onto.
	if _, err := os.Stat(fc.pidFilePath("mvm-6")); !os.IsNotExist(err) {
		t.Errorf("a failed boot left its pid file behind: %v", err)
	}
	fc.mu.Lock()
	tracked := len(fc.procs)
	fc.mu.Unlock()
	if tracked != 0 {
		t.Errorf("a failed boot left %d process(es) in the engine's map", tracked)
	}
}

// TestStopRemovesThePidFile. The pid file is this engine's own record of a
// process that no longer exists; it lives outside the jail, so removeJail
// does not take it, and State and PID read it on the next boot.
func TestStopRemovesThePidFile(t *testing.T) {
	fc, stateDir, _ := jailTestEngine(t, &fakeStarter{})

	// A pid file left by a previous runnerd, naming a process this engine
	// never started and that does not identify as this VM's VMM: the case
	// the identity check exists for, and the one where nothing else would
	// ever remove the file.
	path := fc.pidFilePath("mvm-8")
	if err := os.MkdirAll(filepath.Dir(path), microvmDirMode); err != nil {
		t.Fatalf("make the instance directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), microvmFileMode); err != nil {
		t.Fatalf("write the pid file: %v", err)
	}

	if err := fc.Stop(context.Background(), "mvm-8"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the pid file survived Stop: %v", err)
	}
	if _, err := os.Stat(jailInstanceDir(stateDir, "mvm-8")); !os.IsNotExist(err) {
		t.Errorf("the jail survived Stop: %v", err)
	}
}

// TestJailUIDComesFromTheSlotAndSurvivesARunnerdRestart.
//
// The uid is what keeps one tenant's Firecracker out of another's disk images
// and off its VMM, and a jail's contents are 0660 owned by it. An in-memory
// allocator did not survive a restart and the uid was in no record on disk,
// so a restarted runner handed the first uid out again — to a new tenant,
// while a VM that outlived the restart still owned that uid and a jail full
// of files belonging to it. Deriving the uid from the session's network slot,
// which IS in the record and IS re-adopted on recovery, is what closes it.
func TestJailUIDComesFromTheSlotAndSurvivesARunnerdRestart(t *testing.T) {
	fc, stateDir, fs := jailTestEngine(t, &fakeStarter{})

	kernel := writeJailImage(t, stateDir, "vmlinux.bin", 0o644)
	workspace := writeJailImage(t, stateDir, "ws.ext4", 0o600)
	survivor := VMMConfig{
		ID:                "mvm-1",
		SlotIndex:         1,
		KernelPath:        kernel,
		WorkspaceDiskPath: workspace,
	}
	first, err := fc.jailSpecFor(survivor)
	if err != nil {
		t.Fatalf("jailSpecFor: %v", err)
	}
	if err := fc.prepareJail(first, survivor); err != nil {
		t.Fatalf("prepareJail: %v", err)
	}

	// The restart: a new engine over the same state directory, with no
	// memory of anything that ran before it. The survivor is re-associated
	// with the slot it still holds (see reassociateSlot), and that is all the
	// new engine is given.
	restarted := jailTestEngineIn(t, stateDir, fs, &fakeStarter{})
	again, err := restarted.jailSpecFor(survivor)
	if err != nil {
		t.Fatalf("jailSpecFor after the restart: %v", err)
	}
	if again.UID != first.UID || again.GID != first.GID {
		t.Fatalf("the recovered session's uid is %d after the restart and %d before it; its jail is owned by the old one", again.UID, first.UID)
	}

	// The create that follows cannot be given the slot the survivor holds —
	// the pool refuses it (Pool.Adopt) — so it cannot be given that uid
	// either.
	next := VMMConfig{ID: "mvm-2", SlotIndex: 2, KernelPath: kernel}
	second, err := restarted.jailSpecFor(next)
	if err != nil {
		t.Fatalf("jailSpecFor for the create after the restart: %v", err)
	}
	if second.UID == first.UID {
		t.Fatalf("a VM created after the restart was handed uid %d, which the surviving VM owns", second.UID)
	}

	// And the survivor's jail is closed to it: owned by the survivor's uid,
	// grouped to the runner, readable by nobody else — which is the whole of
	// what a per-VM uid buys.
	for _, path := range []string{
		jailRootDir(stateDir, "mvm-1"),
		filepath.Join(jailRootDir(stateDir, "mvm-1"), jailWorkspacePath),
	} {
		owner, ok := fs.ownerOf(path)
		if !ok {
			t.Fatalf("%s was never given to the surviving VM's uid", path)
		}
		if owner[0] != first.UID {
			t.Errorf("%s is owned by %d, want the surviving VM's %d", path, owner[0], first.UID)
		}
		if owner[0] == second.UID {
			t.Errorf("%s is owned by %d, which is the NEW VM's uid: it can open the survivor's images", path, second.UID)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode().Perm()&0o007 != 0 {
			t.Errorf("%s is mode %#o, so the new VM can read it whatever its uid is", path, info.Mode().Perm())
		}
	}
}

// TestUIDsAreTheSlotIndexAboveTheRange. Two VMs sharing a uid share every
// file-permission decision the kernel makes about them.
func TestUIDsAreTheSlotIndexAboveTheRange(t *testing.T) {
	r, err := newUIDRange(200000, 8)
	if err != nil {
		t.Fatalf("newUIDRange: %v", err)
	}
	seen := map[int]int{}
	for idx := 1; idx < 8; idx++ {
		uid, err := r.forSlot(idx)
		if err != nil {
			t.Fatalf("forSlot(%d): %v", idx, err)
		}
		if other, clash := seen[uid]; clash {
			t.Fatalf("slots %d and %d were both given uid %d", other, idx, uid)
		}
		seen[uid] = idx
		if uid < 1000 {
			t.Fatalf("slot %d was given uid %d, inside the system range", idx, uid)
		}
	}
	// The same slot is the same uid, in this process and in the next one.
	// That is the property a restart depends on.
	if a, _ := r.forSlot(3); a != 200003 {
		t.Fatalf("forSlot(3) = %d, want 200003", a)
	}
	// A slot the range cannot cover is refused rather than folded back onto
	// somebody else's uid.
	if _, err := r.forSlot(8); err == nil {
		t.Fatal("a slot index outside the uid range was given a uid anyway")
	}
	// And a VM with no slot has no uid of its own.
	if _, err := r.forSlot(0); err == nil {
		t.Fatal("a VM with no network slot was given a uid")
	}
}

// TestUIDRangeRefusesTheSystemRange. A jailed VM must never run as a real
// account on this host — and the refusal has to leave behind a value that
// cannot hand one out either, since the engine keeps it.
func TestUIDRangeRefusesTheSystemRange(t *testing.T) {
	r, err := newUIDRange(33, 10)
	if err == nil {
		t.Fatal("the range accepted a start inside the system uids")
	}
	if _, err := r.forSlot(1); err == nil {
		t.Fatal("the zero uid range handed out a uid; an engine that failed to construct could jail a VM as a system account")
	}
}

// TestEngineWithARefusedUIDRangeLaunchesNothing. The engine keeps the refusal
// rather than a half-built allocator: the shape this replaced stored nil and
// dereferenced it at the first launch that got past initErr.
func TestEngineWithARefusedUIDRangeLaunchesNothing(t *testing.T) {
	fc := NewFirecrackerEngine(FirecrackerOpts{
		VMMPath:  "/bin/sh",
		StateDir: t.TempDir(),
		Jail:     JailOpts{JailerPath: "/bin/sh", UIDFirst: 33, UIDCount: 10},
	})
	if fc.initErr == nil {
		t.Fatal("an engine with a uid range inside the system uids constructed cleanly")
	}
	if err := fc.Launch(context.Background(), VMMConfig{ID: "mvm-1", SlotIndex: 1}); err == nil {
		t.Fatal("it launched a VM anyway")
	}
	// And the same one level down, so no future caller that gets past
	// initErr can reach a uid this range refused.
	if _, err := fc.jailSpecFor(VMMConfig{ID: "mvm-1", SlotIndex: 1}); err == nil {
		t.Fatal("jailSpecFor handed out a uid from a range the constructor refused")
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
	err := fc.Launch(context.Background(), VMMConfig{ID: "mvm-1", SlotIndex: 1})
	if err == nil {
		t.Fatal("an engine with no jailer launched a VM")
	}
	if !strings.Contains(err.Error(), "jailer") {
		t.Fatalf("error = %q, want it to name the jailer", err)
	}
}

// fakeSignaller records who was signalled, which is the only thing
// killProcessTree's safety is about.
type fakeSignaller struct {
	pgid    int
	pgidErr error
	err     error
	sent    []signalled
}

type signalled struct {
	target int
	sig    syscall.Signal
}

func (f *fakeSignaller) Getpgid(int) (int, error) { return f.pgid, f.pgidErr }

func (f *fakeSignaller) Kill(pid int, sig syscall.Signal) error {
	f.sent = append(f.sent, signalled{pid, sig})
	return f.err
}

func (f *fakeSignaller) targets() []int {
	out := make([]int, len(f.sent))
	for i, s := range f.sent {
		out[i] = s.target
	}
	return out
}

// TestKillProcessTreeOnlySignalsAGroupItLeads.
//
// A pid recovered from a pid file after a runnerd restart belongs to a group
// this process knows nothing about, and a group signal goes to -pgid — which,
// if that pgid happened to be runnerd's own, reaches every session on the
// host. WHO is signalled is therefore the property, and it is asserted here
// rather than inferred from a nil error.
func TestKillProcessTreeOnlySignalsAGroupItLeads(t *testing.T) {
	// A pid that does not lead its group: signalled alone, and the group it
	// merely belongs to is not touched.
	notALeader := &fakeSignaller{pgid: 4242}
	if err := killProcessTree(notALeader, 99, syscall.SIGTERM); err != nil {
		t.Fatalf("killProcessTree: %v", err)
	}
	if got := notALeader.targets(); !reflect.DeepEqual(got, []int{99}) {
		t.Fatalf("signalled %v, want exactly the pid 99 and nothing else; a negative target is a whole process group", got)
	}

	// A group leader: the group is signalled, as -pid, which is what catches
	// anything a jailed VMM left beside itself.
	leader := &fakeSignaller{pgid: 1234}
	if err := killProcessTree(leader, 1234, syscall.SIGKILL); err != nil {
		t.Fatalf("killProcessTree: %v", err)
	}
	if got := leader.targets(); !reflect.DeepEqual(got, []int{-1234}) {
		t.Fatalf("signalled %v, want the group -1234", got)
	}
	if leader.sent[0].sig != syscall.SIGKILL {
		t.Fatalf("signal = %v, want SIGKILL", leader.sent[0].sig)
	}

	// A pid whose group cannot be read is signalled alone. Guessing the group
	// is the one mistake this function exists to avoid.
	unknown := &fakeSignaller{pgid: 7, pgidErr: errors.New("no such process")}
	if err := killProcessTree(unknown, 7, syscall.SIGTERM); err != nil {
		t.Fatalf("killProcessTree: %v", err)
	}
	if got := unknown.targets(); !reflect.DeepEqual(got, []int{7}) {
		t.Fatalf("signalled %v, want the pid alone when its group is unknown", got)
	}

	// A pid that is not there is not an error: teardown is retried, and a
	// retry has to be able to finish.
	gone := &fakeSignaller{pgid: 5, err: syscall.ESRCH}
	if err := killProcessTree(gone, 5, syscall.SIGTERM); err != nil {
		t.Fatalf("killProcessTree of an absent pid: %v", err)
	}
	// Anything else is reported.
	refused := &fakeSignaller{pgid: 5, err: syscall.EPERM}
	if err := killProcessTree(refused, 5, syscall.SIGTERM); err == nil {
		t.Fatal("a signal this process may not send was reported as success")
	}

	// And pid 0, which to kill(2) is the CALLER's own process group: never
	// signalled, whatever the signal.
	never := &fakeSignaller{}
	if err := killProcessTree(never, 0, syscall.SIGKILL); err != nil {
		t.Fatalf("killProcessTree of pid 0: %v", err)
	}
	if len(never.sent) != 0 {
		t.Fatalf("pid 0 was signalled (%v), which is this process's own group", never.targets())
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
	if err := os.WriteFile(path, []byte(fmt.Sprintf("not a real image: %s", name)), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	// WriteFile applies the process umask, which is not what this test is
	// asking for.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	return path
}
