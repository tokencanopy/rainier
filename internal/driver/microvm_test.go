// internal/driver/microvm_test.go
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

// testMicrovm builds a driver over a fresh state directory with the whole
// test seam injected — simulated hypervisor, simulated TAP manager, no-op
// formatter — and a base rootfs that exists, because the driver now refuses
// to boot a session off an image this host does not have.
//
// Every microVM test goes through here. Nothing in production may construct
// the simulated trio, and the cost of that rule is one helper.
func testMicrovm(t *testing.T, opts MicrovmOpts) (*Microvm, *SimulatedEngine) {
	t.Helper()
	if opts.StateDir == "" {
		opts.StateDir = t.TempDir()
	}
	if opts.BaseRootfs == "" {
		opts.BaseRootfs = writeFakeRootfs(t, opts.StateDir)
	}
	sim, _ := opts.Engine.(*SimulatedEngine)
	if opts.Engine == nil {
		sim = NewSimulatedEngineWithDir(opts.StateDir)
		opts.Engine = sim
	}
	if opts.Tap == nil {
		opts.Tap = NewSimulatedTapManager()
	}
	if opts.Format == nil {
		opts.Format = SimulatedDiskFormatter{}
	}
	m, err := NewMicrovm(opts)
	if err != nil {
		t.Fatalf("NewMicrovm: %v", err)
	}
	return m, sim
}

// writeFakeRootfs puts a file where a base rootfs image would be. It is not a
// filesystem and nothing boots it; it is there so the driver's "this host has
// the image" check has something true to find.
func writeFakeRootfs(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "base-rootfs.ext4")
	if err := os.WriteFile(path, []byte("not a real ext4 image"), 0o600); err != nil {
		t.Fatalf("write fake rootfs: %v", err)
	}
	return path
}

func TestMicrovmSatisfiesContract(t *testing.T) {
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
		return d, func() {}
	})
}

// TestMicrovmRefusesToStartWithoutAHost is the fail-closed half: there is no
// combination of missing pieces that yields a working-looking driver.
func TestMicrovmRefusesToStartWithoutAHost(t *testing.T) {
	dir := t.TempDir()
	rootfs := writeFakeRootfs(t, dir)

	cases := []struct {
		name string
		opts MicrovmOpts
		want string
	}{
		{
			name: "no state directory",
			opts: MicrovmOpts{KernelPath: rootfs, BaseRootfs: rootfs},
			want: "state directory",
		},
		{
			name: "no kernel",
			opts: MicrovmOpts{StateDir: dir, BaseRootfs: rootfs},
			want: "guest kernel",
		},
		{
			name: "kernel that is not there",
			opts: MicrovmOpts{StateDir: dir, KernelPath: filepath.Join(dir, "absent"), BaseRootfs: rootfs},
			want: "guest kernel",
		},
		{
			name: "no rootfs",
			opts: MicrovmOpts{StateDir: dir, KernelPath: rootfs},
			want: "base rootfs image",
		},
		{
			name: "half an injected seam",
			opts: MicrovmOpts{StateDir: dir, Engine: NewSimulatedEngine()},
			want: "one seam",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewMicrovm(tc.opts)
			if err == nil {
				t.Fatalf("NewMicrovm(%+v) succeeded; a microVM runner that cannot boot a VM must refuse to start", tc.opts)
			}
			if m != nil {
				t.Fatal("NewMicrovm returned a driver alongside its error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name %q", err, tc.want)
			}
		})
	}
}

// TestMicrovmColdSuspendedSessionIsNotGone pins finding 5 against an engine
// that answers the way Firecracker does.
//
// A cold park terminates the VM, so the real engine reports the session as
// gone — it has no process left to ask. If the driver took that at face value
// the session would read as destroyed: Inspect would say gone, List would
// drop it, and runnerd.Recover would forget a session whose files are sitting
// right there on disk.
func TestMicrovmColdSuspendedSessionIsNotGone(t *testing.T) {
	stateDir := t.TempDir()
	m, _ := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Engine:     &goneWhenStoppedEngine{SimulatedEngine: NewSimulatedEngineWithDir(stateDir)},
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-cold", DialURL: "ws://runner.example.com:8080"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}

	if g, err := m.Inspect(ctx, h.ID); err != nil || g.State != StateSuspended {
		t.Fatalf("Inspect after a cold park = %+v, %v; want %s — the VM process is gone BECAUSE the driver parked it", g, err, StateSuspended)
	}
	if st := listedState(t, m, "sess-cold"); st != StateSuspended {
		t.Fatalf("List after a cold park reported %q; runnerd.Recover would drop the session", st)
	}

	// And after a restart: a new driver over the same state directory, with a
	// new engine that has no memory of the VM at all.
	m2, _ := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Engine:     &goneWhenStoppedEngine{SimulatedEngine: NewSimulatedEngineWithDir(stateDir)},
	})
	if st := listedState(t, m2, "sess-cold"); st != StateSuspended {
		t.Fatalf("List on a restarted driver reported %q, want %s", st, StateSuspended)
	}
	if g, err := m2.Inspect(ctx, h.ID); err != nil || g.State != StateSuspended {
		t.Fatalf("Inspect on a restarted driver = %+v, %v; want %s", g, err, StateSuspended)
	}
}

// goneWhenStoppedEngine is the simulated engine with one behavior corrected
// to match Firecracker: a stopped VM has no process left, so State reports it
// gone rather than stopped.
type goneWhenStoppedEngine struct {
	*SimulatedEngine
}

func (g *goneWhenStoppedEngine) State(ctx context.Context, id string) (VMMState, error) {
	st, err := g.SimulatedEngine.State(ctx, id)
	if err == nil && st == VMMStateStopped {
		return VMMStateGone, nil
	}
	return st, err
}

func listedState(t *testing.T, m *Microvm, sessionID string) State {
	t.Helper()
	listed, err := m.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, l := range listed {
		if l.SessionID == sessionID {
			return l.Handle.State
		}
	}
	t.Fatalf("List does not contain session %s: %+v", sessionID, listed)
	return ""
}

func TestMicrovmRestartRecovery(t *testing.T) {
	stateDir := t.TempDir()
	m1, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	ctx := context.Background()

	h1, err := m1.Create(ctx, Spec{SessionID: "sess-recover-1"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := m1.Create(ctx, Spec{SessionID: "sess-recover-2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Suspend(ctx, h2.ID, false); err != nil {
		t.Fatal(err)
	}

	// A complete runner restart: a new engine AND a new driver over the same
	// state directory.
	m2, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})

	listed, err := m2.List(ctx)
	if err != nil {
		t.Fatalf("List on a recovered driver: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("recovered %d sessions, want 2", len(listed))
	}
	foundMap := make(map[string]State)
	for _, l := range listed {
		foundMap[l.SessionID] = l.Handle.State
	}
	if st := foundMap["sess-recover-1"]; st != StateRunning {
		t.Errorf("recovered sess-recover-1 state = %s, want running", st)
	}
	if st := foundMap["sess-recover-2"]; st != StateSuspended {
		t.Errorf("recovered sess-recover-2 state = %s, want suspended", st)
	}

	if g1, err := m2.Inspect(ctx, h1.ID); err != nil || g1.State != StateRunning {
		t.Errorf("m2.Inspect(h1) = %+v, %v; want running", g1, err)
	}
	if g2, err := m2.Inspect(ctx, h2.ID); err != nil || g2.State != StateSuspended {
		t.Errorf("m2.Inspect(h2) = %+v, %v; want suspended", g2, err)
	}
}

func TestMicrovmWorkspaceFilesSurviveColdPark(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-persist"})
	if err != nil {
		t.Fatal(err)
	}

	diskFile, err := m.workspaceDiskPath("sess-persist")
	if err != nil {
		t.Fatal(err)
	}

	testData := []byte("uncommitted agent work that must survive cold park")
	if err := os.WriteFile(diskFile, testData, 0o600); err != nil {
		t.Fatalf("write test disk image: %v", err)
	}

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateSuspended {
		t.Fatalf("state after cold suspend = %s, want suspended", g.State)
	}
	if data, err := os.ReadFile(diskFile); err != nil || string(data) != string(testData) {
		t.Fatalf("disk file corrupted or missing during cold park: %v", err)
	}

	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatalf("resume after cold park: %v", err)
	}
	if !restarted {
		t.Error("cold resume did not report restarted = true")
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateRunning {
		t.Fatalf("state after resume = %s, want running", g.State)
	}
	if data, err := os.ReadFile(diskFile); err != nil || string(data) != string(testData) {
		t.Fatalf("disk contents missing or changed after resume: %v", err)
	}

	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := os.Stat(diskFile); !os.IsNotExist(err) {
		t.Errorf("workspace disk %s still exists after Destroy", diskFile)
	}
}

func TestMicrovmGuestEnvTranslationAndBootstrap(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	spec := Spec{
		SessionID:       "sess-trans",
		DialURL:         "ws://runner.example.com:8080",
		ProxyURL:        "http://proxy.example.com:3128",
		Setup:           "npm install -g pnpm",
		SetupTimeoutSec: 600,
		Repos: []RepoSpec{
			{Owner: "tokencanopy", Name: "rainier", BaseBranch: "main", SessionBranch: "rainier/feat", Dir: "rainier"},
		},
		Init:           "pnpm test",
		InitTimeoutSec: 180,
		GitAuthorName:  "Test Author",
		GitAuthorEmail: "author@example.invalid",
		Cmd:            []string{"claude", "--model", "haiku"},
		Env:            map[string]string{"APP_ENV": "production"},
	}

	h, err := m.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	cfg, ok := sim.Config(h.ID)
	if !ok {
		t.Fatal("simulated engine did not capture VMMConfig")
	}

	checks := map[string]string{
		"RAINIER_DIAL":             "ws://runner.example.com:8080",
		"RAINIER_SESSION":          "sess-trans",
		"RAINIER_GIT_AUTHOR_NAME":  "Test Author",
		"RAINIER_GIT_AUTHOR_EMAIL": "author@example.invalid",
		"APP_ENV":                  "production",
		"RAINIER_SETUP_TIMEOUT":    "600",
		"RAINIER_INIT_TIMEOUT":     "180",
	}
	for k, want := range checks {
		if got := cfg.Env[k]; got != want {
			t.Errorf("cfg.Env[%q] = %q, want %q", k, got, want)
		}
	}
	if cfg.Env["HTTP_PROXY"] == "" || cfg.Env["NO_PROXY"] == "" {
		t.Errorf("proxy variables were not injected into guest env: %+v", cfg.Env)
	}
	if cfg.TapDevice == "" {
		t.Errorf("TapDevice was not allocated on VMMConfig")
	}

	// The staged session config is the non-secret half, and only that half.
	jsonPath := filepath.Join(m.instanceDir(h.ID), "session.json")
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read session.json: %v", err)
	}
	var gsc guestSessionConfig
	if err := json.Unmarshal(data, &gsc); err != nil {
		t.Fatalf("unmarshal session.json: %v", err)
	}
	if gsc.SessionID != "sess-trans" {
		t.Errorf("gsc.SessionID = %q, want sess-trans", gsc.SessionID)
	}
	if !reflect.DeepEqual(gsc.Cmd, []string{"claude", "--model", "haiku"}) {
		t.Errorf("gsc.Cmd = %v, want claude --model haiku", gsc.Cmd)
	}
	if strings.Contains(string(data), "APP_ENV") || strings.Contains(string(data), "production") {
		t.Errorf("session.json carries the session environment:\n%s", data)
	}
}

// TestMicrovmWritesNoDecryptedEnvironment is finding 4: an environment's
// resolved secrets belong to a session, not to the host, and this driver
// writes none of them down. It also pins the modes on everything it does
// write.
func TestMicrovmWritesNoDecryptedEnvironment(t *testing.T) {
	const secret = "must-not-reach-host-disk"
	stateDir := t.TempDir()
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "sess-secret",
		DialURL:   "ws://runner.example.com:8080",
		Setup:     "echo setting up",
		Env:       map[string]string{"DEPLOY_TOKEN": secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Snapshot(ctx, h.ID, "rainier-env:leak-check", nil); err != nil {
		t.Fatal(err)
	}

	err = filepath.WalkDir(stateDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == stateDir {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			if info.Mode().Perm() != microvmDirMode {
				t.Errorf("directory %s has mode %04o, want %04o", path, info.Mode().Perm(), microvmDirMode)
			}
			return nil
		}
		if info.Mode().Perm() != microvmFileMode {
			t.Errorf("file %s has mode %04o, want %04o", path, info.Mode().Perm(), microvmFileMode)
		}
		// Disk images are the guest's own storage and are sparse by the
		// gigabyte; the metadata this driver writes is what is being audited.
		if info.Size() > 1<<20 {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), secret) {
			t.Errorf("%s carries a decrypted environment value", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk state dir: %v", err)
	}

	// The instance record and the staged guest config carry not even the
	// KEYS: the first is the file a runnerd restart reads back, the second is
	// bound for the guest, and neither has any business describing an
	// environment the driver was told to keep in memory.
	for _, name := range []string{"instance.json", "session.json"} {
		data, err := os.ReadFile(filepath.Join(m.instanceDir(h.ID), name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(data), "DEPLOY_TOKEN") {
			t.Errorf("%s names a key from the session's decrypted environment:\n%s", name, data)
		}
	}
}

// TestMicrovmColdResumeAfterRestartRefuses is the other half of the same
// decision: having refused to write the environment down, the driver says so
// rather than boot a guest with an empty one.
func TestMicrovmColdResumeAfterRestartRefuses(t *testing.T) {
	stateDir := t.TempDir()
	m1, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	ctx := context.Background()

	h, err := m1.Create(ctx, Spec{SessionID: "sess-resume", Env: map[string]string{"TOKEN": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}

	m2, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	if _, err := m2.Resume(ctx, h.ID); err == nil {
		t.Fatal("cold resume across a restart succeeded; it would have booted a guest with no environment at all")
	} else if !strings.Contains(err.Error(), "vsock") {
		t.Errorf("error = %q, want it to name the follow-up work (vsock bootstrap token)", err)
	}
	// And the session is still there to be resumed once that lands.
	if st := listedState(t, m2, "sess-resume"); st != StateSuspended {
		t.Errorf("a refused resume changed the session's state to %q", st)
	}
}

// TestMicrovmCreatesTheAgentHomeDisk is finding 10: /drives/home must never
// point at a file that is not there.
func TestMicrovmCreatesTheAgentHomeDisk(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "sess-home",
		Home:      &HomeMount{Volume: "rainier-home-agent-ws", Path: "/rainier/agents"},
	})
	if err != nil {
		t.Fatal(err)
	}

	cfg, ok := sim.Config(h.ID)
	if !ok {
		t.Fatal("simulated engine did not capture VMMConfig")
	}
	if cfg.HomeDiskPath == "" {
		t.Fatal("VMMConfig carries no home disk path for a spec with an agent home")
	}
	if _, err := os.Stat(cfg.HomeDiskPath); err != nil {
		t.Fatalf("home disk %s does not exist at launch: %v", cfg.HomeDiskPath, err)
	}
	if cfg.HomeDiskPath == cfg.WorkspaceDiskPath {
		t.Fatal("the agent home and the workspace are the same image")
	}

	// The home outlives the session that mounted it: Destroy takes the
	// workspace and leaves the home for the next session of the same person.
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.HomeDiskPath); err != nil {
		t.Fatalf("Destroy removed the agent home: %v", err)
	}
}

// TestMicrovmCreateFailsClosedWithoutAFormatter proves the formatting half
// fails closed: an unformattable disk is an error, not an unformatted file
// handed to the guest.
func TestMicrovmCreateFailsClosedWithoutAFormatter(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, Format: failingFormatter{}})
	if _, err := m.Create(context.Background(), Spec{SessionID: "sess-nofmt"}); err == nil {
		t.Fatal("Create succeeded with no way to format the workspace disk")
	}
	leftover, err := m.workspaceDiskPath("sess-nofmt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); err == nil {
		t.Error("an unformatted disk image was left behind for the next create to find")
	}
}

type failingFormatter struct{}

func (failingFormatter) Format(string) error { return errors.New("mkfs.ext4 is not on PATH") }

func TestMicrovmSnapshotRefAssociationAndStrip(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "sess-snapref",
		Env: map[string]string{
			"SECRET_KEY": "super_secret_value",
			"PUBLIC_KEY": "public_value",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	const ref = "rainier-env:test-env-001"
	strip := []string{"SECRET_KEY", "RAINIER_SETUP_B64"}
	snap, err := m.Snapshot(ctx, h.ID, ref, strip)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Ref != ref {
		t.Fatalf("snap.Ref = %q, want %q verbatim", snap.Ref, ref)
	}

	keys, ok := m.SnapshotEnvKeys(ref)
	if !ok {
		t.Fatal("no manifest for a committed ref")
	}
	if !slices.Contains(keys, "PUBLIC_KEY") {
		t.Errorf("manifest keys = %v, want PUBLIC_KEY among them", keys)
	}
	if slices.Contains(keys, "SECRET_KEY") {
		t.Errorf("SECRET_KEY was not stripped from the snapshot manifest: %v", keys)
	}
	// Keys, and no values: the manifest is a file on a shared host that
	// outlives the session.
	if raw := m.snapshotManifestBytes(ref); strings.Contains(string(raw), "public_value") {
		t.Errorf("the manifest carries environment VALUES:\n%s", raw)
	}

	// A snapshot ref must not become a bootable rootfs. It used to: the
	// driver copied the session's WORKSPACE under the ref and resolveRootfs
	// handed that file back as a root filesystem for any later create naming
	// it, so a `rainier-env:*` entry was one tenant's workspace booted as
	// another session's root.
	if _, err := m.locateRootfs(ref); err == nil {
		t.Fatal("a snapshot ref resolved to a bootable rootfs")
	}
}

// TestMicrovmFirecrackerSnapshotRefuses is findings 1 and 2 together: no
// guest memory image, and no workspace masquerading as an environment image.
func TestMicrovmFirecrackerSnapshotRefuses(t *testing.T) {
	fc := NewFirecrackerEngine("/nonexistent/bin/firecracker", t.TempDir())
	_, err := fc.Snapshot(context.Background(), "mvm-1", "rainier-env:x", nil)
	if err == nil {
		t.Fatal("FirecrackerEngine.Snapshot succeeded; it must refuse until the image work lands")
	}
	for _, want := range []string{"not implemented", "Guest memory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
}

func TestMicrovmPIDVerification(t *testing.T) {
	myPID := os.Getpid()
	if isFirecrackerPID(myPID, "/nonexistent/socket.sock") {
		t.Error("isFirecrackerPID returned true for the test runner process")
	}
	if isFirecrackerPID(99999999, "") {
		t.Error("isFirecrackerPID returned true for a nonexistent PID")
	}
}

func TestMicrovmDestroyContainerEngineFailure(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-stopfail"})
	if err != nil {
		t.Fatal(err)
	}

	sim.FailOnStop(h.ID, errors.New("hypervisor process hung"))
	if err := m.DestroyContainer(ctx, h.ID); err == nil {
		t.Fatal("DestroyContainer should have failed when engine.Stop fails")
	}

	// The instance must be preserved so capacity accounting is not corrupted.
	if g, _ := m.Inspect(ctx, h.ID); g.State == StateGone {
		t.Error("DestroyContainer prematurely deleted the instance when engine.Stop failed")
	}
	if used, _, _ := m.Capacity(ctx); used != 1 {
		t.Errorf("capacity after a failed DestroyContainer = %d, want 1", used)
	}

	sim.FailOnStop(h.ID, nil)
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("clean destroy: %v", err)
	}
}

func TestMicrovmFirecrackerFailsClosed(t *testing.T) {
	fc := NewFirecrackerEngine("/nonexistent/bin/firecracker", t.TempDir())
	if err := fc.Launch(context.Background(), VMMConfig{ID: "mvm-fail"}); err == nil {
		t.Fatal("FirecrackerEngine.Launch must fail closed when the binary is missing")
	}
}

func TestMicrovmFirecrackerClientConfiguration(t *testing.T) {
	// Not t.TempDir(): its directory name carries the test's name, and a unix
	// socket path has a hard ~104-byte limit.
	dir, err := os.MkdirTemp("", "fcsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sockPath := filepath.Join(dir, "s.sock")
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var mu sync.Mutex
	calls := make(map[string]bool)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls[r.Method+" "+r.URL.Path] = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	go server.Serve(listener)
	defer server.Close()

	fc := newFirecrackerClient(sockPath)
	ctx := context.Background()

	for _, call := range []struct {
		endpoint string
		payload  map[string]any
	}{
		{"/machine-config", map[string]any{"vcpu_count": 4}},
		{"/boot-source", map[string]any{"kernel_image_path": "/vmlinux"}},
		{"/drives/rootfs", map[string]any{"path_on_host": "/rootfs"}},
		{"/drives/home", map[string]any{"path_on_host": "/home.ext4"}},
		{"/actions", map[string]any{"action_type": "InstanceStart"}},
	} {
		if err := fc.putJSON(ctx, call.endpoint, call.payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := fc.patchJSON(ctx, "/vm", map[string]any{"state": "Paused"}); err != nil {
		t.Fatal(err)
	}
	if err := fc.patchJSON(ctx, "/vm", map[string]any{"state": "Resumed"}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, call := range []string{
		"PUT /machine-config",
		"PUT /boot-source",
		"PUT /drives/rootfs",
		"PUT /drives/home",
		"PUT /actions",
		"PATCH /vm",
	} {
		if !calls[call] {
			t.Errorf("Firecracker API client missed call %s", call)
		}
	}
}

// TestMicrovmLaunchSpeaksNoMMDS: MMDS answered at the metadata address the
// host is required to drop, so the driver must not configure it at all.
func TestMicrovmLaunchSpeaksNoMMDS(t *testing.T) {
	src, err := os.ReadFile("microvm.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{`"/mmds/config"`, `"/mmds"`} {
		if strings.Contains(string(src), endpoint) {
			t.Errorf("the driver still PUTs %s; the guest control channel is vsock (ADR-0003 §2.7 item 2)", endpoint)
		}
	}
}

// TestMicrovmSlotSizeIsTheADRFloor pins the per-session envelope: 4 vCPU and
// 8 GiB by default, and whatever the operator configured otherwise.
func TestMicrovmSlotSizeIsTheADRFloor(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-size"})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := sim.Config(h.ID)
	if cfg.VCPU != defaultMicrovmVCPU || cfg.MemoryMiB != defaultMicrovmMemoryMiB {
		t.Fatalf("default slot = %d vCPU / %d MiB, want %d / %d (ADR-0003 §5.1 floor)",
			cfg.VCPU, cfg.MemoryMiB, defaultMicrovmVCPU, defaultMicrovmMemoryMiB)
	}

	m2, sim2 := testMicrovm(t, MicrovmOpts{TotalSlots: 2, VCPU: 8, MemoryMiB: 16384})
	h2, err := m2.Create(ctx, Spec{SessionID: "sess-size-2"})
	if err != nil {
		t.Fatal(err)
	}
	cfg2, _ := sim2.Config(h2.ID)
	if cfg2.VCPU != 8 || cfg2.MemoryMiB != 16384 {
		t.Fatalf("configured slot = %d vCPU / %d MiB, want 8 / 16384", cfg2.VCPU, cfg2.MemoryMiB)
	}
}

func TestMicrovmStateReconciliation(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-reconcile"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	// An unexpected hypervisor crash.
	sim.SetState(h.ID, VMMStateStopped)

	h2, err := m.Inspect(ctx, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if h2.State == StateRunning {
		t.Errorf("Inspect failed to reconcile a stopped engine state: got %s", h2.State)
	}
	listed, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Handle.State == StateRunning {
		t.Errorf("List failed to reflect the stopped engine state: %+v", listed)
	}
}

// TestMicrovmPrepullNeverFabricatesAnImage is finding 6's second half: a ref
// this host cannot boot is an error, not an empty file and a recorded pull.
func TestMicrovmPrepullNeverFabricatesAnImage(t *testing.T) {
	stateDir := t.TempDir()
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2, StateDir: stateDir})
	ctx := context.Background()

	const missing = "rainier-env:never-fetched"
	if err := m.Prepull(ctx, missing); err == nil {
		t.Fatal("Prepull of an unresolvable ref = nil, want an error")
	}
	cached := filepath.Join(stateDir, "rootfs", mustSanitizeRef(t, missing)+".ext4")
	if _, err := os.Stat(cached); err == nil {
		t.Fatalf("Prepull left a placeholder image at %s", cached)
	}
	if got := m.Pulls(); len(got) != 0 {
		t.Errorf("Pulls() = %v after a failed prepull, want none recorded", got)
	}
	if err := m.Prepull(ctx, ""); err == nil {
		t.Error("Prepull with an empty ref = nil, want an error")
	}

	// A ref this host does have resolves, and is recorded.
	const present = "rainier-env:e1-aaa"
	if err := os.WriteFile(filepath.Join(stateDir, "rootfs", mustSanitizeRef(t, present)+".ext4"), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Prepull(ctx, present); err != nil {
		t.Fatalf("Prepull of a cached ref: %v", err)
	}
	if got := m.Pulls(); !reflect.DeepEqual(got, []string{present}) {
		t.Errorf("Pulls() = %v, want %v", got, []string{present})
	}
}

// TestMicrovmCreateRefusesAnAbsentImage is the same rule on the create path:
// a session never boots off an image nobody fetched.
func TestMicrovmCreateRefusesAnAbsentImage(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()
	if _, err := m.Create(ctx, Spec{SessionID: "sess-noimage", Image: "rainier-env:absent"}); err == nil {
		t.Fatal("Create with an image this host does not have succeeded")
	}
	if used, _, _ := m.Capacity(ctx); used != 0 {
		t.Errorf("a failed Create left %d slots occupied", used)
	}
}

func TestMicrovmRecordsSnapshotStrips(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "mvm-sess-s", Env: map[string]string{"TOKEN": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Snapshot(ctx, h.ID, "rainier-env:e1-aaa", []string{"TOKEN", "RAINIER_SETUP_B64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Snapshot(ctx, h.ID, "", nil); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"TOKEN", "RAINIER_SETUP_B64"}, nil}
	if got := m.Strips(); !reflect.DeepEqual(got, want) {
		t.Errorf("Strips() = %v, want %v (in call order)", got, want)
	}
}

// TestMicrovmConcurrentLifecycleCalls is the lock-hygiene regression: with
// the driver mutex no longer held across engine calls, creates and the
// read-only views run against each other without deadlocking. Under -race it
// is also the check that releasing the mutex opened no data race.
func TestMicrovmConcurrentLifecycleCalls(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 16})
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := m.Create(ctx, Spec{SessionID: fmt.Sprintf("sess-conc-%d", i)})
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			if _, err := m.Inspect(ctx, h.ID); err != nil {
				t.Errorf("Inspect: %v", err)
			}
			if err := m.Suspend(ctx, h.ID, true); err != nil {
				t.Errorf("Suspend: %v", err)
			}
			if _, err := m.Resume(ctx, h.ID); err != nil {
				t.Errorf("Resume: %v", err)
			}
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.List(ctx); err != nil {
				t.Errorf("List: %v", err)
			}
			if _, _, err := m.Capacity(ctx); err != nil {
				t.Errorf("Capacity: %v", err)
			}
		}()
	}
	wg.Wait()

	listed, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 8 {
		t.Fatalf("listed %d sessions after 8 concurrent creates, want 8", len(listed))
	}
}

func mustSanitizeRef(t *testing.T, ref string) string {
	t.Helper()
	seg, err := sanitizeRef(ref)
	if err != nil {
		t.Fatalf("sanitizeRef(%q): %v", ref, err)
	}
	return seg
}

// TestMicrovmHostileNamesNeverBecomePaths: every name this driver joins onto
// the state directory comes from somewhere else — a control plane, a spec, a
// directory listing — and filepath.Join CLEANS its result, so "../../x" does
// not produce a silly path, it produces a real one outside the state
// directory that RemoveWorkspace would then delete.
func TestMicrovmHostileNamesNeverBecomePaths(t *testing.T) {
	stateDir := t.TempDir()
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	ctx := context.Background()

	// A file outside the state directory that no driver call may reach.
	outside := filepath.Join(filepath.Dir(stateDir), "rainier-ws-victim.ext4")
	if err := os.WriteFile(outside, []byte("someone else's data"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(outside) })

	hostile := []string{
		"../victim",
		"..",
		".",
		"a/b",
		`a\b`,
		"sess/../../escape",
		"",
	}
	for _, id := range hostile {
		t.Run("session-"+id, func(t *testing.T) {
			if _, err := m.workspaceDiskPath(id); err == nil {
				t.Errorf("workspaceDiskPath(%q) produced a path", id)
			}
			if _, err := m.homeDiskPath(id); err == nil {
				t.Errorf("homeDiskPath(%q) produced a path", id)
			}
			if id != "" {
				// An empty session id names no workspace and is a documented
				// no-op; every other hostile spelling must be refused.
				if err := m.RemoveWorkspace(ctx, id); err == nil {
					t.Errorf("RemoveWorkspace(%q) = nil, want a refusal", id)
				}
			}
			if _, err := m.Create(ctx, Spec{SessionID: id, Image: ""}); id != "" && err == nil {
				t.Errorf("Create with session id %q succeeded", id)
			}
		})
	}

	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("a file outside the state directory was removed: %v", err)
	}
	// The empty id keeps its documented behavior.
	if err := m.RemoveWorkspace(ctx, ""); err != nil {
		t.Errorf(`RemoveWorkspace("") = %v, want nil`, err)
	}
}

// TestMicrovmHostileSnapshotRefsAreRefused: url.PathEscape leaves "." and
// ".." exactly as they are, so a ref of ".." used to name the parent of the
// refs directory and a commit wrote its manifest one level up.
func TestMicrovmHostileSnapshotRefsAreRefused(t *testing.T) {
	stateDir := t.TempDir()
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-ref"})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"..", ".", ""} {
		if ref != "" {
			// An empty ref is the dev surface's case: the driver mints one.
			if _, err := m.Snapshot(ctx, h.ID, ref, nil); err == nil {
				t.Errorf("Snapshot to ref %q succeeded", ref)
			}
		}
		if _, err := sanitizeRef(ref); err == nil {
			t.Errorf("sanitizeRef(%q) = nil error", ref)
		}
	}

	// A ref with separators in it is not refused, it is escaped: the result
	// has to be ONE entry under the refs directory, and the assertion is that
	// it stays there rather than that the spelling is rejected.
	for _, ref := range []string{"../..", "a/../..", `a\..\..`, "rainier-env:x/y"} {
		seg, err := sanitizeRef(ref)
		if err != nil {
			continue
		}
		if seg != filepath.Base(seg) || strings.ContainsAny(seg, `/\`) {
			t.Errorf("sanitizeRef(%q) = %q, which is not one path entry", ref, seg)
		}
		dir, err := m.snapshotRefDir(ref)
		if err != nil {
			t.Fatal(err)
		}
		if want := filepath.Join(stateDir, "snapshots", "refs"); filepath.Dir(dir) != want {
			t.Errorf("ref %q resolved to %s, outside %s", ref, dir, want)
		}
	}
	// Nothing was written above the refs directory.
	stray := filepath.Join(stateDir, "snapshots", "manifest.json")
	if _, err := os.Stat(stray); err == nil {
		t.Errorf("a snapshot wrote %s, one level above its ref directory", stray)
	}
	// An ordinary ref with a colon still works.
	if _, err := m.Snapshot(ctx, h.ID, "rainier-env:ok-1", nil); err != nil {
		t.Fatalf("snapshot to an ordinary ref: %v", err)
	}
}

// TestMicrovmResumeOfARunningSessionTouchesNothing is driver.go's Resume
// contract at the engine boundary: an already-running session restarts
// nothing, and the driver must not ask the hypervisor to unpause a VM that
// was never paused (Firecracker answers 400).
func TestMicrovmResumeOfARunningSessionTouchesNothing(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-rr"})
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingResumeEngine{SimulatedEngine: sim}
	m.engine = counting

	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatalf("Resume of a running session = %v, want nil", err)
	}
	if restarted {
		t.Error("Resume of a running session reported a restart")
	}
	if n := counting.resumes.Load(); n != 0 {
		t.Errorf("Resume of a running session called the hypervisor %d times", n)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateRunning {
		t.Errorf("state after a no-op resume = %s", g.State)
	}
}

type countingResumeEngine struct {
	*SimulatedEngine
	resumes atomic.Int64
}

func (c *countingResumeEngine) Resume(ctx context.Context, id string) error {
	c.resumes.Add(1)
	return c.SimulatedEngine.Resume(ctx, id)
}

// TestMicrovmReconcileDoesNotClobberAConcurrentSuspend is the lost update.
// Inspect and List ask the hypervisor with the mutex released; a Suspend that
// completes inside that window must not be overwritten by the stale answer,
// which would leak a slot and send the next Resume down the warm path into a
// VM that is no longer there.
func TestMicrovmReconcileDoesNotClobberAConcurrentSuspend(t *testing.T) {
	stateDir := t.TempDir()
	gate := make(chan struct{})
	m, _ := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Engine:     &gatedStateEngine{SimulatedEngine: NewSimulatedEngineWithDir(stateDir), gate: gate},
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-lost"})
	if err != nil {
		t.Fatal(err)
	}

	// Inspect blocks inside engine.State holding the answer "running".
	done := make(chan Handle, 1)
	go func() {
		g, _ := m.Inspect(ctx, h.ID)
		done <- g
	}()
	<-gate // Inspect has read the state and is about to return it.

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	gate <- struct{}{} // let Inspect finish with its now-stale answer.
	<-done

	if g, _ := m.Inspect(ctx, h.ID); g.State != StateSuspended {
		t.Fatalf("state after a cold suspend that raced a reconcile = %s, want %s", g.State, StateSuspended)
	}
	if used, _, _ := m.Capacity(ctx); used != 0 {
		t.Errorf("a cold-suspended session still occupies %d slot(s): the stale reconcile won", used)
	}
	if st := listedState(t, m, "sess-lost"); st != StateSuspended {
		t.Errorf("List reports %q after the raced suspend", st)
	}
}

// gatedStateEngine reads the state, then parks on a channel before returning
// it, so a test can run a whole Suspend inside the window Inspect leaves open.
type gatedStateEngine struct {
	*SimulatedEngine
	gate chan struct{}
	once sync.Once
}

func (g *gatedStateEngine) State(ctx context.Context, id string) (VMMState, error) {
	st, err := g.SimulatedEngine.State(ctx, id)
	g.once.Do(func() {
		g.gate <- struct{}{}
		<-g.gate
	})
	return st, err
}

// TestFirecrackerStateReadsInstanceInfo pins the endpoint and the parse. GET
// /vm is not a Firecracker route — /vm takes a PATCH and nothing else — so
// asking for it 404s, and the previous code fell through its own error
// handling and reported every VM as running, warm-paused ones included.
func TestFirecrackerStateReadsInstanceInfo(t *testing.T) {
	dir, err := os.MkdirTemp("", "fcinfo")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	var body atomic.Value
	body.Store(`{"id":"mvm-1","state":"Running"}`)
	var paths sync.Map

	sockPath := filepath.Join(dir, "s.sock")
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.Store(r.Method+" "+r.URL.Path, true)
		if r.URL.Path != "/" {
			// Exactly what Firecracker does with a route it does not serve.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body.Load().(string))
	})}
	go server.Serve(listener)
	defer server.Close()

	c := newFirecrackerClient(sockPath)
	ctx := context.Background()

	for _, tc := range []struct {
		json string
		want VMMState
	}{
		{`{"state":"Running"}`, VMMStateRunning},
		{`{"state":"Paused"}`, VMMStatePaused},
	} {
		body.Store(tc.json)
		got, err := instanceState(ctx, c)
		if err != nil {
			t.Fatalf("instanceState(%s): %v", tc.json, err)
		}
		if got != tc.want {
			t.Errorf("instanceState(%s) = %q, want %q", tc.json, got, tc.want)
		}
	}
	if _, ok := paths.Load("GET /"); !ok {
		t.Error("instanceState did not ask GET /")
	}
	if _, ok := paths.Load("GET /vm"); ok {
		t.Error("instanceState asked GET /vm, which Firecracker does not serve")
	}

	// A state this driver did not create the VM in is an error, not Running:
	// State's caller leaves its own record alone when State errors.
	body.Store(`{"state":"Not started"}`)
	if got, err := instanceState(ctx, c); err == nil {
		t.Errorf("instanceState of a not-started VM = %q, want an error", got)
	}
	body.Store(`not json at all`)
	if got, err := instanceState(ctx, c); err == nil {
		t.Errorf("instanceState of an unparseable body = %q, want an error", got)
	}
}

// TestFirecrackerStopReapsTheChild: a VMM this engine started is a child, and
// signalling one without Waiting it leaves a zombie per stopped VM.
func TestFirecrackerStopReapsTheChild(t *testing.T) {
	dir, err := os.MkdirTemp("", "fcreap")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	fc := NewFirecrackerEngine("/nonexistent/bin/firecracker", dir)

	// A stand-in for a launched VMM: a real child of this process that
	// ignores nothing and exits on SIGTERM.
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	fc.mu.Lock()
	fc.procs["mvm-reap"] = cmd
	fc.mu.Unlock()

	if err := fc.Stop(context.Background(), "mvm-reap"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Wait having been called, the pid is fully released: signalling it now
	// finds nothing. An un-Waited child would still answer signal 0 as a
	// zombie.
	if err := syscall.Kill(pid, 0); err == nil {
		t.Error("the stopped VMM is still in the process table; Stop never Waited it")
	}
	fc.mu.Lock()
	_, stillTracked := fc.procs["mvm-reap"]
	fc.mu.Unlock()
	if stillTracked {
		t.Error("Stop left the process in the engine's map")
	}
	// A second Stop of the same id is a no-op, not a double Wait.
	if err := fc.Stop(context.Background(), "mvm-reap"); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}
