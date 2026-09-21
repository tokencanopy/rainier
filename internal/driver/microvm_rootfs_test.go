// internal/driver/microvm_rootfs_test.go
//
// The per-session copy-on-write root filesystem: that a create makes one out
// of the environment image, that every teardown takes it away, and that the
// two devices which are NOT part of an environment — the workspace and the
// agent home — are never cloned.
package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// testMicrovmCloning is testMicrovm with a cloner that records what it was
// asked to copy.
func testMicrovmCloning(t *testing.T, opts MicrovmOpts) (*Microvm, *SimulatedEngine, *fakeCloner) {
	t.Helper()
	c := &fakeCloner{}
	opts.Clone = c
	m, sim := testMicrovm(t, opts)
	return m, sim, c
}

// TestMicrovmCreateClonesTheEnvironmentImage: the session boots its own
// writable copy, the image it came from is untouched, and neither the
// workspace nor the agent home is ever cloned — they are separate block
// devices, which is what keeps them out of every published image by
// construction (ADR-0003 §2.7 item 3, §4.1).
func TestMicrovmCreateClonesTheEnvironmentImage(t *testing.T) {
	stateDir := shortTempDir(t)
	m, sim, cloner := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 2, StateDir: stateDir})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "sess-cow",
		Home:      &HomeMount{Volume: "home-cow", Path: "/rainier/agents"},
	})
	if err != nil {
		t.Fatal(err)
	}

	cfg, ok := sim.Config(h.ID)
	if !ok {
		t.Fatal("the engine was never launched")
	}
	want, err := m.sessionRootfsPath(h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootfsPath != want {
		t.Fatalf("the VM boots %q, want its own rootfs at %q", cfg.RootfsPath, want)
	}
	if cfg.BaseImagePath != m.opts.BaseRootfs {
		t.Errorf("base image = %q, want %q", cfg.BaseImagePath, m.opts.BaseRootfs)
	}
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("the session's rootfs was not created: %v", err)
	}

	// Exactly one clone, from the environment image to this session's rootfs.
	if got := cloner.sources(); len(got) != 1 || got[0] != m.opts.BaseRootfs {
		t.Fatalf("cloned %v, want one copy of the environment image %q", got, m.opts.BaseRootfs)
	}
	if cloner.clones[0].dst != want {
		t.Errorf("cloned into %q, want %q", cloner.clones[0].dst, want)
	}

	// The workspace and the agent home are devices of their own and were
	// never copied. A driver that cloned either would be putting one tenant's
	// files on the path to a published environment image.
	for _, never := range []string{cfg.WorkspaceDiskPath, cfg.HomeDiskPath} {
		if never == "" {
			t.Fatal("this create was meant to carry both a workspace and an agent home")
		}
		for _, p := range cloner.clones {
			if p.src == never || p.dst == never {
				t.Errorf("%s was cloned; the workspace and the agent home are separate devices and are never part of a rootfs", never)
			}
		}
	}

	// And the image itself is still what it was: a session writes to its copy.
	if data, err := os.ReadFile(m.opts.BaseRootfs); err != nil || !strings.Contains(string(data), "not a real ext4") {
		t.Errorf("the environment image was modified by a create: %q, %v", data, err)
	}
}

// TestMicrovmRootfsIsRemovedOnEveryTeardown walks the three paths that end a
// VM — the crash path, the explicit teardown, and a cold park — and pins that
// each takes the session's rootfs with it. A copy of an environment image per
// dead session is an environment image's worth of NVMe per dead session.
func TestMicrovmRootfsIsRemovedOnEveryTeardown(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		end  func(t *testing.T, m *Microvm, id string)
		// keepsWorkspace says whether the session's workspace must survive
		// this teardown, which is the other half of what each path means.
		keepsWorkspace bool
	}{
		{
			name: "DestroyContainer",
			end: func(t *testing.T, m *Microvm, id string) {
				if err := m.DestroyContainer(ctx, id); err != nil {
					t.Fatalf("DestroyContainer: %v", err)
				}
			},
			keepsWorkspace: true,
		},
		{
			name: "Destroy",
			end: func(t *testing.T, m *Microvm, id string) {
				if err := m.Destroy(ctx, id); err != nil {
					t.Fatalf("Destroy: %v", err)
				}
			},
		},
		{
			name: "cold Suspend",
			end: func(t *testing.T, m *Microvm, id string) {
				if err := m.Suspend(ctx, id, false); err != nil {
					t.Fatalf("Suspend cold: %v", err)
				}
			},
			keepsWorkspace: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, _ := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 2})
			h, err := m.Create(ctx, Spec{SessionID: "sess-teardown"})
			if err != nil {
				t.Fatal(err)
			}
			rootfs, err := m.sessionRootfsPath(h.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(rootfs); err != nil {
				t.Fatalf("no rootfs to tear down: %v", err)
			}

			tc.end(t, m, h.ID)

			if _, err := os.Stat(rootfs); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("%s left the session's root filesystem at %s", tc.name, rootfs)
			}
			// A warm pause is the one suspension that keeps it, because the
			// VM is still there with the file open.
			if got := workspaceExists(t, m, "sess-teardown"); got != tc.keepsWorkspace {
				t.Errorf("after %s the workspace exists = %v, want %v", tc.name, got, tc.keepsWorkspace)
			}
		})
	}
}

// TestMicrovmWarmSuspendKeepsTheRootfs is the asymmetry that makes the test
// above mean something: a warm pause freezes the VM in place, and the file it
// has open is not something to unlink underneath it.
func TestMicrovmWarmSuspendKeepsTheRootfs(t *testing.T) {
	m, _, _ := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-warm"})
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err := m.sessionRootfsPath(h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rootfs); err != nil {
		t.Fatalf("a warm pause removed the root filesystem of a VM that is still running: %v", err)
	}
}

// TestMicrovmColdResumeClonesAFreshRootfs is ADR-0003 §2.2 and §4.1 in one
// assertion: a resumed session is a fresh boot with preserved FILES. What the
// parked session wrote into its root is gone; the workspace is not.
func TestMicrovmColdResumeClonesAFreshRootfs(t *testing.T) {
	m, sim, cloner := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 2})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-resume"})
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err := m.sessionRootfsPath(h.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Something the guest wrote into its root filesystem.
	if err := os.WriteFile(rootfs, []byte("state the guest wrote into its root"), microvmFileMode); err != nil {
		t.Fatal(err)
	}
	workspacePath, err := m.workspaceDiskPath("sess-resume")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workspacePath, []byte("an hour of the user's work"), microvmFileMode); err != nil {
		t.Fatal(err)
	}

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted {
		t.Fatal("a cold resume reported no restart")
	}

	// Two clones now: the create's and the resume's, both from the same
	// environment image.
	if got := cloner.sources(); len(got) != 2 || got[1] != m.opts.BaseRootfs {
		t.Fatalf("clones = %v, want a second copy of %q on the resume", got, m.opts.BaseRootfs)
	}
	data, err := os.ReadFile(rootfs)
	if err != nil {
		t.Fatalf("the resumed session has no root filesystem: %v", err)
	}
	if strings.Contains(string(data), "state the guest wrote") {
		t.Error("the cold resume kept the parked session's root filesystem; a resume is a fresh boot with preserved files, and only the workspace persists")
	}
	// The workspace is the part that persists, and the resumed VM is still
	// attached to it.
	ws, err := os.ReadFile(workspacePath)
	if err != nil || !strings.Contains(string(ws), "an hour of the user's work") {
		t.Errorf("the cold resume did not preserve the workspace: %q, %v", ws, err)
	}
	cfg, _ := sim.Config(h.ID)
	if cfg.WorkspaceDiskPath != workspacePath {
		t.Errorf("the resumed VM attached %q, want the session's workspace %q", cfg.WorkspaceDiskPath, workspacePath)
	}
}

// TestMicrovmCreateLeavesNoRootfsWhenItFails: a create that got as far as the
// clone and then could not launch must not leave a copy of an environment
// image behind for nobody.
func TestMicrovmCreateLeavesNoRootfsWhenItFails(t *testing.T) {
	stateDir := shortTempDir(t)
	m, sim, _ := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 2, StateDir: stateDir})
	sim.FailLaunch(errors.New("firecracker: no such device"))
	ctx := context.Background()

	if _, err := m.Create(ctx, Spec{SessionID: "sess-failed"}); err == nil {
		t.Fatal("Create succeeded with an engine that refuses to launch")
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "rootfs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		slices.Sort(names)
		t.Errorf("a failed create left %v behind in rootfs/", names)
	}
}

// TestMicrovmCreateRefusesWhenTheCloneFails is the fail-closed half: a host
// that cannot make the copy has no root filesystem to boot, and a session that
// booted anyway would be one sharing the environment image with every other
// session of that environment — writable.
func TestMicrovmCreateRefusesWhenTheCloneFails(t *testing.T) {
	c := &fakeCloner{err: errors.New("no space left on device")}
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2, Clone: c})
	ctx := context.Background()

	_, err := m.Create(ctx, Spec{SessionID: "sess-noclone"})
	if err == nil {
		t.Fatal("Create succeeded on a host that could not copy the environment image")
	}
	if !strings.Contains(err.Error(), "clone the environment image") {
		t.Errorf("error = %q, want it to name the clone", err)
	}
	if used, _, _ := m.Capacity(ctx); used != 0 {
		t.Errorf("a failed create left %d slot(s) occupied", used)
	}
}
