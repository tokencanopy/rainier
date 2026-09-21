// internal/driver/microvm_teardown_test.go
//
// Destroy's two halves, and the id that used to fall between them.
package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMicrovmDestroyRemovesTheWorkspaceOfARecordOnlyOnDisk is the leak.
//
// Destroy resolved the session id from the in-memory record only, and an id
// this driver did not know resolved to "". RemoveWorkspace treats an empty id
// as a no-op — it must, since "rainier-ws-" alone is a real volume name — so
// the teardown reported success and left a tenant's workspace disk on the host
// with nothing left to name it. Here the live record is dropped and the disk
// one is not, which is the shape of a Destroy arriving after a reconcile.
func TestMicrovmDestroyRemovesTheWorkspaceOfARecordOnlyOnDisk(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-orphan"})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := m.workspaceDiskPath("sess-orphan")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace); err != nil {
		t.Fatalf("no workspace to leak: %v", err)
	}

	// The live record goes, the one on disk stays: the state a driver is in
	// when a record failed to parse at startup, or when something removed the
	// entry without finishing the teardown.
	m.mu.Lock()
	delete(m.instances, h.ID)
	m.mu.Unlock()

	rootfs, err := m.sessionRootfsPath(h.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("Destroy of an id only the disk knows: %v", err)
	}
	if _, err := os.Stat(workspace); err == nil {
		t.Fatalf("Destroy left the workspace disk at %s with nothing left to name it", workspace)
	}
	if workspaceExists(t, m, "sess-orphan") {
		t.Error("the workspace volume is still listed after a full teardown")
	}

	// And the rest of the session went with it. A teardown that removed only
	// the workspace left the VM running, its jail standing, its rootfs copy on
	// disk and its record in place — so the NEXT runnerd recovered that record,
	// reported the session running, and held a capacity slot for good, for a
	// session whose files this very call had just deleted.
	if _, err := os.Stat(rootfs); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Destroy left the session's root filesystem at %s: %v", rootfs, err)
	}
	if _, err := os.Stat(m.instanceMetaPath(h.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Destroy left the instance record at %s: %v", m.instanceMetaPath(h.ID), err)
	}
	if st, _ := m.engine.State(ctx, h.ID); st != VMMStateStopped && st != VMMStateGone {
		t.Errorf("Destroy left the VM %s; a tenant's guest kept running on an unlinked workspace", st)
	}

	// The restart proves it: nothing comes back.
	restarted, _, _ := testMicrovmCloning(t, MicrovmOpts{
		TotalSlots: 2, StateDir: m.opts.StateDir, BaseRootfs: m.opts.BaseRootfs,
	})
	listed, err := restarted.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 {
		t.Errorf("a destroyed session came back after a restart: %+v", listed)
	}
	if used, _, _ := restarted.Capacity(ctx); used != 0 {
		t.Errorf("a destroyed session is still holding %d capacity slot(s)", used)
	}
}

// TestMicrovmDestroyOfAnUnknownIdIsAnError is the other half: an id that names
// nothing anywhere cannot be resolved to a session, and reporting success
// would be this driver claiming to have removed files it never found. The
// message names the call that CAN finish the job.
func TestMicrovmDestroyOfAnUnknownIdIsAnError(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()

	err := m.Destroy(ctx, "mvm-404")
	if err == nil {
		t.Fatal("Destroy of an id this host has never seen reported success")
	}
	for _, want := range []string{"mvm-404", "RemoveWorkspace"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}

	// And it is still the crash path's business to be a no-op: reclaiming a
	// slot for a container that is already gone is a teardown that ran second,
	// not a failure.
	if err := m.DestroyContainer(ctx, "mvm-404"); err != nil {
		t.Errorf("DestroyContainer of an unknown id = %v, want nil", err)
	}
}

// TestMicrovmDestroyRemovesNothingItCannotName pins the fence the leak fix
// must not have widened: a session id that could name a path outside the state
// directory is refused, and Destroy's new disk lookup is not a way around it.
func TestMicrovmDestroyRemovesNothingItCannotName(t *testing.T) {
	stateDir := shortTempDir(t)
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2, StateDir: stateDir})
	ctx := context.Background()

	// A file outside the state directory that no driver call may reach.
	outside := filepath.Join(filepath.Dir(stateDir), "rainier-ws-victim.ext4")
	if err := os.WriteFile(outside, []byte("someone else's data"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(outside) })

	// A record on disk naming a hostile session id, which is what a corrupted
	// or hand-edited state directory looks like.
	rec := instanceRecord{ID: "mvm-77", SessionID: "../victim", State: StateSuspended, Cold: true}
	if err := m.saveRecord(rec); err != nil {
		t.Fatal(err)
	}

	if err := m.Destroy(ctx, "mvm-77"); err == nil {
		t.Fatal("Destroy accepted a record naming a session id outside the state directory")
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("a file outside the state directory was removed: %v", err)
	}
}
