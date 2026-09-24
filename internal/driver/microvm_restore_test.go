// internal/driver/microvm_restore_test.go
package driver

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tokencanopy/rainier/checkpoint"
)

// The deep-dormant resume (ADR-0003 §2.3): the workspace image has been
// deleted and the checkpoint is the only copy of the session's work left.
//
// The rule every test here is about: the restore happens when the image is
// ABSENT and never otherwise, it leaves nothing behind, and a resume that
// cannot produce a workspace refuses rather than booting an empty one.

// restoredTree reads back what the simulated formatter recorded, which is the
// tree the restore actually handed to mkfs.
func restoredTree(t *testing.T, imagePath string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(imagePath)
	if err != nil {
		t.Fatalf("reading the rebuilt workspace image: %v", err)
	}
	out := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("reading the rebuilt workspace image: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		switch hdr.Typeflag {
		case tar.TypeSymlink:
			out[hdr.Name] = "-> " + hdr.Linkname
		default:
			out[hdr.Name] = string(body)
		}
	}
}

// suspendedWithCheckpoint is a session that has been cold suspended, so its
// checkpoint is committed and verified, with its workspace image then REMOVED
// the way a deep-dormant transition removes it.
func suspendedWithCheckpoint(t *testing.T) (*Microvm, *streamingHost, string, string) {
	t.Helper()
	m, _, host, id, stateDir := checkpointScene(t, nil)
	if err := m.Suspend(context.Background(), id, false); err != nil {
		t.Fatalf("the cold suspend failed: %v", err)
	}
	return m, host, id, stateDir
}

// TestAColdResumeRestoresTheWorkspaceWhenTheImageIsGone is the whole feature:
// the disk is gone, the checkpoint is not, and the session comes back with its
// files.
func TestAColdResumeRestoresTheWorkspaceWhenTheImageIsGone(t *testing.T) {
	m, _, id, _ := suspendedWithCheckpoint(t)
	ctx := context.Background()

	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatalf("removing the workspace image: %v", err)
	}
	disk, err := m.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the workspace image is still there: %v", err)
	}

	restarted, err := m.Resume(ctx, id)
	if err != nil {
		t.Fatalf("the cold resume failed: %v", err)
	}
	if !restarted {
		t.Error("the cold resume did not report a restart")
	}

	tree := restoredTree(t, disk)
	want := map[string]string{
		"README.md":   "hello " + workspaceMarker,
		"src":         "",
		"src/main.go": "package main // " + workspaceMarker,
		"entry":       "-> src/main.go",
	}
	if len(tree) != len(want) {
		t.Fatalf("the rebuilt workspace holds %v, want %v", tree, want)
	}
	for name, body := range want {
		if tree[name] != body {
			t.Errorf("the rebuilt workspace holds %q at %s, want %q", tree[name], name, body)
		}
	}
	// Nothing Rainier owns came back, because nothing Rainier owns went in.
	for name := range tree {
		if strings.HasPrefix(name, ".rainier") {
			t.Errorf("the rebuilt workspace holds %s", name)
		}
	}
	assertNoScratch(t, m, id)
}

// TestAColdResumeLeavesAWorkspaceThatIsStillThereAlone. The ordinary cold
// resume is unchanged: a parked session's disk is right where it was, and
// restoring over it would replace a newer filesystem with an older copy of it.
func TestAColdResumeLeavesAWorkspaceThatIsStillThereAlone(t *testing.T) {
	m, _, id, _ := suspendedWithCheckpoint(t)
	ctx := context.Background()

	disk, err := m.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(disk)
	if err != nil {
		t.Fatalf("the parked session's workspace image is missing: %v", err)
	}

	if _, err := m.Resume(ctx, id); err != nil {
		t.Fatalf("the cold resume failed: %v", err)
	}

	after, err := os.Stat(disk)
	if err != nil {
		t.Fatalf("the workspace image went away: %v", err)
	}
	if after.ModTime() != before.ModTime() || after.Size() != before.Size() {
		t.Error("the cold resume rebuilt a workspace image that was still there")
	}
	assertNoScratch(t, m, id)
}

// TestAColdResumeRefusesWhenThereIsNoCheckpoint. An empty workspace that looks
// like a successful resume is the failure a person discovers by finding their
// work missing.
func TestAColdResumeRefusesWhenThereIsNoCheckpoint(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, _, _, id, _ := checkpointScene(t, store)
	ctx := context.Background()

	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	// The checkpoint the suspend took is forgotten, standing in for a session
	// whose image went away without one ever having been committed.
	m.mu.Lock()
	m.instances[id].CheckpointGeneration = 0
	m.mu.Unlock()
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}

	_, err := m.Resume(ctx, id)
	if err == nil {
		t.Fatal("a resume with no workspace and no checkpoint reported success")
	}
	if !strings.Contains(err.Error(), "nothing to restore") {
		t.Errorf("the refusal is %v", err)
	}
}

// TestAColdResumeRefusesATamperedCheckpointAndLeavesNothingBehind: the
// checkpoint committed, somebody with write access to the store changed it, and
// what comes back is not the tenant's work. No image, no scratch directory, no
// VM.
func TestAColdResumeRefusesATamperedCheckpointAndLeavesNothingBehind(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, sim, _, id, _ := checkpointScene(t, store)
	ctx := context.Background()

	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}
	// One flipped bit in the content object, which is exactly what the frame
	// AEAD exists to catch.
	for _, k := range store.Keys() {
		if !strings.Contains(k, "/content.") {
			continue
		}
		b, _ := store.Object(k)
		b[len(b)/2] ^= 0xff
		if err := store.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
		if err := store.PutIfAbsent(ctx, k, func(w io.Writer) error {
			_, err := w.Write(b)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}

	_, err := m.Resume(ctx, id)
	if err == nil {
		t.Fatal("a tampered checkpoint was restored")
	}
	if !strings.Contains(err.Error(), "checkpoint restore stage") {
		t.Errorf("the refusal does not name the stage: %v", err)
	}
	disk, err := m.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk); !errors.Is(err, os.ErrNotExist) {
		t.Error("a refused restore left a workspace image behind")
	}
	assertNoScratch(t, m, id)
	if st, _ := sim.State(ctx, id); st == VMMStateRunning {
		t.Error("a refused restore started the VM anyway")
	}
}

// TestAFailedMkfsLeavesNoHalfBuiltImage. A file that exists is a file the next
// create finds with Stat and hands to a guest as a filesystem, so a mkfs that
// failed takes its image with it.
func TestAFailedMkfsLeavesNoHalfBuiltImage(t *testing.T) {
	m, _, id, _ := suspendedWithCheckpoint(t)
	ctx := context.Background()
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}
	m.format = failingFormatter{}

	_, err := m.Resume(ctx, id)
	if err == nil {
		t.Fatal("a resume whose mkfs failed reported success")
	}
	if !strings.Contains(err.Error(), "checkpoint mkfs stage") {
		t.Errorf("the refusal does not name the stage: %v", err)
	}
	disk, err := m.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk); !errors.Is(err, os.ErrNotExist) {
		t.Error("a failed mkfs left a half-built workspace image behind")
	}
	assertNoScratch(t, m, id)
}

// TestARestoreGivesTheTreeToTheGuestsUser. A checkpoint records modes and not
// owners, so a restored tree belongs to whoever restored it — root, on a real
// host — and the guest's agent runs as the session image's user. Without this
// the session comes back unable to write its own files.
//
// The uid under test is this process's own, because a test that could give a
// file away would be a test running as root.
func TestARestoreGivesTheTreeToTheGuestsUser(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := chownTree(root, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("chowning a restored tree: %v", err)
	}
	// And 0,0 is "leave it", which is what a host whose guest runs as root
	// wants and what keeps this path out of a test's way.
	if err := chownTree(root, 0, 0); err != nil {
		t.Fatalf("chowning to 0:0 should be a no-op: %v", err)
	}
	if err := chownTree(filepath.Join(root, "nope"), os.Getuid(), os.Getgid()); err == nil {
		t.Error("chowning a tree that is not there reported success")
	}
}

// TestARestoreThatCannotGiveTheTreeAwayFailsClosed is the ownership step at its
// CALL SITE rather than as a function: a restore that could not hand the files
// to the guest's user would come back with a workspace the agent cannot write,
// which is a session that looks resumed and is not. It refuses instead, and
// leaves no image behind.
func TestARestoreThatCannotGiveTheTreeAwayFailsClosed(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, which can give a file to anybody; the refusal under test is EPERM")
	}
	store := checkpoint.NewMemoryStore()
	keys, err := checkpoint.NewStaticKeyWrapper("selfhosted/checkpoint/v1", [32]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	stateDir := shortTempDir(t)
	m, _ := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Checkpoint: &CheckpointOpts{
			Store: store, Keys: keys, KeyRef: keys.Ref(),
			// A uid this unprivileged test process is not and cannot give a
			// file to. In production it is the session image's own user; what
			// is under test is what happens when the chown is refused.
			OwnerUID: otherUID(), OwnerGID: otherUID(),
		},
	})
	m.SetHost(&streamingHost{root: guestWorkspace(t)})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-ckpt", BootstrapToken: "token_example"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}

	_, err = m.Resume(ctx, h.ID)
	if err == nil {
		t.Fatal("a restore that could not give the tree to the guest's user reported success")
	}
	if !strings.Contains(err.Error(), "checkpoint owner stage") {
		t.Fatalf("the refusal does not name the stage: %v", err)
	}
	// And it says nothing about which file it was on: no name from inside the
	// workspace, and no path at all — the scratch directory's own path would
	// name the instance and then the tenant's tree.
	for _, leak := range []string{"README", "main.go", "/"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("the refusal carries %q, which is a path or a name from inside the workspace: %v", leak, err)
		}
	}
	disk, err := m.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk); !errors.Is(err, os.ErrNotExist) {
		t.Error("a refused restore left a workspace image behind")
	}
	assertNoScratch(t, m, h.ID)
}

// otherUID is a uid this process is not, so that an Lchown to it is refused.
func otherUID() int {
	if os.Getuid() == 65534 {
		return 65533
	}
	return 65534
}

// TestAHalfBuiltImageIsNeverAtTheFinalPath. The trigger for restoring at all is
// the image's ABSENCE, so a 10 GiB file created at the final path and then
// populated by a mkfs measured in minutes is, for those minutes, a file a crash
// leaves behind — and the next resume would see it, skip the restore, and hand
// the guest an unformatted disk its own /init would helpfully format EMPTY.
func TestAHalfBuiltImageIsNeverAtTheFinalPath(t *testing.T) {
	m, _, id, _ := suspendedWithCheckpoint(t)
	ctx := context.Background()
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}
	disk, err := m.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	// A formatter that looks at the world from inside mkfs: the final path must
	// not exist while the filesystem is still being built.
	watching := &watchingFormatter{final: disk}
	m.format = watching

	if _, err := m.Resume(ctx, id); err != nil {
		t.Fatalf("the cold resume failed: %v", err)
	}
	if watching.finalExisted {
		t.Error("the workspace image was at its final path while mkfs was still populating it")
	}
	if _, err := os.Stat(disk); err != nil {
		t.Errorf("the finished image is not at its final path: %v", err)
	}
	// And a mkfs that fails leaves neither the final name nor the partial one.
	// The session is parked again first: a Resume of a RUNNING session restarts
	// nothing and would prove nothing here.
	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}
	m.format = failingFormatter{}
	if _, err := m.Resume(ctx, id); err == nil {
		t.Fatal("a failed mkfs reported success")
	}
	for _, p := range []string{disk, disk + ".partial"} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a failed mkfs left %s behind", filepath.Base(p))
		}
	}
}

// watchingFormatter records whether the FINAL image path existed at the moment
// the filesystem was being written.
type watchingFormatter struct {
	SimulatedDiskFormatter
	final        string
	finalExisted bool
}

func (w *watchingFormatter) FormatFromDir(path, dir string) error {
	if _, err := os.Stat(w.final); err == nil {
		w.finalExisted = true
	}
	return w.SimulatedDiskFormatter.FormatFromDir(path, dir)
}

// TestRemoveWorkspaceKeepsTheCheckpoint. Deleting a checkpoint is a retention
// decision, and this driver does not have the policy: it does not know how old
// the checkpoint is, whether a deep-dormant tier is relying on it, or whether
// a restore is in flight somewhere else. RemoveWorkspace removes the IMAGE.
func TestRemoveWorkspaceKeepsTheCheckpoint(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, _, _, id, _ := checkpointScene(t, store)
	ctx := context.Background()
	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	before := store.Keys()

	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}

	if after := store.Keys(); len(after) != len(before) {
		t.Errorf("RemoveWorkspace left %v in the store, want %v", after, before)
	}
	rec, _ := m.diskRecord(id)
	if rec.CheckpointGeneration != 1 {
		t.Errorf("RemoveWorkspace forgot the checkpoint generation (%d)", rec.CheckpointGeneration)
	}
}

// TestAResumeWithNoStoreSaysSoRatherThanBootingEmpty: a runner with no
// checkpoint configuration cannot restore anything, and a session whose image
// is gone is not one it can bring back.
func TestAResumeWithNoStoreSaysSoRatherThanBootingEmpty(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-nostore"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveWorkspace(ctx, "sess-nostore"); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Resume(ctx, h.ID); err == nil {
		t.Fatal("a resume with no workspace and no store reported success")
	} else if !strings.Contains(err.Error(), "no checkpoint store") {
		t.Errorf("the refusal is %v", err)
	}
}

// assertNoScratch is the rule every path above shares: the directory a restore
// unpacks a tenant's files into does not outlive the restore.
func assertNoScratch(t *testing.T, m *Microvm, id string) {
	t.Helper()
	dir := filepath.Join(m.opts.StateDir, "restore", id)
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the restore scratch directory %s is still there (%v)", dir, err)
	}
}

// TestTheScratchDirectoryIsPrivateAndReplaced.
func TestTheScratchDirectoryIsPrivateAndReplaced(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	dir, err := m.scratchDir("i-scratch")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("the scratch directory is mode %v, want 0700", info.Mode().Perm())
	}
	// A tree a previous attempt left behind is replaced rather than restored
	// into: the library refuses a non-empty target, and a half-restored tree is
	// not one to build a filesystem from.
	if err := os.WriteFile(filepath.Join(dir, "leftover"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := m.scratchDir("i-scratch")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(again)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("the scratch directory still holds %d entries from a previous attempt", len(entries))
	}
	if _, err := m.scratchDir("../escape"); err == nil {
		t.Error("an instance id that is not a path segment was accepted")
	}
}

// recordingFormatter proves the restore path is taken only when it should be.
type recordingFormatter struct {
	SimulatedDiskFormatter
	mu    sync.Mutex
	dirs  []string
	paths []string
}

func (r *recordingFormatter) FormatFromDir(path, dir string) error {
	r.mu.Lock()
	r.dirs = append(r.dirs, dir)
	r.paths = append(r.paths, path)
	r.mu.Unlock()
	return r.SimulatedDiskFormatter.FormatFromDir(path, dir)
}

func (r *recordingFormatter) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.dirs)
}

// TestTheRestorePathIsTakenOnlyWhenTheImageIsAbsent, counted at the formatter
// rather than inferred from a file: the assertion is about which code ran.
func TestTheRestorePathIsTakenOnlyWhenTheImageIsAbsent(t *testing.T) {
	format := &recordingFormatter{}
	stateDir := shortTempDir(t)
	keys, err := checkpoint.NewStaticKeyWrapper("selfhosted/checkpoint/v1", [32]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Format:     format,
		Checkpoint: &CheckpointOpts{Store: checkpoint.NewMemoryStore(), Keys: keys, KeyRef: keys.Ref()},
	})
	m.SetHost(&streamingHost{root: guestWorkspace(t)})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-ckpt", BootstrapToken: "token_example"})
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Resume(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if format.calls() != 0 {
		t.Fatalf("a resume with its workspace image still there rebuilt it %d time(s)", format.calls())
	}

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Resume(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if format.calls() != 1 {
		t.Fatalf("a resume with no workspace image rebuilt it %d time(s), want 1", format.calls())
	}
}

// restartedOver is the same host coming back: a second driver over the state
// directory a previous one left, which recovers the record off disk and has no
// guest configuration behind it (ADR-0003 §2.7 item 1).
func restartedOver(t *testing.T, stateDir string, ckpt *CheckpointOpts) (*Microvm, *recordingFormatter) {
	t.Helper()
	format := &recordingFormatter{}
	m, _ := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Format:     format,
		Checkpoint: ckpt,
	})
	m.SetHost(&streamingHost{root: guestWorkspace(t)})
	return m, format
}

// TestAColdResumeAfterARestartSaysWhichThingItIsWaitingFor is the precondition
// the note's §7 now states, held as a test rather than as prose.
//
// A session dormant long enough for the deep-dormant tier to have deleted its
// workspace image has almost certainly outlived the runnerd that created it —
// so the restore path is behind the refusal that a recovered record has no
// guest configuration, and no ordering of the two changes that: this driver
// cannot invent a session id, a proxy or a boot chain, and booting a guest that
// was never told what it is is the outcome the refusal exists for.
//
// What it CAN do is say which of the two things it is waiting for, and spend
// nothing finding out. So: the refusal names both, no restore runs, and no
// tenant tree is unpacked onto this host for a resume that is certain to fail.
func TestAColdResumeAfterARestartSaysWhichThingItIsWaitingFor(t *testing.T) {
	keys, err := checkpoint.NewStaticKeyWrapper("selfhosted/checkpoint/v1", [32]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	ckpt := &CheckpointOpts{Store: checkpoint.NewMemoryStore(), Keys: keys, KeyRef: keys.Ref()}
	stateDir := shortTempDir(t)
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir, Checkpoint: ckpt})
	m.SetHost(&streamingHost{root: guestWorkspace(t)})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-ckpt", BootstrapToken: "token_example"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("the cold suspend failed: %v", err)
	}
	// The deep-dormant transition: the image goes, the checkpoint stays.
	if err := m.RemoveWorkspace(ctx, "sess-ckpt"); err != nil {
		t.Fatal(err)
	}

	again, format := restartedOver(t, stateDir, ckpt)
	_, err = again.Resume(ctx, h.ID)
	if err == nil {
		t.Fatal("a cold resume of a recovered record reported success")
	}
	for _, want := range []string{"did not survive a runnerd restart", "generation 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
	// Nothing was spent on it: no mkfs, no scratch tree, and the image is
	// still absent rather than half built.
	if format.calls() != 0 {
		t.Errorf("the refused resume ran %d restore(s)", format.calls())
	}
	assertNoScratch(t, again, h.ID)
	disk, err := again.workspaceDiskPath("sess-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused resume left something at the workspace image path (%v)", err)
	}
}

// TestAColdResumeAfterARestartWithNoCheckpointSaysThatToo is the same refusal
// for the session nothing can recover: the image is gone and no checkpoint of
// it was ever committed. Said plainly, because the alternative is a person
// discovering it by finding their work missing.
func TestAColdResumeAfterARestartWithNoCheckpointSaysThatToo(t *testing.T) {
	stateDir := shortTempDir(t)
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-nockpt"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveWorkspace(ctx, "sess-nockpt"); err != nil {
		t.Fatal(err)
	}

	keys, err := checkpoint.NewStaticKeyWrapper("selfhosted/checkpoint/v1", [32]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	again, format := restartedOver(t, stateDir, &CheckpointOpts{
		Store: checkpoint.NewMemoryStore(), Keys: keys, KeyRef: keys.Ref(),
	})
	_, err = again.Resume(ctx, h.ID)
	if err == nil {
		t.Fatal("a cold resume of a recovered record with no workspace reported success")
	}
	if !strings.Contains(err.Error(), "no checkpoint of it was ever committed") {
		t.Errorf("the refusal does not say the work is gone: %v", err)
	}
	if format.calls() != 0 {
		t.Errorf("the refused resume ran %d restore(s)", format.calls())
	}
}

// TestAPreviousRunsRestoreScratchIsRemovedAtStartup is reclaimRestoreScratch,
// which had no test: what is under <state>/restore is a tenant's whole
// workspace in plaintext, and the note's §7 is only allowed to call the scratch
// directory "no new exposure" because nothing leaves one behind. A host that
// was powered off mid-restore leaves one behind, so the next start removes it.
func TestAPreviousRunsRestoreScratchIsRemovedAtStartup(t *testing.T) {
	stateDir := shortTempDir(t)
	planted := filepath.Join(stateDir, "restore", "i-old")
	if err := os.MkdirAll(filepath.Join(planted, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(planted, "src", "f"), []byte(workspaceMarker), 0o600); err != nil {
		t.Fatal(err)
	}

	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})

	if _, err := os.Stat(planted); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a previous run's restore scratch tree survived the start (%v)", err)
	}
	// And nothing of it is anywhere else under the state directory either.
	assertNoWorkspacePlaintext(t, m.opts.StateDir)
}

// assertNoWorkspacePlaintext walks a state directory for the fixture
// workspace's marker: if it is there, this host has a tenant's files on its own
// disk in the clear.
func assertNoWorkspacePlaintext(t *testing.T, stateDir string) {
	t.Helper()
	err := filepath.WalkDir(stateDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		// The workspace disk image is a 10 GiB sparse file; reading it whole
		// would be the test's own problem rather than the driver's.
		if info.Size() > 1<<20 {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), workspaceMarker) {
			return errors.New(strings.TrimPrefix(path, stateDir) + " holds workspace plaintext")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

// TestTheScratchDirectorysRemovalErrorNamesNoPath. What scratchDir's RemoveAll
// fails on is an entry INSIDE a previous restore of this workspace, so its
// *fs.PathError names a tenant's file — and this error reaches a session's
// error column (tenancy §15.1).
func TestTheScratchDirectorysRemovalErrorNamesNoPath(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can remove a directory whatever its mode, so there is no failure to observe")
	}
	stateDir := shortTempDir(t)
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir})

	const secret = "a-tenant-file-name-that-must-not-travel"
	locked := filepath.Join(stateDir, "restore", "i-scratch", secret)
	if err := os.MkdirAll(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "child"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Readable and searchable, not writable: the walk finds the child and
	// cannot unlink it.
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })

	_, err := m.scratchDir("i-scratch")
	if err == nil {
		t.Fatal("scratchDir succeeded over a directory it cannot clear")
	}
	for _, forbidden := range []string{secret, "child", stateDir, "/"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Errorf("the error carries %q out of the workspace: %v", forbidden, err)
		}
	}
}
