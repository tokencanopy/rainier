// internal/driver/microvm_snapshot_test.go
//
// Publishing a session's root filesystem as an environment image: what goes
// into it, what cannot, what the guest is asked first, and what the rest of
// the host is doing while it happens.
package driver

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMicrovmSnapshotPublishesTheRootfsByDigest is the happy path end to end:
// the guest is flushed, the VM is paused, the rootfs is copied into the image
// store under its own digest, the ref comes back verbatim, and the session is
// running again when it returns.
func TestMicrovmSnapshotPublishesTheRootfsByDigest(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	host := &stubMicrovmHost{}
	m.SetHost(host)
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-snap", BootstrapToken: "token_example"})
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err := m.sessionRootfsPath(h.ID)
	if err != nil {
		t.Fatal(err)
	}
	// What the session's setup script "installed".
	const built = "a toolchain the setup script installed"
	if err := os.WriteFile(rootfs, []byte(built), microvmFileMode); err != nil {
		t.Fatal(err)
	}

	const ref = "rainier-env:e1-abc123"
	snap, err := m.Snapshot(ctx, h.ID, ref, nil)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Ref != ref {
		t.Fatalf("snap.Ref = %q, want %q verbatim", snap.Ref, ref)
	}

	// The guest was asked to flush, exactly once, before anything was copied.
	if got := host.flushCount(); got != 1 {
		t.Errorf("the guest was flushed %d times, want 1", got)
	}

	// The image is in the store under the digest of its own bytes.
	manifest, ok := m.images.manifest(ref)
	if !ok {
		t.Fatal("no manifest was published for the ref")
	}
	blob, ok := m.images.have(manifest.Digest)
	if !ok {
		t.Fatalf("the manifest names %s, which is not in the store", manifest.Digest)
	}
	data, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != built {
		t.Errorf("the published image is %q, want the session's root filesystem %q", data, built)
	}
	if manifest.SizeBytes != int64(len(built)) {
		t.Errorf("manifest size = %d, want %d", manifest.SizeBytes, len(built))
	}
	if manifest.InstanceID != h.ID {
		t.Errorf("manifest instance = %q, want %q", manifest.InstanceID, h.ID)
	}
	// 0600, like everything else this driver writes.
	fi, err := os.Stat(blob)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != microvmFileMode {
		t.Errorf("published image mode = %#o, want %#o", fi.Mode().Perm(), microvmFileMode)
	}

	// And the session is running again: a snapshot freezes a VM for the copy
	// and must never be what leaves a user's terminal frozen.
	if st, err := sim.State(ctx, h.ID); err != nil || st != VMMStateRunning {
		t.Errorf("after a snapshot the VM is %s (%v), want running", st, err)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateRunning {
		t.Errorf("after a snapshot the session reads as %s, want running", g.State)
	}
}

// TestMicrovmSnapshotExcludesTheWorkspaceAndHome is ADR-0003 §4.1's exclusion,
// proved rather than asserted: a marker is planted in each of the three
// devices, and only the root filesystem's reaches the published image.
//
// The Docker driver gets this for free (`docker commit` skips mounted
// volumes). Here it is by construction — the workspace and the agent home are
// separate block devices and there is no step that reads them — and the
// tenancy specification's snapshot-exclusion tests require it be shown for
// this driver too.
func TestMicrovmSnapshotExcludesTheWorkspaceAndHome(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "sess-markers",
		Home:      &HomeMount{Volume: "home-markers", Path: "/rainier/agents"},
	})
	if err != nil {
		t.Fatal(err)
	}

	const (
		rootMarker      = "ROOT-MARKER-environment-image"
		workspaceMarker = "WORKSPACE-MARKER-one-tenants-source-code"
		homeMarker      = "HOME-MARKER-a-credential-set"
	)
	rootfs, err := m.sessionRootfsPath(h.ID)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := m.workspaceDiskPath("sess-markers")
	if err != nil {
		t.Fatal(err)
	}
	home, err := m.homeDiskPath("home-markers")
	if err != nil {
		t.Fatal(err)
	}
	for path, marker := range map[string]string{
		rootfs:    rootMarker,
		workspace: workspaceMarker,
		home:      homeMarker,
	} {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		if _, err := f.WriteAt([]byte(marker), 0); err != nil {
			t.Fatalf("plant a marker in %s: %v", path, err)
		}
		f.Close()
	}

	const ref = "rainier-env:markers"
	if _, err := m.Snapshot(ctx, h.ID, ref, nil); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	manifest, ok := m.images.manifest(ref)
	if !ok {
		t.Fatal("no manifest was published")
	}
	blob, ok := m.images.have(manifest.Digest)
	if !ok {
		t.Fatal("the published digest is not in the store")
	}
	// Bounded: if this ever DID publish the workspace, that file is a 10 GiB
	// sparse image and reading it whole would take the test machine down
	// instead of failing the assertion. Every marker is at offset 0.
	f, err := os.Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(io.LimitReader(f, 1<<20))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), rootMarker) {
		t.Error("the published image does not carry the root filesystem's marker, so the assertions below prove nothing")
	}
	for what, marker := range map[string]string{
		"workspace":  workspaceMarker,
		"agent home": homeMarker,
	} {
		if strings.Contains(string(data), marker) {
			t.Errorf("the published environment image carries the %s's contents", what)
		}
	}
	// The other two devices are also still intact: a snapshot reads a file,
	// it does not move one.
	for _, path := range []string{workspace, home} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("a snapshot disturbed %s: %v", path, err)
		}
	}
}

// TestMicrovmSnapshotRefusesWhenTheGuestCannotBeFlushed: what is copied is the
// host's view of a file the guest has been writing into, so a guest that
// cannot be made to sync is a guest whose environment image would be missing
// whatever it had not written yet. Refusing costs the next session a re-run of
// its setup script; publishing would cost every later session of that
// environment an image nobody can vouch for.
func TestMicrovmSnapshotRefusesWhenTheGuestCannotBeFlushed(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	host := &stubMicrovmHost{flushErr: errors.New("the sandbox did not report a flush within 30s")}
	m.SetHost(host)
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-noflush"})
	if err != nil {
		t.Fatal(err)
	}

	const ref = "rainier-env:unflushed"
	if _, err := m.Snapshot(ctx, h.ID, ref, nil); err == nil {
		t.Fatal("Snapshot succeeded for a guest that never flushed")
	}
	if _, ok := m.images.manifest(ref); ok {
		t.Error("a snapshot that could not flush the guest published a manifest anyway")
	}
	if n := blobCount(t, m.images); n != 0 {
		t.Errorf("a refused snapshot left %d image(s) in the store", n)
	}
	if n := tempCount(t, m.images); n != 0 {
		t.Errorf("a refused snapshot left %d file(s) staged", n)
	}
	// The session is untouched: it was never paused, and it is still running.
	if st, _ := sim.State(ctx, h.ID); st != VMMStateRunning {
		t.Errorf("a refused snapshot left the VM %s", st)
	}
}

// TestMicrovmSnapshotResumesAFailedCommit: the VM is paused for the copy, and
// a failure between the pause and the publish must not leave a user's session
// frozen because somebody tried to cache an environment.
func TestMicrovmSnapshotResumesAFailedCommit(t *testing.T) {
	c := &fakeCloner{err: errors.New("no space left on device")}
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 2, Clone: c})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	// The create's own clone has to work, so the cloner refuses only from
	// here on.
	c.err = nil
	h, err := m.Create(ctx, Spec{SessionID: "sess-failsnap"})
	if err != nil {
		t.Fatal(err)
	}
	c.err = errors.New("no space left on device")

	if _, err := m.Snapshot(ctx, h.ID, "rainier-env:failed", nil); err == nil {
		t.Fatal("Snapshot succeeded with a cloner that refuses")
	}
	if st, err := sim.State(ctx, h.ID); err != nil || st != VMMStateRunning {
		t.Fatalf("a failed snapshot left the VM %s (%v); the session's terminal would have stopped answering", st, err)
	}
	if n := tempCount(t, m.images); n != 0 {
		t.Errorf("a failed snapshot left %d file(s) staged", n)
	}
}

// TestMicrovmSnapshotRefusesAColdParkedSession: a parked session has no root
// filesystem — it was discarded when the VM ended — and committing the base
// image under a new ref would publish an environment that never ran its setup.
func TestMicrovmSnapshotRefusesAColdParkedSession(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 2})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-parked"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	_, err = m.Snapshot(ctx, h.ID, "rainier-env:parked", nil)
	if err == nil {
		t.Fatal("Snapshot of a cold-parked session succeeded")
	}
	if !strings.Contains(err.Error(), "cold-parked") {
		t.Errorf("error = %q, want it to name the state", err)
	}
	if _, ok := m.images.manifest("rainier-env:parked"); ok {
		t.Error("a snapshot of a parked session published a manifest")
	}
}

// TestMicrovmSnapshotDoesNotHoldTheDriverMutex: a snapshot is a copy of a
// filesystem, and every other session on the host has to keep working while
// one is in flight. The lock is what Inspect, Capacity, Create and Destroy all
// take.
func TestMicrovmSnapshotDoesNotHoldTheDriverMutex(t *testing.T) {
	cloning := make(chan struct{})
	release := make(chan struct{})
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4, Clone: &blockingCloner{
		inner: &fakeCloner{}, blockOn: 2, entered: cloning, release: release,
	}})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-slowsnap"})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := m.Snapshot(ctx, h.ID, "rainier-env:slow", nil)
		done <- err
	}()
	select {
	case <-cloning:
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot never reached the copy")
	}

	// With the copy in flight, the rest of the host answers.
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		if _, _, err := m.Capacity(ctx); err != nil {
			t.Errorf("Capacity during a snapshot: %v", err)
		}
		if _, err := m.Inspect(ctx, h.ID); err != nil {
			t.Errorf("Inspect during a snapshot: %v", err)
		}
		if _, err := m.Create(ctx, Spec{SessionID: "sess-other"}); err != nil {
			t.Errorf("Create during a snapshot of another session: %v", err)
		}
	}()
	select {
	case <-answered:
	case <-time.After(5 * time.Second):
		t.Fatal("the driver mutex was held across the snapshot's copy; every other session on this host was blocked behind it")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
}

// TestMicrovmSnapshotDoesNotThawASessionSomebodyElseStopped: the snapshot
// pauses the VM for its copy and unpauses it afterwards, but a session that
// stopped being a running one while the copy ran is not the snapshot's to
// restart. Unconditionally resuming would thaw a VM its owner has just frozen.
func TestMicrovmSnapshotDoesNotThawASessionSomebodyElseStopped(t *testing.T) {
	cloning := make(chan struct{})
	release := make(chan struct{})
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4, Clone: &blockingCloner{
		inner: &fakeCloner{}, blockOn: 2, entered: cloning, release: release,
	}})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-racing"})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := m.Snapshot(ctx, h.ID, "rainier-env:racing", nil)
		done <- err
	}()
	select {
	case <-cloning:
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot never reached the copy")
	}

	// The session's owner freezes it while the copy is in flight.
	if err := m.Suspend(ctx, h.ID, true); err != nil {
		t.Fatalf("warm Suspend during a snapshot: %v", err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("the snapshot published an image for a session that stopped running while it was being copied")
	}
	if _, ok := m.images.manifest("rainier-env:racing"); ok {
		t.Error("a snapshot of a session that was suspended mid-copy published a manifest")
	}

	if st, _ := sim.State(ctx, h.ID); st != VMMStatePaused {
		t.Errorf("the VM is %s after a snapshot finished inside a warm suspend, want paused: the snapshot thawed a session its owner had just frozen", st)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateSuspended {
		t.Errorf("the session reads as %s, want suspended", g.State)
	}
}

// TestMicrovmSnapshotKeepsTheSessionRunningWhileItPublishes is the other side
// of the same pause: a VM this driver froze for a copy is not a suspended
// session, and it is unfrozen as soon as the copy exists rather than after the
// image has been digested and stored.
//
// Both halves have bitten. An Inspect landing inside a snapshot used to
// reconcile the record to "suspended" — the control plane would then see a
// working session as stopped, and the snapshot's own resume would stand down,
// because it declines to thaw a session somebody else stopped. And digesting a
// multi-gigabyte image with the guest still frozen is tens of seconds of a
// terminal that does not answer, per environment-cache build.
func TestMicrovmSnapshotKeepsTheSessionRunningWhileItPublishes(t *testing.T) {
	cloning := make(chan struct{})
	release := make(chan struct{})
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4, Clone: &blockingCloner{
		inner: &fakeCloner{}, blockOn: 2, entered: cloning, release: release,
	}})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-frozen"})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := m.Snapshot(ctx, h.ID, "rainier-env:frozen", nil)
		done <- err
	}()
	select {
	case <-cloning:
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot never reached the copy")
	}

	// Mid-copy the VM really is paused — that is the point of the pause — and
	// the SESSION is still a running one to everything above this driver.
	if st, _ := sim.State(ctx, h.ID); st != VMMStatePaused {
		t.Errorf("the VM is %s during the copy, want paused: nothing may write to the file being copied", st)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateRunning {
		t.Errorf("Inspect reads %s during a snapshot, want running: the control plane would see a working session as stopped", g.State)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if st, _ := sim.State(ctx, h.ID); st != VMMStateRunning {
		t.Errorf("the VM is %s after the snapshot, want running", st)
	}
	if _, ok := m.images.manifest("rainier-env:frozen"); !ok {
		t.Error("the snapshot published nothing")
	}
}

// TestMicrovmSnapshotResumesBeforeItDigestsTheImage pins the ORDER of the last
// two steps, which is the difference between a pause that lasts a reflink and
// one that lasts a read of the whole image.
//
// Digesting a multi-gigabyte rootfs is tens of seconds. The copy is a file of
// its own the moment the clone returns, so the guest is let go first and the
// store work happens behind it; a resume deferred to the end of the method
// instead would freeze a user's terminal for every environment-cache build.
func TestMicrovmSnapshotResumesBeforeItDigestsTheImage(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	gated := &gatedResumeEngine{SimulatedEngine: sim, entered: make(chan struct{}), release: make(chan struct{})}
	m.engine = gated
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-order"})
	if err != nil {
		t.Fatal(err)
	}

	const ref = "rainier-env:order"
	done := make(chan error, 1)
	go func() {
		_, err := m.Snapshot(ctx, h.ID, ref, nil)
		done <- err
	}()

	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot never resumed the VM")
	}
	// The resume is in flight, so the copy exists — and nothing has been
	// published yet. A snapshot that digested the image first would have the
	// manifest here and the guest still frozen.
	if _, ok := m.images.manifest(ref); ok {
		t.Error("the image was published before the VM was resumed; the guest stayed frozen for the digest of the whole image")
	}
	close(gated.release)

	if err := <-done; err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if _, ok := m.images.manifest(ref); !ok {
		t.Error("the snapshot published nothing")
	}
}

// gatedResumeEngine parks the first Resume until a test lets it go, so the
// order of "unfreeze the guest" and "store the image" can be asserted.
type gatedResumeEngine struct {
	*SimulatedEngine
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (e *gatedResumeEngine) Resume(ctx context.Context, id string) error {
	e.once.Do(func() {
		close(e.entered)
		<-e.release
	})
	return e.SimulatedEngine.Resume(ctx, id)
}

// blockingCloner parks the Nth clone until a test lets it go, standing in for
// a copy of a multi-gigabyte image.
type blockingCloner struct {
	inner   Cloner
	blockOn int
	n       int
	entered chan struct{}
	release chan struct{}
}

func (c *blockingCloner) Clone(src, dst string) (CloneMethod, error) {
	c.n++
	if c.n == c.blockOn {
		close(c.entered)
		<-c.release
	}
	return c.inner.Clone(src, dst)
}
