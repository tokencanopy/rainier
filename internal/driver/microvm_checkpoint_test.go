// internal/driver/microvm_checkpoint_test.go
package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tokencanopy/rainier/checkpoint"
	"github.com/tokencanopy/rainier/internal/wstream"
)

// The durability barrier, against the simulated engine, a guest that streams a
// real workspace, and an in-memory blob store.
//
// Every test here is about one rule: a cold suspend does not succeed until the
// manifest has committed AND verified, and every failure before that leaves the
// VM running.

// workspaceMarker is a string that exists only inside the fixture workspace. If
// it appears anywhere under the state directory, this host wrote a tenant's
// plaintext to its own disk.
const workspaceMarker = "marker-that-must-never-reach-host-disk"

// guestWorkspace builds the tree a fake guest streams.
func guestWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range map[string]string{
		"README.md":             "hello " + workspaceMarker,
		"src/main.go":           "package main // " + workspaceMarker,
		".rainier/session.json": `{"token":"must-not-travel"}`,
	} {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("src/main.go", filepath.Join(root, "entry")); err != nil {
		t.Fatal(err)
	}
	return root
}

// streamingHost is the runner above the driver, for the checkpoint path: it
// answers StreamWorkspace by streaming a real directory in the real format, so
// what the driver's barrier consumes is what a guest produces.
type streamingHost struct {
	stubMicrovmHost
	root string
	// cut, when positive, is how many bytes to stream before failing — a guest
	// that died half way through.
	cut int64
	// err, when set, is a handshake that failed before a byte moved.
	err error

	mu     sync.Mutex
	calls  int
	commit []uint64
}

func (h *streamingHost) StreamWorkspace(ctx context.Context, _ string, dst io.Writer) (WorkspaceStream, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	if h.err != nil {
		return WorkspaceStream{}, h.err
	}
	if h.cut > 0 {
		// Written straight into the sink and then abandoned, which is what the
		// driver sees when a guest dies mid-stream: some bytes, and no end
		// marker ever.
		var buf strings.Builder
		if _, err := wstream.Write(ctx, os.DirFS(h.root), &buf, wstream.Limits{}); err != nil {
			return WorkspaceStream{}, err
		}
		s := buf.String()
		if int64(len(s)) > h.cut {
			s = s[:h.cut]
		}
		if _, err := io.WriteString(dst, s); err != nil {
			return WorkspaceStream{}, err
		}
		return WorkspaceStream{}, errors.New("the sandbox connection ended part way through its workspace stream")
	}
	rep, err := wstream.Write(ctx, os.DirFS(h.root), dst, wstream.Limits{})
	if err != nil {
		return WorkspaceStream{}, err
	}
	return WorkspaceStream{Entries: rep.Entries, Bytes: rep.Bytes, Nonce: 7}, nil
}

func (h *streamingHost) CheckpointCommitted(_ string, nonce uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commit = append(h.commit, nonce)
}

func (h *streamingHost) committedNonces() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.commit...)
}

// checkpointScene is a microVM driver with a checkpoint configuration, a guest
// that streams a real workspace, and one created session.
func checkpointScene(t *testing.T, store checkpoint.BlobStore) (*Microvm, *SimulatedEngine, *streamingHost, string, string) {
	t.Helper()
	stateDir := shortTempDir(t)
	if store == nil {
		store = checkpoint.NewMemoryStore()
	}
	keys, err := checkpoint.NewStaticKeyWrapper("selfhosted/checkpoint/v1", [32]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	m, sim := testMicrovm(t, MicrovmOpts{
		TotalSlots: 4,
		StateDir:   stateDir,
		Checkpoint: &CheckpointOpts{Store: store, Keys: keys, KeyRef: keys.Ref()},
	})
	host := &streamingHost{root: guestWorkspace(t)}
	m.SetHost(host)

	h, err := m.Create(context.Background(), Spec{SessionID: "sess-ckpt", BootstrapToken: "token_example"})
	if err != nil {
		t.Fatal(err)
	}
	return m, sim, host, h.ID, stateDir
}

// TestAColdSuspendCommitsAndVerifiesTheWorkspaceCheckpoint is ADR-0003 §4.4:
// the suspend does not report success until the manifest has committed and the
// committed bytes have been read back.
func TestAColdSuspendCommitsAndVerifiesTheWorkspaceCheckpoint(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, sim, host, id, _ := checkpointScene(t, store)
	ctx := context.Background()

	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatalf("the cold suspend failed: %v", err)
	}

	// Two objects: the content and the manifest whose put-if-absent IS the
	// commit.
	keys := store.Keys()
	if len(keys) != 2 {
		t.Fatalf("the store holds %v, want a content object and a manifest", keys)
	}
	var manifestKey string
	for _, k := range keys {
		if strings.HasSuffix(k, "/manifest.json") {
			manifestKey = k
		}
	}
	if manifestKey == "" {
		t.Fatalf("the store holds %v, with no manifest", keys)
	}

	// The record names the checkpoint, and it is PERSISTED: a cold resume after
	// a runnerd restart is exactly the case where the image may be gone and
	// this is the only copy left.
	rec, ok := m.diskRecord(id)
	if !ok {
		t.Fatal("no instance record was written")
	}
	if rec.CheckpointGeneration != 1 {
		t.Errorf("the record says generation %d, want 1", rec.CheckpointGeneration)
	}
	if rec.CheckpointKey != manifestKey {
		t.Errorf("the record names checkpoint %q, the store holds %q", rec.CheckpointKey, manifestKey)
	}
	if rec.CheckpointAt == "" {
		t.Error("the record does not say when the checkpoint was taken")
	}

	// The VM was terminated only after all of that.
	if st, _ := sim.State(ctx, id); st != VMMStateStopped {
		t.Errorf("the VM is %v after a cold suspend, want stopped", st)
	}
	if got := host.committedNonces(); len(got) != 1 || got[0] != 7 {
		t.Errorf("the guest was told %v about its checkpoint, want the one suspend's nonce", got)
	}

	// And it is restorable, which is the whole claim: verify through the store,
	// with the same context the driver used.
	reader, err := checkpoint.NewReader(store, mustKeys(t), checkpoint.ReaderOptions{
		Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := reader.Verify(ctx, checkpoint.Context{
		Workspace: workspaceVolume("sess-ckpt"), Session: "sess-ckpt", Generation: 1})
	if err != nil {
		t.Fatalf("the committed checkpoint does not verify: %v", err)
	}
	// The tree, minus the Rainier-owned directory the driver excludes whatever
	// the guest sent.
	if rep.Entries != 4 {
		t.Errorf("the checkpoint carries %d entries, want 4 (.rainier excluded)", rep.Entries)
	}
	if rep.Symlinks != 1 {
		t.Errorf("the checkpoint carries %d symlinks, want 1", rep.Symlinks)
	}
}

func mustKeys(t *testing.T) checkpoint.Wrapper {
	t.Helper()
	keys, err := checkpoint.NewStaticKeyWrapper("selfhosted/checkpoint/v1", [32]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// TestAColdSuspendKeepsTheVMWhenTheStoreRefuses. Nothing committed, so nothing
// may be lost: the VM stays, the generation does not advance, and the error
// names the stage.
func TestAColdSuspendKeepsTheVMWhenTheStoreRefuses(t *testing.T) {
	store := &refusingStore{MemoryStore: checkpoint.NewMemoryStore(), refuse: errors.New("the bucket is not reachable")}
	m, sim, _, id, _ := checkpointScene(t, store)
	ctx := context.Background()

	err := m.Suspend(ctx, id, false)
	if err == nil {
		t.Fatal("a cold suspend with no store reported success")
	}
	if !strings.Contains(err.Error(), "checkpoint write stage") {
		t.Errorf("the refusal does not name the stage: %v", err)
	}
	assertVMKept(t, m, sim, id)
	if keys := store.Keys(); len(keys) != 0 {
		t.Errorf("a failed checkpoint left %v in the store", keys)
	}
}

// TestAColdSuspendKeepsTheVMWhenTheGuestDiesMidStream.
func TestAColdSuspendKeepsTheVMWhenTheGuestDiesMidStream(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, sim, host, id, _ := checkpointScene(t, store)
	host.cut = 64 // a header, an index, and nothing that could be a whole tree
	ctx := context.Background()

	err := m.Suspend(ctx, id, false)
	if err == nil {
		t.Fatal("a cold suspend whose guest died mid-stream reported success")
	}
	if !strings.Contains(err.Error(), "checkpoint stream stage") {
		t.Errorf("the refusal does not name the stage: %v", err)
	}
	assertVMKept(t, m, sim, id)
	if keys := store.Keys(); len(keys) != 0 {
		t.Errorf("a failed stream left %v in the store", keys)
	}
}

// TestAColdSuspendKeepsTheVMWhenTheHandshakeFails: the sandbox never answered,
// or predates the vocabulary entirely.
func TestAColdSuspendKeepsTheVMWhenTheHandshakeFails(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, sim, host, id, _ := checkpointScene(t, store)
	host.err = errors.New("the sandbox did not acknowledge the cold suspend")
	ctx := context.Background()

	if err := m.Suspend(ctx, id, false); err == nil {
		t.Fatal("a cold suspend whose guest never answered reported success")
	}
	assertVMKept(t, m, sim, id)
}

// TestAColdSuspendKeepsTheVMWhenVerifyFails is PRD §10's restore test as a
// GATE rather than a report: the manifest committed, the bytes came back wrong,
// and that is a failed suspend.
func TestAColdSuspendKeepsTheVMWhenVerifyFails(t *testing.T) {
	store := &tamperingStore{MemoryStore: checkpoint.NewMemoryStore()}
	m, sim, _, id, _ := checkpointScene(t, store)
	ctx := context.Background()

	err := m.Suspend(ctx, id, false)
	if err == nil {
		t.Fatal("a cold suspend whose checkpoint does not verify reported success")
	}
	if !strings.Contains(err.Error(), "checkpoint verify stage") {
		t.Errorf("the refusal does not name the stage: %v", err)
	}
	assertVMKept(t, m, sim, id)
	// The generation is NOT recorded: a checkpoint that cannot be read back is
	// not one, and the retry must not believe it has a newer one than it does.
	rec, _ := m.diskRecord(id)
	if rec.CheckpointGeneration != 0 {
		t.Errorf("a failed verify recorded generation %d", rec.CheckpointGeneration)
	}
}

// TestAColdSuspendWithNoCheckpointConfigurationIsUnchanged. The contract suite
// and the local dev surface have no store and no key; their cold suspend is
// what it was before this existed.
func TestAColdSuspendWithNoCheckpointConfigurationIsUnchanged(t *testing.T) {
	m, sim := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "sess-plain"})
	if err != nil {
		t.Fatal(err)
	}
	if m.CheckpointsColdSuspend() {
		t.Error("a driver with no checkpoint configuration claims it checkpoints cold suspends")
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("the cold suspend failed: %v", err)
	}
	if st, _ := sim.State(ctx, h.ID); st != VMMStateStopped {
		t.Errorf("the VM is %v, want stopped", st)
	}
}

// TestEachColdSuspendTakesTheNextGeneration. The generation is the count of
// COMMITTED checkpoints, so the second suspend of a session's life takes 2 —
// and both are readable, because a put-if-absent never overwrites.
func TestEachColdSuspendTakesTheNextGeneration(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, _, _, id, _ := checkpointScene(t, store)
	ctx := context.Background()

	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatalf("the first cold suspend failed: %v", err)
	}
	if _, err := m.Resume(ctx, id); err != nil {
		t.Fatalf("the cold resume failed: %v", err)
	}
	if err := m.Suspend(ctx, id, false); err != nil {
		t.Fatalf("the second cold suspend failed: %v", err)
	}

	rec, _ := m.diskRecord(id)
	if rec.CheckpointGeneration != 2 {
		t.Fatalf("the record says generation %d after two cold suspends, want 2", rec.CheckpointGeneration)
	}
	reader, err := checkpoint.NewReader(store, mustKeys(t), checkpoint.ReaderOptions{
		Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	for gen := uint64(1); gen <= 2; gen++ {
		if _, err := reader.Verify(ctx, checkpoint.Context{
			Workspace: workspaceVolume("sess-ckpt"), Session: "sess-ckpt", Generation: gen}); err != nil {
			t.Errorf("generation %d does not verify: %v", gen, err)
		}
	}
}

// TestAColdSuspendWritesNoPlaintextWorkspaceUnderTheStateDir is the claim the
// whole streaming design exists to make: the workspace goes from a socket,
// through one frame at a time, into a store, and never onto this host's disk.
//
// It is the same walk TestMicrovmWritesNoDecryptedEnvironment does, applied to
// the other thing this host must not be holding.
func TestAColdSuspendWritesNoPlaintextWorkspaceUnderTheStateDir(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, _, _, id, stateDir := checkpointScene(t, store)

	if err := m.Suspend(context.Background(), id, false); err != nil {
		t.Fatalf("the cold suspend failed: %v", err)
	}

	err := filepath.WalkDir(stateDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
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
		for _, leak := range []string{workspaceMarker, "must-not-travel", wstream.Magic} {
			if strings.Contains(string(b), leak) {
				return fmt.Errorf("%s holds workspace plaintext (%q)", strings.TrimPrefix(path, stateDir), leak)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// And the checkpoint itself carries none of it in the clear either: the
	// manifest is plaintext JSON by design, so it is the one object worth
	// checking by hand.
	for _, k := range store.Keys() {
		b, _ := store.Object(k)
		for _, leak := range []string{workspaceMarker, "README", "main.go", "entry"} {
			if strings.Contains(string(b), leak) {
				t.Errorf("the object %s carries %q in the clear", k, leak)
			}
		}
	}
}

// TestTwoConcurrentColdSuspendsAreRefusedRatherThanRaced: two of them would
// each read part of one stream and commit two checkpoints of two halves of a
// workspace, both verified against their own halves and both wrong.
func TestTwoConcurrentColdSuspendsAreRefusedRatherThanRaced(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	m, _, host, id, _ := checkpointScene(t, store)

	// A host whose stream blocks until released, so the second suspend
	// certainly arrives while the first is in flight.
	blocking := &blockingHost{
		streamingHost: host,
		gate:          make(chan struct{}),
		entered:       make(chan struct{}, 1),
	}
	m.SetHost(blocking)

	first := make(chan error, 1)
	go func() { first <- m.Suspend(context.Background(), id, false) }()
	<-blocking.entered

	second := m.Suspend(context.Background(), id, false)
	if second == nil || !strings.Contains(second.Error(), "already in flight") {
		t.Errorf("the second concurrent cold suspend gave %v, want a refusal", second)
	}
	close(blocking.gate)
	if err := <-first; err != nil {
		t.Fatalf("the first cold suspend failed: %v", err)
	}
}

type blockingHost struct {
	*streamingHost
	gate    chan struct{}
	entered chan struct{}
}

func (b *blockingHost) StreamWorkspace(ctx context.Context, sessionID string, dst io.Writer) (WorkspaceStream, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.gate
	return b.streamingHost.StreamWorkspace(ctx, sessionID, dst)
}

// assertVMKept is the rule every failure path above shares: a suspend that did
// not produce a checkpoint did not stop the VM either.
func assertVMKept(t *testing.T, m *Microvm, sim *SimulatedEngine, id string) {
	t.Helper()
	st, err := sim.State(context.Background(), id)
	if err != nil {
		t.Fatalf("asking the engine about %s: %v", id, err)
	}
	if st != VMMStateRunning {
		t.Errorf("the VM is %v after a failed cold suspend, want it still running", st)
	}
	rec, ok := m.diskRecord(id)
	if !ok {
		t.Fatal("the instance record is gone after a failed cold suspend")
	}
	if rec.State != StateRunning {
		t.Errorf("the record says %v after a failed cold suspend, want running", rec.State)
	}
	// The session's own rootfs is still there: a failed cold suspend has not
	// begun tearing anything down.
	path, err := m.sessionRootfsPath(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("a failed cold suspend removed the session's root filesystem: %v", err)
	}
}

// refusingStore is a blob store that will not take an object — a bucket that is
// unreachable, a credential that expired, a quota that is full.
type refusingStore struct {
	*checkpoint.MemoryStore
	refuse error
}

func (r *refusingStore) PutIfAbsent(context.Context, string, func(io.Writer) error) error {
	return r.refuse
}

// tamperingStore commits what it is given and then changes it, which is a
// checkpoint that committed and cannot be read back: bit rot, a bug, or an
// attacker with write access to the bucket and no key.
type tamperingStore struct {
	*checkpoint.MemoryStore
}

func (s *tamperingStore) PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error {
	if err := s.MemoryStore.PutIfAbsent(ctx, key, write); err != nil {
		return err
	}
	if strings.Contains(key, "/content.") {
		b, ok := s.MemoryStore.Object(key)
		if ok && len(b) > 0 {
			b[len(b)/2] ^= 0xff
			// Put it back under a second key-free path: MemoryStore's own
			// overwrite is unexported, so the tamper is done by deleting and
			// re-putting, which is exactly what an attacker with write access
			// to the bucket has.
			_ = s.MemoryStore.Delete(ctx, key)
			_ = s.MemoryStore.PutIfAbsent(ctx, key, func(w io.Writer) error {
				_, err := w.Write(b)
				return err
			})
		}
	}
	return nil
}
