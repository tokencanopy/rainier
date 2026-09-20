package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic identities only, as the tenancy specification's §18 requires:
// workspace_alpha, session ids that are obviously fictional, example.test.
func testContext() Context {
	return Context{Workspace: "workspace-alpha", Session: "sess-0000000000000001", Generation: 7}
}

const testKeyRef KeyRef = "example.test/keys/checkpoint/1"

func testKey() [dekLen]byte {
	var k [dekLen]byte
	for i := range k {
		k[i] = byte(0x40 + i)
	}
	return k
}

func testWrapper(t *testing.T) *StaticKeyWrapper {
	t.Helper()
	w, err := NewStaticKeyWrapper(testKeyRef, testKey())
	if err != nil {
		t.Fatalf("NewStaticKeyWrapper: %v", err)
	}
	return w
}

// authorizeRecorder is the authorization hook plus the evidence that it ran, in
// the right order, with the right argument.
type authorizeRecorder struct {
	mu     sync.Mutex
	calls  int
	last   Manifest
	refuse error
}

func (a *authorizeRecorder) hook(_ context.Context, _ Context, m Manifest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.last = m
	return a.refuse
}

// openRecorder wraps a BlobStore and remembers the order objects were opened in,
// which is how the "authorization runs before any content byte" test is made
// non-vacuous.
type openRecorder struct {
	BlobStore
	mu    sync.Mutex
	opens []string
}

func (o *openRecorder) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	o.mu.Lock()
	o.opens = append(o.opens, key)
	o.mu.Unlock()
	return o.BlobStore.Open(ctx, key)
}

func (o *openRecorder) opened() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.opens)
}

// fixedTime is every mtime in the fixtures, so that a tree digest and a content
// digest are functions of the fixture and not of the clock.
var fixedTime = time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)

// fixtureTree builds the tree every round-trip test uses: nested directories, an
// empty directory, an empty file, an executable, a file large enough to cross
// several frames, a non-ASCII name, a contained symlink, and a planted
// credential-shaped file inside an excluded subtree.
func fixtureTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	mkdir := func(p string, mode fs.FileMode) {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(full, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Chmod(full, mode); err != nil {
			t.Fatalf("chmod: %v", err)
		}
	}
	write := func(p string, mode fs.FileMode, b []byte) {
		full := filepath.Join(root, p)
		if err := os.WriteFile(full, b, mode); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := os.Chmod(full, mode); err != nil {
			t.Fatalf("chmod: %v", err)
		}
	}

	// Every directory's mode is set explicitly, including the parents, so that
	// the golden manifest does not depend on the umask of whoever runs the tests.
	mkdir("src", 0o755)
	mkdir("src/inner", 0o755)
	mkdir("empty", 0o700)
	mkdir(".rainier/agents/claude", 0o700)
	write("README.md", 0o644, []byte("# a workspace\n"))
	write("src/main.go", 0o644, []byte("package main\n\nfunc main() {}\n"))
	write("src/inner/deep.txt", 0o600, []byte("deep\n"))
	write("run.sh", 0o755, []byte("#!/bin/sh\necho hi\n"))
	write("empty.txt", 0o644, nil)
	write("ünïcødé näme.txt", 0o644, []byte("unicode\n"))
	write("big.bin", 0o644, pseudorandom(200<<10))
	// The planted credential: a shape a reviewer would recognize, inside a path
	// the exclusion list names. It must never be opened, let alone stored.
	write(".rainier/agents/claude/.credentials.json", 0o600,
		[]byte(`{"token":"`+plantedCredential+`"}`))

	if err := os.Symlink("inner/deep.txt", filepath.Join(root, "src", "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	stampTree(t, root)
	return root
}

// plantedCredential is a credential-SHAPED string and not a credential: it names
// nothing, authenticates nowhere, and exists so a test can search the bytes a
// store received for it.
const plantedCredential = "not-a-real-token-0000000000000000"

// stampTree sets every mtime to fixedTime, deepest first, so that creating a
// child cannot leave its parent's mtime at the wall clock. Symlinks are skipped:
// os.Chtimes follows them, and their mtime is neither restored nor digested.
func stampTree(t *testing.T, root string) {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink == 0 {
			paths = append(paths, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Deepest first.
	slices.SortFunc(paths, func(a, b string) int { return len(b) - len(a) })
	for _, p := range paths {
		if err := os.Chtimes(p, fixedTime, fixedTime); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}
}

func pseudorandom(n int) []byte {
	b := make([]byte, n)
	x := uint32(0x9e3779b9)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

// harness is a store, a writer and a reader over one fixture.
type harness struct {
	store *MemoryStore
	w     *Writer
	r     *Reader
	auth  *authorizeRecorder
	c     Context
}

func newHarness(t *testing.T, frameSize int64) *harness {
	t.Helper()
	store := NewMemoryStore()
	keys := testWrapper(t)
	w, err := NewWriter(store, keys, WriterOptions{
		Prefix:    "checkpoints",
		KeyRef:    testKeyRef,
		FrameSize: frameSize,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	auth := &authorizeRecorder{}
	r, err := NewReader(store, keys, ReaderOptions{Prefix: "checkpoints", Authorize: auth.hook})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return &harness{store: store, w: w, r: r, auth: auth, c: testContext()}
}

func TestRoundTrip(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	root := fixtureTree(t)
	ctx := context.Background()

	res, err := h.w.Write(ctx, h.c, DirSource(root, DefaultExclusions()...))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.Manifest.Frames < 3 {
		t.Fatalf("the fixture should cross several frames, got %d", res.Manifest.Frames)
	}
	if got, want := h.store.Keys(), []string{res.ContentKey, res.ManifestKey}; !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("stored keys = %v, want exactly the content object and the manifest", got)
	}

	rep, err := h.r.Verify(ctx, h.c)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rep.Restored {
		t.Error("Verify reported Restored")
	}
	if rep.Entries != res.Manifest.Entries || rep.TreeDigest != res.Manifest.TreeDigest {
		t.Errorf("Verify aggregates disagree with the manifest: %+v", rep)
	}
	// 3 directories (src, src/inner, empty), 7 files, 1 symlink. .rainier and
	// everything under it is excluded.
	if rep.Dirs != 3 || rep.Files != 7 || rep.Symlinks != 1 {
		t.Errorf("dirs=%d files=%d symlinks=%d, want 3/7/1", rep.Dirs, rep.Files, rep.Symlinks)
	}
	if res.Manifest.Skipped != 0 {
		t.Errorf("skipped = %d, want 0", res.Manifest.Skipped)
	}

	target := filepath.Join(t.TempDir(), "restored")
	rrep, err := h.r.Restore(ctx, h.c, target)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !rrep.Restored || rrep.TreeDigest != rep.TreeDigest {
		t.Fatalf("Restore report disagrees with Verify: %+v vs %+v", rrep, rep)
	}
	compareTrees(t, root, target, DefaultExclusions())

	// The strongest equality available without a second copy: checkpoint the
	// RESTORED tree and compare tree digests. Names, modes, sizes, contents and
	// file modification times all feed that digest, so an equal digest is an
	// equal tree.
	next := h.c
	next.Generation++
	again, err := h.w.Write(ctx, next, DirSource(target, DefaultExclusions()...))
	if err != nil {
		t.Fatalf("Write of the restored tree: %v", err)
	}
	if again.Manifest.TreeDigest != res.Manifest.TreeDigest {
		t.Errorf("the restored tree has a different tree digest:\n%s\n%s",
			again.Manifest.TreeDigest, res.Manifest.TreeDigest)
	}
	// And the plaintext is reproducible, not merely equivalent: directory and
	// symlink modification times are pinned, so the same tree produces the same
	// tar stream byte for byte. The CIPHERTEXT still differs, because the data key
	// is fresh per checkpoint.
	if again.Manifest.PlainBytes != res.Manifest.PlainBytes {
		t.Errorf("the restored tree produced a different plaintext length: %d vs %d",
			again.Manifest.PlainBytes, res.Manifest.PlainBytes)
	}
	if again.Manifest.ContentDigest == res.Manifest.ContentDigest {
		t.Error("two checkpoints produced identical ciphertext")
	}
}

// compareTrees walks both trees and compares every entry the checkpoint carries.
func compareTrees(t *testing.T, want, got string, exclude []string) {
	t.Helper()
	collect := func(root string) map[string]string {
		out := map[string]string{}
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			if rel == "." {
				return nil
			}
			rel = filepath.ToSlash(rel)
			for _, e := range exclude {
				if rel == e || strings.HasPrefix(rel, e+"/") {
					if d.IsDir() {
						return fs.SkipDir
					}
					return nil
				}
			}
			switch {
			case d.Type()&fs.ModeSymlink != 0:
				target, err := os.Readlink(p)
				if err != nil {
					return err
				}
				out[rel] = "L " + target
			case d.IsDir():
				fi, err := d.Info()
				if err != nil {
					return err
				}
				out[rel] = "D " + fi.Mode().Perm().String()
			default:
				fi, err := d.Info()
				if err != nil {
					return err
				}
				b, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				out[rel] = "F " + fi.Mode().Perm().String() + " " +
					fi.ModTime().UTC().Format(time.RFC3339Nano) + " " +
					string(sha256hex(b))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
		return out
	}
	a, b := collect(want), collect(got)
	if len(a) != len(b) {
		t.Fatalf("entry counts differ: source %d, restored %d", len(a), len(b))
	}
	for name, av := range a {
		bv, ok := b[name]
		if !ok {
			t.Errorf("restored tree is missing an entry")
			continue
		}
		if av != bv {
			t.Errorf("entry differs after restore: %q vs %q", av, bv)
		}
	}
}

func TestExclusionIsPrunedAndNeverRead(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	root := fixtureTree(t)
	ctx := context.Background()

	// The recorder is a plain fs.FS with no ReadLink, so the fixture's symlink is
	// removed rather than pretended about — a source that cannot read a link is
	// refused, which TestSymlinkRefusals' sibling case relies on.
	if err := os.Remove(filepath.Join(root, "src", "link.txt")); err != nil {
		t.Fatal(err)
	}
	rec := &recordingFS{FS: os.DirFS(root)}
	res, err := h.w.Write(ctx, h.c, Source{FS: recordingWithoutLinks(t, rec), Exclude: DefaultExclusions()})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Nothing under an excluded path was ever OPENED. This is the assertion that
	// matters: a filter applied after reading would still keep the bytes out of
	// the archive, and would still have read a credential off the disk.
	for _, name := range rec.names() {
		if name == ".rainier" || strings.HasPrefix(name, ".rainier/") {
			t.Errorf("an excluded path was opened during the walk")
		}
	}

	// And no byte the store received contains the planted value. With encryption
	// this is nearly tautological, which is exactly why it is not the primary
	// assertion — it is here to catch a future writer that staged plaintext.
	for _, key := range h.store.Keys() {
		b, _ := h.store.Object(key)
		if bytes.Contains(b, []byte(plantedCredential)) {
			t.Fatalf("a stored object contains the planted credential")
		}
	}

	// And the restored tree does not have it.
	target := filepath.Join(t.TempDir(), "restored")
	if _, err := h.r.Restore(ctx, h.c, target); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(target, ".rainier")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the excluded directory was restored")
	}
	if res.Manifest.Entries == 0 {
		t.Error("nothing was checkpointed at all")
	}
}

// recordingFS remembers every name opened through it.
type recordingFS struct {
	fs.FS
	mu     sync.Mutex
	opened []string
}

func (r *recordingFS) Open(name string) (fs.File, error) {
	r.mu.Lock()
	r.opened = append(r.opened, name)
	r.mu.Unlock()
	return r.FS.Open(name)
}

func (r *recordingFS) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.opened)
}

// recordingWithoutLinks returns the recorder as a plain fs.FS, which has no
// ReadLink, so this test's tree must not contain one. It deletes the fixture's
// symlink rather than pretending.
func recordingWithoutLinks(t *testing.T, r *recordingFS) fs.FS {
	t.Helper()
	return struct{ fs.FS }{r}
}

func TestSymlinkRefusals(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, target string }{
		{"absolute", "/etc/passwd"},
		{"escaping", "../../etc/passwd"},
		{"escaping through a clean-looking path", "sub/../../../etc/passwd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, MinFrameSize)
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(tc.target, filepath.Join(root, "bad")); err != nil {
				t.Fatal(err)
			}
			_, err := h.w.Write(ctx, h.c, DirSource(root))
			if !errors.Is(err, ErrSource) {
				t.Fatalf("Write error = %v, want ErrSource", err)
			}
			if strings.Contains(err.Error(), tc.target) || strings.Contains(err.Error(), "bad") {
				t.Errorf("the error quotes a path or a target: %v", err)
			}
			assertNoContentInError(t, root, err)
		})
	}
}

func TestContainedDanglingSymlinkTravels(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("gone.txt", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := h.w.Write(ctx, h.c, DirSource(root)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	target := filepath.Join(t.TempDir(), "r")
	if _, err := h.r.Restore(ctx, h.c, target); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got, err := os.Readlink(filepath.Join(target, "dangling")); err != nil || got != "gone.txt" {
		t.Errorf("dangling link = %q, %v", got, err)
	}
}

func TestExoticEntryIsSkippedAndCounted(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(root, "dev.sock"))
	if err != nil {
		t.Skipf("this environment cannot create a unix socket: %v", err)
	}
	defer ln.Close()

	h := newHarness(t, MinFrameSize)
	res, err := h.w.Write(context.Background(), h.c, DirSource(root))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.Manifest.Skipped != 1 {
		t.Errorf("skipped = %d, want 1", res.Manifest.Skipped)
	}
	if res.Manifest.Entries != 1 {
		t.Errorf("entries = %d, want 1 (the socket is not an entry)", res.Manifest.Entries)
	}
}

func TestPutIfAbsentMakesTheCommitAtomic(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	root := fixtureTree(t)
	ctx := context.Background()

	first, err := h.w.Write(ctx, h.c, DirSource(root, DefaultExclusions()...))
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}
	_, err = h.w.Write(ctx, h.c, DirSource(root, DefaultExclusions()...))
	if !errors.Is(err, ErrExists) {
		t.Fatalf("second Write error = %v, want ErrExists", err)
	}
	// The loser left its orphaned content object behind and did not touch the
	// winner's manifest.
	b, ok := h.store.Object(first.ManifestKey)
	if !ok {
		t.Fatal("the committed manifest is gone")
	}
	m, err := ParseManifest(b)
	if err != nil {
		t.Fatalf("the committed manifest no longer parses: %v", err)
	}
	if m.ContentKey != first.ContentKey {
		t.Error("the committed manifest names a different content object")
	}
	if _, err := h.r.Verify(ctx, h.c); err != nil {
		t.Fatalf("the winner no longer verifies: %v", err)
	}
}

func TestAuthorizationIsRequiredAndRunsFirst(t *testing.T) {
	ctx := context.Background()

	t.Run("nil hook is refused at construction", func(t *testing.T) {
		_, err := NewReader(NewMemoryStore(), testWrapper(t), ReaderOptions{})
		if !errors.Is(err, ErrNoAuthorization) {
			t.Fatalf("NewReader error = %v, want ErrNoAuthorization", err)
		}
	})

	t.Run("a refusal stops the read before any content", func(t *testing.T) {
		store := NewMemoryStore()
		keys := testWrapper(t)
		w, err := NewWriter(store, keys, WriterOptions{KeyRef: testKeyRef, FrameSize: MinFrameSize})
		if err != nil {
			t.Fatal(err)
		}
		c := testContext()
		res, err := w.Write(ctx, c, DirSource(fixtureTree(t), DefaultExclusions()...))
		if err != nil {
			t.Fatal(err)
		}
		rec := &openRecorder{BlobStore: store}
		auth := &authorizeRecorder{refuse: errors.New("the destination is in another product region")}
		r, err := NewReader(rec, keys, ReaderOptions{Authorize: auth.hook})
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "r")
		if _, err := r.Restore(ctx, c, target); !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("Restore error = %v, want ErrNotAuthorized", err)
		}
		if auth.calls != 1 {
			t.Errorf("the hook ran %d times, want 1", auth.calls)
		}
		if auth.last.ContentKey != res.ContentKey {
			t.Error("the hook did not see the parsed manifest")
		}
		if opened := rec.opened(); len(opened) != 1 || !strings.HasSuffix(opened[0], manifestObject) {
			t.Errorf("objects opened = %v, want only the manifest", opened)
		}
		if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
			t.Error("the restore target was created despite the refusal")
		}
	})
}

func TestRestoreRefusesANonEmptyTarget(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	if _, err := h.w.Write(ctx, h.c, DirSource(fixtureTree(t), DefaultExclusions()...)); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "occupied"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.r.Restore(ctx, h.c, target); !errors.Is(err, ErrTargetNotEmpty) {
		t.Fatalf("Restore error = %v, want ErrTargetNotEmpty", err)
	}
}

func TestPreflightReadsNoContent(t *testing.T) {
	store := NewMemoryStore()
	keys := testWrapper(t)
	w, err := NewWriter(store, keys, WriterOptions{KeyRef: testKeyRef, FrameSize: MinFrameSize})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c := testContext()
	res, err := w.Write(ctx, c, DirSource(fixtureTree(t), DefaultExclusions()...))
	if err != nil {
		t.Fatal(err)
	}
	rec := &openRecorder{BlobStore: store}
	auth := &authorizeRecorder{}
	r, err := NewReader(rec, keys, ReaderOptions{Authorize: auth.hook})
	if err != nil {
		t.Fatal(err)
	}
	pf, err := r.Preflight(ctx, c)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if pf.Manifest.TreeDigest != res.Manifest.TreeDigest {
		t.Error("Preflight returned a different manifest")
	}
	if pf.Summary.CreatedAt.IsZero() || pf.Summary.KeyRef != testKeyRef {
		t.Errorf("summary = %+v", pf.Summary)
	}
	if opened := rec.opened(); len(opened) != 1 || !strings.HasSuffix(opened[0], manifestObject) {
		t.Errorf("objects opened = %v, want only the manifest", opened)
	}
}

func TestPreflightFailsWhenTheKeyVersionIsElsewhere(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	if _, err := h.w.Write(ctx, h.c, DirSource(fixtureTree(t), DefaultExclusions()...)); err != nil {
		t.Fatal(err)
	}
	other, err := NewStaticKeyWrapper("example.test/keys/checkpoint/2", testKey())
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(h.store, other, ReaderOptions{
		Prefix:    "checkpoints",
		Authorize: (&authorizeRecorder{}).hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Preflight(ctx, h.c); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("Preflight error = %v, want ErrKeyUnavailable", err)
	}
}

func TestDeleteRemovesTheManifestFirst(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	res, err := h.w.Write(ctx, h.c, DirSource(fixtureTree(t), DefaultExclusions()...))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.r.Delete(ctx, h.c); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := h.store.Object(res.ManifestKey); ok {
		t.Error("the manifest survived")
	}
	if _, ok := h.store.Object(res.ContentKey); ok {
		t.Error("the content object survived")
	}
	// Idempotent.
	if err := h.r.Delete(ctx, h.c); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if _, err := h.r.Verify(ctx, h.c); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Verify after Delete = %v, want ErrNotFound", err)
	}
}

func TestVerifyFindsNoCheckpoint(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	if _, err := h.r.Verify(context.Background(), h.c); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Verify error = %v, want ErrNotFound", err)
	}
}

func TestSourceValidation(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		src  Source
	}{
		{"no file system", Source{}},
		{"absolute exclusion", Source{FS: os.DirFS(t.TempDir()), Exclude: []string{"/etc"}}},
		{"unclean exclusion", Source{FS: os.DirFS(t.TempDir()), Exclude: []string{"a/./b"}}},
		{"escaping exclusion", Source{FS: os.DirFS(t.TempDir()), Exclude: []string{"../x"}}},
		{"root exclusion", Source{FS: os.DirFS(t.TempDir()), Exclude: []string{"."}}},
		{"empty exclusion", Source{FS: os.DirFS(t.TempDir()), Exclude: []string{""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := h.w.Write(ctx, h.c, tc.src); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Write error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestContextValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    Context
	}{
		{"zero", Context{}},
		{"no workspace", Context{Session: "s", Generation: 1}},
		{"no session", Context{Workspace: "w", Generation: 1}},
		{"generation zero", Context{Workspace: "w", Session: "s"}},
		{"slash in workspace", Context{Workspace: "a/b", Session: "s", Generation: 1}},
		{"NUL in session", Context{Workspace: "w", Session: "s\x00b", Generation: 1}},
		{"leading dot", Context{Workspace: ".hidden", Session: "s", Generation: 1}},
		{"over the length limit", Context{Workspace: strings.Repeat("a", maxIDLen+1), Session: "s", Generation: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.c.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate = %v, want ErrInvalid", err)
			}
		})
	}
	if err := testContext().Validate(); err != nil {
		t.Fatalf("the test context is not valid: %v", err)
	}
}

func TestWriterOptionValidation(t *testing.T) {
	store := NewMemoryStore()
	keys := testWrapper(t)
	for _, tc := range []struct {
		name string
		opts WriterOptions
	}{
		{"frame too small", WriterOptions{FrameSize: MinFrameSize - 1}},
		{"frame too large", WriterOptions{FrameSize: MaxFrameSize + 1}},
		{"absolute prefix", WriterOptions{Prefix: "/x"}},
		{"trailing slash prefix", WriterOptions{Prefix: "x/"}},
		{"dots in prefix", WriterOptions{Prefix: "x/../y"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewWriter(store, keys, tc.opts); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewWriter = %v, want ErrInvalid", err)
			}
		})
	}
	if _, err := NewWriter(nil, keys, WriterOptions{}); !errors.Is(err, ErrInvalid) {
		t.Error("a nil store was accepted")
	}
	if _, err := NewWriter(store, nil, WriterOptions{}); !errors.Is(err, ErrInvalid) {
		t.Error("a nil wrapper was accepted")
	}
}

func sha256hex(b []byte) string {
	h := newTreeHasher()
	h.h.Write(b)
	return h.sum()
}

// sinkWriter is used by the tamper tests to rebuild a stored object.
func replaceObject(t *testing.T, store *MemoryStore, key string, mutate func([]byte) []byte) {
	t.Helper()
	b, ok := store.Object(key)
	if !ok {
		t.Fatalf("no object to mutate")
	}
	store.overwrite(key, mutate(bytes.Clone(b)))
}
