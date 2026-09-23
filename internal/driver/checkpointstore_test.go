// internal/driver/checkpointstore_test.go
package driver

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tokencanopy/rainier/checkpoint"
)

// The self-hosted store and key. The store's one load-bearing property is
// put-if-absent: the format's atomic commit IS that conditional write, so a
// store that overwrote, or that left a half-written object visible, would make
// the durability barrier a barrier against nothing.

func dirStore(t *testing.T) (*DirBlobStore, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "checkpoints")
	s, err := NewDirBlobStore(dir)
	if err != nil {
		t.Fatalf("NewDirBlobStore: %v", err)
	}
	return s, dir
}

func put(t *testing.T, s *DirBlobStore, key, body string) error {
	t.Helper()
	return s.PutIfAbsent(context.Background(), key, func(w io.Writer) error {
		_, err := io.WriteString(w, body)
		return err
	})
}

func TestDirStoreRoundTrip(t *testing.T) {
	s, _ := dirStore(t)
	ctx := context.Background()
	const key = "rainier.checkpoint.v1/ws/ws-1/sess/sess-1/gen/00000000000000000001/manifest.json"

	if err := put(t, s, key, "the manifest"); err != nil {
		t.Fatalf("put: %v", err)
	}
	rc, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "the manifest" {
		t.Errorf("the object read back as %q", got)
	}
}

// TestDirStoreNeverOverwrites is the atomic commit. Replacing a committed
// manifest is the one operation this format has no answer for: the data key
// inside it is the only way to read the content object it names.
func TestDirStoreNeverOverwrites(t *testing.T) {
	s, _ := dirStore(t)
	const key = "p/manifest.json"

	if err := put(t, s, key, "first"); err != nil {
		t.Fatal(err)
	}
	err := put(t, s, key, "second")
	if !errors.Is(err, checkpoint.ErrExists) {
		t.Fatalf("a second put gave %v, want ErrExists", err)
	}
	rc, err := s.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "first" {
		t.Errorf("the object is now %q; the first writer's bytes were replaced", got)
	}
}

// TestDirStoreConcurrentPutsHaveOneWinner: two writers at one generation, which
// is a retry racing a reconciler. Exactly one may believe it committed.
func TestDirStoreConcurrentPutsHaveOneWinner(t *testing.T) {
	s, _ := dirStore(t)
	const key = "p/manifest.json"
	const writers = 8

	var wg sync.WaitGroup
	results := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = put(t, s, key, strings.Repeat("x", i+1))
		}()
	}
	wg.Wait()

	won := 0
	for i, err := range results {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, checkpoint.ErrExists):
			t.Errorf("writer %d lost with %v, want ErrExists", i, err)
		}
	}
	if won != 1 {
		t.Fatalf("%d writers committed the same object, want exactly 1", won)
	}
}

// TestDirStoreLeavesNothingWhenTheWriteFails. PutIfAbsent's contract is that an
// errored write leaves NOTHING at the key — and, here, nothing beside it
// either: a partial file left in the directory is one a later listing counts
// and a later operator wonders about.
func TestDirStoreLeavesNothingWhenTheWriteFails(t *testing.T) {
	s, dir := dirStore(t)
	ctx := context.Background()
	const key = "p/content.abc"

	want := errors.New("the writer failed half way")
	err := s.PutIfAbsent(ctx, key, func(w io.Writer) error {
		_, _ = io.WriteString(w, "half an object")
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("the put gave %v, want the writer's own error", err)
	}
	if _, err := s.Open(ctx, key); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Errorf("a failed write left an object at the key (%v)", err)
	}
	var left []string
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			left = append(left, p)
		}
		return nil
	})
	if len(left) != 0 {
		t.Errorf("a failed write left %v behind", left)
	}
}

func TestDirStoreOpenAndDeleteOfAMissingObject(t *testing.T) {
	s, _ := dirStore(t)
	ctx := context.Background()
	if _, err := s.Open(ctx, "p/nothing"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Errorf("opening a missing object gave %v, want ErrNotFound", err)
	}
	// A missing object is a successful delete: deletion is retried, and a
	// ledger that cannot report "already gone" as done never converges.
	if err := s.Delete(ctx, "p/nothing"); err != nil {
		t.Errorf("deleting a missing object gave %v", err)
	}
}

// TestDirStoreRefusesAKeyThatIsNotOne. The keys this store sees are built by
// the checkpoint package from identifiers it has already validated; this is the
// last hop before os.Remove, and the last hop trusts nobody.
func TestDirStoreRefusesAKeyThatIsNotOne(t *testing.T) {
	s, dir := dirStore(t)
	ctx := context.Background()
	for _, key := range []string{
		"", "..", "../escape", "a/../../escape", "a//b", "a/./b", "a/\x00b",
		strings.Repeat("a", maxCheckpointKeyLen+1),
	} {
		t.Run(key, func(t *testing.T) {
			if err := put(t, s, key, "x"); err == nil {
				t.Errorf("the key %q was accepted", key)
			}
			if _, err := s.Open(ctx, key); err == nil {
				t.Errorf("the key %q was opened", key)
			}
			if err := s.Delete(ctx, key); err == nil {
				t.Errorf("the key %q was deleted", key)
			}
		})
	}
	// And nothing escaped the root while we were trying.
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape")); !errors.Is(err, os.ErrNotExist) {
		t.Error("a key with .. in it wrote outside the store directory")
	}
}

// TestDirStoreIsPrivate: the object layout names a workspace and a session, so
// a directory listing tells a local user which sessions this host holds.
func TestDirStoreIsPrivate(t *testing.T) {
	s, dir := dirStore(t)
	if err := put(t, s, "ws/ws-1/manifest.json", "x"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{dir, filepath.Join(dir, "ws"), filepath.Join(dir, "ws", "ws-1")} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Errorf("%s is mode %v, want 0700", p, info.Mode().Perm())
		}
	}
}

// TestDirStoreServesTheCheckpointLibrary is the whole point of the type: a
// checkpoint written through it is one the library reads back.
func TestDirStoreServesTheCheckpointLibrary(t *testing.T) {
	s, _ := dirStore(t)
	ctx := context.Background()
	keys, err := checkpoint.NewStaticKeyWrapper(SelfHostedCheckpointKeyRef, [32]byte{4, 2})
	if err != nil {
		t.Fatal(err)
	}
	w, err := checkpoint.NewWriter(s, keys, checkpoint.WriterOptions{KeyRef: SelfHostedCheckpointKeyRef})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := checkpoint.Context{Workspace: "ws-1", Session: "sess-1", Generation: 1}
	if _, err := w.Write(ctx, c, checkpoint.DirSource(root)); err != nil {
		t.Fatalf("writing a checkpoint into the directory store: %v", err)
	}
	r, err := checkpoint.NewReader(s, keys, checkpoint.ReaderOptions{
		Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Verify(ctx, c); err != nil {
		t.Fatalf("verifying it back: %v", err)
	}
	// A second write at the same generation loses, and leaves the winner's
	// objects alone.
	if _, err := w.Write(ctx, c, checkpoint.DirSource(root)); !errors.Is(err, checkpoint.ErrExists) {
		t.Fatalf("a second checkpoint at the same generation gave %v, want ErrExists", err)
	}
	if _, err := r.Verify(ctx, c); err != nil {
		t.Fatalf("the committed checkpoint no longer verifies after a lost race: %v", err)
	}
}

func TestNewDirBlobStoreRefusesWhatItCannotUse(t *testing.T) {
	if _, err := NewDirBlobStore(""); err == nil {
		t.Error("an empty store directory was accepted")
	}
	file := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDirBlobStore(file); err == nil {
		t.Error("a store directory that is a file was accepted")
	}
}

// TestLoadCheckpointKeyAcceptsBothSpellings: 64 hex characters is what
// `openssl rand -hex 32` produces and what fits in a secret manager; 32 raw
// bytes is what `openssl rand 32 > key` produces. Both are what operators
// actually have.
func TestLoadCheckpointKeyAcceptsBothSpellings(t *testing.T) {
	want := [32]byte{}
	for i := range want {
		want[i] = byte(i + 1)
	}
	dir := t.TempDir()

	rawPath := filepath.Join(dir, "raw.key")
	if err := os.WriteFile(rawPath, want[:], 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpointKey(rawPath)
	if err != nil || got != want {
		t.Fatalf("the raw key loaded as %x, %v", got, err)
	}

	// With a trailing newline, because a file written by `echo` has one and
	// refusing that would be refusing the most likely correct key in the world.
	hexPath := filepath.Join(dir, "hex.key")
	if err := os.WriteFile(hexPath, []byte(hex.EncodeToString(want[:])+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = LoadCheckpointKey(hexPath)
	if err != nil || got != want {
		t.Fatalf("the hex key loaded as %x, %v", got, err)
	}
}

// TestLoadCheckpointKeyFailsClosed walks the conditions a real deployment gets
// wrong. Every one of them is a runner that must not start.
func TestLoadCheckpointKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, body []byte, mode os.FileMode) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, body, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	valid := bytes.Repeat([]byte{9}, 32)

	cases := []struct {
		name string
		path string
		want string
	}{
		{"no file at all", filepath.Join(dir, "absent.key"), "reading the checkpoint key file"},
		{"no path at all", "", "required"},
		{"group readable", write("group.key", valid, 0o640), "must not be readable by group or other"},
		{"world readable", write("world.key", valid, 0o644), "must not be readable by group or other"},
		{"too short", write("short.key", bytes.Repeat([]byte{1}, 16), 0o600), "must be 32 raw bytes or 64 hex"},
		{"64 characters of not-hex", write("nothex.key", []byte(strings.Repeat("z", 64)), 0o600), "is not hex"},
		{"all zeros", write("zero.key", make([]byte, 32), 0o600), "all zeros"},
		{"a directory", dir, "not a regular file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadCheckpointKey(tc.path)
			if err == nil {
				t.Fatalf("the key was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal is %q, want it to name %q", err, tc.want)
			}
			// A refusal about a key file must not quote the key.
			if strings.Contains(err.Error(), string(valid)) || strings.Contains(err.Error(), hex.EncodeToString(valid)) {
				t.Errorf("the refusal quotes key material: %v", err)
			}
		})
	}
}
