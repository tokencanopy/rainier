// internal/driver/clone_test.go
package driver

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// fakeCloner is a Cloner with a filesystem behind it but no opinion about
// extents: it records every (src, dst) pair and answers with whichever method
// the test told it to. It is what the driver's own tests use to see WHAT was
// cloned — that a create clones the base image and not the workspace, that a
// cold resume clones again — without turning those tests into assertions about
// the filesystem the test happens to run on.
type fakeCloner struct {
	method CloneMethod
	// err, when set, is what every clone reports.
	err error

	// mu guards clones. A snapshot's copy and another session's create can be
	// in flight at once — which is the point of one of the tests — so the
	// recorder has to be safe to call from two goroutines even where the test
	// that does it is otherwise ordered by channels.
	mu     sync.Mutex
	clones []clonedPair
}

type clonedPair struct {
	src, dst string
	method   CloneMethod
}

func (c *fakeCloner) Clone(src, dst string) (CloneMethod, error) {
	if c.err != nil {
		return "", c.err
	}
	method := c.method
	if method == "" {
		method = CloneReflink
	}
	// The bytes are copied for real, so a test can plant a marker in a source
	// and look for it in the result — but only for a source small enough to
	// hold in memory. A session's workspace is a 10 GiB sparse image and is
	// never something a clone should have been pointed at, so a test that
	// points one there gets a failed assertion rather than a dead machine.
	if fi, err := os.Stat(src); err == nil && fi.Size() > 64<<20 {
		return "", errors.New("fakeCloner: refusing to clone a disk-sized file; only an environment image and a session rootfs are ever cloned")
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(dst, data, microvmFileMode); err != nil {
		return "", err
	}
	c.mu.Lock()
	c.clones = append(c.clones, clonedPair{src: src, dst: dst, method: method})
	c.mu.Unlock()
	return method, nil
}

// copies is what this cloner was asked to do, in order.
func (c *fakeCloner) copies() []clonedPair {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]clonedPair(nil), c.clones...)
}

func (c *fakeCloner) sources() []string {
	out := []string{}
	for _, p := range c.copies() {
		out = append(out, p.src)
	}
	return out
}

// writeSparse makes a file with a hole in the middle: data, a gap nothing was
// written to, and data again. It is the shape of a real environment image —
// an ext4 filesystem sized for what a session might need and occupying what it
// actually holds.
func writeSparse(t *testing.T, path string, size int64, chunks map[int64][]byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatalf("size %s: %v", path, err)
	}
	for off, data := range chunks {
		if _, err := f.WriteAt(data, off); err != nil {
			t.Fatalf("write %s at %d: %v", path, off, err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync %s: %v", path, err)
	}
}

// blocksOf is how much of a file the filesystem actually allocated, in 512-
// byte units — the number that tells a sparse copy from one that filled in the
// holes. A platform whose Stat has no such field skips the assertion rather
// than failing it.
func blocksOf(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skipf("this platform's stat does not report allocated blocks")
	}
	return st.Blocks
}

func readAll(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

// TestFileClonerCopiesEveryByte is the correctness floor, whichever path the
// filesystem under the test takes: the copy is the source, hole for hole and
// byte for byte, and the two files are independent afterwards.
func TestFileClonerCopiesEveryByte(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "base.ext4")
	const size = 1 << 20
	writeSparse(t, src, size, map[int64][]byte{
		0:          []byte("superblock"),
		512 << 10:  []byte("a file somewhere in the middle"),
		size - 512: []byte("the tail"),
	})

	c := NewFileCloner()
	dst := filepath.Join(dir, "session.ext4")
	method, err := c.Clone(src, dst)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if method != CloneReflink && method != CloneSparseCopy {
		t.Fatalf("Clone reported method %q", method)
	}
	if got := c.Methods(); len(got) != 1 || got[0] != method {
		t.Errorf("Methods() = %v, want one %q", got, method)
	}
	if want, got := readAll(t, src), readAll(t, dst); !bytes.Equal(want, got) {
		t.Fatalf("the clone is not the source: %d bytes vs %d", len(got), len(want))
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != size {
		t.Errorf("clone size = %d, want %d", fi.Size(), size)
	}
	// 0600: one tenant's root filesystem on a host that runs others.
	if fi.Mode().Perm() != microvmFileMode {
		t.Errorf("clone mode = %#o, want %#o", fi.Mode().Perm(), microvmFileMode)
	}

	// Copy-on-write means the two files stop being the same file the moment
	// either is written. A reflink that shared pages rather than extents would
	// fail here, and so would an implementation that hard-linked.
	f, err := os.OpenFile(dst, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("the guest wrote this"), 4096); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if bytes.Contains(readAll(t, src), []byte("the guest wrote this")) {
		t.Fatal("writing to the clone changed the image it was cloned from")
	}
}

// TestFileClonerFallsBackWhenReflinkIsRefused is the fallback path, made
// deterministic: whether an FICLONE succeeds is a property of the filesystem
// the test runs on, so the ioctl is the seam and the refusal is injected.
func TestFileClonerFallsBackWhenReflinkIsRefused(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "base.ext4")
	const size = 256 << 10
	writeSparse(t, src, size, map[int64][]byte{
		0:         []byte("superblock"),
		128 << 10: []byte("data past a hole"),
	})

	refused := 0
	c := &FileCloner{reflink: func(dst, src *os.File) error {
		refused++
		return errNoReflink
	}}
	dst := filepath.Join(dir, "session.ext4")
	method, err := c.Clone(src, dst)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if refused != 1 {
		t.Errorf("the reflink was attempted %d times, want 1", refused)
	}
	if method != CloneSparseCopy {
		t.Fatalf("method = %q, want %q when the filesystem refuses to reflink", method, CloneSparseCopy)
	}
	if got := c.Methods(); len(got) != 1 || got[0] != CloneSparseCopy {
		t.Errorf("Methods() = %v", got)
	}
	if want, got := readAll(t, src), readAll(t, dst); !bytes.Equal(want, got) {
		t.Fatal("the fallback copy is not the source")
	}
	// And the holes are still holes: a fallback that wrote zeroes would make
	// every session's rootfs occupy the image's full apparent size, which on a
	// host packing a dozen sessions is the difference between fitting and not.
	if occupied := blocksOf(t, dst); occupied > blocksOf(t, src)*4+64 {
		t.Errorf("the copy occupies %d blocks where the source occupies %d; the holes were filled in",
			occupied, blocksOf(t, src))
	}
}

// TestFileClonerReportsAFailedReflink separates the two kinds of FICLONE
// failure: one means "this filesystem does not do that" and is the fallback's
// cue, the other means the copy went wrong and must not be papered over with
// a slow copy that might succeed and hide it.
func TestFileClonerReportsAFailedReflink(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "base.ext4")
	writeSparse(t, src, 4096, map[int64][]byte{0: []byte("superblock")})

	boom := errors.New("the device is on fire")
	c := &FileCloner{reflink: func(dst, src *os.File) error { return boom }}
	dst := filepath.Join(dir, "session.ext4")
	if _, err := c.Clone(src, dst); !errors.Is(err, boom) {
		t.Fatalf("Clone = %v, want the reflink's own failure", err)
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Error("a failed clone left a partial root filesystem behind")
	}
}

// TestFileClonerRefusesAnExistingDestination: a clone that found a file where
// the session's rootfs goes is a clone about to overwrite something nobody
// accounted for.
func TestFileClonerRefusesAnExistingDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "base.ext4")
	writeSparse(t, src, 4096, map[int64][]byte{0: []byte("superblock")})
	dst := filepath.Join(dir, "session.ext4")
	if err := os.WriteFile(dst, []byte("somebody else's"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := NewFileCloner()
	if _, err := c.Clone(src, dst); err == nil {
		t.Fatal("Clone over an existing file succeeded")
	}
	if got := readAll(t, dst); string(got) != "somebody else's" {
		t.Errorf("the existing file was modified: %q", got)
	}
}

// TestFileClonerRefusesAMissingSource: an image that is not there is an error
// and never an empty root filesystem.
func TestFileClonerRefusesAMissingSource(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "session.ext4")
	if _, err := NewFileCloner().Clone(filepath.Join(dir, "absent.ext4"), dst); err == nil {
		t.Fatal("Clone of a missing image succeeded")
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Error("a clone of a missing image created a destination anyway")
	}
}
