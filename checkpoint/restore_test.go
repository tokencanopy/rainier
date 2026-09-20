package checkpoint

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// crafter builds a tar stream by hand, which is how the restore path's
// structural rules are tested against entries this package's Writer would never
// produce. Every write is checked: a fixture archive/tar refuses to encode is a
// test that silently examines an empty stream and passes.
type crafter struct {
	t  *testing.T
	tw *tar.Writer
}

func (c *crafter) dir(name string, mode int64) {
	c.hdr(&tar.Header{Typeflag: tar.TypeDir, Name: name, Mode: mode, ModTime: time.Unix(0, 0)})
}

func (c *crafter) link(name, target string) {
	c.hdr(&tar.Header{Typeflag: tar.TypeSymlink, Name: name, Linkname: target, ModTime: time.Unix(0, 0)})
}

func (c *crafter) file(name string, mode int64, body string) {
	c.hdr(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: mode,
		Size: int64(len(body)), ModTime: time.Unix(0, 0)})
	if body == "" {
		return
	}
	if _, err := c.tw.Write([]byte(body)); err != nil {
		c.t.Fatalf("crafting the fixture: %v", err)
	}
}

func (c *crafter) hdr(h *tar.Header) {
	c.t.Helper()
	if err := c.tw.WriteHeader(h); err != nil {
		c.t.Fatalf("crafting the fixture: %v", err)
	}
}

func craft(t *testing.T, fn func(*crafter)) *tar.Reader {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	fn(&crafter{t: t, tw: tw})
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return tar.NewReader(bytes.NewReader(buf.Bytes()))
}

// bigManifest is a manifest permissive enough that the per-entry byte bound is
// not what refuses a crafted stream.
func bigManifest() Manifest { return Manifest{FileBytes: 1 << 30} }

// TestChainedSymlinkCannotEscapeTheTarget is the one the per-link lexical check
// does not catch on its own, and the reason consumeStream keeps a directory
// stack.
//
// Each link below is contained when checked against its OWN name:
// Join("a", "..") is ".", and Join("a/d", "..") is "a". But `a/d/up` is created
// inside whatever `a/d` points at — the target root — so its ".." leaves the
// target, and `a/d/up/pwned` lands next to it. Composing two individually
// contained links escapes; requiring every entry's parent to be a directory this
// walk created is what stops it.
func TestChainedSymlinkCannotEscapeTheTarget(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "t")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}

	stream := func() *tar.Reader {
		return craft(t, func(c *crafter) {
			c.dir("a", 0o755)
			c.link("a/d", "..")
			c.link("a/d/up", "..")
			c.file("a/d/up/pwned", 0o644, "pwned")
		})
	}

	var st streamStats
	err := consumeStream(context.Background(), stream(), target, bigManifest(), &st)
	if !errors.Is(err, ErrEntry) {
		t.Fatalf("Restore of a chained-symlink stream = %v, want ErrEntry", err)
	}

	ents, readErr := os.ReadDir(base)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, e := range ents {
		if e.Name() != "t" {
			t.Errorf("ESCAPE: %q was created outside the restore target", e.Name())
		}
	}

	// And Verify refuses the same stream, so the durability barrier could never
	// have called this checkpoint good.
	var vst streamStats
	if err := consumeStream(context.Background(), stream(), "", bigManifest(), &vst); !errors.Is(err, ErrEntry) {
		t.Fatalf("Verify of a chained-symlink stream = %v, want ErrEntry", err)
	}
}

// TestEntryParentMustBeADirectoryThisWalkCreated covers the rule the chained
// symlink is one instance of: an entry whose parent is a link, a file, or absent
// entirely is refused, in both modes.
func TestEntryParentMustBeADirectoryThisWalkCreated(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*crafter)
	}{
		{"parent is a symbolic link", func(c *crafter) {
			c.link("l", ".")
			c.file("l/x", 0o644, "")
		}},
		{"parent is a regular file", func(c *crafter) {
			c.file("f", 0o644, "")
			c.file("f/x", 0o644, "")
		}},
		{"parent has no entry at all", func(c *crafter) {
			c.file("missing/x", 0o644, "")
		}},
		{"a directory reopened after the walk left it", func(c *crafter) {
			c.dir("a", 0o755)
			c.file("a/x", 0o644, "")
			c.dir("b", 0o755)
			c.file("a/y", 0o644, "") // out of depth-first order
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"restore", "verify"} {
				target := ""
				if mode == "restore" {
					target = filepath.Join(t.TempDir(), "r")
					if err := os.MkdirAll(target, 0o755); err != nil {
						t.Fatal(err)
					}
				}
				var st streamStats
				err := consumeStream(context.Background(), craft(t, tc.fn), target, bigManifest(), &st)
				if !errors.Is(err, ErrEntry) {
					t.Errorf("%s = %v, want ErrEntry", mode, err)
				}
			}
		})
	}
}

// chmodWritable makes a tree removable again. A 0555 directory cannot have its
// children unlinked, so t.TempDir's own cleanup would fail on the fixtures below.
func chmodWritable(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() {
		filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				os.Chmod(p, 0o700)
			}
			return nil
		})
	})
}

// TestWriteProtectedDirectoryRoundTrips is the shape `go mod download` leaves
// behind for every module it extracts: a directory at 0555 with files inside it.
// A restorer that applied the recorded mode when it created the directory could
// not write the children — and Verify, which writes nothing, would never notice.
// That combination is the one the durability barrier cannot survive, because it
// opens the disk-deletion door on Verify's word.
func TestWriteProtectedDirectoryRoundTrips(t *testing.T) {
	root := t.TempDir()
	chmodWritable(t, root)

	modcache := filepath.Join(root, "modcache", "example.test", "pkg@v1.0.0")
	if err := os.MkdirAll(filepath.Join(modcache, "internal"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"go.mod", "internal/deep.go"} {
		if err := os.WriteFile(filepath.Join(modcache, f), []byte("package pkg\n"), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	// Innermost first, so the chmod of a parent does not block the next write.
	for _, d := range []string{filepath.Join(modcache, "internal"), modcache} {
		if err := os.Chmod(d, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	stampTree(t, root)

	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	res, err := h.w.Write(ctx, h.c, DirSource(root))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := h.r.Verify(ctx, h.c); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	target := filepath.Join(t.TempDir(), "restored")
	chmodWritable(t, target)
	if _, err := h.r.Restore(ctx, h.c, target); err != nil {
		t.Fatalf("Restore of a tree with a write-protected directory: %v", err)
	}
	compareTrees(t, root, target, nil)

	// And the modes actually landed, rather than being left at the 0700 the walk
	// creates directories with.
	for _, rel := range []string{
		"modcache/example.test/pkg@v1.0.0",
		"modcache/example.test/pkg@v1.0.0/internal",
	} {
		fi, err := os.Lstat(filepath.Join(target, rel))
		if err != nil {
			t.Fatalf("lstat: %v", err)
		}
		if fi.Mode().Perm() != 0o555 {
			t.Errorf("restored directory mode = %v, want 0555", fi.Mode().Perm())
		}
	}
	// Re-checkpointing the restored tree agrees, which pins the modes into the
	// digest rather than into this test's eyes only.
	next := h.c
	next.Generation++
	again, err := h.w.Write(ctx, next, DirSource(target))
	if err != nil {
		t.Fatalf("Write of the restored tree: %v", err)
	}
	if again.Manifest.TreeDigest != res.Manifest.TreeDigest {
		t.Error("the restored tree has a different tree digest")
	}
}

func TestSparseAndOversizedEntriesAreRefused(t *testing.T) {
	// The sparse guard is unit-tested rather than driven through a crafted
	// stream, because archive/tar's WRITER refuses to encode a reserved
	// GNU.sparse.* record at all — only its reader implements them, which is
	// exactly the asymmetry the guard exists for. A stream carrying one has to
	// come from somewhere other than Go's encoder.
	t.Run("a PAX sparse record", func(t *testing.T) {
		for _, key := range []string{"GNU.sparse.major", "GNU.sparse.size", "GNU.sparse.map"} {
			hdr := &tar.Header{Typeflag: tar.TypeReg, Name: "sparse.bin",
				PAXRecords: map[string]string{key: "1"}}
			if err := noSparseRecords(hdr, 1); !errors.Is(err, ErrEntry) {
				t.Errorf("%s = %v, want ErrEntry", key, err)
			}
		}
		// An ordinary PAX record — the one a long name produces — is fine.
		hdr := &tar.Header{Typeflag: tar.TypeReg, Name: "a", PAXRecords: map[string]string{"path": "a"}}
		if err := noSparseRecords(hdr, 1); err != nil {
			t.Errorf("an ordinary PAX record was refused: %v", err)
		}
	})

	t.Run("an entry claiming more bytes than the manifest allows", func(t *testing.T) {
		stream := craft(t, func(c *crafter) {
			c.file("a", 0o644, "aaaa")
			c.file("b", 0o644, "bbbb")
		})
		// The manifest commits to five file bytes; the second entry asks to take
		// the total to eight, and is refused before a byte of it is written.
		target := filepath.Join(t.TempDir(), "r")
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatal(err)
		}
		var st streamStats
		err := consumeStream(context.Background(), stream, target, Manifest{FileBytes: 5}, &st)
		if !errors.Is(err, ErrMismatch) {
			t.Fatalf("= %v, want ErrMismatch", err)
		}
		if _, err := os.Stat(filepath.Join(target, "b")); !errors.Is(err, fs.ErrNotExist) {
			t.Error("the oversized entry was created before it was refused")
		}
	})
}

func TestRestoreIntoARelativeTarget(t *testing.T) {
	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	if _, err := h.w.Write(ctx, h.c, DirSource(fixtureTree(t))); err != nil {
		t.Fatal(err)
	}
	// A relative target, and "." in particular, must be the caller's business and
	// never reported as a bad checkpoint.
	dir := t.TempDir()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(prev) })

	if _, err := h.r.Restore(ctx, h.c, "."); err != nil {
		t.Fatalf(`Restore(".") = %v`, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
		t.Errorf("the tree did not land in the working directory: %v", err)
	}
}

// ctxBlindStore ignores the context, so a cancellation test exercises this
// package's own checks rather than MemoryStore's.
type ctxBlindStore struct{ BlobStore }

func (s ctxBlindStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	return s.BlobStore.Open(context.Background(), key)
}

func TestCancellationStopsAWriteAndARestore(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		store := newDiscardStore()
		w, err := NewWriter(store, testWrapper(t), WriterOptions{KeyRef: testKeyRef})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = w.Write(ctx, testContext(), DirSource(fixtureTree(t)))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Write with a cancelled context = %v, want context.Canceled", err)
		}
	})

	t.Run("restore", func(t *testing.T) {
		h := newHarness(t, MinFrameSize)
		if _, err := h.w.Write(context.Background(), h.c, DirSource(fixtureTree(t))); err != nil {
			t.Fatal(err)
		}
		r, err := NewReader(ctxBlindStore{h.store}, testWrapper(t), ReaderOptions{
			Prefix:    "checkpoints",
			Authorize: (&authorizeRecorder{}).hook,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		target := filepath.Join(t.TempDir(), "r")
		if _, err := r.Restore(ctx, h.c, target); !errors.Is(err, context.Canceled) {
			t.Fatalf("Restore with a cancelled context = %v, want context.Canceled", err)
		}
	})
}

func TestSetuidIsDropped(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "suid")
	if err := os.WriteFile(p, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o4755); err != nil {
		t.Skipf("this environment cannot set the setuid bit: %v", err)
	}
	if fi, err := os.Lstat(p); err != nil || fi.Mode()&fs.ModeSetuid == 0 {
		t.Skip("this environment did not keep the setuid bit")
	}
	stampTree(t, root)

	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	if _, err := h.w.Write(ctx, h.c, DirSource(root)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	target := filepath.Join(t.TempDir(), "r")
	if _, err := h.r.Restore(ctx, h.c, target); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	fi, err := os.Lstat(filepath.Join(target, "suid"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 {
		t.Errorf("restored mode = %v, want the setuid bit dropped", fi.Mode())
	}
	if fi.Mode().Perm() != 0o755 {
		t.Errorf("restored permissions = %v, want 0755", fi.Mode().Perm())
	}
}

// TestFrameWriterBoundaries exercises the "hold a full buffer until Close knows
// whether it is final" rule at the sizes where it can go wrong: empty, one byte
// short of a frame, EXACTLY a frame (where a naive implementation seals a full
// buffer as non-final and then has no final frame), one byte over, and a single
// Write spanning many frames.
func TestFrameWriterBoundaries(t *testing.T) {
	key := bytes.Repeat([]byte{1}, dekLen)
	prefix := bytes.Repeat([]byte{2}, noncePrefixLen)

	for _, n := range []int{1, MinFrameSize - 1, MinFrameSize, MinFrameSize + 1,
		2 * MinFrameSize, 10*MinFrameSize + 3} {
		var buf bytes.Buffer
		fw, err := newFrameWriter(&buf, testContext(), key, prefix, MinFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		plain := pseudorandom(n)
		// One Write for the whole thing, which is the case that spans frames.
		if _, err := fw.Write(plain); err != nil {
			t.Fatalf("n=%d Write: %v", n, err)
		}
		if err := fw.Close(); err != nil {
			t.Fatalf("n=%d Close: %v", n, err)
		}
		wantFrames := uint32((n + MinFrameSize - 1) / MinFrameSize)
		if n == 0 {
			wantFrames = 1
		}
		if fw.frames != wantFrames {
			t.Errorf("n=%d frames = %d, want %d", n, fw.frames, wantFrames)
		}
		if int64(buf.Len()) != fw.plain+int64(fw.frames)*tagLen {
			t.Errorf("n=%d ciphertext length %d does not match the manifest arithmetic", n, buf.Len())
		}
		fr, err := newFrameReader(bytes.NewReader(buf.Bytes()), testContext(), key, prefix,
			MinFrameSize, fw.frames, fw.plain)
		if err != nil {
			t.Fatalf("n=%d newFrameReader: %v", n, err)
		}
		got, err := io.ReadAll(fr)
		if err != nil {
			t.Fatalf("n=%d read back: %v", n, err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("n=%d did not round trip", n)
		}
	}
}

// sizeLyingFS reports a size that is not what Open will produce, which is what a
// source that was not quiesced looks like from the walk's point of view.
type sizeLyingFS struct {
	fs.FS
	name  string
	delta int64
}

func (s sizeLyingFS) Open(name string) (fs.File, error) {
	f, err := s.FS.Open(name)
	if err != nil || name != s.name {
		return f, err
	}
	return lyingFile{File: f, delta: s.delta}, nil
}

type lyingFile struct {
	fs.File
	delta int64
}

func (l lyingFile) Stat() (fs.FileInfo, error) {
	fi, err := l.File.Stat()
	if err != nil {
		return nil, err
	}
	return lyingInfo{FileInfo: fi, delta: l.delta}, nil
}

type lyingInfo struct {
	fs.FileInfo
	delta int64
}

func (l lyingInfo) Size() int64 { return l.FileInfo.Size() + l.delta }

// statLyingFS makes WalkDir's DirEntry.Info() report the wrong size, which is the
// value the tar header promises.
type statLyingFS struct {
	fs.FS
	name  string
	delta int64
}

func (s statLyingFS) Open(name string) (fs.File, error) {
	f, err := s.FS.Open(name)
	if err != nil {
		return nil, err
	}
	if _, ok := f.(fs.ReadDirFile); ok {
		return dirLying{File: f, fsys: s}, nil
	}
	return f, nil
}

type dirLying struct {
	fs.File
	fsys statLyingFS
}

func (d dirLying) ReadDir(n int) ([]fs.DirEntry, error) {
	ents, err := d.File.(fs.ReadDirFile).ReadDir(n)
	for i, e := range ents {
		if e.Name() == d.fsys.name {
			ents[i] = lyingEntry{DirEntry: e, delta: d.fsys.delta}
		}
	}
	return ents, err
}

type lyingEntry struct {
	fs.DirEntry
	delta int64
}

func (l lyingEntry) Info() (fs.FileInfo, error) {
	fi, err := l.DirEntry.Info()
	if err != nil {
		return nil, err
	}
	return lyingInfo{FileInfo: fi, delta: l.delta}, nil
}

// TestQuiesceViolationIsDetected covers both directions: a file that shrank
// between the walk's stat and the read desynchronizes the tar stream, and one
// that grew would be a silent truncation that verified. ADR-0003 §4.4 puts
// quiescing on the caller; this is the library detecting the violation.
func TestQuiesceViolationIsDetected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delta int64
	}{
		{"the file shrank under the walk", +16},
		{"the file grew under the walk", -16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "moving.bin"), pseudorandom(4096), 0o644); err != nil {
				t.Fatal(err)
			}
			h := newHarness(t, MinFrameSize)
			src := Source{FS: statLyingFS{FS: os.DirFS(root), name: "moving.bin", delta: tc.delta}}
			_, err := h.w.Write(context.Background(), h.c, src)
			if !errors.Is(err, ErrSource) {
				t.Fatalf("Write = %v, want ErrSource", err)
			}
			if !strings.Contains(err.Error(), "quiesced") {
				t.Errorf("the error does not name the cause: %v", err)
			}
			assertNoContentInError(t, root, err)
		})
	}
}

// TestSourceAndRestoreErrorsCarryNoPath exercises the two error families that
// actually have a path available to leak. The tamper tests assert the same rule
// on errors that never had one, which is a weaker thing to prove.
func TestSourceAndRestoreErrorsCarryNoPath(t *testing.T) {
	const distinctive = "a-very-distinctive-directory-name"
	const filename = "an-equally-distinctive-file.txt"

	t.Run("a refused source entry", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, distinctive)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("/etc/shadow", filepath.Join(dir, filename)); err != nil {
			t.Fatal(err)
		}
		h := newHarness(t, MinFrameSize)
		_, err := h.w.Write(context.Background(), h.c, DirSource(root))
		if !errors.Is(err, ErrSource) {
			t.Fatalf("Write = %v, want ErrSource", err)
		}
		for _, leak := range []string{distinctive, filename, "/etc/shadow", root} {
			if strings.Contains(err.Error(), leak) {
				t.Errorf("the error quotes %q: %v", leak, err)
			}
		}
		assertNoContentInError(t, root, err)
	})

	t.Run("a destination that cannot hold the tree", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, distinctive)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, filename), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		stampTree(t, root)

		h := newHarness(t, MinFrameSize)
		ctx := context.Background()
		if _, err := h.w.Write(ctx, h.c, DirSource(root)); err != nil {
			t.Fatal(err)
		}
		// An empty but write-protected target: it passes prepareTarget and then
		// refuses the first entry.
		target := filepath.Join(t.TempDir(), "readonly")
		if err := os.Mkdir(target, 0o555); err != nil {
			t.Fatal(err)
		}
		chmodWritable(t, target)
		_, err := h.r.Restore(ctx, h.c, target)
		if !errors.Is(err, ErrRestore) {
			t.Fatalf("Restore into a write-protected target = %v, want ErrRestore", err)
		}
		if !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("the error lost the errno that makes it actionable: %v", err)
		}
		for _, leak := range []string{distinctive, filename, target, root} {
			if strings.Contains(err.Error(), leak) {
				t.Errorf("the error quotes %q: %v", leak, err)
			}
		}
	})
}

func TestKeyRefCharsetIsEnforced(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  KeyRef
	}{
		{"a newline", "keys/1\nkey_ref=forged"},
		{"a NUL", "keys/1\x00x"},
		{"an ANSI escape", "keys/\x1b[31m1"},
		{"a space", "keys/ 1"},
		{"a high byte", "keys/\xff1"},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewStaticKeyWrapper(tc.ref, testKey()); !errors.Is(err, ErrInvalid) {
				t.Errorf("NewStaticKeyWrapper = %v, want ErrInvalid", err)
			}
			if tc.ref == "" {
				return // an empty WriterOptions.KeyRef means "the wrapper's default"
			}
			if _, err := NewWriter(NewMemoryStore(), testWrapper(t), WriterOptions{KeyRef: tc.ref}); !errors.Is(err, ErrInvalid) {
				t.Errorf("NewWriter = %v, want ErrInvalid", err)
			}
		})
	}
	// And a manifest carrying one is refused on parse, which is the path that
	// matters: the field is attacker-controlled for anyone who can write to the
	// bucket, and it reaches an operator's eyes through Summary.
	c := commit(t)
	c.setManifestField(t, "key_ref", "keys/1\n\x1b[31mRED")
	if _, err := c.h.r.Verify(context.Background(), c.h.c); !errors.Is(err, ErrManifest) {
		t.Fatalf("Verify = %v, want ErrManifest", err)
	}
}
