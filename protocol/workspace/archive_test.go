package workspace_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/tokencanopy/rainier/protocol/workspace"
)

// writeTree lays out a small but representative source tree: nested
// directories, an executable, an empty file, and a relative symlink of the
// kind node_modules/.bin is full of.
func writeTree(t *testing.T, root string) {
	t.Helper()
	mkdir(t, filepath.Join(root, "src"))
	mkdir(t, filepath.Join(root, "bin"))
	write(t, filepath.Join(root, "README.md"), "# hello\n", 0o644)
	write(t, filepath.Join(root, "src", "main.go"), "package main\n", 0o644)
	write(t, filepath.Join(root, "src", "empty"), "", 0o644)
	write(t, filepath.Join(root, "bin", "run.sh"), "#!/bin/sh\necho hi\n", 0o755)
	if err := os.Symlink("../src/main.go", filepath.Join(root, "bin", "main.go")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
}

func mkdir(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", p, err)
	}
}

func write(t *testing.T, p, body string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatalf("chmod %s: %v", p, err)
	}
}

// snapshot renders a directory tree as a comparable description: every entry's
// relative path, kind, mode and content (or link target). Two trees with the
// same snapshot round-tripped byte for byte.
func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	// A destination that was never created is the emptiest tree there is —
	// which is exactly what a refused archive is supposed to leave behind.
	if _, err := os.Lstat(root); os.IsNotExist(err) {
		return out
	}
	err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
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
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			out[rel] = "symlink -> " + target
		case fi.IsDir():
			out[rel] = "dir"
		default:
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			out[rel] = "file " + fi.Mode().Perm().String() + " " + string(b)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

// TestTarGzUntarGzRoundTrip is the whole transfer's fidelity in one test: a
// tree in, the same tree out, modes and symlinks included.
func TestTarGzUntarGzRoundTrip(t *testing.T) {
	src := t.TempDir()
	writeTree(t, src)

	archive := filepath.Join(t.TempDir(), "a.tgz")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	n, err := workspace.TarGz(f, src, workspace.MaxBytes)
	if err != nil {
		t.Fatalf("TarGz: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	if n <= 0 {
		t.Fatalf("TarGz wrote %d bytes", n)
	}
	fi, err := os.Stat(archive)
	if err != nil {
		t.Fatalf("stat archive: %v", err)
	}
	if fi.Size() != n {
		t.Fatalf("TarGz reported %d bytes, file is %d", n, fi.Size())
	}

	dest := t.TempDir()
	if err := workspace.UntarGz(archive, dest, workspace.MaxExtractBytes); err != nil {
		t.Fatalf("UntarGz: %v", err)
	}

	want, got := snapshot(t, src), snapshot(t, dest)
	if len(want) != len(got) {
		t.Fatalf("extracted %d entries, want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("entry %q = %q, want %q", k, got[k], v)
		}
	}
}

// TestTarGzRefusesOverTheLimit proves the compressed cap is enforced while
// writing rather than after: nothing about the archive's real size is knowable
// up front, so a transfer that would blow the cap has to die mid-stream.
func TestTarGzRefusesOverTheLimit(t *testing.T) {
	src := t.TempDir()
	// Incompressible, so the cap is reached in the compressed stream too.
	blob := make([]byte, 512<<10)
	if _, err := rand.Read(blob); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "blob"), blob, 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}

	var buf bytes.Buffer
	_, err := workspace.TarGz(&buf, src, 64<<10)
	if err == nil {
		t.Fatal("TarGz over the limit returned nil")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("TarGz error = %v, want it to say the archive is too large", err)
	}
}

// TestTarGzRefusesIrregularFiles: a socket or a device node has no meaning on
// the far side of a transfer, and silently skipping one would ship a tree that
// is not the tree the user asked to push.
func TestTarGzRefusesIrregularFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no mkfifo on windows")
	}
	src := t.TempDir()
	fifo := filepath.Join(src, "pipe")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	var buf bytes.Buffer
	if _, err := workspace.TarGz(&buf, src, workspace.MaxBytes); err == nil {
		t.Fatal("TarGz of a tree containing a fifo returned nil")
	}
}

// TestTarGzRefusesEscapingSymlinksBeforeAnyBytesMove: the far end applies
// checkLink on extraction, so a tree containing an absolute or escaping
// symlink was always refused — but only after up to 256MiB had crossed the
// wire, chunk by chunk, to fail on the last one. The same rule applied at pack
// time turns that into an error about a file on the pusher's own disk, named,
// before the first byte.
//
// It is the same function on purpose. A rule implemented twice is a rule that
// is eventually implemented once.
func TestTarGzRefusesEscapingSymlinksBeforeAnyBytesMove(t *testing.T) {
	cases := []struct{ name, link string }{
		{"absolute", "/usr/bin/node"},
		{"escaping", "../../../etc/passwd"},
		{"escaping from a subdirectory", "../../outside"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := t.TempDir()
			mkdir(t, filepath.Join(src, "vendor"))
			write(t, filepath.Join(src, "keep.txt"), "innocent\n", 0o644)
			if err := os.Symlink(c.link, filepath.Join(src, "vendor", "node")); err != nil {
				t.Fatalf("symlink: %v", err)
			}
			var buf bytes.Buffer
			if _, err := workspace.TarGz(&buf, src, workspace.MaxBytes); err == nil {
				t.Fatal("TarGz packed a symlink the far end is going to refuse")
			} else if !strings.Contains(err.Error(), "vendor/node") {
				t.Fatalf("TarGz error = %v, want it to name the offending file", err)
			}
		})
	}

	// The rule is containment, not a ban on symlinks: the relative ones every
	// node_modules/.bin is full of still travel.
	t.Run("a symlink that stays inside still travels", func(t *testing.T) {
		src := t.TempDir()
		writeTree(t, src)
		var buf bytes.Buffer
		if _, err := workspace.TarGz(&buf, src, workspace.MaxBytes); err != nil {
			t.Fatalf("TarGz refused an ordinary relative symlink: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// hostile archives
//
// These are the tests that matter most: the archive is untrusted at BOTH ends
// (a client pushing into a sandbox, a sandbox answering a pull), so an entry
// that escapes its destination must be refused before ANY entry is written.
// ---------------------------------------------------------------------------

// buildTar writes a gzipped tar of the given headers/bodies to a temp file and
// returns its path — the only way to produce archives no honest TarGz would.
func buildTar(t *testing.T, entries []tar.Header, bodies []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hostile.tgz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	tw := tar.NewWriter(zw)
	for i, h := range entries {
		hdr := h
		if i < len(bodies) {
			hdr.Size = int64(len(bodies[i]))
		}
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatalf("write header %q: %v", hdr.Name, err)
		}
		if i < len(bodies) && bodies[i] != "" {
			if _, err := io.WriteString(tw, bodies[i]); err != nil {
				t.Fatalf("write body %q: %v", hdr.Name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return path
}

func TestUntarGzRefusesEscapingEntries(t *testing.T) {
	cases := []struct {
		name    string
		entries []tar.Header
		bodies  []string
	}{
		{
			name: "parent traversal",
			entries: []tar.Header{
				{Name: "keep.txt", Mode: 0o644, Typeflag: tar.TypeReg},
				{Name: "../escaped.txt", Mode: 0o644, Typeflag: tar.TypeReg},
			},
			bodies: []string{"innocent", "owned"},
		},
		{
			name: "absolute path",
			entries: []tar.Header{
				{Name: "/etc/passwd", Mode: 0o644, Typeflag: tar.TypeReg},
			},
			bodies: []string{"owned"},
		},
		{
			name: "buried traversal",
			entries: []tar.Header{
				{Name: "a/b/../../../escaped.txt", Mode: 0o644, Typeflag: tar.TypeReg},
			},
			bodies: []string{"owned"},
		},
		{
			name: "escaping symlink",
			entries: []tar.Header{
				{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../../etc/passwd"},
			},
		},
		{
			name: "absolute symlink",
			entries: []tar.Header{
				{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
			},
		},
		{
			name: "hard link",
			entries: []tar.Header{
				{Name: "link", Typeflag: tar.TypeLink, Linkname: "../../etc/passwd"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archive := buildTar(t, tc.entries, tc.bodies)
			dest := filepath.Join(t.TempDir(), "dest")
			err := workspace.UntarGz(archive, dest, workspace.MaxExtractBytes)
			if err == nil {
				t.Fatal("UntarGz accepted a hostile archive")
			}
			// Nothing at all may have been written: the refusal has to come
			// from the validating pass, not from the extracting one halfway
			// through.
			if entries := snapshot(t, dest); len(entries) != 0 {
				t.Fatalf("hostile archive left %v behind; extraction must be all-or-nothing", entries)
			}
			if _, err := os.Lstat(filepath.Join(filepath.Dir(dest), "escaped.txt")); err == nil {
				t.Fatal("an entry escaped the destination directory")
			}
		})
	}
}

// TestUntarGzRefusesADecompressionBomb: the compressed cap says nothing about
// what an archive expands to, so the extract has its own bound.
func TestUntarGzRefusesADecompressionBomb(t *testing.T) {
	archive := buildTar(t,
		[]tar.Header{{Name: "big", Mode: 0o644, Typeflag: tar.TypeReg}},
		[]string{strings.Repeat("a", 8<<10)})
	dest := filepath.Join(t.TempDir(), "dest")
	err := workspace.UntarGz(archive, dest, 1<<10)
	if err == nil {
		t.Fatal("UntarGz extracted past its limit")
	}
	if entries := snapshot(t, dest); len(entries) != 0 {
		t.Fatalf("over-limit archive left %v behind", entries)
	}
}

// TestUntarGzRefusesGarbage: a non-gzip body (a truncated stream, an error page
// that reached the archive path) must be an error, never a panic.
func TestUntarGzRefusesGarbage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "garbage.tgz")
	if err := os.WriteFile(path, []byte("not a gzip stream at all"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := workspace.UntarGz(path, filepath.Join(t.TempDir(), "dest"), workspace.MaxExtractBytes); err == nil {
		t.Fatal("UntarGz accepted garbage")
	}
}
