package xfer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// ---------------------------------------------------------------------------
// path rules
// ---------------------------------------------------------------------------

// TestValidatePath pins the shape check every hop applies before a transfer
// path travels any further: the CLI before it sends, controld before it
// forwards, sessiond before it touches a file. A path that gets past this and
// still escapes would have to escape Resolve too, which is the point of having
// both.
func TestValidatePath(t *testing.T) {
	ok := []string{
		"repo",
		"repo/sub/dir",
		"./repo",
		WorkspaceRoot,
		WorkspaceRoot + "/repo/sub",
	}
	for _, p := range ok {
		if err := ValidatePath(p); err != nil {
			t.Errorf("ValidatePath(%q) = %v, want nil", p, err)
		}
	}

	bad := []string{
		"",
		"..",
		"../etc",
		"repo/../../etc",
		"/etc/passwd",
		"/workspaceother",
		WorkspaceRoot + "/../etc",
		"repo\x00/sub",
	}
	for _, p := range bad {
		if err := ValidatePath(p); err == nil {
			t.Errorf("ValidatePath(%q) = nil, want a refusal", p)
		}
	}
}

// TestResolveStaysUnderRoot is the file-system half of the same rule: whatever
// a caller sends, the path a transfer actually opens is inside the session's
// workspace or the transfer does not happen.
func TestResolveStaysUnderRoot(t *testing.T) {
	root := t.TempDir()

	got, err := Resolve(root, "repo/sub")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if want := filepath.Join(root, "repo/sub"); got != want {
		t.Fatalf("Resolve = %q, want %q", got, want)
	}

	// An absolute path INSIDE the root is honored as given.
	abs := filepath.Join(root, "repo")
	if got, err := Resolve(root, abs); err != nil || got != abs {
		t.Fatalf("Resolve(%q) = %q, %v; want the path back unchanged", abs, got, err)
	}

	for _, p := range []string{"", "..", "../outside", "repo/../../outside", "/etc/passwd"} {
		if got, err := Resolve(root, p); err == nil {
			t.Errorf("Resolve(%q) = %q, want a refusal", p, got)
		}
	}
}

// TestResolveRefusesASymlinkOutOfTheRoot: the lexical rules above say nothing
// about what a name POINTS at, and a workspace is a directory the session's own
// agent writes to. `/workspace/vendor -> /etc` would make "vendor/passwd" a
// perfectly well-formed transfer path that lands outside the workspace.
func TestResolveRefusesASymlinkOutOfTheRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if got, err := Resolve(root, "escape"); err == nil {
		t.Fatalf("Resolve of a symlink out of the root = %q, want a refusal", got)
	}
	// And through it, at a path that does not exist yet — the push case, where
	// the destination is about to be created.
	if got, err := Resolve(root, "escape/newdir"); err == nil {
		t.Fatalf("Resolve through a symlink out of the root = %q, want a refusal", got)
	}
	// A symlink that stays inside is fine: this is a containment rule, not a
	// ban on symlinks.
	if err := os.MkdirAll(filepath.Join(root, "real"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Symlink("real", filepath.Join(root, "inside")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := Resolve(root, "inside/sub"); err != nil {
		t.Fatalf("Resolve of a symlink that stays inside the root: %v", err)
	}
	// A destination that simply does not exist yet is the ordinary push case.
	if _, err := Resolve(root, "brand/new/dir"); err != nil {
		t.Fatalf("Resolve of a path that does not exist yet: %v", err)
	}
}

// ---------------------------------------------------------------------------
// archive round trip
// ---------------------------------------------------------------------------

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
	n, err := TarGz(f, src, MaxBytes)
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
	if err := UntarGz(archive, dest, MaxExtractBytes); err != nil {
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
	_, err := TarGz(&buf, src, 64<<10)
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
	if _, err := TarGz(&buf, src, MaxBytes); err == nil {
		t.Fatal("TarGz of a tree containing a fifo returned nil")
	}
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
			err := UntarGz(archive, dest, MaxExtractBytes)
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
	err := UntarGz(archive, dest, 1<<10)
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
	if err := UntarGz(path, filepath.Join(t.TempDir(), "dest"), MaxExtractBytes); err == nil {
		t.Fatal("UntarGz accepted garbage")
	}
}

// ---------------------------------------------------------------------------
// wire shapes
// ---------------------------------------------------------------------------

// TestChunkWireShape pins the JSON three programs exchange. Data is []byte so
// it rides as base64 with no hand-rolled encoding at any hop — the field a
// renamed tag would silently empty.
func TestChunkWireShape(t *testing.T) {
	b, err := json.Marshal(PushChunk{Xfer: "abc", Path: "repo", Seq: 3, Data: []byte("hi"), Done: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"xfer":"abc","path":"repo","seq":3,"data":"aGk=","done":true}`
	if string(b) != want {
		t.Fatalf("PushChunk = %s, want %s", b, want)
	}

	var back PushChunk
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(back.Data) != "hi" || back.Seq != 3 || !back.Done || back.Path != "repo" || back.Xfer != "abc" {
		t.Fatalf("round trip = %+v", back)
	}

	if b, err := json.Marshal(PushAck{Seq: 7, Synced: true}); err != nil || string(b) != `{"seq":7,"synced":true}` {
		t.Fatalf("PushAck = %s, %v", b, err)
	}
	if b, err := json.Marshal(PullRequest{Xfer: "abc", Path: "repo", Seq: 0}); err != nil ||
		string(b) != `{"xfer":"abc","path":"repo","seq":0}` {
		t.Fatalf("PullRequest = %s, %v", b, err)
	}
	if b, err := json.Marshal(PullChunk{Seq: 0, Data: []byte("hi")}); err != nil ||
		string(b) != `{"seq":0,"data":"aGk="}` {
		t.Fatalf("PullChunk = %s, %v", b, err)
	}
	if b, err := json.Marshal(DiffAnswer{}); err != nil || string(b) != `{"repos":[]}` {
		t.Fatalf("empty DiffAnswer = %s, %v; a session with no repos answers an empty array, never null", b, err)
	}
	if b, err := json.Marshal(DiffAnswer{Repos: []RepoDiff{{
		Repo: "o/n", BaseBranch: "main", SessionBranch: "rainier/x", Stat: " f | 1 +\n"}}}); err != nil ||
		string(b) != `{"repos":[{"repo":"o/n","base_branch":"main","session_branch":"rainier/x","stat":" f | 1 +\n"}]}` {
		t.Fatalf("DiffAnswer = %s, %v", b, err)
	}
}
