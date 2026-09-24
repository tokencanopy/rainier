package wstream

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

// fixture builds the tree every test below streams: a file, a directory with a
// file in it, a symbolic link, and a Rainier-owned path that must never travel.
//
// It is a real directory rather than an fstest.MapFS because the thing under
// test is what a guest's own workspace produces — modes, modification times and
// symbolic links as the file system reports them.
func fixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel, content string, mode os.FileMode) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "alpha", 0o644)
	write("dir/b.txt", "bravo", 0o600)
	write(".rainier/session.json", "{}", 0o600)
	if err := os.Symlink("dir/b.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	return root
}

// entry is one decoded stream entry, index side and tar side together, which is
// how every assertion below is written: the two sections are one statement
// about one tree or the stream is wrong.
type entry struct {
	kind    byte
	name    string
	hdr     *tar.Header
	content string
}

// decode reads a whole stream back: the magic, the index, then the tar. It is
// deliberately a SECOND implementation of the reader rather than a call into
// fs.go, so that the format these tests pin is the one on the wire and not
// whatever the host's parser happens to accept.
func decode(t *testing.T, b []byte) []entry {
	t.Helper()
	if !bytes.HasPrefix(b, []byte(Magic)) {
		t.Fatalf("the stream does not begin with the magic line")
	}
	r := bytes.NewReader(b[len(Magic):])
	var out []entry
	for {
		n, err := binary.ReadUvarint(r)
		if err != nil {
			t.Fatalf("reading an index length: %v", err)
		}
		if n == 0 {
			break
		}
		rec := make([]byte, n)
		if _, err := io.ReadFull(r, rec); err != nil {
			t.Fatalf("reading an index record: %v", err)
		}
		out = append(out, entry{kind: rec[0], name: string(rec[1:])})
	}
	tr := tar.NewReader(r)
	for i := 0; ; i++ {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			if i != len(out) {
				t.Fatalf("the tar carries %d entries, the index names %d", i, len(out))
			}
			return out
		}
		if err != nil {
			t.Fatalf("reading tar entry %d: %v", i, err)
		}
		if i >= len(out) {
			t.Fatalf("the tar carries more entries than the index names")
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("reading tar body %d: %v", i, err)
		}
		out[i].hdr = hdr
		out[i].content = string(body)
	}
}

func stream(t *testing.T, fsys fs.FS, lim Limits) ([]byte, Report, error) {
	t.Helper()
	var buf bytes.Buffer
	rep, err := Write(context.Background(), fsys, &buf, lim)
	return buf.Bytes(), rep, err
}

// TestWriteCarriesTheTreeInWalkOrder is the whole format in one test: the index
// names every entry in fs.WalkDir order, the tar carries the same entries in
// the same order, and the two agree about what each one is.
//
// The ORDER is the load-bearing part. The checkpoint writer walks in fs.WalkDir
// order and the host's cursor only moves forward, so a stream in any other
// order is a suspend that fails rather than a checkpoint of a tree nobody had.
func TestWriteCarriesTheTreeInWalkOrder(t *testing.T) {
	root := fixture(t)
	raw, rep, err := stream(t, os.DirFS(root), Limits{Exclude: []string{".rainier"}})
	if err != nil {
		t.Fatal(err)
	}
	got := decode(t, raw)

	want := []struct {
		kind byte
		name string
	}{
		{KindFile, "a.txt"},
		{KindDir, "dir"},
		{KindFile, "dir/b.txt"},
		{KindSymlink, "link"},
	}
	if len(got) != len(want) {
		t.Fatalf("the stream carries %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].kind != w.kind || got[i].name != w.name {
			t.Errorf("entry %d is %c %q, want %c %q", i, got[i].kind, got[i].name, w.kind, w.name)
		}
		if got[i].hdr.Name != w.name {
			t.Errorf("tar entry %d names %q, want %q", i, got[i].hdr.Name, w.name)
		}
	}
	if rep.Entries != 4 {
		t.Errorf("the report counts %d entries, want 4", rep.Entries)
	}
	if rep.Bytes != int64(len(raw)) {
		t.Errorf("the report counts %d bytes, the stream is %d", rep.Bytes, len(raw))
	}
}

// TestWriteCarriesModesContentAndLinkTargets: the attributes a restored
// workspace is the same workspace because of.
func TestWriteCarriesModesContentAndLinkTargets(t *testing.T) {
	root := fixture(t)
	info, err := os.Stat(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := stream(t, os.DirFS(root), Limits{Exclude: []string{".rainier"}})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]entry{}
	for _, e := range decode(t, raw) {
		byName[e.name] = e
	}

	if got := byName["a.txt"]; got.content != "alpha" || got.hdr.Mode != 0o644 {
		t.Errorf("a.txt is mode %o with content %q, want 0644 and \"alpha\"", got.hdr.Mode, got.content)
	}
	if got := byName["dir/b.txt"]; got.content != "bravo" || got.hdr.Mode != 0o600 {
		t.Errorf("dir/b.txt is mode %o with content %q, want 0600 and \"bravo\"", got.hdr.Mode, got.content)
	}
	// The link travels as a LINK with its target recorded, never followed and
	// never resolved into a second copy of the file.
	if got := byName["link"]; got.hdr.Linkname != "dir/b.txt" || got.content != "" {
		t.Errorf("link is %q with %d bytes of body, want a target of dir/b.txt and no body",
			got.hdr.Linkname, len(got.content))
	}
	// The modification time survives to the second, which is what tar holds and
	// what the checkpoint's tree digest commits to.
	if got, want := byName["a.txt"].hdr.ModTime, info.ModTime().Truncate(time.Second); !got.Equal(want) {
		t.Errorf("a.txt's modification time is %v, want %v", got, want)
	}
}

// TestWriteExcludesThePathsTheHostWouldRefuse. The exclusion is a promise about
// the bytes that LEAVE the guest, so the excluded directory must not be in the
// index, the tar, or the byte count.
func TestWriteExcludesThePathsTheHostWouldRefuse(t *testing.T) {
	root := fixture(t)
	raw, _, err := stream(t, os.DirFS(root), Limits{Exclude: []string{".rainier"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range decode(t, raw) {
		if strings.HasPrefix(e.name, ".rainier") {
			t.Fatalf("the stream carries %q, which is excluded", e.name)
		}
	}
	if bytes.Contains(raw, []byte("session.json")) {
		t.Error("an excluded file's name appears in the stream's bytes")
	}
}

// TestWriteSkipsAndCountsAnEntryTheFormatCannotCarry: a socket left behind by a
// dev server must not be able to defeat the durability barrier, and must not be
// silent either.
func TestWriteSkipsAndCountsAnEntryTheFormatCannotCarry(t *testing.T) {
	fsys := fstest.MapFS{
		"a.txt":  &fstest.MapFile{Data: []byte("alpha"), Mode: 0o644},
		"a.sock": &fstest.MapFile{Mode: fs.ModeSocket | 0o600},
	}
	raw, rep, err := stream(t, fsys, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Entries != 1 || rep.Skipped != 1 {
		t.Fatalf("the report counts %d entries and %d skipped, want 1 and 1", rep.Entries, rep.Skipped)
	}
	for _, e := range decode(t, raw) {
		if e.name == "a.sock" {
			t.Fatal("the stream carries a socket")
		}
	}
}

// TestWriteRefusesASymlinkThatLeavesTheTree is the containment rule at the
// guest's end: a workspace containing "creds -> /etc/shadow" must not stream
// the host's — or the guest's — /etc/shadow anywhere.
func TestWriteRefusesASymlinkThatLeavesTheTree(t *testing.T) {
	for _, target := range []string{"/etc/shadow", "../../etc/shadow"} {
		root := t.TempDir()
		if err := os.Symlink(target, filepath.Join(root, "creds")); err != nil {
			t.Fatal(err)
		}
		_, _, err := stream(t, os.DirFS(root), Limits{})
		if !errors.Is(err, ErrEntry) {
			t.Fatalf("a symlink to %q streamed with error %v, want ErrEntry", target, err)
		}
		if err != nil && strings.Contains(err.Error(), "shadow") {
			t.Errorf("the refusal quotes the symlink target: %v", err)
		}
	}
}

// TestWriteRefusesEachLimit walks the table of limits, because a limit nothing
// tests is a limit that is one typo from being absent.
func TestWriteRefusesEachLimit(t *testing.T) {
	root := fixture(t)
	cases := []struct {
		name string
		lim  Limits
	}{
		{"entries", Limits{MaxEntries: 2}},
		{"index bytes", Limits{MaxIndexBytes: 8}},
		{"one entry's bytes", Limits{MaxEntryBytes: 2}},
		{"total bytes", Limits{MaxTotalBytes: 64}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.lim.Exclude = []string{".rainier"}
			_, _, err := stream(t, os.DirFS(root), tc.lim)
			if !errors.Is(err, ErrLimit) {
				t.Fatalf("the stream ended with %v, want ErrLimit", err)
			}
		})
	}
	t.Run("a negative limit is configuration, not permission", func(t *testing.T) {
		_, _, err := stream(t, os.DirFS(root), Limits{MaxEntries: -1})
		if !errors.Is(err, ErrLimit) {
			t.Fatalf("a negative limit gave %v, want ErrLimit", err)
		}
	})
	t.Run("an exclusion that is not a clean relative path", func(t *testing.T) {
		_, _, err := stream(t, os.DirFS(root), Limits{Exclude: []string{"/etc"}})
		if !errors.Is(err, ErrLimit) {
			t.Fatalf("an absolute exclusion gave %v, want ErrLimit", err)
		}
	})
}

// TestWriteReportsTheCountsItReachedOnAFailure. The counts travel in the end
// marker, and a failure is exactly when somebody needs them: a stream that
// stopped at entry 900 is a different problem from one that never started.
func TestWriteRefusalCarriesTheCountsItReached(t *testing.T) {
	root := fixture(t)
	_, rep, err := stream(t, os.DirFS(root), Limits{MaxEntries: 2, Exclude: []string{".rainier"}})
	if err == nil {
		t.Fatal("the stream did not refuse")
	}
	if rep.Entries == 0 || rep.Bytes == 0 {
		t.Fatalf("the refusal reports %d entries and %d bytes, want the counts it reached", rep.Entries, rep.Bytes)
	}
}

// TestWriteRefusesAFileThatChangedUnderIt is the quiesce check. A file that
// shrank would desynchronize the tar; one that grew would be silently truncated
// into a checkpoint that VERIFIED, which is the one failure the barrier must
// not have. Both are a failed suspend.
func TestWriteRefusesAFileThatChangedUnderIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim int64
		data  string
	}{
		{"shrank", 10, "short"},
		{"grew", 2, "longer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsys := &lyingFS{
				MapFS: fstest.MapFS{"a.txt": &fstest.MapFile{Data: []byte(tc.data), Mode: 0o644}},
				name:  "a.txt",
				size:  tc.claim,
			}
			_, _, err := stream(t, fsys, Limits{})
			if !errors.Is(err, ErrSource) {
				t.Fatalf("a file that %s gave %v, want ErrSource", tc.name, err)
			}
			if !strings.Contains(err.Error(), "quiesced") {
				t.Errorf("the refusal does not name the cause: %v", err)
			}
		})
	}
}

// TestWriteRefusesATreeThatChangedBetweenThePasses. The index and the bodies
// come from two walks; a tree that changed between them would produce a stream
// whose two halves disagree, which the host can only refuse halfway through.
// Caught here, where there is something to say about it.
func TestWriteRefusesATreeThatChangedBetweenThePasses(t *testing.T) {
	fsys := &mutatingFS{MapFS: fstest.MapFS{
		"a.txt": &fstest.MapFile{Data: []byte("alpha"), Mode: 0o644},
	}}
	_, _, err := stream(t, fsys, Limits{})
	if !errors.Is(err, ErrSource) {
		t.Fatalf("a tree that changed between the passes gave %v, want ErrSource", err)
	}
	if !strings.Contains(err.Error(), "quiesced") {
		t.Errorf("the refusal does not name the cause: %v", err)
	}
}

// TestWriteStopsOnACancelledContext: a cold suspend whose budget expired must
// not run a 10 GiB copy to completion first.
func TestWriteStopsOnACancelledContext(t *testing.T) {
	root := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Write(ctx, os.DirFS(root), new(bytes.Buffer), Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled stream ended with %v, want context.Canceled", err)
	}
}

// TestWriteErrorsNameNoPath. These sentences travel to a session's error
// column, where the tenancy specification's §15.1 prohibited-logging list
// applies: an ordinal is findable with a local walk and is not a path.
func TestWriteErrorsNameNoPath(t *testing.T) {
	root := fixture(t)
	_, _, err := stream(t, os.DirFS(root), Limits{MaxEntryBytes: 1, Exclude: []string{".rainier"}})
	if err == nil {
		t.Fatal("the stream did not refuse")
	}
	for _, leak := range []string{root, "a.txt", "b.txt", "link"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("the refusal quotes %q: %v", leak, err)
		}
	}
}

// lyingFS reports a size its file's bytes do not agree with, which is what a
// file being written to under the walk looks like from here.
type lyingFS struct {
	fstest.MapFS
	name string
	size int64
}

func (l *lyingFS) Open(name string) (fs.File, error) {
	f, err := l.MapFS.Open(name)
	if err != nil || name != l.name {
		return f, err
	}
	return &lyingFile{File: f, size: l.size}, nil
}

func (l *lyingFS) Stat(name string) (fs.FileInfo, error) {
	info, err := l.MapFS.Stat(name)
	if err != nil || name != l.name {
		return info, err
	}
	return &lyingInfo{FileInfo: info, size: l.size}, nil
}

func (l *lyingFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := l.MapFS.ReadDir(name)
	if err != nil {
		return nil, err
	}
	for i, e := range entries {
		if e.Name() == l.name {
			entries[i] = &lyingDirEntry{DirEntry: e, size: l.size}
		}
	}
	return entries, nil
}

type lyingFile struct {
	fs.File
	size int64
}

func (l *lyingFile) Stat() (fs.FileInfo, error) {
	info, err := l.File.Stat()
	if err != nil {
		return nil, err
	}
	return &lyingInfo{FileInfo: info, size: l.size}, nil
}

type lyingInfo struct {
	fs.FileInfo
	size int64
}

func (l *lyingInfo) Size() int64 { return l.size }

type lyingDirEntry struct {
	fs.DirEntry
	size int64
}

func (l *lyingDirEntry) Info() (fs.FileInfo, error) {
	info, err := l.DirEntry.Info()
	if err != nil {
		return nil, err
	}
	return &lyingInfo{FileInfo: info, size: l.size}, nil
}

// mutatingFS grows a file between the two walks, which is a process still
// writing in a workspace that was supposed to be quiesced.
type mutatingFS struct {
	fstest.MapFS
	walks int
}

func (m *mutatingFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "." {
		m.walks++
		if m.walks == 2 {
			m.MapFS["b.txt"] = &fstest.MapFile{Data: []byte("bravo"), Mode: 0o644}
		}
	}
	return m.MapFS.ReadDir(name)
}
