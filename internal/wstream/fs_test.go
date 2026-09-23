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

// The host's end of the stream. Every test here reads a stream the writer in
// this package produced, or one built by hand to be something the writer never
// produces — which is the half that matters, because the guest is the untrusted
// end of this hop.

func mustStream(t *testing.T, fsys fs.FS, lim Limits) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := Write(context.Background(), fsys, &buf, lim); err != nil {
		t.Fatalf("writing the fixture stream: %v", err)
	}
	return buf.Bytes()
}

// walkLikeTheWriter walks f the way checkpoint.Writer's own walk does: it asks
// every entry for its info, reads every regular file, and reads every symbolic
// link's target.
//
// The distinction matters more than it looks. fs.WalkDir alone touches nothing
// but the index — ReadDir is answered from the tree and DirEntry.Info is lazy —
// so a test that only walked would consume no tar entry at all and would prove
// nothing about the cursor or about any rule applied when it moves.
func walkLikeTheWriter(f *FS) error {
	return fs.WalkDir(f, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if _, err := d.Info(); err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return nil
		case d.Type()&fs.ModeSymlink != 0:
			_, err := fs.ReadLink(f, name)
			return err
		default:
			_, err := fs.ReadFile(f, name)
			return err
		}
	})
}

func newFS(t *testing.T, raw []byte, lim Limits) *FS {
	t.Helper()
	f, err := NewFS(bytes.NewReader(raw), lim)
	if err != nil {
		t.Fatalf("NewFS: %v", err)
	}
	return f
}

// TestFSPresentsTheTreeAWalkExpects is the round trip: what fs.WalkDir sees
// over a stream is what it would have seen over the guest's own directory.
func TestFSPresentsTheTreeAWalkExpects(t *testing.T) {
	root := fixture(t)
	f := newFS(t, mustStream(t, os.DirFS(root), Limits{Exclude: []string{".rainier"}}), Limits{Exclude: []string{".rainier"}})

	var visited []string
	err := fs.WalkDir(f, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			visited = append(visited, "d "+name+" "+info.Mode().String())
		case d.Type()&fs.ModeSymlink != 0:
			target, err := fs.ReadLink(f, name)
			if err != nil {
				return err
			}
			visited = append(visited, "l "+name+" -> "+target)
		default:
			body, err := fs.ReadFile(f, name)
			if err != nil {
				return err
			}
			visited = append(visited, "f "+name+" "+string(body)+" "+info.Mode().Perm().String())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the stream: %v", err)
	}
	want := []string{
		"f a.txt alpha -rw-r--r--",
		"d dir drwxr-xr-x",
		"f dir/b.txt bravo -rw-------",
		"l link -> dir/b.txt",
	}
	if len(visited) != len(want) {
		t.Fatalf("the walk saw %v, want %v", visited, want)
	}
	for i := range want {
		if visited[i] != want[i] {
			t.Fatalf("the walk saw %v, want %v", visited, want)
		}
	}
	if f.Indexed() != 4 || f.Entries() != 4 {
		t.Errorf("the stream indexed %d entries and presents %d, want 4 and 4", f.Indexed(), f.Entries())
	}
}

// TestFSCarriesModificationTimes: the checkpoint's tree digest commits to them,
// so a restored workspace is only the same workspace if they survive this hop.
func TestFSCarriesModificationTimes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 9, 23, 10, 30, 0, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(root, "a.txt"), when, when); err != nil {
		t.Fatal(err)
	}
	f := newFS(t, mustStream(t, os.DirFS(root), Limits{}), Limits{})
	info, err := fs.Stat(f, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(when) {
		t.Errorf("a.txt's modification time came back as %v, want %v", info.ModTime(), when)
	}
}

// TestFSExcludesWhatTheHostOwns. The host applies the exclusions too, because a
// check the previous hop performed is a check you are trusting: an entry the
// guest sent anyway is dropped from the tree AND skipped in the stream, which
// is what keeps the cursor in step.
func TestFSExcludesWhatTheHostOwns(t *testing.T) {
	// A stream written with NO exclusions, read by a host that has them.
	root := fixture(t)
	f := newFS(t, mustStream(t, os.DirFS(root), Limits{}), Limits{Exclude: []string{".rainier"}})

	if f.Indexed() != 6 || f.Entries() != 4 {
		t.Fatalf("the stream indexed %d entries and presents %d, want 6 and 4", f.Indexed(), f.Entries())
	}
	if _, err := fs.Stat(f, ".rainier"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("an excluded directory is visible: %v", err)
	}
	// And the tree after it is still readable, which is the part a naive
	// implementation gets wrong: skipping an entry means skipping its BODY too.
	if body, err := fs.ReadFile(f, "dir/b.txt"); err != nil || string(body) != "bravo" {
		t.Errorf("reading past an excluded entry gave %q, %v", body, err)
	}
}

// TestFSRefusesAStreamThatIsNotOne.
func TestFSRefusesAStreamThatIsNotOne(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want error
	}{
		{"empty", nil, ErrTruncated},
		{"another protocol", []byte("{\"kind\":\"suspend_ready\"}\n"), ErrFormat},
		{"a header and nothing else", []byte(Magic), ErrTruncated},
		{"an index that never ends", append([]byte(Magic), 0x05, 'f', 'a', '.', 't'), ErrTruncated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewFS(bytes.NewReader(tc.raw), Limits{}); !errors.Is(err, tc.want) {
				t.Fatalf("NewFS gave %v, want %v", err, tc.want)
			}
		})
	}
}

// index builds an index section by hand, which is how every "a guest that does
// not behave" test below states its case.
func index(entries ...string) []byte {
	out := []byte(Magic)
	var hdr [binary.MaxVarintLen64]byte
	for _, e := range entries {
		n := binary.PutUvarint(hdr[:], uint64(len(e)))
		out = append(out, hdr[:n]...)
		out = append(out, e...)
	}
	return append(out, 0)
}

// tarball appends a tar section carrying exactly these headers.
func tarball(t *testing.T, prefix []byte, headers ...*tar.Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(prefix)
	tw := tar.NewWriter(&buf)
	for _, h := range headers {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err := tw.Write(bytes.Repeat([]byte("x"), int(h.Size))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestFSRefusesAnIndexThatIsNotATree is the path-traversal table. Every one of
// these is a name a guest can put on the wire and none of them may become a
// tree entry, because the checkpoint they would land in is restored into a
// directory on a host.
func TestFSRefusesAnIndexThatIsNotATree(t *testing.T) {
	cases := []struct {
		name  string
		entry string
		want  error
	}{
		{"an absolute path", "f/etc/shadow", ErrEntry},
		{"a parent escape", "f../../etc/shadow", ErrEntry},
		{"a dot-dot element", "fa/../../b", ErrEntry},
		{"an unclean path", "fa//b", ErrEntry},
		{"the root itself", "f.", ErrEntry},
		{"an empty name", "f", ErrEntry},
		{"a NUL", "fa\x00b", ErrEntry},
		{"a kind this format does not carry", "ba.txt", ErrFormat},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewFS(bytes.NewReader(index(tc.entry)), Limits{}); !errors.Is(err, tc.want) {
				t.Fatalf("NewFS gave %v, want %v", err, tc.want)
			}
		})
	}
}

// TestFSRefusesAnIndexOutOfWalkOrder. The cursor is one-way, so an index in any
// order but the walk's is a stream whose entries would be asked for backwards.
// Refused at the head, where there is something to say, rather than half way
// through a checkpoint.
func TestFSRefusesAnIndexOutOfWalkOrder(t *testing.T) {
	cases := []struct {
		name    string
		entries []string
	}{
		{"a child before its directory", []string{"fdir/b.txt", "ddir"}},
		{"a directory re-entered later", []string{"ddir", "fdir/a", "fz", "fdir/b"}},
		{"children out of lexical order", []string{"fb.txt", "fa.txt"}},
		{"the same name twice", []string{"fa.txt", "fa.txt"}},
		{"a child of a file", []string{"fa.txt", "fa.txt/b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewFS(bytes.NewReader(index(tc.entries...)), Limits{}); !errors.Is(err, ErrFormat) {
				t.Fatalf("NewFS gave %v, want ErrFormat", err)
			}
		})
	}
}

// TestFSRefusesEachLimit, on the host's side of the hop this time.
func TestFSRefusesEachLimit(t *testing.T) {
	root := fixture(t)
	raw := mustStream(t, os.DirFS(root), Limits{Exclude: []string{".rainier"}})

	t.Run("entries", func(t *testing.T) {
		if _, err := NewFS(bytes.NewReader(raw), Limits{MaxEntries: 2}); !errors.Is(err, ErrLimit) {
			t.Fatalf("NewFS gave %v, want ErrLimit", err)
		}
	})
	t.Run("index bytes", func(t *testing.T) {
		if _, err := NewFS(bytes.NewReader(raw), Limits{MaxIndexBytes: 4}); !errors.Is(err, ErrLimit) {
			t.Fatalf("NewFS gave %v, want ErrLimit", err)
		}
	})
	t.Run("a record longer than any name", func(t *testing.T) {
		// A length prefix claiming more than a name could ever be, which is
		// what an allocation attack looks like from here.
		raw := append([]byte(Magic), 0xff, 0xff, 0xff, 0xff, 0x0f)
		if _, err := NewFS(bytes.NewReader(raw), Limits{}); !errors.Is(err, ErrFormat) {
			t.Fatalf("NewFS gave %v, want ErrFormat", err)
		}
	})
	t.Run("one entry's bytes", func(t *testing.T) {
		f := newFS(t, raw, Limits{MaxEntryBytes: 2, Exclude: []string{".rainier"}})
		err := walkLikeTheWriter(f)
		if !errors.Is(err, ErrLimit) {
			t.Fatalf("the walk gave %v, want ErrLimit", err)
		}
	})
	t.Run("total bytes", func(t *testing.T) {
		if _, err := NewFS(bytes.NewReader(raw), Limits{MaxTotalBytes: 8}); err == nil {
			t.Fatal("a stream past the total limit was accepted")
		}
	})
}

// TestFSRefusesAnEntryTheIndexDidNotName is the cross-check between the two
// sections: a guest that named one tree and sent another must not be able to
// checkpoint the one the host never validated.
func TestFSRefusesTarThatDisagreesWithTheIndex(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want error
	}{
		{
			"a different name",
			tarball(t, index("fa.txt"), &tar.Header{Typeflag: tar.TypeReg, Name: "b.txt", Mode: 0o644}),
			ErrFormat,
		},
		{
			"a different kind",
			tarball(t, index("fa.txt"), &tar.Header{Typeflag: tar.TypeDir, Name: "a.txt", Mode: 0o755}),
			ErrFormat,
		},
		{
			"a hard link",
			tarball(t, index("fa.txt"), &tar.Header{Typeflag: tar.TypeLink, Name: "a.txt", Linkname: "b", Mode: 0o644}),
			ErrEntry,
		},
		{
			"a device node",
			tarball(t, index("fa.txt"), &tar.Header{Typeflag: tar.TypeChar, Name: "a.txt", Mode: 0o644}),
			ErrEntry,
		},
		{
			"a setuid bit",
			tarball(t, index("fa.txt"), &tar.Header{Typeflag: tar.TypeReg, Name: "a.txt", Mode: 0o4755}),
			ErrEntry,
		},
		{
			"a symlink out of the tree",
			tarball(t, index("lcreds"), &tar.Header{Typeflag: tar.TypeSymlink, Name: "creds", Linkname: "/etc/shadow"}),
			ErrEntry,
		},
		{
			"a symlink climbing out of the tree",
			tarball(t, index("ldir", "ldir/creds"), &tar.Header{Typeflag: tar.TypeSymlink, Name: "dir", Linkname: "x"}),
			ErrFormat, // the index says two symlinks, one of which has children
		},
		{
			"nothing at all",
			index("fa.txt"),
			ErrTruncated,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := NewFS(bytes.NewReader(tc.raw), Limits{})
			if err != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("NewFS gave %v, want %v", err, tc.want)
				}
				return
			}
			err = walkLikeTheWriter(f)
			if !errors.Is(err, tc.want) {
				t.Fatalf("the walk gave %v, want %v", err, tc.want)
			}
		})
	}
}

// TestFSRefusesASecondReadOfAnEntryTheStreamHasPassed. The cursor is the whole
// design; a caller that went back would otherwise be served whatever the stream
// is pointing at now, which is another file's bytes.
func TestFSRefusesAReadAfterTheCursorMovedOn(t *testing.T) {
	fsys := fstest.MapFS{
		"a.txt": &fstest.MapFile{Data: []byte("alpha"), Mode: 0o644},
		"b.txt": &fstest.MapFile{Data: []byte("bravo"), Mode: 0o644},
	}
	f := newFS(t, mustStream(t, fsys, Limits{}), Limits{})

	first, err := f.Open("a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.ReadFile(f, "b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(first); !errors.Is(err, ErrFormat) {
		t.Fatalf("reading a passed entry gave %v, want ErrFormat", err)
	}
	// And the stream stays refused: a cursor that has lost its place cannot
	// produce a better answer to the next question either.
	if _, err := fs.Stat(f, "b.txt"); !errors.Is(err, ErrFormat) {
		t.Fatalf("a later call gave %v, want the same refusal", err)
	}
}

// TestFSRefusesAQuestionAboutAnEntryTheStreamHasPassed is the cursor's other
// half, and the quietest way this design could go wrong: a caller that asked
// about an earlier entry must not be answered from the CURRENT one. A ReadLink
// served that way would checkpoint a symbolic link with somebody else's
// destination, in a checkpoint that then verified.
func TestFSRefusesAQuestionAboutAnEntryTheStreamHasPassed(t *testing.T) {
	fsys := fstest.MapFS{
		"a-link": &fstest.MapFile{Data: []byte("first-target"), Mode: fs.ModeSymlink | 0o777},
		"b-link": &fstest.MapFile{Data: []byte("second-target"), Mode: fs.ModeSymlink | 0o777},
		"c.txt":  &fstest.MapFile{Data: []byte("charlie"), Mode: 0o644},
	}
	raw := mustStream(t, fsys, Limits{})

	t.Run("readlink", func(t *testing.T) {
		f := newFS(t, raw, Limits{})
		if target, err := fs.ReadLink(f, "b-link"); err != nil || target != "second-target" {
			t.Fatalf("reading the second link gave %q, %v", target, err)
		}
		target, err := fs.ReadLink(f, "a-link")
		if err == nil {
			t.Fatalf("reading a passed link returned %q", target)
		}
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("reading a passed link gave %v, want ErrFormat", err)
		}
	})
	t.Run("stat", func(t *testing.T) {
		f := newFS(t, raw, Limits{})
		if _, err := fs.Stat(f, "c.txt"); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.Stat(f, "a-link"); !errors.Is(err, ErrFormat) {
			t.Fatalf("stat of a passed entry gave %v, want ErrFormat", err)
		}
	})
	t.Run("open", func(t *testing.T) {
		f := newFS(t, raw, Limits{})
		if _, err := fs.ReadFile(f, "c.txt"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Open("a-link"); !errors.Is(err, ErrFormat) {
			t.Fatalf("opening a passed entry gave %v, want ErrFormat", err)
		}
	})
}

// TestFSDrainConsumesWhatTheWalkDidNotWant. A walk stops at the last entry it
// was interested in, and the guest is blocked writing whatever follows — the
// tar's trailing blocks at least, and everything the HOST excluded that sorts
// after that. A guest blocked writing cannot send the marker that says its tree
// was complete, so those bytes are not optional to consume.
func TestFSDrainConsumesWhatTheWalkDidNotWant(t *testing.T) {
	fsys := fstest.MapFS{
		"a.txt":          &fstest.MapFile{Data: []byte("alpha"), Mode: 0o644},
		"node_modules/x": &fstest.MapFile{Data: bytes.Repeat([]byte("x"), 128<<10), Mode: 0o644},
	}
	// The guest sends everything; this host excludes a directory the guest
	// never heard of, and it is the LAST thing in the stream.
	raw := mustStream(t, fsys, Limits{})
	f := newFS(t, raw, Limits{Exclude: []string{"node_modules"}})
	if err := walkLikeTheWriter(f); err != nil {
		t.Fatalf("walking the stream: %v", err)
	}
	if err := f.Drain(); err != nil {
		t.Fatalf("draining the rest of the stream: %v", err)
	}
	// And the drain is bounded by the stream's own byte limit rather than by a
	// second, smaller number: the same stream against a total limit it exceeds
	// is refused rather than silently half-read.
	tight := newFS(t, raw, Limits{Exclude: []string{"node_modules"}, MaxTotalBytes: 64 << 10})
	err := walkLikeTheWriter(tight)
	if err == nil {
		err = tight.Drain()
	}
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("draining past the total limit gave %v, want ErrLimit", err)
	}
}

// TestFSRefusesAnEntryThatEndsEarly: a stream cut off inside a file's body.
func TestFSRefusesABodyThatEndsEarly(t *testing.T) {
	fsys := fstest.MapFS{"a.txt": &fstest.MapFile{Data: bytes.Repeat([]byte("x"), 4096), Mode: 0o644}}
	raw := mustStream(t, fsys, Limits{})
	f := newFS(t, raw[:len(raw)-2048], Limits{})
	if _, err := fs.ReadFile(f, "a.txt"); !errors.Is(err, ErrTruncated) {
		t.Fatalf("reading a truncated body gave %v, want ErrTruncated", err)
	}
}

// TestFSHoldsNoContent is the bounded-memory claim, as a claim about the type
// rather than about a measurement: the only per-entry state is a name and a
// kind, so a stream of any SIZE costs the same as a stream of any other, for
// the same number of entries.
func TestFSHoldsNoContent(t *testing.T) {
	big := 8 << 20
	fsys := fstest.MapFS{"big.bin": &fstest.MapFile{Data: bytes.Repeat([]byte("x"), big), Mode: 0o644}}
	raw := mustStream(t, fsys, Limits{})

	before := heapInUse()
	f := newFS(t, raw, Limits{})
	after := heapInUse()
	// The index of one entry, and nothing that grew with the 8 MiB behind it.
	if grew := int64(after) - int64(before); grew > int64(big)/4 {
		t.Fatalf("building the tree grew the heap by %d bytes for an %d byte stream", grew, big)
	}
	entry, err := f.Open("big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, entry); err != nil || n != int64(big) {
		t.Fatalf("reading the entry gave %d bytes, %v", n, err)
	}
}

// TestFSErrorsNameNoPath: these travel to a session's error column.
func TestFSErrorsNameNoPath(t *testing.T) {
	raw := tarball(t, index("fsecret-project/api-key.txt"),
		&tar.Header{Typeflag: tar.TypeChar, Name: "secret-project/api-key.txt", Mode: 0o644})
	f, err := NewFS(bytes.NewReader(raw), Limits{})
	if err == nil {
		err = walkLikeTheWriter(f)
	}
	if err == nil {
		t.Fatal("the stream was accepted")
	}
	// fs.PathError carries the path by construction, so the assertion is about
	// the sentence this package produced, which is what a caller logs.
	var pe *fs.PathError
	msg := err.Error()
	if errors.As(err, &pe) {
		msg = pe.Err.Error()
	}
	for _, leak := range []string{"secret-project", "api-key"} {
		if strings.Contains(msg, leak) {
			t.Errorf("the refusal quotes %q: %q", leak, msg)
		}
	}
}
