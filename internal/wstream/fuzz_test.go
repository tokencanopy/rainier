package wstream

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
)

// FuzzFS is the host's parser against bytes a guest chose.
//
// The guest is the untrusted end of this hop and everything it sends is parsed
// before anything is authenticated: the magic, the index's length prefixes and
// names, every tar header, every body length. So the property under test is the
// one a suspend depends on — a stream this parser cannot make sense of is
// REFUSED, with one of this package's own sentinels, rather than panicking,
// hanging, or returning a tree that is not what the bytes said.
//
// The limits are deliberately tiny: a fuzzer that could ask for 8 GiB would
// spend its time allocating instead of finding parses.
func FuzzFS(f *testing.F) {
	small := Limits{MaxEntries: 64, MaxIndexBytes: 4 << 10, MaxEntryBytes: 1 << 16, MaxTotalBytes: 1 << 20}

	f.Add([]byte(nil))
	f.Add([]byte(Magic))
	f.Add(index("fa.txt"))
	f.Add(index("ddir", "fdir/a.txt", "llink"))
	f.Add(tarballNoT(index("fa.txt"), &tar.Header{Typeflag: tar.TypeReg, Name: "a.txt", Mode: 0o644, Size: 3}))
	f.Add(tarballNoT(index("ddir", "fdir/a.txt"),
		&tar.Header{Typeflag: tar.TypeDir, Name: "dir", Mode: 0o755},
		&tar.Header{Typeflag: tar.TypeReg, Name: "dir/a.txt", Mode: 0o644, Size: 5}))
	// A whole stream the writer in this package produced, which is the shape
	// every mutation starts from.
	var buf bytes.Buffer
	if _, err := Write(context.Background(), fstest.MapFS{
		"a.txt":     &fstest.MapFile{Data: []byte("alpha"), Mode: 0o644},
		"dir/b.txt": &fstest.MapFile{Data: []byte("bravo"), Mode: 0o600},
		"link":      &fstest.MapFile{Data: []byte("dir/b.txt"), Mode: fs.ModeSymlink | 0o777},
	}, &buf, small); err == nil {
		f.Add(buf.Bytes())
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		fsys, err := NewFS(bytes.NewReader(raw), small)
		if err != nil {
			requireSentinel(t, err)
			return
		}
		if err := walkLikeTheWriter(fsys); err != nil {
			requireSentinel(t, err)
		}
	})
}

// requireSentinel is the property: every refusal is one of this package's own,
// so that a caller can tell "this stream is not one" from "this host is broken"
// — and so that no error escapes with a message nobody wrote.
func requireSentinel(t *testing.T, err error) {
	t.Helper()
	for _, s := range []error{ErrFormat, ErrEntry, ErrLimit, ErrSource, ErrTruncated, fs.ErrNotExist, fs.ErrInvalid, os.ErrDeadlineExceeded} {
		if errors.Is(err, s) {
			return
		}
	}
	t.Fatalf("a refusal that is not one of this package's: %#v (%v)", err, err)
}

// tarballNoT is tarball without a *testing.T, for the seed corpus: a seed that
// cannot be built is a seed that is silently dropped rather than a test that
// fails, which is the right trade for a corpus entry.
func tarballNoT(prefix []byte, headers ...*tar.Header) []byte {
	var buf bytes.Buffer
	buf.Write(prefix)
	tw := tar.NewWriter(&buf)
	for _, h := range headers {
		if tw.WriteHeader(h) != nil {
			return prefix
		}
		if h.Size > 0 {
			if _, err := tw.Write(bytes.Repeat([]byte("x"), int(h.Size))); err != nil {
				return prefix
			}
		}
	}
	if tw.Close() != nil {
		return prefix
	}
	return buf.Bytes()
}
