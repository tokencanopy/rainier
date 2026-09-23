package wstream

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"time"
)

// Report is what one stream carried: the counts the guest states in its end
// marker, and the host checks against what it received.
//
// Skipped is the entries that are neither a directory, a regular file nor a
// symbolic link — a socket a dev server left behind, a FIFO, a device node.
// They are skipped and COUNTED rather than refused, for the reason the
// checkpoint package skips them: a stray socket must not be able to defeat the
// durability barrier, and a count in the end marker means it is never silent.
type Report struct {
	Entries int64
	Bytes   int64
	Skipped int64
}

// Write streams fsys to w in the format this package's comment describes, and
// returns what it carried.
//
// It walks the tree TWICE — once for the index, once for the bodies — rather
// than buffering the index, because a guest holding its own workspace's shape
// in memory is the same O(entries) cost this design refuses on the host, paid
// inside a VM with less memory. The workspace is quiesced by the time this
// runs (the agent is stopped, the execs are dead, the disks are synced), so the
// two walks see the same tree; a tree that changed between them is caught here
// rather than left for the host, by a digest over the names each pass visited.
//
// Nothing here follows a symbolic link. fs.WalkDir does not, fs.ReadLink reads
// the link itself, and a link whose target leaves the tree is refused rather
// than resolved — which is what keeps a workspace containing
// "creds -> /etc/shadow" from streaming /etc/shadow out of the guest.
// The Report is filled in as far as the stream got, on the failure paths as
// well as on the successful one: the counts travel to the host in the end
// marker, and "it failed after 900 entries and 40 MB" is the only thing a
// person can act on in a diagnostic that may not name a file.
func Write(ctx context.Context, fsys fs.FS, w io.Writer, lim Limits) (Report, error) {
	cw := &countWriter{w: &sinkWriter{w: w}}
	var entries, skipped int64
	rep := func() Report { return Report{Entries: entries, Bytes: cw.n, Skipped: skipped} }

	lim, err := lim.resolve()
	if err != nil {
		return rep(), err
	}
	if fsys == nil {
		return rep(), fmt.Errorf("%w: there is no tree to stream", ErrSource)
	}

	if _, err := io.WriteString(cw, Magic); err != nil {
		return rep(), writeErr(err)
	}

	indexDigest := sha256.New()
	var indexBytes int64
	var rec []byte
	skipped, err = walkTree(ctx, fsys, lim, func(ord int64, name string, kind byte, _ fs.DirEntry) error {
		entries = ord
		if ord > lim.MaxEntries {
			return fmt.Errorf("%w: the tree has more than %d entries", ErrLimit, lim.MaxEntries)
		}
		rec = append(rec[:0], kind)
		rec = append(rec, name...)
		indexBytes += int64(len(rec))
		if indexBytes > lim.MaxIndexBytes {
			return fmt.Errorf("%w: the tree's names are more than %d bytes", ErrLimit, lim.MaxIndexBytes)
		}
		hashRecord(indexDigest, kind, name)
		return writeRecord(cw, rec)
	})
	if err != nil {
		return rep(), err
	}
	if _, err := cw.Write([]byte{0}); err != nil { // the zero-length record that ends the index
		return rep(), writeErr(err)
	}

	bodyDigest := sha256.New()
	tw := tar.NewWriter(cw)
	buf := make([]byte, copyBufSize)
	if _, err := walkTree(ctx, fsys, lim, func(ord int64, name string, kind byte, d fs.DirEntry) error {
		hashRecord(bodyDigest, kind, name)
		return writeEntry(ctx, tw, cw, fsys, ord, name, kind, d, lim, buf)
	}); err != nil {
		return rep(), err
	}
	if err := tw.Close(); err != nil {
		return rep(), writeErr(err)
	}
	// The two passes must have seen the same tree. They ran seconds apart on a
	// quiesced workspace, so a difference is a process still writing in there —
	// and a stream whose index and bodies disagree is one the host refuses
	// halfway through, with nothing useful to say about why.
	if !bytes.Equal(indexDigest.Sum(nil), bodyDigest.Sum(nil)) {
		return rep(), fmt.Errorf("%w: the workspace changed while it was being streamed; it was not quiesced", ErrSource)
	}
	if cw.n > lim.MaxTotalBytes {
		return rep(), fmt.Errorf("%w: the stream is over %d bytes", ErrLimit, lim.MaxTotalBytes)
	}
	return rep(), nil
}

// copyBufSize is the one copy buffer a stream holds, the same size the
// checkpoint package uses for the same job.
const copyBufSize = 32 << 10

// writeEntry writes one entry's header and, for a regular file, its bytes.
func writeEntry(ctx context.Context, tw *tar.Writer, cw *countWriter, fsys fs.FS,
	ord int64, name string, kind byte, d fs.DirEntry, lim Limits, buf []byte) error {
	switch kind {
	case KindDir:
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("%w: entry %d could not be inspected (%s)", ErrSource, ord, category(err))
		}
		return headerErr(ord, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeDir,
			Name:     name,
			Mode:     int64(info.Mode().Perm()),
			ModTime:  modTime(info),
		}))

	case KindSymlink:
		target, err := fs.ReadLink(fsys, name)
		if err != nil {
			return fmt.Errorf("%w: entry %d is a symbolic link that could not be read (%s)", ErrSource, ord, category(err))
		}
		if err := validLink(name, target); err != nil {
			return fmt.Errorf("%w: entry %d is a symbolic link that is not allowed (%s)", ErrEntry, ord, err)
		}
		return headerErr(ord, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeSymlink,
			Name:     name,
			Linkname: target,
			ModTime:  time.Unix(0, 0).UTC(),
		}))

	default: // KindFile
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("%w: entry %d could not be inspected (%s)", ErrSource, ord, category(err))
		}
		size := info.Size()
		if size > lim.MaxEntryBytes {
			return fmt.Errorf("%w: entry %d is larger than %d bytes", ErrLimit, ord, lim.MaxEntryBytes)
		}
		if cw.n+size > lim.MaxTotalBytes {
			return fmt.Errorf("%w: the stream is over %d bytes", ErrLimit, lim.MaxTotalBytes)
		}
		if err := headerErr(ord, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     int64(info.Mode().Perm()),
			Size:     size,
			ModTime:  modTime(info),
		})); err != nil {
			return err
		}
		return copyFile(ctx, tw, fsys, ord, name, size, buf)
	}
}

// copyFile writes exactly the length the header promised, and fails on either
// side of that promise.
//
// A file that SHRANK would desynchronize the tar stream; a file that GREW would
// be silently truncated into a checkpoint that verified. Both mean the
// workspace was not quiesced, which is a failed suspend rather than a
// checkpoint of a torn file — the same choice, for the same reason, that
// checkpoint.writeFileEntry makes on the host's side of a local tree.
func copyFile(ctx context.Context, tw io.Writer, fsys fs.FS, ord int64, name string, size int64, buf []byte) error {
	f, err := fsys.Open(name)
	if err != nil {
		return fmt.Errorf("%w: entry %d could not be opened (%s)", ErrSource, ord, category(err))
	}
	defer f.Close()

	n, err := io.CopyBuffer(tw, io.LimitReader(&ctxReader{ctx: ctx, r: f}, size), buf)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// io.Copy reports the DESTINATION's failures too, and a destination
		// that stopped taking bytes is not a workspace that could not be read.
		// Reporting it as one would put "this file is unreadable" in a session's
		// error column for a connection that died.
		if d := destErr(err); d != nil {
			return d
		}
		return fmt.Errorf("%w: entry %d could not be read (%s)", ErrSource, ord, category(err))
	}
	if n != size {
		return fmt.Errorf("%w: entry %d shrank while it was being read; the workspace was not quiesced", ErrSource, ord)
	}
	var probe [1]byte
	if k, _ := f.Read(probe[:]); k > 0 {
		return fmt.Errorf("%w: entry %d grew while it was being read; the workspace was not quiesced", ErrSource, ord)
	}
	return nil
}

// walkTree visits every entry this format carries, in fs.WalkDir order, and is
// the ONE place the two passes agree about what an entry is: the same
// exclusions, the same kinds, the same ordinals, the same skips.
func walkTree(ctx context.Context, fsys fs.FS, lim Limits, fn func(ord int64, name string, kind byte, d fs.DirEntry) error) (skipped int64, err error) {
	ord := int64(0)
	err = fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			// fs.WalkDir hands back the file system's own *fs.PathError, which
			// carries the path. Reduced to its errno, never forwarded.
			return fmt.Errorf("%w: entry %d could not be read (%s)", ErrSource, ord+1, category(err))
		}
		if name == "." {
			return nil // the root is the destination, not an entry
		}
		if lim.excluded(name) {
			if d.IsDir() {
				// Pruned rather than filtered: an excluded directory is never
				// opened and nothing inside it is ever read, which is what
				// makes an exclusion a promise about the bytes that left the
				// guest rather than about the archive.
				return fs.SkipDir
			}
			return nil
		}
		var kind byte
		switch {
		case d.IsDir():
			kind = KindDir
		case d.Type()&fs.ModeSymlink != 0:
			kind = KindSymlink
		case d.Type().IsRegular():
			kind = KindFile
		default:
			// A socket, a FIFO, a device node. Skipped and counted rather than
			// refused, and skipped BEFORE an ordinal is spent on it, so that the
			// ordinals in an error name entries the stream actually carries.
			skipped++
			return nil
		}
		ord++
		if err := validName(name); err != nil {
			return fmt.Errorf("%w: entry %d is not a valid name (%s)", ErrEntry, ord, err)
		}
		return fn(ord, name, kind, d)
	})
	return skipped, err
}

// writeRecord writes one index record: its length as a uvarint, then its bytes.
func writeRecord(w io.Writer, rec []byte) error {
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(rec)))
	if _, err := w.Write(hdr[:n]); err != nil {
		return writeErr(err)
	}
	if _, err := w.Write(rec); err != nil {
		return writeErr(err)
	}
	return nil
}

// hashRecord folds one (kind, name) into a digest. The separator is a NUL,
// which no name may contain, so two different trees cannot hash the same.
func hashRecord(h hash.Hash, kind byte, name string) {
	h.Write([]byte{kind, 0})
	h.Write([]byte(name))
	h.Write([]byte{0})
}

func headerErr(ord int64, err error) error {
	if err == nil {
		return nil
	}
	if d := destErr(err); d != nil {
		return d
	}
	return fmt.Errorf("%w: entry %d could not be written (%s)", ErrSource, ord, category(err))
}

// sinkWriter marks the errors that came from the DESTINATION — the caller's
// transport — rather than from the tree, and destErr reads the mark off again.
//
// The distinction is not cosmetic. Everything this package writes goes through
// archive/tar and io.Copy, both of which return a destination's error as their
// own, so without the mark a connection that died would be reported as "entry 7
// could not be read" — which sends a person looking at a file when the problem
// is a socket. With it, the caller's own sentence about its own transport
// survives the trip out unchanged, which is what lets sessiond promise that
// nothing in that sentence is a path.
type sinkWriter struct{ w io.Writer }

type sinkError struct{ err error }

func (e *sinkError) Error() string { return e.err.Error() }
func (e *sinkError) Unwrap() error { return e.err }

func (s *sinkWriter) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	if err != nil {
		return n, &sinkError{err: err}
	}
	return n, nil
}

// destErr returns the destination's own error when err came from the
// destination, and nil when it did not.
func destErr(err error) error {
	var se *sinkError
	if errors.As(err, &se) {
		return se.err
	}
	return nil
}

// writeErr is destErr with a fallback, for the places where the error can only
// have come from the destination but the code must not depend on that: a nil
// returned from destErr there would swallow a failure whole.
func writeErr(err error) error {
	if d := destErr(err); d != nil {
		return d
	}
	return fmt.Errorf("%w: the stream could not be written (%s)", ErrSource, category(err))
}

// modTime is the modification time an entry carries: truncated to the second,
// deliberately and here rather than by archive/tar.
//
// tar's own header holds whole seconds, and sub-second precision is
// representable only as a PAX record — which costs a 1 KiB block PER ENTRY, so
// a large tree would pay megabytes for nanoseconds that the checkpoint format
// truncates away at the other end anyway (checkpoint.entryModTime).
func modTime(info fs.FileInfo) time.Time { return info.ModTime().Truncate(time.Second) }

// countWriter counts what passes through it, which is how the stream's byte
// total is known without a second pass.
type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// ctxReader makes a long copy cancellable: the context is checked once per copy
// buffer, so one enormous file does not make a cancelled suspend run to
// completion.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// category reduces a file-system error to the part that is safe to print. An
// *fs.PathError and an *os.LinkError both carry the path that failed — which is
// session content — wrapped around an errno that carries nothing.
func category(err error) string {
	var pe *fs.PathError
	if errors.As(err, &pe) && pe.Err != nil {
		return pe.Err.Error()
	}
	var le *os.LinkError
	if errors.As(err, &le) && le.Err != nil {
		return le.Err.Error()
	}
	if err == nil {
		return "unknown error"
	}
	// Anything else comes from a file system this package did not write, whose
	// text is not known to be path-free. Reduced rather than forwarded: a leak
	// is worse than a vague sentence.
	return "input/output error"
}
