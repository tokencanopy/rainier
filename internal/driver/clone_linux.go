//go:build linux

package driver

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// reflinkFile asks the filesystem to share src's extents with dst:
// ioctl(dst, FICLONE, src). XFS with reflink=1 and btrfs do it; everything
// else refuses, and the refusal is the fallback's cue rather than an error.
//
// It clones the WHOLE file and replaces dst's contents, which is why Clone
// opens dst empty: there is nothing to preserve and nothing to append to.
func reflinkFile(dst, src *os.File) error {
	return unix.IoctlFileClone(int(dst.Fd()), int(src.Fd()))
}

// isNoReflink reports whether an FICLONE failure means "this filesystem does
// not do that" rather than "this copy went wrong".
//
// The kernel has several ways of saying the same thing, and which one arrives
// depends on the filesystem: EOPNOTSUPP from one that has no clone operation
// at all, ENOTTY from one whose ioctl table does not know the number, EXDEV
// when the two files are on different filesystems (a state directory and an
// image directory on separate mounts — legal, and a copy), EINVAL from a few
// that check their arguments before their capabilities. All of them mean the
// same thing to a caller with a fallback, and none of them has left anything
// in the destination.
func isNoReflink(err error) bool {
	if errors.Is(err, errNoReflink) {
		return true
	}
	for _, e := range []error{unix.EOPNOTSUPP, unix.ENOTTY, unix.EXDEV, unix.EINVAL, unix.ENOSYS} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// sparseCopyFile copies src into dst preserving its holes.
//
// An environment image is a sparse ext4 file: a 10 GiB filesystem that has a
// gigabyte written into it occupies a gigabyte, and a copy that read every
// byte would make the copy occupy ten. So the source's extents are walked with
// SEEK_DATA and SEEK_HOLE and only the data is copied, with copy_file_range so
// the bytes never round-trip through this process's address space.
//
// A filesystem that does not implement SEEK_DATA is not an error: the whole
// file is copied instead, which is correct and merely fatter.
func sparseCopyFile(dst, src *os.File, size int64) error {
	if size == 0 {
		return dst.Truncate(0)
	}
	srcFD, dstFD := int(src.Fd()), int(dst.Fd())

	var off int64
	for off < size {
		dataStart, err := unix.Seek(srcFD, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				// No data at or after off: the rest of the file is a hole, and
				// the truncate below is what gives the copy its length.
				break
			}
			if isNoSeekData(err) {
				return copyWholeFile(dst, src, size)
			}
			return fmt.Errorf("find the next data extent: %w", err)
		}
		if dataStart >= size {
			break
		}
		dataEnd, err := unix.Seek(srcFD, dataStart, unix.SEEK_HOLE)
		if err != nil {
			if isNoSeekData(err) {
				return copyWholeFile(dst, src, size)
			}
			return fmt.Errorf("find the end of a data extent: %w", err)
		}
		if dataEnd > size {
			dataEnd = size
		}
		if err := copyExtent(dstFD, srcFD, dst, src, dataStart, dataEnd-dataStart); err != nil {
			return err
		}
		off = dataEnd
	}
	// The length is set last and unconditionally: a file that ends in a hole
	// has had nothing written at its tail, and without this the copy would be
	// short by exactly that hole.
	return dst.Truncate(size)
}

// isNoSeekData reports whether a seek failure means the filesystem has no
// notion of extents (so the whole file must be copied) rather than a real I/O
// failure.
func isNoSeekData(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENOSYS)
}

// copyExtent moves one data extent, at the same offset in both files.
//
// copy_file_range keeps the bytes in the kernel, and on a filesystem that
// supports it it can also share extents outright. It is allowed to move less
// than it was asked for, so this loops; a kernel or filesystem that refuses it
// altogether falls back to an ordinary read-and-write of the same range.
func copyExtent(dstFD, srcFD int, dst, src *os.File, offset, length int64) error {
	for length > 0 {
		rOff, wOff := offset, offset
		n, err := unix.CopyFileRange(srcFD, &rOff, dstFD, &wOff, int(min(length, 1<<30)), 0)
		if err != nil {
			if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
				return copyExtentByHand(dst, src, offset, length)
			}
			return fmt.Errorf("copy an extent: %w", err)
		}
		if n == 0 {
			// The source ended earlier than its own extent map said. Nothing
			// further to copy, and the truncate above gives the file its
			// length.
			return nil
		}
		offset += int64(n)
		length -= int64(n)
	}
	return nil
}

// copyExtentByHand is copyExtent through this process, for a kernel or
// filesystem pairing that will not do copy_file_range.
func copyExtentByHand(dst, src *os.File, offset, length int64) error {
	if _, err := io.Copy(
		&offsetWriter{f: dst, off: offset},
		io.NewSectionReader(src, offset, length),
	); err != nil {
		return fmt.Errorf("copy an extent: %w", err)
	}
	return nil
}

// offsetWriter writes at an explicit offset, so the copy never depends on
// either file's seek position.
type offsetWriter struct {
	f   *os.File
	off int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}
