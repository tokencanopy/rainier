//go:build !linux

package driver

import (
	"errors"
	"os"
)

// reflinkFile is nil's honest twin off Linux: there is no FICLONE, so every
// clone on such a host is a copy. It is a function rather than a nil field so
// that the one place that decides — cloneInto — reads the same on every
// platform.
func reflinkFile(dst, src *os.File) error { return errNoReflink }

func isNoReflink(err error) bool { return errors.Is(err, errNoReflink) }

// sparseCopyFile has no portable way to find a file's holes, so it copies the
// whole file. A microVM host is Linux by construction (ADR-0003 §2.1); this
// build exists so the package still compiles for a developer on a Mac.
func sparseCopyFile(dst, src *os.File, size int64) error {
	return copyWholeFile(dst, src, size)
}
