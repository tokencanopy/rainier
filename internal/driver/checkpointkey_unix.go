//go:build unix

package driver

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

// checkKeyOwner requires the checkpoint key file to be owned by the user this
// runner runs as.
//
// The mode check beside it (no group or other bits) is about who ELSE can read
// the key; this one is about who WROTE it. A file at mode 0600 owned by another
// user is a file whose owner knows the key and whose owner can replace it — so
// a runner pointed at one, by a path typo or by a shared directory somebody else
// can create in, would encrypt every workspace checkpoint on this host under a
// key that is not its own. That is not a theoretical arrangement: a runner
// commonly runs as root, and root can read a 0600 file belonging to anybody.
func checkKeyOwner(path string, info fs.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// A file system that does not report ownership. There is nothing to
		// check and nothing to claim, so the mode check stands alone.
		return nil
	}
	if uid := os.Getuid(); int(st.Uid) != uid {
		return fmt.Errorf("microvm: the checkpoint key file %s is owned by uid %d, not by the user this runner runs as (%d)",
			path, st.Uid, uid)
	}
	return nil
}
