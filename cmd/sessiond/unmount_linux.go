//go:build linux

package main

import (
	"errors"
	"log"
	"os"

	"golang.org/x/sys/unix"
)

// unmountAgentHome unmounts the agent home before this VM ends.
//
// It is the one filesystem act a cold suspend needs from inside the guest:
// the home is a block device of its own (ADR-0003 §4.1 keeps it out of the
// workspace disk and its checkpoint), and a device detached with dirty pages
// above it is a home that comes back needing a fsck for a credential set
// somebody logged in for once.
//
// MNT_DETACH is deliberate. A lazy unmount detaches the tree immediately and
// lets the kernel finish when the last reference goes, which is what a
// process still holding a file under it turns an ordinary unmount into
// EBUSY over — and the alternative to detaching is refusing to answer the
// host, which ends the VM anyway with nothing flushed at all.
//
// Every failure is a log line and not an error. A session with no home
// mounted (no creator, or an older control plane) is the ordinary case and
// reports ENOENT or EINVAL; a failure here must not be what stops this
// process answering the host.
func unmountAgentHome(path string) {
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := unix.Unmount(path, unix.MNT_DETACH); err != nil {
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
			// Not a mount point: nothing was mounted here, which is what a
			// session with no agent home looks like from inside.
			return
		}
		log.Printf("unmounting the agent home at %s before this VM ends: %v", path, err)
		return
	}
	log.Printf("unmounted the agent home at %s", path)
}
