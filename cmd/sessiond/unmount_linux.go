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
// MNT_DETACH is deliberate, and the sync(2) in front of it is what makes it
// safe. A lazy unmount detaches the tree immediately and lets the kernel
// finish when the last reference goes, which is what turns an ordinary
// unmount into EBUSY when a process is still holding a file under it — and
// the alternative to detaching is refusing to answer the host, which ends the
// VM anyway with nothing flushed at all. But "finish when the last reference
// goes" is precisely a writeback this VM may not live to see: the host
// terminates it on the ready answer, so a detach on its own is NOT a promise
// that the device is clean. sync() before it is: it returns once every dirty
// page on every mounted filesystem has been handed to its device, so what the
// lazy detach has left to do afterwards is teardown rather than data.
//
// (sync() is whole-machine rather than per-mount, which inside a microVM
// whose only writable devices are this session's own is exactly the right
// scope — the workspace disk wants flushing before this VM ends for the same
// reason. syncfs(2) on a descriptor under the mount would be narrower and
// buys nothing here.)
//
// Every failure is a log line and not an error. A session with no home
// mounted (no creator, or an older control plane) is the ordinary case and
// reports ENOENT or EINVAL; a failure here must not be what stops this
// process answering the host.
func unmountAgentHome(path string) {
	if _, err := os.Stat(path); err != nil {
		return
	}
	// Before the detach: see above. It cannot fail and returns nothing.
	unix.Sync()
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
