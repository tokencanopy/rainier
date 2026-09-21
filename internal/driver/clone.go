// internal/driver/clone.go
//
// Copy-on-write file copies: how a session gets its own writable root
// filesystem out of an environment image without paying for the bytes
// (ADR-0003 §2.7 item 3).
//
// The host state directory on a Rainier microVM host is XFS with reflinks
// enabled, so `Create` asks the filesystem to share the image's extents with
// the session's copy: ioctl(FICLONE), which is constant-time whatever the
// image's size, and after which each file's writes are its own. btrfs answers
// the same ioctl. Everything else — ext4, tmpfs, overlayfs, an NFS mount, a
// developer's laptop — refuses it, and there the copy is made by hand with the
// holes preserved.
//
// The fallback is real and it is also a performance cliff: a sparse copy of a
// multi-gigabyte image is seconds of I/O on the create path, per session,
// where the reflink is microseconds. It is a fallback rather than a refusal
// because a runner whose state directory landed on ext4 should still work, and
// because the tests and every developer machine live on that branch — but a
// PRODUCTION host taking it is a misconfigured host, which is why the method
// each clone took is returned rather than swallowed.
package driver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// CloneMethod names how a copy was actually made. It is returned rather than
// logged-and-forgotten because the difference is three orders of magnitude on
// the create path, and because "did this host really reflink" is a question an
// operator asks of a host and a test asks of a fake.
type CloneMethod string

const (
	// CloneReflink is ioctl(FICLONE): the filesystem shares the source's
	// extents with the copy, and neither file sees the other's later writes.
	CloneReflink CloneMethod = "reflink"
	// CloneSparseCopy is the fallback: every byte is read and written, holes
	// left as holes, on a filesystem that will not share extents.
	CloneSparseCopy CloneMethod = "sparse-copy"
)

// errNoReflink is what a platform or filesystem without FICLONE answers.
var errNoReflink = errors.New("this filesystem does not support reflinks")

// Cloner makes dst a copy-on-write copy of src.
//
// It is an interface for the same reason MicrovmEngine and DiskFormatter are:
// what it does is a HOST capability, and the alternative to naming it here is
// a driver that silently does something else — which, for a root filesystem,
// means either a session writing into the image every other session of that
// environment boots, or a create that copies gigabytes and calls it instant.
//
// dst must not exist. A clone into an existing file would have to decide
// whether to truncate it, and the one caller that ever wants that (a relaunch
// into a jail that was not torn down) is better served by removing the old
// file where a reader can see it happen.
type Cloner interface {
	Clone(src, dst string) (CloneMethod, error)
}

// FileCloner is the host implementation: reflink where the filesystem allows
// it, a hole-preserving copy where it does not.
type FileCloner struct {
	// reflink is the ioctl, as a field so a test can watch the fallback path
	// on a filesystem that would happily have reflinked — which is the only
	// way to exercise it deterministically, since whether a reflink SUCCEEDS
	// is a property of the machine the test runs on. nil means "this platform
	// has no such call", which is what every non-Linux build gets.
	reflink func(dst, src *os.File) error

	mu sync.Mutex
	// methods records what each clone did, newest last. It is how a host-side
	// investigation, and the driver's own tests, answer "is this host actually
	// reflinking" without a syscall trace.
	methods []CloneMethod
}

func NewFileCloner() *FileCloner { return &FileCloner{reflink: reflinkFile} }

// Methods returns the clone methods this cloner has taken, in order.
func (c *FileCloner) Methods() []CloneMethod {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CloneMethod(nil), c.methods...)
}

func (c *FileCloner) record(m CloneMethod) {
	c.mu.Lock()
	c.methods = append(c.methods, m)
	c.mu.Unlock()
}

// Clone copies src to dst, sharing extents if the filesystem will.
//
// A failure leaves NO destination file. Half a root filesystem is the one
// outcome that must never be mistaken for an image: it mounts, or it does not,
// and either way what is inside it is whatever the copy got to.
func (c *FileCloner) Clone(src, dst string) (CloneMethod, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", fmt.Errorf("clone %s: %w", src, err)
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return "", fmt.Errorf("clone %s: %w", src, err)
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("clone %s: not a regular file", src)
	}

	// O_EXCL: the destination is this session's, and a clone that found one
	// there is a clone about to overwrite something nobody accounted for.
	// 0600, like every other file this driver writes — the copy is one
	// tenant's root filesystem on a host that runs others.
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_EXCL, microvmFileMode)
	if err != nil {
		return "", fmt.Errorf("clone %s to %s: %w", src, dst, err)
	}
	method, err := c.cloneInto(out, in, fi.Size())
	closeErr := out.Close()
	if err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(dst)
		return "", fmt.Errorf("clone %s to %s: %w", src, dst, err)
	}
	c.record(method)
	return method, nil
}

// cloneInto is Clone with both files already open, so the error paths above
// have exactly one place to clean up.
func (c *FileCloner) cloneInto(out, in *os.File, size int64) (CloneMethod, error) {
	if c.reflink != nil {
		err := c.reflink(out, in)
		if err == nil {
			return CloneReflink, nil
		}
		// A filesystem that refuses to share extents is the ordinary case off
		// XFS and btrfs, and it is not a failure — it is the fallback's cue.
		// FICLONE writes nothing when it refuses, so the destination is still
		// the empty file this opened.
		if !isNoReflink(err) {
			return "", fmt.Errorf("reflink: %w", err)
		}
	}
	if err := sparseCopyFile(out, in, size); err != nil {
		return "", err
	}
	return CloneSparseCopy, nil
}

// copyWholeFile is the last-resort copy: every byte, holes and all, used where
// the platform has no way to find a file's extents. It still ends with a
// truncate, so a file that ends in a hole keeps its length.
func copyWholeFile(out, in *os.File, size int64) error {
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Truncate(size)
}
