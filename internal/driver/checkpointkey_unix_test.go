//go:build unix

package driver

import (
	"io/fs"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// checkKeyOwner is the one refusal in LoadCheckpointKey that cannot be
// exercised against a real file: a test has no privilege to create a file owned
// by somebody else, which is exactly why the guard was untested. The ownership
// is the only thing it reads, so the fs.FileInfo is the seam to fake — the
// alternative is a guard nothing exercises until a real host hits it.
type fakeKeyInfo struct{ sys any }

func (fakeKeyInfo) Name() string       { return "checkpoint.key" }
func (fakeKeyInfo) Size() int64        { return 32 }
func (fakeKeyInfo) Mode() fs.FileMode  { return 0o600 }
func (fakeKeyInfo) ModTime() time.Time { return time.Time{} }
func (fakeKeyInfo) IsDir() bool        { return false }
func (f fakeKeyInfo) Sys() any         { return f.sys }

// TestAKeyFileOwnedBySomebodyElseIsRefused. A 0600 file owned by another user
// is a key that user knows and can replace, and a runner commonly runs as root,
// which can read one. The mode check beside this one cannot see it.
func TestAKeyFileOwnedBySomebodyElseIsRefused(t *testing.T) {
	foreign := uint32(os.Getuid()) + 1
	err := checkKeyOwner("/var/lib/rainier/checkpoint.key", fakeKeyInfo{sys: &syscall.Stat_t{Uid: foreign}})
	if err == nil {
		t.Fatal("a checkpoint key file owned by another uid was accepted")
	}
	if !strings.Contains(err.Error(), "owned by uid") {
		t.Errorf("the refusal does not say what is wrong: %v", err)
	}
}

// TestAKeyFileOwnedByThisRunnerIsAccepted, so the refusal above is about
// ownership and not about every file.
func TestAKeyFileOwnedByThisRunnerIsAccepted(t *testing.T) {
	info := fakeKeyInfo{sys: &syscall.Stat_t{Uid: uint32(os.Getuid())}}
	if err := checkKeyOwner("/var/lib/rainier/checkpoint.key", info); err != nil {
		t.Errorf("a key file owned by this runner was refused: %v", err)
	}
}

// TestAFileSystemThatReportsNoOwnerIsNotRefused: there is nothing to check and
// nothing to claim, so the mode check stands alone rather than this one
// refusing every file on such a filesystem.
func TestAFileSystemThatReportsNoOwnerIsNotRefused(t *testing.T) {
	if err := checkKeyOwner("/var/lib/rainier/checkpoint.key", fakeKeyInfo{sys: nil}); err != nil {
		t.Errorf("a filesystem that does not report ownership was refused: %v", err)
	}
}
