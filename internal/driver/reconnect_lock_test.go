package driver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGuestStateOwnershipIsExclusive(t *testing.T) {
	dir := t.TempDir()
	first, err := lockGuestState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lockGuestState(dir); err == nil {
		second.Close()
		t.Fatal("two runners acquired recovery ownership")
	}
	first.Close()
	next, err := lockGuestState(dir)
	if err != nil {
		t.Fatal("exited owner prevented recovery")
	}
	next.Close()
}
func TestGuestStateOwnershipRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("unchanged_test"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "guest-reconnect.lock")); err != nil {
		t.Fatal(err)
	}
	if f, err := lockGuestState(dir); err == nil {
		f.Close()
		t.Fatal("followed recovery ownership symlink")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "unchanged_test" {
		t.Fatal("altered symlink target")
	}
}

func TestGuestStateOwnershipCannotBeBypassedByDisablingReconnect(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{GuestReconnect: true})
	defer m.stateLock.Close()
	opts := m.opts
	opts.GuestReconnect = false
	other, err := NewMicrovm(opts)
	if err == nil {
		if other.stateLock != nil {
			other.stateLock.Close()
		}
		t.Fatal("capability-off driver bypassed active ownership")
	}
}
