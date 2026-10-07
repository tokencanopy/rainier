package driver

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestGuestRecoveryListenerRefusesActiveOwner(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "active.sock")
	old, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := bindRecoveredGuestListener(path, os.Geteuid()); err == nil {
		next.Close()
		t.Fatal("replaced active listener")
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("changed active listener inode")
	}
}
func TestGuestRecoveryListenerAcceptsOnlyStaleOwnedSocket(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "stale.sock")
	old, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	old.SetUnlinkOnClose(false)
	old.Close()
	next, err := bindRecoveredGuestListener(path, os.Geteuid())
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
	if err := os.WriteFile(path, []byte("synthetic marker"), 0600); err != nil {
		t.Fatal(err)
	}
	if next, err := bindRecoveredGuestListener(path, os.Geteuid()); err == nil {
		next.Close()
		t.Fatal("replaced a non-socket")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "synthetic marker" {
		t.Fatal("changed non-socket")
	}
}
