package driver

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestGuestHostIdentityPinsProcessAndNamespace(t *testing.T) {
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	if err := os.MkdirAll(filepath.Join(proc, "42"), 0700); err != nil {
		t.Fatal(err)
	}
	write := func(path, value string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	stat := "42 (firecracker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 912345 0"
	write(filepath.Join(proc, "42", "stat"), stat)
	write(filepath.Join(proc, "42", "cmdline"), "/firecracker\x00--id\x00mvm-1\x00")
	uid, gid := strconv.Itoa(os.Geteuid()), strconv.Itoa(os.Getegid())
	write(filepath.Join(proc, "42", "status"), "Uid:\t"+uid+"\t"+uid+"\t"+uid+"\t"+uid+"\nGid:\t"+gid+"\t"+gid+"\t"+gid+"\t"+gid+"\n")
	ns := filepath.Join(root, "namespace")
	write(ns, "synthetic namespace identity")
	boot := filepath.Join(root, "boot-id")
	write(boot, "11111111-2222-4333-8444-555555555555\n")
	got, err := readGuestHostIdentity(proc, boot, ns, 42, "mvm-1", os.Geteuid(), os.Getegid())
	if err != nil {
		t.Fatal(err)
	}
	if got.StartTime != 912345 || got.BootID != "11111111-2222-4333-8444-555555555555" || got.NamespaceInode == 0 {
		t.Fatalf("bad identity: %+v", got)
	}
	// Replacement namespace at the same name must not compare as the original.
	if err := os.Rename(ns, ns+".old"); err != nil {
		t.Fatal(err)
	}
	write(ns, "replacement")
	next, err := readGuestHostIdentity(proc, boot, ns, 42, "mvm-1", os.Geteuid(), os.Getegid())
	if err != nil || next == got {
		t.Fatalf("namespace replacement not distinguished: %v", err)
	}
	write(filepath.Join(proc, "42", "cmdline"), "/firecracker\x00--id\x00mvm-10\x00")
	if _, err := readGuestHostIdentity(proc, boot, ns, 42, "mvm-1", os.Geteuid(), os.Getegid()); err == nil {
		t.Fatal("accepted another VM")
	}
	write(filepath.Join(proc, "42", "cmdline"), "/firecracker\x00--id\x00mvm-1\x00")
	if _, err := readGuestHostIdentity(proc, boot, ns, 42, "mvm-1", os.Geteuid()+1, os.Getegid()); err == nil {
		t.Fatal("accepted wrong process owner")
	}
}

func TestGuestRecoveryBootPathIsCanonical(t *testing.T) {
	for _, tc := range []struct {
		path string
		boot int
	}{
		{"/v1.sock", 1}, {"/v27.sock", 27}, {"/v0.sock", 0}, {"/v01.sock", 0}, {"/tmp/v1.sock", 0}, {"/v-1.sock", 0}, {"/v1.sock/../v2.sock", 0},
	} {
		boot, err := guestRecoveryBoot(tc.path)
		if tc.boot == 0 {
			if err == nil {
				t.Errorf("accepted %q", tc.path)
			}
		} else if err != nil || boot != tc.boot {
			t.Errorf("%q: %d %v", tc.path, boot, err)
		}
	}
}

func TestGuestReconnectRefusesUnverifiableEngine(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{GuestReconnect: true})
	defer m.stateLock.Close()
	h, err := m.Create(context.Background(), Spec{SessionID: "session-test", GuestReconnect: 1})
	if err == nil {
		m.Destroy(context.Background(), h.ID)
		t.Fatal("enabled reconnect without a VM identity verifier")
	}
	used, _, _ := m.Capacity(context.Background())
	if used != 0 {
		t.Fatal("refused reconnect reserved capacity")
	}
}
