//go:build linux

package driver

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// Exercise actual Linux ownership checks: CAP_CHOWN does not imply CAP_FOWNER.
// The normal root harness otherwise hides chmod-after-chown failures.
func TestMicrovmJailOwnershipWithoutFownerOnLinux(t *testing.T) {
	if dir := os.Getenv("RAINIER_JAIL_OWNER_CHILD"); dir != "" {
		path := filepath.Join(dir, "image")
		if err := os.WriteFile(path, []byte("synthetic"), 0600); err != nil {
			t.Fatal(err)
		}
		f := &FirecrackerEngine{chown: os.Chown}
		// The second call represents a resume: the inode already belongs to a VM.
		for _, uid := range []int{200001, 200002} {
			if err := f.chownJailPath(path, uid, os.Getgid(), 0660); err != nil {
				t.Fatal(err)
			}
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if st.Mode().Perm() != 0660 || st.Sys().(*syscall.Stat_t).Uid != uint32(uid) {
				t.Fatalf("wrong final ownership or permissions: %+v", st)
			}
		}
		return
	}
	if os.Getenv("RAINIER_MICROVM_KVM_TEST") != "1" || os.Geteuid() != 0 {
		t.Skip("requires the opted-in root hardware harness to launch a non-root child with only CAP_CHOWN")
	}
	dir, err := os.MkdirTemp("/tmp", "rainier-jail-owner-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := os.Chown(dir, 65534, 65534); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestMicrovmJailOwnershipWithoutFownerOnLinux$", "-test.v")
	cmd.Env = append(os.Environ(), "RAINIER_JAIL_OWNER_CHILD="+dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}, AmbientCaps: []uintptr{0}}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("non-root ownership lifecycle: %v\n%s", err, out)
	}
}
