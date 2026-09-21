// internal/driver/microvm_caps_test.go
package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCapStatus writes a /proc/self/status-shaped fixture carrying the given
// effective capability mask, and points the reader at it.
func writeCapStatus(t *testing.T, capEff uint64) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "status")
	body := fmt.Sprintf("Name:\trunnerd\nUid:\t1000\t1000\t1000\nCapInh:\t0000000000000000\nCapPrm:\t%016x\nCapEff:\t%016x\nCapBnd:\t000001ffffffffff\n", capEff, capEff)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write cap status fixture: %v", err)
	}
	old := capStatusPath
	capStatusPath = path
	t.Cleanup(func() { capStatusPath = old })
}

// allMicrovmCaps is the mask a correctly configured runner holds.
func allMicrovmCaps() uint64 {
	var mask uint64
	for _, c := range microvmCapabilities {
		mask |= uint64(1) << c.Bit
	}
	return mask
}

// fakeCgroupV2 builds a directory that looks like a delegated cgroup v2
// subtree, so the check has something true to find.
func fakeCgroupV2(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu memory pids\n"), 0o644); err != nil {
		t.Fatalf("write cgroup.controllers: %v", err)
	}
	return root
}

// TestMissingCapabilityFailsClosedAndNamesIt. A runner that started without
// one of these would register with the control plane, accept a placement, and
// fail at the first create — on a host nobody was looking at.
func TestMissingCapabilityFailsClosedAndNamesIt(t *testing.T) {
	root := fakeCgroupV2(t)
	for _, missing := range microvmCapabilities {
		t.Run(missing.Name, func(t *testing.T) {
			writeCapStatus(t, allMicrovmCaps()&^(uint64(1)<<missing.Bit))
			err := checkMicrovmPrivileges(MicrovmOpts{CgroupRoot: root})
			if err == nil {
				t.Fatalf("a runner without %s started anyway", missing.Name)
			}
			if !strings.Contains(err.Error(), missing.Name) {
				t.Fatalf("error = %q, want it to name %s", err, missing.Name)
			}
			// And what it is for, so the operator knows what they are
			// granting rather than copying a flag they cannot evaluate.
			if !strings.Contains(err.Error(), missing.Why) {
				t.Fatalf("error = %q, want it to say what %s is for", err, missing.Name)
			}
		})
	}
}

// TestFullCapabilitiesPassWithoutBeingRoot is the point of the whole check:
// ADR-0003 §4.5 wants runnerd unprivileged, so holding the named capabilities
// has to be enough. This test process is not root and the fixture says so.
func TestFullCapabilitiesPassWithoutBeingRoot(t *testing.T) {
	writeCapStatus(t, allMicrovmCaps())
	if err := checkMicrovmPrivileges(MicrovmOpts{CgroupRoot: fakeCgroupV2(t)}); err != nil {
		t.Fatalf("a runner holding every named capability was refused: %v", err)
	}
}

// TestCgroupV1IsRefused. The jailer is asked for cgroup v2 and ADR-0003 §4.6
// meters each VM from cpu.stat and memory.current under it; a v1 host would
// produce sessions nobody can bill for, which is one of the bakeoff's hard
// gates.
func TestCgroupV1IsRefused(t *testing.T) {
	writeCapStatus(t, allMicrovmCaps())
	// A directory with no cgroup.controllers: cgroup v1, or not a cgroup
	// mount at all.
	err := checkMicrovmPrivileges(MicrovmOpts{CgroupRoot: t.TempDir()})
	if err == nil {
		t.Fatal("a host with no cgroup v2 mount started anyway")
	}
	if !strings.Contains(err.Error(), "cgroup v2") {
		t.Fatalf("error = %q, want it to name cgroup v2", err)
	}
}

// TestTheRequirementsListMatchesTheCheck: the text an operator is handed and
// the list the check reads are one list, so a capability cannot be added to
// the check and left out of the instructions.
func TestTheRequirementsListMatchesTheCheck(t *testing.T) {
	doc := MicrovmHostRequirements()
	for _, c := range microvmCapabilities {
		if !strings.Contains(doc, c.Name) {
			t.Errorf("the requirements text does not mention %s", c.Name)
		}
	}
	for _, want := range []string{"/dev/kvm", "cgroup v2", "AmbientCapabilities=", "requires running as root", "euid 0"} {
		if !strings.Contains(doc, want) {
			t.Errorf("the requirements text does not mention %q", want)
		}
	}
}

// TestUnreadableCapabilitiesFailClosed: a runner that cannot tell whether it
// may build a session's network must not assume it may.
func TestUnreadableCapabilitiesFailClosed(t *testing.T) {
	old := capStatusPath
	capStatusPath = filepath.Join(t.TempDir(), "absent")
	t.Cleanup(func() { capStatusPath = old })

	if err := checkMicrovmPrivileges(MicrovmOpts{CgroupRoot: fakeCgroupV2(t)}); err == nil {
		t.Fatal("a runner that could not read its own capabilities started anyway")
	}
}
