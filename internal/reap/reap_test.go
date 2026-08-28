package reap

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReapOnNonLinuxIsNoop(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("linux has real reaping")
	}
	ch := Reap(0)
	select {
	case <-ch:
		t.Fatal("no-op reaper must never fire")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestReapCollectsOrphan(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reaping is linux-only")
	}
	// A process whose parent exits before it does becomes an orphan reparented
	// to the subreaper. `sh -c '(sleep 0.2 &) ; exit 0'` leaves a grandchild.
	Reap(-1) // reap everything; we only assert no zombie accumulates
	cmd := exec.Command("sh", "-c", "(sleep 0.2 &) ; exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	time.Sleep(400 * time.Millisecond)
	// If reaping works, the orphaned `sleep` has been collected — assert no
	// defunct child remains by reading /proc for our zombie children.
	if hasZombieChild(t) {
		t.Fatal("zombie child not reaped")
	}
}

func hasZombieChild(t *testing.T) bool {
	t.Helper()
	entries, _ := filepath.Glob("/proc/[0-9]*/stat")
	me := os.Getpid()
	for _, p := range entries {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		fields := strings.Fields(string(b))
		if len(fields) < 4 {
			continue
		}
		state, ppid := fields[2], fields[3]
		if state == "Z" && ppid == strconv.Itoa(me) {
			return true
		}
	}
	return false
}
