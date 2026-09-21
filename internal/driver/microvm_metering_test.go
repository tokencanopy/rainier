// internal/driver/microvm_metering_test.go
package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCgroup builds a cgroup v2 directory for one VM, the way the jailer
// would have.
func fakeCgroup(t *testing.T, root, parent, id string, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(root, parent, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("make fake cgroup: %v", err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

const sampleCPUStat = `usage_usec 123456789
user_usec 100000000
system_usec 23456789
nr_periods 0
nr_throttled 0
throttled_usec 0
`

// TestUsageReadsTheVMsCgroup is ADR-0003 §4.6: CPU and memory measured from
// outside the guest, where a tenant cannot influence them.
func TestUsageReadsTheVMsCgroup(t *testing.T) {
	cgroupRoot := t.TempDir()
	m, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2, CgroupRoot: cgroupRoot})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-meter"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cfg, ok := instanceConfig(m, h.ID)
	if !ok {
		t.Fatal("no record for the new session")
	}
	if cfg.CgroupPath == "" {
		t.Fatal("the session was launched with no cgroup recorded; nothing can meter it")
	}

	fakeCgroup(t, cgroupRoot, defaultJailCgroupParent, h.ID, map[string]string{
		"cpu.stat":       sampleCPUStat,
		"memory.current": "2147483648\n",
		"memory.peak":    "3221225472\n",
	})

	usage, err := m.Usage(ctx, h.ID)
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if usage.InstanceID != h.ID || usage.SessionID != "sess-meter" {
		t.Fatalf("a reading was filed against %s/%s, want %s/sess-meter", usage.InstanceID, usage.SessionID, h.ID)
	}
	if usage.CPUUsageUsec != 123456789 || usage.CPUUserUsec != 100000000 || usage.CPUSystemUsec != 23456789 {
		t.Fatalf("cpu = %+v", usage)
	}
	if usage.MemoryCurrentBytes != 2147483648 {
		t.Fatalf("memory.current = %d, want 2147483648", usage.MemoryCurrentBytes)
	}
	if !usage.HasPeak || usage.MemoryPeakBytes != 3221225472 {
		t.Fatalf("memory.peak = %d (present %v), want 3221225472", usage.MemoryPeakBytes, usage.HasPeak)
	}
	if usage.Taken.IsZero() {
		t.Fatal("the reading carries no timestamp, so nothing above it can turn two readings into a rate")
	}
}

// TestUsageWithoutMemoryPeak: memory.peak arrived in 6.8 and a host on an
// older kernel is a host, not a misconfiguration. Absent must read as absent
// and not as zero, which a GiB-second bill would take at face value.
func TestUsageWithoutMemoryPeak(t *testing.T) {
	cgroupRoot := t.TempDir()
	m, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2, CgroupRoot: cgroupRoot})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-nopeak"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	fakeCgroup(t, cgroupRoot, defaultJailCgroupParent, h.ID, map[string]string{
		"cpu.stat":       sampleCPUStat,
		"memory.current": "1024\n",
	})

	usage, err := m.Usage(ctx, h.ID)
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if usage.HasPeak {
		t.Fatalf("a reading from a kernel with no memory.peak reported a peak of %d", usage.MemoryPeakBytes)
	}
	if usage.MemoryCurrentBytes != 1024 {
		t.Fatalf("memory.current = %d", usage.MemoryCurrentBytes)
	}
}

// TestUsageRefusesToInventAZero. A zero that means "not running" and a zero
// that means "used nothing" are not the same fact, and a bill cannot tell
// them apart afterwards.
func TestUsageRefusesToInventAZero(t *testing.T) {
	cgroupRoot := t.TempDir()
	m, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2, CgroupRoot: cgroupRoot})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	if _, err := m.Usage(ctx, "mvm-nobody"); err == nil {
		t.Error("Usage answered for an id this driver does not know")
	}

	h, err := m.Create(ctx, Spec{SessionID: "sess-gone"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// A live session whose cgroup is not there: the VM died, or the jailer
	// never made it. Either way there is nothing to report.
	if _, err := m.Usage(ctx, h.ID); err == nil {
		t.Error("Usage answered for a session with no cgroup on this host")
	}

	// A cold-parked session has no VM at all, and this host is spending
	// nothing on it.
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold Suspend: %v", err)
	}
	_, err = m.Usage(ctx, h.ID)
	if err == nil {
		t.Fatal("Usage answered for a cold-parked session")
	}
	if !strings.Contains(err.Error(), "cold-parked") {
		t.Fatalf("error = %q, want it to say the session is parked", err)
	}
}

// TestUsageRejectsACgroupItCannotRead. A truncated cpu.stat or a
// memory.current reading "max" must be an error, not a session that used
// nothing.
func TestUsageRejectsACgroupItCannotRead(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{
			name:  "no cpu.stat",
			files: map[string]string{"memory.current": "1\n"},
			want:  "cpu.stat",
		},
		{
			name:  "cpu.stat with no usage_usec",
			files: map[string]string{"cpu.stat": "nr_periods 0\n", "memory.current": "1\n"},
			want:  "usage_usec",
		},
		{
			name:  "no memory.current",
			files: map[string]string{"cpu.stat": sampleCPUStat},
			want:  "memory.current",
		},
		{
			name:  "memory.current is not a count",
			files: map[string]string{"cpu.stat": sampleCPUStat, "memory.current": "max\n"},
			want:  "not a byte count",
		},
		{
			name:  "memory.peak is not a count",
			files: map[string]string{"cpu.stat": sampleCPUStat, "memory.current": "1\n", "memory.peak": "max\n"},
			want:  "not a byte count",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := fakeCgroup(t, root, "parent", tc.name, tc.files)
			_, err := readCgroupUsage(dir)
			if err == nil {
				t.Fatalf("readCgroupUsage accepted %v", tc.files)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name %q", err, tc.want)
			}
		})
	}
}

// TestCgroupStatWithUnknownKeysIsStillReadable: the kernel adds keys, and a
// reading must not break because a host is newer than this driver.
func TestCgroupStatWithUnknownKeysIsStillReadable(t *testing.T) {
	dir := fakeCgroup(t, t.TempDir(), "parent", "vm", map[string]string{
		"cpu.stat":       sampleCPUStat + "something_new not-a-number\nburst_usec 42\n",
		"memory.current": "7\n",
	})
	usage, err := readCgroupUsage(dir)
	if err != nil {
		t.Fatalf("readCgroupUsage: %v", err)
	}
	if usage.CPUUsageUsec != 123456789 {
		t.Fatalf("usage_usec = %d", usage.CPUUsageUsec)
	}
}

// TestMeteringIsOptionalAndDockerHasNone. The interface is optional so that
// adding it does not make every driver — and every fake the contract suite
// runs — carry a stub.
func TestMeteringIsOptionalAndDockerHasNone(t *testing.T) {
	var d Driver = NewDocker(DockerOpts{})
	if _, ok := d.(MeteringDriver); ok {
		t.Error("the docker driver claims to meter; it measures nothing and would report zeroes")
	}
	m, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 1})
	var mv Driver = m
	if _, ok := mv.(MeteringDriver); !ok {
		t.Error("the microvm driver does not implement MeteringDriver")
	}
}
