// internal/driver/microvm_metering.go
//
// Host-side metering (ADR-0003 §4.6): CPU and memory measured from OUTSIDE
// the guest, from the cgroup the jailer created for its VMM.
//
// Outside, and that is the whole point. The bakeoff's metering-integrity gate
// is about facts a tenant cannot influence, and anything the guest reports
// about itself is a number an untrusted agent process wrote. The cgroup is
// the host's own accounting of the VMM process: it counts the hypervisor, its
// vCPU threads and its I/O, which is what the host actually spent on that
// session.
//
// Nothing here emits a usage fact. Emitting is runnerd's job — ADR-0003 §4.6
// has it going out over protocol/runner.FromRunner, tagged with the execution
// generation so stale generations are fenced — and that lands with the usage
// work. This is the read, behind an interface the runner can ask for.
package driver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Usage is one reading of what a session has cost this host.
//
// Every field is cumulative-since-boot or instantaneous, never a rate: a rate
// is a difference between two readings taken at known times, and the
// component that decides what "known" means is the one that bills. Taken is
// the host's clock at the moment of the read, so the caller can compute that
// difference without having to have timed the call itself.
type Usage struct {
	// InstanceID is the driver's session record, so a reading can never be
	// filed against the wrong session.
	InstanceID string
	SessionID  string

	// CPUUsageUsec is cpu.stat's usage_usec: total CPU time consumed by
	// everything in the VM's cgroup since it was created. User and System
	// are its two halves, when the kernel reports them.
	CPUUsageUsec  uint64
	CPUUserUsec   uint64
	CPUSystemUsec uint64

	// MemoryCurrentBytes is memory.current: what the cgroup is using right
	// now. MemoryPeakBytes is memory.peak, the high-water mark — which is
	// the number a GiB-second bill wants and which older kernels do not
	// have, so HasPeak says whether it was there rather than reporting zero
	// as though the VM never used any memory.
	MemoryCurrentBytes uint64
	MemoryPeakBytes    uint64
	HasPeak            bool

	Taken time.Time
}

// MeteringDriver is a driver that can report what a session has cost the
// host.
//
// It is an optional interface rather than a method on Driver for the reason
// CapabilityDriver is: most drivers have nothing to say. The Docker driver
// measures nothing today and adding a method to Driver would make every
// implementation carry a stub, including the fakes the contract suite runs.
// A runner asks for this one with a type assertion and does without when it
// is not there.
type MeteringDriver interface {
	// Usage reads the host's accounting for one session. An id this driver
	// does not know, or a session with no live VM, is an error rather than a
	// zero reading: a zero that means "not running" and a zero that means
	// "used nothing" are not the same fact, and a bill cannot tell them
	// apart afterwards.
	Usage(ctx context.Context, id string) (Usage, error)
}

var _ MeteringDriver = (*Microvm)(nil)

// Usage reads the cgroup the jailer made for this session's VMM.
func (m *Microvm) Usage(_ context.Context, id string) (Usage, error) {
	m.mu.Lock()
	inst, ok := m.instances[id]
	var (
		sessionID string
		cgroup    string
		state     State
		cold      bool
	)
	if ok {
		sessionID, cgroup, state, cold = inst.SessionID, inst.Cfg.CgroupPath, inst.State, inst.Cold
	}
	m.mu.Unlock()

	if !ok {
		return Usage{}, fmt.Errorf("microvm usage: no such id %s", id)
	}
	if state == StateSuspended && cold {
		return Usage{}, fmt.Errorf("microvm usage: %s is cold-parked and has no VM, so this host is spending nothing on it; a zero reading here would be indistinguishable from a running session that used nothing", id)
	}
	if cgroup == "" {
		return Usage{}, fmt.Errorf("microvm usage: %s has no cgroup recorded; it was launched by an engine that does not jail", id)
	}

	usage, err := readCgroupUsage(cgroup)
	if err != nil {
		return Usage{}, err
	}
	usage.InstanceID = id
	usage.SessionID = sessionID
	return usage, nil
}

// readCgroupUsage reads one cgroup v2 directory.
//
// cpu.stat and memory.current are required; memory.peak is not, because it
// arrived in 6.8 and a host on an older kernel is a host, not a
// misconfiguration. Everything else in those files is ignored rather than
// rejected: the kernel adds keys.
func readCgroupUsage(dir string) (Usage, error) {
	cpu, err := readCgroupKeyed(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return Usage{}, fmt.Errorf("microvm usage: read cpu.stat: %w", err)
	}
	usage := Usage{
		CPUUsageUsec:  cpu["usage_usec"],
		CPUUserUsec:   cpu["user_usec"],
		CPUSystemUsec: cpu["system_usec"],
		Taken:         time.Now(),
	}

	current, err := readCgroupValue(filepath.Join(dir, "memory.current"))
	if err != nil {
		return Usage{}, fmt.Errorf("microvm usage: read memory.current: %w", err)
	}
	usage.MemoryCurrentBytes = current

	if peak, err := readCgroupValue(filepath.Join(dir, "memory.peak")); err == nil {
		usage.MemoryPeakBytes, usage.HasPeak = peak, true
	} else if !errors.Is(err, os.ErrNotExist) {
		return Usage{}, fmt.Errorf("microvm usage: read memory.peak: %w", err)
	}

	return usage, nil
}

// readCgroupValue reads a single-value cgroup file.
//
// "max" is a real value in several of these files and is not a number. It is
// not one this function is ever asked for — memory.current and memory.peak
// are always counts — so it is an error rather than a silent zero, which
// would be a session that used no memory.
func readCgroupValue(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	text := strings.TrimSpace(string(data))
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s contains %q, which is not a byte count", path, text)
	}
	return n, nil
}

// readCgroupKeyed reads a "key value" cgroup file such as cpu.stat.
func readCgroupKeyed(path string) (map[string]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := map[string]uint64{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, value, ok := strings.Cut(strings.TrimSpace(scanner.Text()), " ")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			// A key this driver does not read, carrying something it does
			// not understand, is not a reason to fail a whole reading.
			continue
		}
		out[key] = n
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if _, ok := out["usage_usec"]; !ok {
		return nil, fmt.Errorf("%s has no usage_usec line", path)
	}
	return out, nil
}
