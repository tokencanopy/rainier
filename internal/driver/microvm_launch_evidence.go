package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// atomicMetadata publishes a complete, durable version in the same directory.
func atomicMetadata(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, microvmDirMode); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".metadata-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err = tmp.Chmod(microvmFileMode); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

func (f *FirecrackerEngine) launchMarkerPath(id string) string {
	return filepath.Join(f.stateDir, "instances", id, "launch.pending")
}
func hostLaunchBoot() string {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || len(strings.TrimSpace(string(data))) == 0 {
		return "unknown"
	}
	return strings.TrimSpace(string(data))
}

// launchEvidence distinguishes missing bookkeeping from process exit. A marker
// is durable before Start, so a missing PID on the same host boot is ambiguous.
// Preserve the jail/resources for host inspection; a proven host reboot ends
// that process lifetime and permits cleanup. No /proc scan or empty cgroup can
// prove absence while a jailer might still be entering its namespace/cgroup.
func (f *FirecrackerEngine) launchEvidence(id string, pid int) (gone bool, err error) {
	data, err := os.ReadFile(f.launchMarkerPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.New("microvm: cannot read launch evidence")
	}
	boot := strings.TrimSpace(string(data))
	current := hostLaunchBoot()
	if boot != "" && boot != "unknown" && current != "unknown" && boot != current {
		return true, nil
	}
	if pid <= 0 {
		return false, errors.New("microvm: launch lacks process evidence; retain resources for host inspection")
	}
	var birth launchProcessIdentity
	data, err = os.ReadFile(f.processIdentityPath(id))
	if err != nil || json.Unmarshal(data, &birth) != nil || birth.PID != pid || birth.StartTime == 0 {
		return false, errors.New("microvm: launch process lifetime is unknown; retain resources")
	}
	if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return true, nil
	}
	start, err := f.startTime(pid)
	if err != nil || start != birth.StartTime {
		return false, errors.New("microvm: process lifetime no longer matches launch")
	}

	if !isFirecrackerPID(pid, id) {
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return true, nil
		}
		return false, errors.New("microvm: launch process identity is uncertain; retain resources")
	}
	return false, nil
}

// Wait is started once per child and remains usable by later teardown attempts.
func (f *FirecrackerEngine) waitChild(id string, proc vmmProcess) chan error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.waits == nil {
		f.waits = map[string]chan error{}
	}
	if c := f.waits[id]; c != nil {
		return c
	}
	c := make(chan error, 1)
	f.waits[id] = c
	go func() { c <- proc.Wait(); close(c) }()
	return c
}
func (f *FirecrackerEngine) forgetExited(id string) {
	f.mu.Lock()
	delete(f.procs, id)
	delete(f.waits, id)
	f.mu.Unlock()
}
func (f *FirecrackerEngine) removeLaunchEvidence(id string) error {
	for _, path := range []string{f.pidFilePath(id), f.processIdentityPath(id), f.launchMarkerPath(id)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	dir := filepath.Dir(f.pidFilePath(id))
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return syncDir(dir)
}

// finishFailedLaunch may only remove the jail after its own child has exited.
func (f *FirecrackerEngine) finishFailedLaunch(id string, proc vmmProcess) {
	ctx, cancel := context.WithTimeout(context.Background(), firecrackerKillTimeout)
	defer cancel()
	_ = proc.Kill()
	if !awaitExit(ctx, f.waitChild(id, proc), proc.Pid(), firecrackerKillTimeout) {
		return
	}
	f.forgetExited(id)
	_ = f.removeJail(id)
	_ = f.removeLaunchEvidence(id)
}

// The birth timestamp distinguishes a reused PID even when the next process
// carries the same exact instance ID. The launch marker supplies host boot scope.
type launchProcessIdentity struct {
	PID       int    `json:"pid"`
	StartTime uint64 `json:"start_time"`
}

func (f *FirecrackerEngine) processIdentityPath(id string) string {
	return filepath.Join(f.stateDir, "instances", id, "process.json")
}
func (f *FirecrackerEngine) saveProcessIdentity(id string, pid int) error {
	start, err := f.startTime(pid)
	if err != nil || start == 0 {
		return errors.New("microvm: cannot establish child birth identity")
	}
	data, err := json.Marshal(launchProcessIdentity{PID: pid, StartTime: start})
	if err != nil {
		return err
	}
	return atomicMetadata(f.processIdentityPath(id), data)
}

// Every signal requires current authority, including escalation after waiting.
// A changed or unreadable lifetime retains the jail for later reconciliation.
func (f *FirecrackerEngine) signalVM(id string, pid int, signal syscall.Signal) error {
	gone, err := f.launchEvidence(id, pid)
	if err != nil {
		return err
	}
	if gone {
		return nil
	}
	if !isFirecrackerPID(pid, id) {
		return errors.New("microvm: signal target identity is uncertain")
	}
	return killProcessTree(f.signals, pid, signal)
}

// Recovered VMs have no child Wait handle. Observe their durable lifetime on
// every poll so a replacement PID cannot extend the original VM's wait.
func (f *FirecrackerEngine) awaitVMExit(ctx context.Context, id string, waited chan error, pid int, timeout time.Duration) bool {
	if waited != nil {
		return awaitExit(ctx, waited, pid, timeout)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		gone, err := f.launchEvidence(id, pid)
		if err != nil {
			return false
		}
		if gone || errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return true
		}
		select {
		case <-poll.C:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}
