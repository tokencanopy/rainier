//go:build linux

package driver

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type exitWaitSignaller struct{ calls int }

func (s *exitWaitSignaller) Getpgid(pid int) (int, error) { return syscall.Getpgid(pid) }
func (s *exitWaitSignaller) Kill(pid int, signal syscall.Signal) error {
	s.calls++
	if err := syscall.Kill(pid, signal); err != nil {
		return err
	}
	if pid < 0 {
		pid = -pid
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if e == nil && strings.Fields(strings.SplitN(string(b), ") ", 2)[1])[0] == "Z" {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("synthetic child did not reach zombie state")
}
func TestRecoveredVMMStopAcceptsOriginalExitedProcess(t *testing.T) {
	if os.Getenv("RAINIER_SYNTHETIC_EXIT_CHILD") == "1" {
		fmt.Println("ready")
		for {
			time.Sleep(time.Hour)
		}
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	child := exec.Command(exe, "-test.run=^TestRecoveredVMMStopAcceptsOriginalExitedProcess$", "--", "--id", "mvm-exit-probe")
	child.Args[0] = "firecracker"
	child.Env = append(os.Environ(), "RAINIER_SYNTHETIC_EXIT_CHILD=1")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	fc := NewFirecrackerEngine(FirecrackerOpts{StateDir: t.TempDir()})
	id := "mvm-exit-probe"
	pid := child.Process.Pid
	if e = atomicMetadata(fc.launchMarkerPath(id), []byte(hostLaunchBoot())); e != nil {
		t.Fatal(e)
	}
	if e = fc.saveProcessIdentity(id, pid); e != nil {
		t.Fatal(e)
	}
	if e = atomicMetadata(fc.pidFilePath(id), []byte(strconv.Itoa(pid))); e != nil {
		t.Fatal(e)
	}
	sig := &exitWaitSignaller{}
	fc.signals = sig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = fc.Stop(ctx, id); e != nil {
		t.Fatalf("Stop after authentic child exits: %v (signals=%d)", e, sig.calls)
	}
	if sig.calls != 1 {
		t.Fatalf("signals=%d", sig.calls)
	}
}

func TestVMMStopRecognizesAlreadyExitedOriginalProcess(t *testing.T) {
	for _, tracked := range []bool{false, true} {
		t.Run(fmt.Sprintf("tracked=%v", tracked), func(t *testing.T) {
			fc, _, _ := jailTestEngine(t, &fakeStarter{})
			fc.startTime = processStartTime
			id := "mvm-exited"
			proc, pid := fakeFirecracker(t, id)
			defer func() { _ = proc.Kill(); _ = proc.Wait() }()
			if err := atomicMetadata(fc.launchMarkerPath(id), []byte(hostLaunchBoot())); err != nil {
				t.Fatal(err)
			}
			if err := fc.saveProcessIdentity(id, pid); err != nil {
				t.Fatal(err)
			}
			if err := atomicMetadata(fc.pidFilePath(id), []byte(strconv.Itoa(pid))); err != nil {
				t.Fatal(err)
			}
			exit := &exitWaitSignaller{}
			if err := exit.Kill(pid, syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			// The process is genuinely exited but cannot be reaped until we Wait it.
			// A different birth must still be rejected even when that PID is a zombie.
			actual := fc.startTime
			fc.startTime = func(int) (uint64, error) { v, e := actual(pid); return v + 1, e }
			if gone, err := fc.launchEvidence(id, pid); err == nil || gone {
				t.Fatal("mismatched lifetime accepted")
			}
			fc.startTime = actual
			if tracked {
				fc.procs[id] = proc
			}
			signals := &fakeSignaller{}
			fc.signals = signals
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := fc.Stop(ctx, id); err != nil {
				t.Fatal(err)
			}
			if len(signals.sent) != 0 {
				t.Fatal("exited process received another signal")
			}
			if tracked && syscall.Kill(pid, 0) == nil {
				t.Fatal("tracked child was not reaped")
			}
		})
	}
}
