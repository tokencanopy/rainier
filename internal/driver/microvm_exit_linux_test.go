//go:build linux

package driver

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

// A dead group leader is not proof that the other VMM threads have exited.
func TestVMMExitRequiresEntireThreadGroup(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("native pthread regression requires a C compiler")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "leader.c")
	binary := filepath.Join(dir, "firecracker")
	program := `#include <pthread.h>
#include <unistd.h>
static void *worker(void *unused) { for (;;) pause(); return 0; }
int main(void) { pthread_t thread; if (pthread_create(&thread, 0, worker, 0)) return 2; pthread_exit(0); }
`
	if err := os.WriteFile(source, []byte(program), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(cc, "-pthread", source, "-o", binary).CombinedOutput(); err != nil {
		t.Fatalf("compile pthread witness: %v %s", err, out)
	}
	child := exec.Command(binary, "--id", "mvm-threads")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	pid := child.Process.Pid
	deadline := time.Now().Add(3 * time.Second)
	for {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(strings.SplitN(string(data), ") ", 2)[1])
		if fields[0] == "Z" && fields[17] == "2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader did not exit with its worker still alive")
		}
		time.Sleep(time.Millisecond)
	}
	birth, err := processStartTime(pid)
	if err != nil {
		t.Fatal(err)
	}
	if processExited(pid, birth) {
		t.Fatal("zombie leader granted teardown while a worker remained alive")
	}
	fc := NewFirecrackerEngine(FirecrackerOpts{StateDir: dir})
	if err := atomicMetadata(fc.launchMarkerPath("mvm-threads"), []byte(hostLaunchBoot())); err != nil {
		t.Fatal(err)
	}
	if err := fc.saveProcessIdentity("mvm-threads", pid); err != nil {
		t.Fatal(err)
	}
	if gone, _ := fc.launchEvidence("mvm-threads", pid); gone {
		t.Fatal("live thread group accepted as gone")
	}
}
