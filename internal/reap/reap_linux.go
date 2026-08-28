//go:build linux

// Package reap installs a SIGCHLD-driven loop that wait()s orphaned
// grandchildren reparented to this process (PID 1 in the sandbox).
package reap

import (
	"os"
	"os/signal"
	"syscall"
)

// Reap installs a SIGCHLD-driven loop that wait()s orphaned grandchildren
// reparented to this process (PID 1 in the sandbox). directChild is the pid
// whose status the caller wants; Reap delivers that status on the returned
// channel exactly once and reaps all other children silently. On non-Linux
// it is a no-op and the channel never fires (host tests still build).
func Reap(directChild int) <-chan int {
	// Become a subreaper so orphaned grandchildren reparent here even if we
	// aren't literally PID 1 (belt-and-suspenders; PID 1 is already a reaper).
	_ = unixPrSetChildSubreaper()
	out := make(chan int, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGCHLD)
	go func() {
		for range sigs {
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if pid <= 0 || err != nil {
					break
				} // no more reapable children right now
				if pid == directChild {
					select {
					case out <- ws.ExitStatus():
					default:
					}
				}
			}
		}
	}()
	return out
}

func unixPrSetChildSubreaper() error {
	// PR_SET_CHILD_SUBREAPER = 36
	_, _, errno := syscall.Syscall(syscall.SYS_PRCTL, 36, 1, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
