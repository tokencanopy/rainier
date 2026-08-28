//go:build linux

// Package reap installs a SIGCHLD-driven loop that wait()s children
// reparented to this process (PID 1 in the sandbox), and is the single
// authoritative waiter for every child on Linux so no other caller races it
// with its own wait4/cmd.Wait.
package reap

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const prSetChildSubreaper = 36

var (
	mu      sync.Mutex
	cond    = sync.NewCond(&mu)
	codes   = map[int]int{} // pid -> exit code, for pids the reaper has reaped
	started bool
)

// Start installs the SIGCHLD reaper. Safe to call once. After Start, AwaitExit
// returns the reaped exit code for a given child pid (blocking until reaped).
func Start() {
	mu.Lock()
	if started {
		mu.Unlock()
		return
	}
	started = true
	mu.Unlock()

	if err := setChildSubreaper(); err != nil {
		log.Printf("reap: PR_SET_CHILD_SUBREAPER failed (orphan reaping may be limited): %v", err)
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGCHLD)
	go func() {
		for range sigs {
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if err == syscall.EINTR {
					continue
				} // retry on interrupt
				if pid <= 0 || err != nil {
					break
				} // no more reapable now
				mu.Lock()
				codes[pid] = ws.ExitStatus()
				cond.Broadcast()
				mu.Unlock()
			}
		}
	}()
}

// AwaitExit blocks until the reaper has reaped pid, returning (code, true).
// Only meaningful after Start(); if the reaper is not running it returns (0,false).
func AwaitExit(pid int) (int, bool) {
	mu.Lock()
	defer mu.Unlock()
	if !started {
		return 0, false
	}
	for {
		if c, ok := codes[pid]; ok {
			delete(codes, pid)
			return c, true
		}
		cond.Wait()
	}
}

func setChildSubreaper() error {
	_, _, errno := syscall.Syscall(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
