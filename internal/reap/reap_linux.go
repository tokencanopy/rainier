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
	codes   = map[int]Status{} // pid -> outcome, for pids the reaper has reaped
	order   []int              // insertion order of `codes`, for the bound below
	started bool
)

// maxUnclaimed bounds how many reaped outcomes may sit waiting for a caller
// that is never going to ask.
//
// Most of what this process reaps IS claimed: the agent's own child and every
// exec go through AwaitStatus, which deletes its entry. What is not claimed
// is the orphans — PR_SET_CHILD_SUBREAPER makes this process the parent of
// every grandchild whose own parent exited, and `git fetch` spawning
// git-remote-https is the ordinary shape of that, not a corner. A session
// outlives everything else in this system (spec §10), so an unbounded map of
// them is a leak measured in weeks.
//
// The oldest are dropped first, which is the safe direction: a waiter is
// normally already waiting when its child exits, so an entry that has sat
// through a thousand later exits is one nobody is coming for. A dropped entry
// that somebody IS waiting for leaves that waiter blocked in AwaitStatus,
// which is why the bound is far above any real concurrency — the cap is eight
// execs plus the agent.
const maxUnclaimed = 4096

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
				record(pid, statusOf(ws))
				cond.Broadcast()
				mu.Unlock()
			}
		}
	}()
}

// record keeps one reaped outcome, dropping the oldest when the table is at
// its bound. Callers hold mu.
func record(pid int, st Status) {
	if _, dup := codes[pid]; !dup {
		order = append(order, pid)
	}
	codes[pid] = st
	for len(order) > maxUnclaimed {
		oldest := order[0]
		order = order[1:]
		delete(codes, oldest)
	}
}

// forget removes a claimed outcome and its place in the order. Callers hold
// mu.
func forget(pid int) {
	delete(codes, pid)
	for i, p := range order {
		if p == pid {
			order = append(order[:i], order[i+1:]...)
			break
		}
	}
}

// statusOf reads a wait status into the portable outcome. A process killed by
// a signal has NO exit status — ExitStatus() on such a status is meaningless
// — which is why the two facts are separate fields rather than one integer
// with reserved values: `rainier exec` reports 128+N for a signal and the
// command's own code otherwise, and a command may legitimately exit 137.
func statusOf(ws syscall.WaitStatus) Status {
	if ws.Signaled() {
		return Status{Signal: ws.Signal()}
	}
	return Status{Code: ws.ExitStatus()}
}

// AwaitStatus blocks until the reaper has reaped pid, returning its whole
// outcome — the exit code, or the signal that killed it. Only meaningful
// after Start(); if the reaper is not running it returns (Status{}, false)
// and the caller falls back to its own wait.
func AwaitStatus(pid int) (Status, bool) {
	mu.Lock()
	defer mu.Unlock()
	if !started {
		return Status{}, false
	}
	for {
		if st, ok := codes[pid]; ok {
			forget(pid)
			return st, true
		}
		cond.Wait()
	}
}

// AwaitExit is AwaitStatus reporting the exit code alone, which is what the
// session's own agent needs: a signalled agent reports -1, exactly as
// cmd.Wait would have.
func AwaitExit(pid int) (int, bool) {
	st, ok := AwaitStatus(pid)
	if !ok {
		return 0, false
	}
	return st.ExitCode(), true
}

func setChildSubreaper() error {
	_, _, errno := syscall.Syscall(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
