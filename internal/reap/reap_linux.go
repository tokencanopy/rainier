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
	mu    sync.Mutex
	cond  = sync.NewCond(&mu)
	codes = map[int]record{} // pid -> outcome, for pids the reaper has reaped
	order []int              // insertion order of `codes`, for the bound below
	// tombs is the eviction TOMBSTONE table: a pid whose outcome the bound
	// below dropped, with the sequence that outcome carried. Without it a
	// waiter whose entry was evicted loops on cond.Wait() forever and its
	// caller never reports a status — which before exec was a pid awaited
	// once at boot and is now every exec, for a session's life.
	tombs      = map[int]uint64{}
	tombsOrder []int
	// seq is the monotonic stamp every record carries, and the thing a caller
	// takes a Mark of immediately before fork. pids wrap (pid_max is 32768 by
	// default), so a fresh child can land on a pid an UNCLAIMED orphan entry
	// still holds; without the stamp its waiter reads that orphan's status as
	// its own and never corrects it.
	seq     uint64
	started bool
)

// record is one reaped outcome and when it was reaped, relative to the marks
// callers take.
type record struct {
	st  Status
	seq uint64
}

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
// that somebody IS waiting for leaves a TOMBSTONE, so that waiter is told "no
// status here" and falls back to its own cmd.Wait rather than parking forever;
// the bound is still far above any real concurrency — the cap is eight execs
// plus the agent — so the tombstone is the safety net and not the mechanism.
//
// The tombstone table is bounded by the same constant, so the guarantee is
// precise rather than absolute: a waiter parks forever only if its child's
// TOMBSTONE is itself evicted, which takes another maxUnclaimed reaps between
// the child exiting and the waiter looking. In practice the waiter is already
// parked before the child exits and is woken by record's own Broadcast, so it
// finds the entry or the tombstone on the first pass. A pid that is never
// reaped at all still parks, and always did — that is a caller waiting on
// something that is not its child.
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
				seq++
				remember(pid, record{st: statusOf(ws), seq: seq})
				cond.Broadcast()
				mu.Unlock()
			}
		}
	}()
}

// remember keeps one reaped outcome, dropping the oldest — and leaving a
// tombstone for it — when the table is at its bound. Callers hold mu.
func remember(pid int, r record) {
	if _, dup := codes[pid]; !dup {
		order = append(order, pid)
	}
	// A pid being recorded again is a wraparound onto an unclaimed entry. Its
	// tombstone, if it has one, is older still and must not outlive it.
	forgetTomb(pid)
	codes[pid] = r
	for len(order) > maxUnclaimed {
		oldest := order[0]
		order = order[1:]
		evicted := codes[oldest]
		delete(codes, oldest)
		entomb(oldest, evicted.seq)
	}
}

// entomb marks a pid's outcome as dropped rather than never-reaped, under the
// same bound and for the same reason: a tombstone nobody comes for is itself
// an entry that would grow without limit.
func entomb(pid int, seq uint64) {
	if _, dup := tombs[pid]; !dup {
		tombsOrder = append(tombsOrder, pid)
	}
	tombs[pid] = seq
	for len(tombsOrder) > maxUnclaimed {
		oldest := tombsOrder[0]
		tombsOrder = tombsOrder[1:]
		delete(tombs, oldest)
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

// forgetTomb removes a tombstone and its place in the order. Callers hold mu.
func forgetTomb(pid int) {
	if _, ok := tombs[pid]; !ok {
		return
	}
	delete(tombs, pid)
	for i, p := range tombsOrder {
		if p == pid {
			tombsOrder = append(tombsOrder[:i], tombsOrder[i+1:]...)
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

// Mark is the sequence this table is at right now. A caller takes one
// IMMEDIATELY BEFORE it forks and hands it back to AwaitStatus, which then
// ignores any outcome recorded before that instant.
//
// It is the answer to pid reuse. pid_max is 32768 by default, so a long-lived
// session spawning a command per CI step wraps; an unclaimed orphan entry —
// `git fetch`'s git-remote-https is the ordinary shape of one — can still hold
// the pid the next fork gets, and a waiter with no mark would read that
// orphan's status as its own and never correct it. An entry that existed
// before this child did cannot be this child's, and a monotonic counter is the
// cheapest way to say so.
//
// Zero is a legal mark and means "anything": it is what a caller that cannot
// take one passes, and it restores the old behaviour exactly. The counter is a
// uint64 incremented once per reaped child, so a wraparound — after which a
// mark would be larger than every later sequence — is 1.8e19 reaps away, which
// is several hundred thousand years at a million a second.
func Mark() uint64 {
	mu.Lock()
	defer mu.Unlock()
	return seq
}

// AwaitStatus blocks until the reaper has reaped pid, returning its whole
// outcome — the exit code, or the signal that killed it.
//
// since is a Mark taken before the fork; an outcome recorded at or before it
// belongs to some earlier holder of this pid and is discarded rather than
// returned.
//
// It reports false in three cases, and the caller's answer to all three is the
// same — fall back to its own cmd.Wait: the reaper is not running (a host
// build, a test), the reaper is running but this outcome was EVICTED by the
// table's bound, or the caller passed a mark no record can satisfy. Before the
// tombstone the second case simply never returned.
func AwaitStatus(pid int, since uint64) (Status, bool) {
	mu.Lock()
	defer mu.Unlock()
	if !started {
		return Status{}, false
	}
	for {
		if r, ok := codes[pid]; ok {
			forget(pid)
			if r.seq > since {
				return r.st, true
			}
			// Some earlier holder of this pid. Dropped, and the wait goes on
			// for the outcome this caller is actually waiting for.
			continue
		}
		if tombSeq, ok := tombs[pid]; ok {
			// Named tombSeq, not seq: the package-level counter is called that,
			// and shadowing it inside the function that is ABOUT the sequence
			// is a rename waiting to bite.
			forgetTomb(pid)
			if tombSeq > since {
				// This child's outcome existed and was evicted. Saying so is
				// what lets cmd.Wait take over instead of this parking
				// forever — which held one of the eight exec slots for the
				// life of the session.
				return Status{}, false
			}
			continue
		}
		cond.Wait()
	}
}

// AwaitExit is AwaitStatus reporting the exit code alone, which is what the
// session's own agent needs: a signalled agent reports -1, exactly as
// cmd.Wait would have.
func AwaitExit(pid int, since uint64) (int, bool) {
	st, ok := AwaitStatus(pid, since)
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
