//go:build linux

// internal/reap/reap_linux_test.go — the tests that reach into this package's
// own table.
//
// They live in a linux-tagged file because every symbol they touch (mu, cond,
// seq, remember, record, tombs, maxUnclaimed) is declared in reap_linux.go.
// reap_test.go has no tag, so putting them there broke `GOOS=darwin go test
// ./...` outright — in the one package whose reap_other.go exists so that
// "host (macOS) builds and tests still compile", and in a change that treats
// macOS as a first-class maintainer platform elsewhere (the EvalSymlinks fixes
// are justified by "t.TempDir on a mac"). A build failure is not a skip.
package reap

import (
	"runtime"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// the bound's two failure modes
//
// These are linux-only and they are executed, not reasoned about: the bounded
// table came in with `rainier exec`, and exec is what makes every pid awaited
// — before it, exactly one pid (the agent's) was ever awaited, once, at boot.
//
// They reach into the package's own state deliberately. Reproducing an
// eviction through real processes means forking four thousand of them, and
// reproducing a pid wraparound means exhausting pid_max; neither is a test, and
// both would be testing the kernel rather than this table.
// ---------------------------------------------------------------------------

// synthPid is far above pid_max, so nothing these tests put in the table can
// ever collide with a real child another test in this package is waiting for.
const synthPid = 1 << 20

// fillTable records n outcomes under mu, which is what pushes the oldest
// entries out through the bound.
func fillTable(base, n int) {
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		seq++
		remember(base+i, record{st: Status{Code: 1}, seq: seq})
	}
	cond.Broadcast()
}

// TestAwaitStatusGivesUpOnAnEvictedOutcome is the tombstone. Before it,
// AwaitStatus looped on cond.Wait() with no escape, so a waiter whose entry
// the bound evicted parked forever — and sandboxexec has no fallback for
// "never returns", so that exec never reported a status and held one of the
// session's eight slots for as long as the session lived.
func TestAwaitStatusGivesUpOnAnEvictedOutcome(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reaping is linux-only")
	}
	Start()
	pid := synthPid + 1
	mark := Mark()

	done := make(chan bool, 1)
	go func() { _, ok := AwaitStatus(pid, mark); done <- ok }()
	// Let the waiter park, so this exercises the case the comment in
	// reap_linux.go admitted to rather than only the tombstone lookup.
	time.Sleep(50 * time.Millisecond)

	// Its outcome arrives and is pushed straight back out, in one locked step
	// so the waiter cannot claim it in between.
	mu.Lock()
	seq++
	remember(pid, record{st: Status{Code: 3}, seq: seq})
	mu.Unlock()
	fillTable(synthPid+1000, maxUnclaimed)

	select {
	case ok := <-done:
		if ok {
			t.Fatal("an evicted outcome was reported as a status")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AwaitStatus parked forever on an evicted outcome")
	}
}

// TestAStaleOutcomeIsNotThisChildsStatus is the pid wraparound. pid_max is
// 32768 by default and a session spawning a command per CI step wraps, so a
// fresh exec can land on the pid an UNCLAIMED orphan entry still holds —
// `git fetch` spawning git-remote-https is the ordinary shape of one. Without
// the mark the waiter reads that orphan's status as its own and never
// corrects it: exec.go sets p.status from it and reports it to the caller.
func TestAStaleOutcomeIsNotThisChildsStatus(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reaping is linux-only")
	}
	Start()
	pid := synthPid + 2

	// An orphan nobody claimed, exited before this child was forked.
	mu.Lock()
	seq++
	remember(pid, record{st: Status{Code: 42}, seq: seq})
	cond.Broadcast()
	mu.Unlock()

	mark := Mark() // the fork happens HERE

	go func() {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		seq++
		remember(pid, record{st: Status{Code: 7}, seq: seq})
		cond.Broadcast()
		mu.Unlock()
	}()

	done := make(chan Status, 1)
	go func() {
		st, _ := AwaitStatus(pid, mark)
		done <- st
	}()
	select {
	case st := <-done:
		if st.Code != 7 {
			t.Fatalf("AwaitStatus reported %d, want this child's own 7 — "+
				"an outcome recorded before the fork is some earlier holder's", st.Code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AwaitStatus never reported this child's outcome")
	}
}

// TestAMarkOfZeroIsTheOldBehaviour: a caller that cannot take a mark still
// gets the first outcome the table has for its pid, which is what every
// caller did before Mark existed.
func TestAMarkOfZeroIsTheOldBehaviour(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reaping is linux-only")
	}
	Start()
	pid := synthPid + 3
	mu.Lock()
	seq++
	remember(pid, record{st: Status{Code: 9}, seq: seq})
	cond.Broadcast()
	mu.Unlock()

	st, ok := AwaitStatus(pid, 0)
	if !ok || st.Code != 9 {
		t.Fatalf("AwaitStatus(pid, 0) = (%+v, %v), want (Status{Code:9}, true)", st, ok)
	}
}

// TestAClaimedOutcomeIsForgotten keeps the table's own bound meaningful: the
// tombstone must not become a second unbounded map by outliving the record it
// replaced, and a claimed record must not leave one behind at all.
func TestAClaimedOutcomeIsForgotten(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reaping is linux-only")
	}
	Start()
	pid := synthPid + 4
	mu.Lock()
	seq++
	remember(pid, record{st: Status{Code: 2}, seq: seq})
	mu.Unlock()
	if _, ok := AwaitStatus(pid, 0); !ok {
		t.Fatal("a recorded outcome was not delivered")
	}
	mu.Lock()
	_, inCodes := codes[pid]
	_, inTombs := tombs[pid]
	nTombs, nTombsOrder := len(tombs), len(tombsOrder)
	mu.Unlock()
	if inCodes || inTombs {
		t.Fatalf("after claiming: codes=%v tombs=%v, want neither", inCodes, inTombs)
	}
	if nTombs != nTombsOrder {
		t.Fatalf("the tombstone table and its order disagree: %d vs %d", nTombs, nTombsOrder)
	}
	if nTombs > maxUnclaimed {
		t.Fatalf("%d tombstones, above the bound of %d", nTombs, maxUnclaimed)
	}
}
