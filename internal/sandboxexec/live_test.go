package sandboxexec

import (
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// the live-exec report
//
// runnerd's idle auto-stop stops a session whose agent child has exited and
// that has had no attachment for the timeout. An ATTACHED exec holds an
// attachment; a DETACHED one holds nothing, so without this report the 30m
// default cold-stops the session and `docker stop`'s SIGTERM kills
// `rainier exec s --detach -- claude --continue`. See
// docs/design/exec-idle-stop.md.
// ---------------------------------------------------------------------------

// liveRecorder collects what the observer was told, in the order it was told.
type liveRecorder struct {
	mu      sync.Mutex
	reports []liveReport
}

type liveReport struct {
	live int
	seq  uint64
}

func (r *liveRecorder) observe(live int, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, liveReport{live: live, seq: seq})
}

func (r *liveRecorder) all() []liveReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]liveReport(nil), r.reports...)
}

// await polls until at least n reports have landed. The last one comes off the
// goroutine that reaps a detached process, so the alternative is a sleep long
// enough to be flaky.
func (r *liveRecorder) await(t *testing.T, n int) []liveReport {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := r.all(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("only %d live report(s) arrived, want %d: %+v", len(r.all()), n, r.all())
	return nil
}

// TestADetachedExecIsReportedLiveAndThenGone is the fact the runner's idle
// sweep needs and had no way to learn: a detached exec is running, and then it
// is not. The caller hanging up in between changes nothing, which is the whole
// meaning of the flag and precisely why an attachment count cannot answer this
// question.
func TestADetachedExecIsReportedLiveAndThenGone(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	var rec liveRecorder
	r.ObserveLive(rec.observe)

	spec := withTool(t, r, "tool")
	spec.Detach, spec.LogPath = true, "run.log"

	a := r.OpenExec(spec)
	p := start.await(t)
	drainAttachment(t, a)
	if got := rec.await(t, 1); got[0].live != 1 {
		t.Fatalf("a started exec reported live=%d, want 1", got[0].live)
	}

	// The caller goes away. A detached run outlives it, and so does the
	// report: the runner must still see a live command.
	a.Close()
	time.Sleep(50 * time.Millisecond)
	if got := rec.all(); len(got) != 1 {
		t.Fatalf("the caller's disconnect moved a detached exec's live count: %+v", got)
	}

	// The session ends it, which is the bound on its life.
	r.KillAll()
	p.exit(Status{Code: 0})
	got := rec.await(t, 2)
	if got[1].live != 0 {
		t.Fatalf("the last exec ending reported live=%d, want 0", got[1].live)
	}
	if got[1].seq <= got[0].seq {
		t.Fatalf("sequence numbers did not advance: %+v", got)
	}
}

// TestLiveReportsAreSequencedInTheOrderTheCountChanged pins the fence the far
// end refuses stale reports with. The observer is called after the lock is
// released, so two transitions can reach the wire in either order; the
// SEQUENCE is assigned under the lock that changed the count, so it always
// says which of them happened later.
func TestLiveReportsAreSequencedInTheOrderTheCountChanged(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	var rec liveRecorder
	r.ObserveLive(rec.observe)

	spec := withTool(t, r, "tool")
	spec.Detach, spec.LogPath = true, "run.log"

	const n = 3
	procs := make([]*fakeProc, 0, n)
	for i := 0; i < n; i++ {
		drainAttachment(t, r.OpenExec(spec))
		procs = append(procs, start.await(t))
	}
	got := rec.await(t, n)
	for i, rep := range got {
		if rep.live != i+1 {
			t.Fatalf("report %d said live=%d, want %d: %+v", i, rep.live, i+1, got)
		}
		if rep.seq != uint64(i+1) {
			t.Fatalf("report %d carried seq=%d, want %d: %+v", i, rep.seq, i+1, got)
		}
	}

	// And LiveReport restates the current count under a FRESH sequence, which
	// is what sessiond sends on every connection so a report dropped while
	// there was no connection is repaired by the next dial.
	live, seq := r.LiveReport()
	if live != n {
		t.Fatalf("LiveReport said live=%d, want %d", live, n)
	}
	if seq <= got[len(got)-1].seq {
		t.Fatalf("LiveReport reused sequence %d, which is not ahead of %d", seq, got[len(got)-1].seq)
	}
	for _, p := range procs {
		p.exit(Status{Code: 0})
	}
}

// TestARefusedExecReportsNothing: the count only moves when the live set does.
// A refusal took no slot, so reporting one would tell the runner a session is
// busy when nothing is running in it — and the session would then hold its
// slot for the life of the runner.
func TestARefusedExecReportsNothing(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	var rec liveRecorder
	r.ObserveLive(rec.observe)

	// The session is on its way out, so every exec is refused.
	r.KillAll()
	spec := withTool(t, r, "tool")
	drainAttachment(t, r.OpenExec(spec))

	time.Sleep(50 * time.Millisecond)
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("a refused exec reported %+v, want nothing", got)
	}
}

// TestNoObserverIsNoReport: every caller that has no relay to report over —
// the tests in this package, a sessiond in dev mode — installs no observer,
// and the runner behaves exactly as it did before this report existed.
func TestNoObserverIsNoReport(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	spec.Detach, spec.LogPath = true, "run.log"
	drainAttachment(t, r.OpenExec(spec))
	p := start.await(t)
	if r.LiveCount() != 1 {
		t.Fatalf("live count with no observer = %d, want 1", r.LiveCount())
	}
	r.KillAll()
	p.exit(Status{Code: 0})
}
