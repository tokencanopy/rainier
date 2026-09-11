package runnerd

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
)

// ---------------------------------------------------------------------------
// a fake clock and a one-session harness
//
// Idle auto-stop is a decision about thirty minutes, so every test here drives
// a clock rather than waiting on one: the table below is the whole rule, and it
// runs in microseconds.
// ---------------------------------------------------------------------------

// idleEpoch is the tests' fixed starting instant. A synthetic date, and the
// only thing that matters about it is that every duration below is relative to
// it.
var idleEpoch = time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

// fakeClock is the Server's `now` in these tests. It takes a lock because the
// sweep reads it from the goroutine under test while the test advances it,
// which is a real (and -race-detectable) concurrent access in the loop test
// below even though the table drives everything from one goroutine.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: idleEpoch} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = idleEpoch.Add(d)
}

// idleHarness is one runner holding one session, with no websockets and no
// docker: the fake driver below the runner and the real registry, Op and sweep
// above it.
type idleHarness struct {
	t     *testing.T
	clk   *fakeClock
	rd    *Server
	fd    *driver.Fake
	id    string
	stops []string // every id stopped, across every sweep, in order
}

// newIdleHarness creates a runner with one running session whose sandbox the
// fake driver holds.
func newIdleHarness(t *testing.T) *idleHarness {
	t.Helper()
	clk := newFakeClock()
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	rd.now = clk.now // before anything serves: no goroutine of this server's exists yet
	h := &idleHarness{t: t, clk: clk, rd: rd, fd: fd, id: "sess-idle-1"}
	if err := rd.CreateWithID(context.Background(), h.id, driver.Spec{Image: "img.invalid"}, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	return h
}

// handle returns the driver handle the session's sandbox is under.
func (h *idleHarness) handle() string {
	h.t.Helper()
	handle, _, ok := h.rd.reg.opTarget(h.id)
	if !ok {
		h.t.Fatalf("session %s is not in the registry", h.id)
	}
	return handle
}

// state returns the fake container's driver state.
func (h *idleHarness) state() driver.State {
	h.t.Helper()
	hd, err := h.fd.Inspect(context.Background(), h.handle())
	if err != nil {
		h.t.Fatalf("inspect: %v", err)
	}
	return hd.State
}

// used is how many slots the fake driver currently counts as occupied — the
// number that ran out in the incident this change exists for.
func (h *idleHarness) used() int {
	h.t.Helper()
	used, _, err := h.fd.Capacity(context.Background())
	if err != nil {
		h.t.Fatalf("capacity: %v", err)
	}
	return used
}

// The actions a table row can script, in the vocabulary of the things that
// actually happen to a session.
type idleAction int

const (
	childExits     idleAction = iota // sessiond reports child_exited
	viewerAttaches                   // an attachment opens (either front)
	viewerDetaches                   // that attachment closes
	operatorStops                    // `rainier stop`: a cold suspend from controld
	sessionResumes                   // `rainier attach` on a stopped session
	warmSuspends                     // a warm suspend: `docker pause`, slot kept
	sweeps                           // the idle loop looks
)

// idleStep is one scripted action at a moment on the fake clock, expressed as
// a duration since the session was created.
type idleStep struct {
	at  time.Duration
	act idleAction
}

// run plays one step.
func (h *idleHarness) run(idle time.Duration, s idleStep) {
	h.t.Helper()
	h.clk.set(s.at)
	ctx := context.Background()
	switch s.act {
	case childExits:
		// Through the real control-frame path, not the registry accessor: the
		// fact has to survive routeControl to be worth anything.
		h.rd.routeControl(h.id, []byte(`{"kind":"child_exited","rc":0}`))
	case viewerAttaches:
		h.rd.reg.attachStarted(h.id)
	case viewerDetaches:
		h.rd.reg.attachEnded(h.id, h.clk.now())
	case operatorStops:
		if err := h.rd.Op(ctx, h.id, "suspend", false); err != nil {
			h.t.Fatalf("operator stop: %v", err)
		}
	case warmSuspends:
		if err := h.rd.Op(ctx, h.id, "suspend", true); err != nil {
			h.t.Fatalf("warm suspend: %v", err)
		}
	case sessionResumes:
		if err := h.rd.Op(ctx, h.id, "resume", false); err != nil {
			h.t.Fatalf("resume: %v", err)
		}
	case sweeps:
		h.stops = append(h.stops, h.rd.sweepIdle(ctx, idle)...)
	}
}

// ---------------------------------------------------------------------------
// the rule
// ---------------------------------------------------------------------------

// TestIdleStopRule is the whole decision, one row per thing a session can do.
// Every row ends with a sweep unless it scripts its own, and asserts how many
// times the session was stopped by the runner — never how many times it was
// looked at.
func TestIdleStopRule(t *testing.T) {
	const idle = 30 * time.Minute
	tests := []struct {
		name string
		// disabled runs the row with --idle-stop 0, the documented way to
		// turn the behaviour off.
		disabled bool
		steps    []idleStep
		// wantStops is how many times the SWEEP stopped this session. An
		// operator's stop is not one.
		wantStops int
		// wantUsed is the slots the driver still counts after the script:
		// 1 while the sandbox is up or warm-paused, 0 once it is stopped.
		wantUsed int
	}{
		{
			name: "child exited and nobody attached, one second short of the timeout",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 29*time.Minute + 59*time.Second, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1,
		},
		{
			name: "child exited and nobody attached, exactly the timeout",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 30 * time.Minute, act: sweeps},
			},
			wantStops: 1,
			wantUsed:  0,
		},
		{
			name: "stopped once, not once per sweep",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 30 * time.Minute, act: sweeps},
				{at: 31 * time.Minute, act: sweeps},
				{at: 90 * time.Minute, act: sweeps},
			},
			wantStops: 1,
			wantUsed:  0,
		},
		{
			name: "child still running and nobody attached for hours",
			steps: []idleStep{
				{at: 6 * time.Hour, act: sweeps},
				{at: 12 * time.Hour, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1,
		},
		{
			name: "child exited but a viewer is attached",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: viewerAttaches},
				{at: 10 * time.Hour, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1,
		},
		{
			name: "the timer runs from the detach, not from the exit",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: viewerAttaches},
				{at: 2 * time.Hour, act: viewerDetaches},
				// Two hours past the exit, but only twenty-nine minutes past
				// the detach.
				{at: 2*time.Hour + 29*time.Minute, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1,
		},
		{
			name: "and fires a timeout after that detach",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: viewerAttaches},
				{at: 2 * time.Hour, act: viewerDetaches},
				{at: 2*time.Hour + 30*time.Minute, act: sweeps},
			},
			wantStops: 1,
			wantUsed:  0,
		},
		{
			name: "one of two viewers leaving is not a detach",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: viewerAttaches},
				{at: 2 * time.Minute, act: viewerAttaches},
				{at: 3 * time.Minute, act: viewerDetaches},
				{at: 10 * time.Hour, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1,
		},
		{
			name:     "a timeout of zero disables it",
			disabled: true,
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 10 * time.Hour, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1,
		},
		{
			name: "a session the operator already stopped is not stopped again",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: operatorStops},
				{at: 10 * time.Hour, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  0, // the operator's stop released it
		},
		{
			name: "a resumed session whose child exits again is eligible again",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 30 * time.Minute, act: sweeps},
				{at: 40 * time.Minute, act: sessionResumes},
				// The new child is working: no exit, no stop, however long.
				{at: 40*time.Minute + 10*time.Hour, act: sweeps},
				{at: 51 * time.Hour, act: childExits},
				{at: 51*time.Hour + 29*time.Minute, act: sweeps},
				{at: 51*time.Hour + 30*time.Minute, act: sweeps},
			},
			wantStops: 2,
			wantUsed:  0,
		},
		{
			name: "a warm pause and unpause does not forget the child exited",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: warmSuspends},
				{at: 2 * time.Minute, act: sessionResumes},
				{at: 32 * time.Minute, act: sweeps},
			},
			wantStops: 1,
			wantUsed:  0,
		},
		{
			name: "a warm-paused session is not stopped while it is paused",
			steps: []idleStep{
				{at: 0, act: childExits},
				{at: 1 * time.Minute, act: warmSuspends},
				{at: 10 * time.Hour, act: sweeps},
			},
			wantStops: 0,
			wantUsed:  1, // a pause holds its slot; that is what cold parking is for
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newIdleHarness(t)
			d := idle
			if tc.disabled {
				d = 0
			}
			for _, s := range tc.steps {
				h.run(d, s)
			}
			if got := len(h.stops); got != tc.wantStops {
				t.Errorf("idle auto-stop fired %d time(s), want %d (stops: %v)", got, tc.wantStops, h.stops)
			}
			if got := h.used(); got != tc.wantUsed {
				t.Errorf("driver reports %d slot(s) used, want %d", got, tc.wantUsed)
			}
			// Never, on any row: the session's files are the user's work, and
			// an idle stop keeps them exactly as `rainier stop` does.
			if vols := h.fd.Volumes(); len(vols) != 1 {
				t.Errorf("workspace volumes = %v, want exactly the session's own kept", vols)
			}
			if _, ok := h.rd.reg.get(h.id); !ok {
				t.Error("the session left the registry; an idle stop must never delete anything")
			}
		})
	}
}

// TestIdleStopLeavesTheSessionResumable pins the other half of "exactly what
// `rainier stop` does": the stopped sandbox is cold-parked, not gone, and a
// resume brings it back with its slot.
func TestIdleStopLeavesTheSessionResumable(t *testing.T) {
	h := newIdleHarness(t)
	h.run(time.Hour, idleStep{at: 0, act: childExits})
	h.run(time.Hour, idleStep{at: time.Hour, act: sweeps})

	if got := h.state(); got != driver.StateSuspended {
		t.Fatalf("container state after the idle stop = %v, want %v", got, driver.StateSuspended)
	}
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "suspended" {
		t.Fatalf("registry state after the idle stop = %q, want %q", state, "suspended")
	}
	// The same state an operator's stop leaves, in the vocabulary controld
	// reads: the announce must say suspended_cold, or the control plane would
	// keep counting this session against the runner's capacity.
	ann := h.rd.Announce()
	if len(ann) != 1 || ann[0].State != "suspended_cold" {
		t.Fatalf("announce after the idle stop = %+v, want one suspended_cold", ann)
	}
	if err := h.rd.Op(context.Background(), h.id, "resume", false); err != nil {
		t.Fatalf("resume after an idle stop: %v", err)
	}
	if got := h.used(); got != 1 {
		t.Fatalf("slots used after the resume = %d, want 1", got)
	}
	if got := h.state(); got != driver.StateRunning {
		t.Fatalf("container state after the resume = %v, want %v", got, driver.StateRunning)
	}
}

// TestIdleStopReportsWhatItFreed pins the log line's numbers at their source:
// the two counts a runner now carries beside used/total, before and after a
// stop. The line itself is log.Printf, but every number in it comes from here.
func TestIdleStopReportsWhatItFreed(t *testing.T) {
	h := newIdleHarness(t)
	if err := h.rd.CreateWithID(context.Background(), "sess-idle-2", driver.Spec{Image: "img.invalid"}, nil); err != nil {
		t.Fatalf("create the second session: %v", err)
	}
	if active, idleExited := h.rd.reg.counts(); active != 2 || idleExited != 0 {
		t.Fatalf("counts with two working agents = (%d active, %d idle-exited), want (2, 0)", active, idleExited)
	}

	h.run(time.Hour, idleStep{at: 0, act: childExits})
	if active, idleExited := h.rd.reg.counts(); active != 1 || idleExited != 1 {
		t.Fatalf("counts after one child exited = (%d active, %d idle-exited), want (1, 1)", active, idleExited)
	}

	h.run(time.Hour, idleStep{at: time.Hour, act: sweeps})
	// Stopped sessions are in neither count: they hold no slot, so there is
	// nothing left to attribute to them.
	if active, idleExited := h.rd.reg.counts(); active != 1 || idleExited != 0 {
		t.Fatalf("counts after the idle stop = (%d active, %d idle-exited), want (1, 0)", active, idleExited)
	}
	if got := h.used(); got != 1 {
		t.Fatalf("slots used after the idle stop = %d, want 1", got)
	}
}

// TestIdleStopRollsBackWhenTheDriverRefuses: a stop that the daemon refuses
// must leave the session running and claimable again, not stuck "suspending"
// forever with a container that is still up.
func TestIdleStopRollsBackWhenTheDriverRefuses(t *testing.T) {
	h := newIdleHarness(t)
	h.run(time.Hour, idleStep{at: 0, act: childExits})

	// Make the driver refuse by taking the container out from under it: the
	// fake answers a Suspend for an unknown handle with an error, which is
	// what a docker daemon that has lost the container does too.
	handle := h.handle()
	if err := h.fd.DestroyContainer(context.Background(), handle); err != nil {
		t.Fatalf("destroy the container: %v", err)
	}
	h.clk.set(time.Hour)
	if stops := h.rd.sweepIdle(context.Background(), time.Hour); len(stops) != 0 {
		t.Fatalf("sweep reported %v stopped, want none — the driver refused", stops)
	}
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "running" {
		t.Fatalf("registry state after a refused stop = %q, want %q", state, "running")
	}
	if _, ok := h.rd.reg.get(h.id); !ok {
		t.Fatal("a refused idle stop removed the session")
	}
}

// TestIdleStopClaimIsExclusive is the race the sweep is built around: two
// sweeps running at once (a slow driver, a tick that overlaps the previous
// one) must not both stop the same session, and the loser must not roll the
// winner's state back.
func TestIdleStopClaimIsExclusive(t *testing.T) {
	h := newIdleHarness(t)
	h.run(time.Hour, idleStep{at: 0, act: childExits})
	h.clk.set(time.Hour)

	var mu sync.Mutex
	var stopped []string
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := h.rd.sweepIdle(context.Background(), time.Hour)
			mu.Lock()
			stopped = append(stopped, got...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(stopped) != 1 {
		t.Fatalf("eight concurrent sweeps stopped the session %d times, want 1: %v", len(stopped), stopped)
	}
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "suspended" {
		t.Fatalf("registry state after the race = %q, want %q", state, "suspended")
	}
}

// TestIdleStopAttachBeatingTheSweepWins: an attachment that opens before the
// sweep claims the session prevents the stop outright. (One that opens after
// the claim rides a container that is already being stopped — see the design
// doc; the runner cannot un-stop it, and the client's reconnect resumes the
// session, which is what attach does with a stopped session anyway.)
func TestIdleStopAttachBeatingTheSweepWins(t *testing.T) {
	h := newIdleHarness(t)
	h.run(time.Hour, idleStep{at: 0, act: childExits})
	h.clk.set(10 * time.Hour)

	h.rd.reg.attachStarted(h.id)
	if stops := h.rd.sweepIdle(context.Background(), time.Hour); len(stops) != 0 {
		t.Fatalf("sweep stopped %v with an attachment open, want none", stops)
	}
	if got := h.state(); got != driver.StateRunning {
		t.Fatalf("container state = %v, want %v — an attached session is never idle", got, driver.StateRunning)
	}
}

// TestRunIdleStopDisabledReturnsImmediately: --idle-stop 0 must not leave a
// goroutine ticking over sessions it will never stop.
func TestRunIdleStopDisabledReturnsImmediately(t *testing.T) {
	h := newIdleHarness(t)
	h.run(0, idleStep{at: 0, act: childExits})
	h.clk.set(100 * time.Hour)

	done := make(chan struct{})
	go func() {
		// A context that is never canceled: the only way this returns is the
		// disabling value.
		h.rd.RunIdleStop(context.Background(), 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunIdleStop with a zero timeout did not return")
	}
	if got := h.used(); got != 1 {
		t.Fatalf("slots used = %d, want 1 — nothing may be stopped when it is disabled", got)
	}
}

// TestRunIdleStopLoopStopsAnIdleSession drives the real loop (ticker and all,
// on a short sweep interval) rather than calling the sweep by hand, so the
// wiring between the two is covered and not just the decision.
func TestRunIdleStopLoopStopsAnIdleSession(t *testing.T) {
	h := newIdleHarness(t)
	h.rd.idleSweep = 5 * time.Millisecond
	h.run(time.Hour, idleStep{at: 0, act: childExits})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		h.rd.RunIdleStop(ctx, time.Hour)
		close(done)
	}()

	// Not idle yet: the clock has not moved.
	time.Sleep(50 * time.Millisecond)
	if got := h.used(); got != 1 {
		t.Fatalf("slots used before the timeout = %d, want 1", got)
	}
	h.clk.set(time.Hour)

	deadline := time.Now().Add(5 * time.Second)
	for h.used() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the idle loop never stopped a session that was an hour idle")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunIdleStop did not return when its context was canceled")
	}
}

// TestIdleSweepInterval pins the clamp at both ends: a stop lands within a
// tenth of its timeout, but the loop neither spins on a tiny timeout nor
// sleeps through a quarter of an hour on a large one.
func TestIdleSweepInterval(t *testing.T) {
	tests := []struct {
		idle time.Duration
		want time.Duration
	}{
		{idle: time.Second, want: time.Second},          // clamped up
		{idle: 30 * time.Second, want: 3 * time.Second}, // a tenth
		{idle: 30 * time.Minute, want: time.Minute},     // clamped down
		{idle: 24 * time.Hour, want: time.Minute},       // clamped down
		{idle: 10 * time.Minute, want: time.Minute},     // exactly a tenth
	}
	for _, tc := range tests {
		if got := idleSweepInterval(tc.idle); got != tc.want {
			t.Errorf("idleSweepInterval(%s) = %s, want %s", tc.idle, got, tc.want)
		}
	}
}

// TestChildExitIsRecordedOnce: sessiond re-delivers a control event it was not
// sure landed, and a second child_exited must not push the idle deadline out.
func TestChildExitIsRecordedOnce(t *testing.T) {
	h := newIdleHarness(t)
	h.run(time.Hour, idleStep{at: 0, act: childExits})
	h.run(time.Hour, idleStep{at: 59 * time.Minute, act: childExits})

	h.clk.set(time.Hour)
	if stops := h.rd.sweepIdle(context.Background(), time.Hour); len(stops) != 1 {
		t.Fatalf("sweep stopped %v, want the session — a re-delivered exit must not move the deadline", stops)
	}
}

// TestIdleStopIdsAreSweptInAStableOrder keeps a multi-session sweep's log
// legible: map iteration order must not decide what a runner reports first.
func TestIdleStopIdsAreSweptInAStableOrder(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	for _, id := range []string{"sess-idle-9", "sess-idle-3", "sess-idle-5"} {
		if err := h.rd.CreateWithID(ctx, id, driver.Spec{Image: "img.invalid"}, nil); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		h.rd.routeControl(id, []byte(`{"kind":"child_exited","rc":0}`))
	}
	h.run(time.Hour, idleStep{at: 0, act: childExits})
	h.clk.set(time.Hour)

	got := h.rd.sweepIdle(ctx, time.Hour)
	want := []string{"sess-idle-1", "sess-idle-3", "sess-idle-5", "sess-idle-9"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sweep stopped %v, want %v in id order", got, want)
	}
}
