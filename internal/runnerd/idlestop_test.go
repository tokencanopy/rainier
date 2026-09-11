package runnerd

import (
	"context"
	"errors"
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
	// boot is the sandbox-boot token the harness's session is currently on,
	// the one a real /register mints. Control frames carry it, and the
	// registry drops the ones that name a boot the session has moved past.
	boot map[string]uint64
}

// newIdleHarness creates a runner with one running session whose sandbox the
// fake driver holds.
func newIdleHarness(t *testing.T) *idleHarness {
	t.Helper()
	clk := newFakeClock()
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	rd.now = clk.now // before anything serves: no goroutine of this server's exists yet
	h := &idleHarness{t: t, clk: clk, rd: rd, fd: fd, id: "sess-idle-1", boot: map[string]uint64{}}
	h.create(h.id)
	return h
}

// register stands in for a container's sessiond dialing /register: it adopts
// the entry's CURRENT boot epoch, exactly as the real handler does, so a frame
// this test sends afterwards is accepted or dropped on the same rule
// production uses.
func (h *idleHarness) register(id string) {
	h.t.Helper()
	h.boot[id] = h.rd.reg.currentBoot(id)
}

// create adds a session and registers a sandbox boot for it, which is what a
// container's sessiond dialing /register does.
func (h *idleHarness) create(id string) {
	h.t.Helper()
	if err := h.rd.CreateWithID(context.Background(), id, driver.Spec{Image: "img.invalid"}, nil); err != nil {
		h.t.Fatalf("create %s: %v", id, err)
	}
	h.register(id)
}

// entry returns a value copy of the session's registry entry.
func (h *idleHarness) entry(id string) sessionEntry {
	h.t.Helper()
	e, ok := h.rd.reg.snapshot(id)
	if !ok {
		h.t.Fatalf("session %s is not in the registry", id)
	}
	return e
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
		h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"child_exited","rc":0}`))
	case viewerAttaches:
		h.rd.reg.attachStarted(h.id)
	case viewerDetaches:
		h.rd.reg.attachEnded(h.id, h.clk.now(), true)
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
		// A cold resume restarts the container, so a real sessiond dials
		// /register again and adopts whatever epoch the resume left. Done
		// unconditionally — a real one does not check first, and a harness
		// that minted only when the production code had already behaved
		// correctly would be asserting its own premise.
		h.register(h.id)
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
	h.create("sess-idle-2")
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
		h.create(id)
		h.register(id)
		h.rd.routeControl(id, h.boot[id], []byte(`{"kind":"child_exited","rc":0}`))
	}
	h.run(time.Hour, idleStep{at: 0, act: childExits})
	h.clk.set(time.Hour)

	got := h.rd.sweepIdle(ctx, time.Hour)
	want := []string{"sess-idle-1", "sess-idle-3", "sess-idle-5", "sess-idle-9"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sweep stopped %v, want %v in id order", got, want)
	}
}

// ---------------------------------------------------------------------------
// regressions from review
// ---------------------------------------------------------------------------

// TestStaleChildExitFromAPreviousBootIsIgnored is the review's worst finding:
// a control frame can outlive the sandbox boot that sent it. A hub read loop
// stalled writing to a wedged viewer drains its buffered frames whenever it
// finally comes back — which can be after the sandbox has been stopped,
// resumed, and re-registered. Without the boot token, that buffered
// child_exited lands on the NEW boot, and half an hour later the runner stops
// a session whose agent is working.
func TestStaleChildExitFromAPreviousBootIsIgnored(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	staleBoot := h.boot[h.id]

	// First life: the agent finishes and the session is idle-stopped.
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.run(30*time.Minute, idleStep{at: 30 * time.Minute, act: sweeps})
	if len(h.stops) != 1 {
		t.Fatalf("first life: stopped %v, want one stop", h.stops)
	}

	// Second life: resumed, a new sessiond registers, a new agent is working.
	h.run(30*time.Minute, idleStep{at: 31 * time.Minute, act: sessionResumes})
	if got := h.boot[h.id]; got == staleBoot {
		t.Fatal("a cold resume left the session on the previous boot's token")
	}
	if e := h.entry(h.id); !e.childExitedAt.IsZero() {
		t.Fatal("a cold resume kept the previous child's exit")
	}

	// The previous boot's buffered frame finally arrives.
	h.clk.set(32 * time.Minute)
	h.rd.routeControl(h.id, staleBoot, []byte(`{"kind":"child_exited","rc":0}`))
	if e := h.entry(h.id); !e.childExitedAt.IsZero() {
		t.Fatal("a child_exited from the previous boot was recorded against the new one")
	}

	// So the working agent is never stopped, however long nobody watches it.
	h.clk.set(100 * time.Hour)
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v — a session whose agent is running must never be stopped", stops)
	}
	if got := h.state(); got != driver.StateRunning {
		t.Fatalf("container state = %v, want %v", got, driver.StateRunning)
	}

	// And the new child's own exit still counts.
	h.run(30*time.Minute, idleStep{at: 100 * time.Hour, act: childExits})
	h.clk.set(100*time.Hour + 30*time.Minute)
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 1 {
		t.Fatalf("stopped %v after the new child exited, want the session", stops)
	}
}

// TestColdResumeForgetsThePreviousChild pins the invariant directly rather
// than through a script that could pass for the wrong reason: after a cold
// resume there is no child-exit fact at all, so nothing can stop the session
// until its new agent reports one.
func TestColdResumeForgetsThePreviousChild(t *testing.T) {
	h := newIdleHarness(t)
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.run(30*time.Minute, idleStep{at: 30 * time.Minute, act: sweeps})
	h.run(30*time.Minute, idleStep{at: 31 * time.Minute, act: sessionResumes})

	e := h.entry(h.id)
	if !e.childExitedAt.IsZero() || !e.lastDetachAt.IsZero() {
		t.Fatalf("after a cold resume: childExitedAt=%v lastDetachAt=%v, want both zero", e.childExitedAt, e.lastDetachAt)
	}
	if e.coldSuspended {
		t.Fatal("after a cold resume the entry still reads as cold-suspended")
	}
	h.clk.set(1000 * time.Hour)
	if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v — a resumed session's new agent is running", stops)
	}
}

// TestOperatorStopThenResumeForgetsThePreviousChild is the same invariant by
// the other route into a cold park: the operator's stop rather than the
// runner's.
func TestOperatorStopThenResumeForgetsThePreviousChild(t *testing.T) {
	h := newIdleHarness(t)
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.run(30*time.Minute, idleStep{at: 1 * time.Minute, act: operatorStops})
	h.run(30*time.Minute, idleStep{at: 2 * time.Minute, act: sessionResumes})

	if e := h.entry(h.id); !e.childExitedAt.IsZero() {
		t.Fatal("a resume after an operator's stop kept the previous child's exit")
	}
	h.clk.set(1000 * time.Hour)
	if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v — a resumed session's new agent is running", stops)
	}
}

// blockingDriver holds one driver call open so a test can sweep while it is in
// flight — the window in which the entry still reads "running" although
// somebody else is already operating on the container.
type blockingDriver struct {
	*driver.Fake
	entered chan struct{} // closed when the call has started
	release chan struct{} // closed by the test to let it finish
	warm    bool          // block Suspend(warm) rather than Snapshot
}

func newBlockingDriver(warm bool) *blockingDriver {
	return &blockingDriver{Fake: driver.NewFake(4), entered: make(chan struct{}),
		release: make(chan struct{}), warm: warm}
}

func (d *blockingDriver) hold() {
	close(d.entered)
	<-d.release
}

func (d *blockingDriver) Suspend(ctx context.Context, id string, warm bool) error {
	if d.warm && warm {
		d.hold()
	}
	return d.Fake.Suspend(ctx, id, warm)
}

func (d *blockingDriver) Snapshot(ctx context.Context, id, ref string, stripEnv []string) (driver.Snapshot, error) {
	if !d.warm {
		d.hold()
	}
	return d.Fake.Snapshot(ctx, id, ref, stripEnv)
}

// newBlockedHarness builds a one-session runner over a driver that will block
// in the named call, with the session already idle.
func newBlockedHarness(t *testing.T, warm bool) (*idleHarness, *blockingDriver) {
	t.Helper()
	clk := newFakeClock()
	bd := newBlockingDriver(warm)
	rd := New(bd, "", "", "")
	rd.now = clk.now
	h := &idleHarness{t: t, clk: clk, rd: rd, fd: bd.Fake, id: "sess-idle-1", boot: map[string]uint64{}}
	h.create(h.id)
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.clk.set(10 * time.Hour)
	return h, bd
}

// TestWarmSuspendInFlightIsNotStopped: a warm suspend has no state marker of
// its own — the entry reads "running" for the whole of `docker pause` — so
// without the in-flight guard a sweep could claim the session mid-pause and
// turn an operator's pause, which deliberately KEEPS the slot, into a stop
// that releases it while controld's row says suspended_warm.
func TestWarmSuspendInFlightIsNotStopped(t *testing.T) {
	h, bd := newBlockedHarness(t, true)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- h.rd.Op(ctx, h.id, "suspend", true) }()
	<-bd.entered

	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v mid-pause; an operator's warm suspend must not become a stop", stops)
	}
	close(bd.release)
	if err := <-done; err != nil {
		t.Fatalf("warm suspend: %v", err)
	}
	if used := h.used(); used != 1 {
		t.Fatalf("slots used after the warm suspend = %d, want 1 — a pause holds its slot", used)
	}
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "suspended" {
		t.Fatalf("registry state = %q, want %q", state, "suspended")
	}
}

// TestSnapshotInFlightIsNotStopped: `docker commit` runs against a LIVE
// container and takes minutes. Stopping the session underneath it breaks the
// commit and the environment cache built on it.
func TestSnapshotInFlightIsNotStopped(t *testing.T) {
	h, bd := newBlockedHarness(t, false)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		_, err := h.rd.OpSnapshot(ctx, h.id, "rainier-env:example")
		done <- err
	}()
	<-bd.entered

	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v mid-snapshot", stops)
	}
	close(bd.release)
	if err := <-done; err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// And once the snapshot is done the session is idle again.
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 1 {
		t.Fatalf("stopped %v after the snapshot finished, want the session", stops)
	}
}

// TestAFailedStopDoesNotClobberAConcurrentDelete: a Delete that arrives while
// `docker stop` is in flight marks the entry "destroying", which is what makes
// the register goroutine stand down instead of destroying the container a
// second time and reporting the session dead. A rollback that reset the state
// unconditionally would wipe that marker.
func TestAFailedStopDoesNotClobberAConcurrentDelete(t *testing.T) {
	h := newIdleHarness(t)
	h.rd.reg.beginColdSuspend(h.id)
	h.rd.reg.setState(h.id, "destroying") // the Delete that overtook the stop

	h.rd.reg.releaseColdSuspend(h.id)
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "destroying" {
		t.Fatalf("state after a failed stop rolled back = %q, want %q", state, "destroying")
	}
	h.rd.reg.finishColdSuspend(h.id)
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "destroying" {
		t.Fatalf("state after a succeeded stop landed = %q, want %q", state, "destroying")
	}
}

// TestFailedBootIsNeverIdleStopped: a session whose boot chain failed has an
// exited child and no viewer, so it looks exactly like a finished agent. It
// must be kept anyway — attaching to a failed session to read the log that
// says why is the whole reason the CLI allows it, and neither a stopped
// sandbox (no hub) nor a failed row (not resumable) can serve that.
func TestFailedBootIsNeverIdleStopped(t *testing.T) {
	for _, kind := range []string{
		`{"kind":"setup_failed","rc":1,"tail":"boom"}`,
		`{"kind":"stage_failed","stage":"clone","rc":128,"tail":"nope"}`,
	} {
		t.Run(kind, func(t *testing.T) {
			h := newIdleHarness(t)
			h.rd.routeControl(h.id, h.boot[h.id], []byte(kind))
			h.run(30*time.Minute, idleStep{at: 0, act: childExits})

			h.clk.set(1000 * time.Hour)
			if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 0 {
				t.Fatalf("stopped %v — a session that failed to boot must stay attachable", stops)
			}
		})
	}
}

// TestRecoveredSessionsAreNeverIdleStopped: after a runnerd restart, Recover
// rebuilds entries from labelled containers with no child-exit fact — it lived
// only in the memory of the process that died. Such a session is treated as
// "child running" and is never stopped, which is the safe direction and the
// documented one.
func TestRecoveredSessionsAreNeverIdleStopped(t *testing.T) {
	ctx := context.Background()
	fd := driver.NewFake(4)
	// A container the previous runnerd left behind.
	if _, err := fd.Create(ctx, driver.Spec{SessionID: "sess-recovered", Image: "img.invalid"}); err != nil {
		t.Fatalf("seed the container: %v", err)
	}
	clk := newFakeClock()
	rd := New(fd, "", "", "")
	rd.now = clk.now
	if err := rd.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if _, ok := rd.reg.get("sess-recovered"); !ok {
		t.Fatal("the session was not recovered")
	}

	clk.set(1000 * time.Hour)
	if stops := rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v — a recovered session's child is not known to have exited", stops)
	}
}

// ---------------------------------------------------------------------------
// regressions from the verification round
// ---------------------------------------------------------------------------

// TestAChildExitSurvivesAPlainRedial: a sessiond redial is not a new sandbox
// boot — the container did not restart and the child did not change — so a
// child_exited still in flight across it must still count. It is the only
// copy: sessiond re-sends only events it never delivered, so dropping this one
// would leave a finished session holding its slot for the life of the runner,
// silently. (An epoch minted per REGISTRATION rather than per boot did exactly
// that.)
func TestAChildExitSurvivesAPlainRedial(t *testing.T) {
	h := newIdleHarness(t)
	inFlight := h.boot[h.id]

	// The conn drops and sessiond redials: a fresh /register, same container.
	h.register(h.id)

	// The frame that was already in flight when it dropped now lands.
	h.clk.set(time.Minute)
	h.rd.routeControl(h.id, inFlight, []byte(`{"kind":"child_exited","rc":0}`))
	if e := h.entry(h.id); e.childExitedAt.IsZero() {
		t.Fatal("a child exit in flight across a redial was dropped; that session would hold its slot forever")
	}
	h.clk.set(31 * time.Minute)
	if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 1 {
		t.Fatalf("sweep stopped %v, want the session", stops)
	}
}

// TestAChildExitInTheResumeWindowIsIgnored covers the gap between a cold
// resume and the restarted sandbox's registration — seconds, during which the
// entry would otherwise still name the connection from BEFORE the stop. A
// frame buffered on that connection landing here would put the old child's
// exit on the new boot, and the new agent gets stopped half an hour later.
// This is what makes resumed()'s epoch bump load-bearing on its own, rather
// than only in combination with the re-registration that follows it.
func TestAChildExitInTheResumeWindowIsIgnored(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	staleBoot := h.boot[h.id]

	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.run(30*time.Minute, idleStep{at: 30 * time.Minute, act: sweeps})
	h.clk.set(31 * time.Minute)
	if err := h.rd.Op(ctx, h.id, "resume", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// Deliberately BEFORE the restarted sandbox registers.
	h.rd.routeControl(h.id, staleBoot, []byte(`{"kind":"child_exited","rc":0}`))
	if e := h.entry(h.id); !e.childExitedAt.IsZero() {
		t.Fatal("a frame from before the restart landed on the new boot")
	}
	h.register(h.id)

	h.clk.set(1000 * time.Hour)
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v — the resumed session's new agent is running", stops)
	}
}

// TestAStaleStageFailureDoesNotPinAHealthySession: markBootFailed takes a
// session out of auto-stop for the rest of its boot, so a stage failure that
// outlived its own boot must be dropped like a stale child exit — otherwise it
// pins a healthy session's slot for good, by a different route.
func TestAStaleStageFailureDoesNotPinAHealthySession(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	staleBoot := h.boot[h.id]

	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.run(30*time.Minute, idleStep{at: 30 * time.Minute, act: sweeps})
	h.run(30*time.Minute, idleStep{at: 31 * time.Minute, act: sessionResumes})

	// The previous boot's failure report arrives late.
	h.rd.routeControl(h.id, staleBoot, []byte(`{"kind":"stage_failed","stage":"clone","rc":128}`))
	if e := h.entry(h.id); e.bootFailed {
		t.Fatal("a stage failure from the previous boot pinned the current one out of idle auto-stop")
	}

	// And the new boot, having worked and finished, is reclaimed normally.
	h.run(30*time.Minute, idleStep{at: 40 * time.Minute, act: childExits})
	h.clk.set(70*time.Minute + time.Minute)
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 1 {
		t.Fatalf("sweep stopped %v, want the session", stops)
	}
}

// TestAResumedSessionForgetsAFailedBoot: a cold resume re-runs the whole boot
// chain, so a previous boot's failure says nothing about the new one and must
// not keep the session exempt for ever.
func TestAResumedSessionForgetsAFailedBoot(t *testing.T) {
	h := newIdleHarness(t)
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"setup_failed","rc":1}`))
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.run(30*time.Minute, idleStep{at: 1 * time.Minute, act: operatorStops})
	h.run(30*time.Minute, idleStep{at: 2 * time.Minute, act: sessionResumes})

	if e := h.entry(h.id); e.bootFailed {
		t.Fatal("a cold resume kept the previous boot's failure")
	}
	h.run(30*time.Minute, idleStep{at: 3 * time.Minute, act: childExits})
	h.clk.set(34 * time.Minute)
	if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 1 {
		t.Fatalf("sweep stopped %v, want the session — its new boot worked and finished", stops)
	}
}

// stopOutcomeDriver reports a failure from Suspend while doing whatever the
// test says the daemon actually did — the shape a `docker stop` killed at its
// deadline leaves behind, where the CLI is gone but the daemon keeps stopping
// the container.
type stopOutcomeDriver struct {
	*driver.Fake
	reallyStopped bool
}

func (d *stopOutcomeDriver) Suspend(ctx context.Context, id string, warm bool) error {
	if d.reallyStopped {
		if err := d.Fake.Suspend(ctx, id, warm); err != nil {
			return err
		}
	}
	return errors.New("signal: killed")
}

// TestAStopThatFailedButLandedIsNotRolledBackToRunning is the verification
// round's worst finding. Bounding `docker stop` means the CLI can be killed at
// its deadline while the daemon goes on stopping the container. Believing the
// error and rolling the entry back to "running" is a lie the session pays for:
// the container dies seconds later, the register goroutine reads a "running"
// entry, takes it for a crash, destroys the container and reports the session
// dead — a merely idle session, gone, for a stop that worked.
func TestAStopThatFailedButLandedIsNotRolledBackToRunning(t *testing.T) {
	for _, tc := range []struct {
		name          string
		reallyStopped bool
		wantState     string
	}{
		{"the daemon stopped it anyway", true, "suspended"},
		{"the stop really did fail", false, "running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			sd := &stopOutcomeDriver{Fake: driver.NewFake(4), reallyStopped: tc.reallyStopped}
			rd := New(sd, "", "", "")
			rd.now = clk.now
			h := &idleHarness{t: t, clk: clk, rd: rd, fd: sd.Fake, id: "sess-idle-1", boot: map[string]uint64{}}
			h.create(h.id)
			h.run(30*time.Minute, idleStep{at: 0, act: childExits})

			h.clk.set(time.Hour)
			if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 0 {
				t.Fatalf("sweep reported %v stopped; the driver returned an error", stops)
			}
			if _, state, _ := h.rd.reg.opTarget(h.id); state != tc.wantState {
				t.Fatalf("registry state = %q, want %q", state, tc.wantState)
			}
		})
	}
}

// TestAFailedAttachHandshakeDoesNotMoveTheIdleClock: the local /attach surface
// has no authentication, so a client looping on a dial that never becomes an
// attachment would otherwise push every deadline on this runner out
// indefinitely, silently.
func TestAFailedAttachHandshakeDoesNotMoveTheIdleClock(t *testing.T) {
	h := newIdleHarness(t)
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})

	// A handshake that never pumped: counted while it is open, and gone
	// without a trace when it fails.
	h.clk.set(20 * time.Minute)
	h.rd.reg.attachStarted(h.id)
	h.rd.reg.attachEnded(h.id, h.clk.now(), false)
	if e := h.entry(h.id); !e.lastDetachAt.IsZero() {
		t.Fatal("a failed handshake moved the idle clock")
	}
	// So the session is still stopped on its original deadline.
	h.clk.set(30 * time.Minute)
	if stops := h.rd.sweepIdle(context.Background(), 30*time.Minute); len(stops) != 1 {
		t.Fatalf("sweep stopped %v, want the session on its original deadline", stops)
	}
}

// TestAnOperatorStopDoesNotClobberAConcurrentDelete: beginColdSuspend is the
// operator's half of the claim, and it must respect Delete's marker for the
// same reason the rollback does.
func TestAnOperatorStopDoesNotClobberAConcurrentDelete(t *testing.T) {
	h := newIdleHarness(t)
	h.rd.reg.setState(h.id, "destroying")
	h.rd.reg.beginColdSuspend(h.id)
	if _, state, _ := h.rd.reg.opTarget(h.id); state != "destroying" {
		t.Fatalf("state = %q, want %q — a stop must not overwrite a teardown in flight", state, "destroying")
	}
}
