// internal/controld/sched_test.go
package controld

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"rainier/internal/rwire"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// cmdRecorder collects session ids in the order fakeRunner receives "create"
// commands for them — the evidence the FIFO/placement tests assert on.
type cmdRecorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *cmdRecorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *cmdRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.ids...)
}

func (r *cmdRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

// sameSet reports whether a and b contain the same elements, ignoring
// order and duplicates.
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

// ackCreate replies to a dispatched command using fakeRunner's own locked
// writer, without going through fakeRunner.reply — that helper calls
// t.Fatalf, which only the goroutine running the test may do (here we're
// answering from a background goroutine that outlives any single assertion).
func ackCreate(t *testing.T, f *fakeRunner, cmd rwire.ToRunner, ok bool, detail string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	m := rwire.FromRunner{Type: "result", ReqID: cmd.ReqID, OK: ok, Detail: detail, Used: f.used, Total: f.total}
	if err := wsjson.Write(ctx, f.c, m); err != nil {
		t.Logf("ackCreate: write: %v", err)
	}
}

// autoAckCreates answers every "create" fakeRunner receives with ok:true,
// recording each session id in arrival order, until the test ends. It
// deliberately leaves the runner's reported capacity untouched — a create
// ack alone must never look like it grew Used; only an explicit event does
// that in these tests, mirroring the "creating rows aren't in the runner's
// docker count yet" gap the scheduler's free-capacity formula compensates
// for.
func autoAckCreates(t *testing.T, f *fakeRunner) *cmdRecorder {
	t.Helper()
	rec := &cmdRecorder{}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case cmd := <-f.cmds:
				if cmd.Type == "create" {
					rec.add(cmd.Session)
					ackCreate(t, f, cmd, true, "")
				}
			}
		}
	}()
	return rec
}

// seedQueued seeds a queued session with a distinct, deterministic
// CreatedAt so OldestQueued's FIFO ordering doesn't depend on wall-clock
// resolution between seeds.
func seedQueued(t *testing.T, st Store, id string, offset int) Session {
	t.Helper()
	base := time.Now().Add(-time.Hour)
	return seedSession(t, st, Session{
		ID:        id,
		State:     StateQueued,
		Name:      id,
		Image:     "img:latest",
		CreatedAt: base.Add(time.Duration(offset) * time.Second),
	})
}

// startRun starts the scheduler loop and arranges for it to stop when the
// test ends.
func startRun(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Run(ctx)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestPickRunner(t *testing.T) {
	for _, tc := range []struct {
		name string
		rs   []runnerView
		want string
		ok   bool
	}{
		{
			name: "max free wins",
			rs:   []runnerView{{Name: "a", Free: 2}, {Name: "b", Free: 5}, {Name: "c", Free: 1}},
			want: "b", ok: true,
		},
		{
			name: "tie breaks to lexicographically smaller name",
			rs:   []runnerView{{Name: "vm2", Free: 3}, {Name: "vm1", Free: 3}, {Name: "vm3", Free: 3}},
			want: "vm1", ok: true,
		},
		{
			name: "zero free everywhere",
			rs:   []runnerView{{Name: "a", Free: 0}, {Name: "b", Free: 0}},
			want: "", ok: false,
		},
		{
			name: "empty slice",
			rs:   nil,
			want: "", ok: false,
		},
		{
			name: "negative free (over-committed) never wins",
			rs:   []runnerView{{Name: "a", Free: -1}, {Name: "b", Free: 0}},
			want: "", ok: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := pickRunner(tc.rs)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("pickRunner(%+v) = (%q, %v), want (%q, %v)", tc.rs, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestPickForSession walks the two rules layered on top of pickRunner's
// most-free choice: an environment's placement pin narrows the candidate set
// to exactly one runner, and a session whose resolved image IS its
// environment's cached snapshot prefers the runner holding that snapshot.
func TestPickForSession(t *testing.T) {
	const snapRef = "rainier-env:env_x-0123456789ab"
	fleet := []runnerView{{Name: "vm1", Free: 3}, {Name: "vm2", Free: 1}, {Name: "vm3", Free: 0}}

	for _, tc := range []struct {
		name string
		rs   []runnerView
		row  Session
		env  *Environment
		want string
		ok   bool
	}{
		{
			name: "no environment is pickRunner's most-free choice",
			rs:   fleet, row: Session{}, env: nil,
			want: "vm1", ok: true,
		},
		{
			name: "an environment with no pin and no cache is unchanged",
			rs:   fleet, row: Session{EnvironmentID: "env_x"}, env: &Environment{ID: "env_x"},
			want: "vm1", ok: true,
		},
		{
			name: "a pin wins over more free capacity elsewhere",
			rs:   fleet, row: Session{EnvironmentID: "env_x"}, env: &Environment{ID: "env_x", Placement: "vm2"},
			want: "vm2", ok: true,
		},
		{
			name: "a pin to a full runner places nothing",
			rs:   fleet, row: Session{EnvironmentID: "env_x"}, env: &Environment{ID: "env_x", Placement: "vm3"},
			want: "", ok: false,
		},
		{
			name: "a pin to a runner that is not connected places nothing",
			rs:   fleet, row: Session{EnvironmentID: "env_x"}, env: &Environment{ID: "env_x", Placement: "vm9"},
			want: "", ok: false,
		},
		{
			// The whole point of the tiebreak: the snapshot exists only in
			// vm2's local image store, so vm2 is worth more than vm1's extra
			// headroom.
			name: "the snapshot holder wins over a runner with more free capacity",
			rs:   fleet,
			row:  Session{EnvironmentID: "env_x", ResolvedImage: snapRef},
			env:  &Environment{ID: "env_x", SnapshotRef: snapRef, SnapshotRunner: "vm2"},
			want: "vm2", ok: true,
		},
		{
			name: "the snapshot holder wins a tie it would lose lexicographically",
			rs:   []runnerView{{Name: "vm1", Free: 2}, {Name: "vm2", Free: 2}},
			row:  Session{EnvironmentID: "env_x", ResolvedImage: snapRef},
			env:  &Environment{ID: "env_x", SnapshotRef: snapRef, SnapshotRunner: "vm2"},
			want: "vm2", ok: true,
		},
		{
			name: "a full snapshot holder falls back to the normal pick",
			rs:   fleet,
			row:  Session{EnvironmentID: "env_x", ResolvedImage: snapRef},
			env:  &Environment{ID: "env_x", SnapshotRef: snapRef, SnapshotRunner: "vm3"},
			want: "vm1", ok: true,
		},
		{
			name: "a disconnected snapshot holder falls back to the normal pick",
			rs:   fleet,
			row:  Session{EnvironmentID: "env_x", ResolvedImage: snapRef},
			env:  &Environment{ID: "env_x", SnapshotRef: snapRef, SnapshotRunner: "vm9"},
			want: "vm1", ok: true,
		},
		{
			// The session resolved to the plain image (the cache was stale, or
			// the holder was full at create): it has no affinity at all.
			name: "a session not running the snapshot ignores the holder",
			rs:   fleet,
			row:  Session{EnvironmentID: "env_x", ResolvedImage: "plain:1"},
			env:  &Environment{ID: "env_x", SnapshotRef: snapRef, SnapshotRunner: "vm2"},
			want: "vm1", ok: true,
		},
		{
			name: "the pin outranks the snapshot holder",
			rs:   fleet,
			row:  Session{EnvironmentID: "env_x", ResolvedImage: snapRef},
			env:  &Environment{ID: "env_x", Placement: "vm1", SnapshotRef: snapRef, SnapshotRunner: "vm2"},
			want: "vm1", ok: true,
		},
		{
			name: "nothing free anywhere places nothing",
			rs:   []runnerView{{Name: "vm1", Free: 0}},
			row:  Session{}, env: nil,
			want: "", ok: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := pickForSession(tc.rs, tc.row, tc.env)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("pickForSession = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestPlacementPinPlacesOnThePinnedRunner drives the pin through the real
// scheduler: vm1 has strictly more free capacity, so the session landing on
// vm2 can only be the environment's placement.
func TestPlacementPinPlacesOnThePinnedRunner(t *testing.T) {
	s, st, ts := newTestControld(t)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 1})
	autoAckCreates(t, f1)
	autoAckCreates(t, f2)

	env := seedEnv(t, st, Environment{Name: "pinned", Image: "img:1", Placement: "vm2"})
	seedSession(t, st, Session{ID: "sess_pinned", State: StateQueued, Name: "pinned1",
		EnvironmentID: env.ID, ResolvedImage: env.Image, CreatedAt: time.Now().Add(-time.Hour)})

	startRun(t, s)

	eventually(t, 3*time.Second, func() error {
		got := getSession(t, st, "sess_pinned")
		if got.State != StateCreating || got.Runner != "vm2" {
			return fmt.Errorf("session = %q on %q, want creating on vm2 (the pin)", got.State, got.Runner)
		}
		return nil
	})
}

// TestPlacementPinQueuesWhenTheRunnerHasNoRoom pins both halves of the
// blocked-pin rule: the pinned session waits (it is never placed elsewhere),
// and — the part a naive "stop the pass" scheduler gets wrong — a younger
// unpinned session behind it is still placed.
func TestPlacementPinQueuesWhenTheRunnerHasNoRoom(t *testing.T) {
	t.Run("pinned to a runner with no free slot", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		// vm2 announces itself full, so the pin has nowhere to land.
		joinRunner(t, s, ts, runnerScript{Name: "vm2", Used: 1, Total: 1})
		rec := autoAckCreates(t, f1)

		env := seedEnv(t, st, Environment{Name: "pinned", Image: "img:1", Placement: "vm2"})
		seedSession(t, st, Session{ID: "sess_blocked", State: StateQueued, Name: "blocked",
			EnvironmentID: env.ID, ResolvedImage: env.Image, CreatedAt: time.Now().Add(-time.Hour)})
		seedQueued(t, st, "sess_behind", 1)

		startRun(t, s)

		// The unpinned session behind it places on vm1...
		wantState(t, st, "sess_behind", StateCreating)
		if got := rec.snapshot(); !sameSet(got, []string{"sess_behind"}) {
			t.Fatalf("vm1 received creates for %v, want only sess_behind", got)
		}
		// ...while the pinned one stays queued and unplaced.
		got := getSession(t, st, "sess_blocked")
		if got.State != StateQueued || got.Runner != "" {
			t.Fatalf("pinned session = %q on %q, want still queued and unplaced", got.State, got.Runner)
		}
	})

	t.Run("pinned to a runner that never connected", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		rec := autoAckCreates(t, f)

		env := seedEnv(t, st, Environment{Name: "hardware", Image: "img:1", Placement: "rainier-gpu"})
		seedSession(t, st, Session{ID: "sess_hw", State: StateQueued, Name: "hw",
			EnvironmentID: env.ID, ResolvedImage: env.Image, CreatedAt: time.Now().Add(-time.Hour)})
		seedQueued(t, st, "sess_any", 1)

		startRun(t, s)

		wantState(t, st, "sess_any", StateCreating)
		if got := rec.snapshot(); !sameSet(got, []string{"sess_any"}) {
			t.Fatalf("vm1 received creates for %v, want only sess_any", got)
		}
		if got := getSession(t, st, "sess_hw"); got.State != StateQueued || got.Runner != "" {
			t.Fatalf("pinned session = %q on %q, want still queued and unplaced", got.State, got.Runner)
		}
	})
}

// TestCacheTiebreakPrefersTheSnapshotHolder drives rule 6 through the real
// scheduler: vm1 has more free capacity, but the session's resolved image is
// a snapshot that exists only in vm2's local image store.
func TestCacheTiebreakPrefersTheSnapshotHolder(t *testing.T) {
	s, st, ts := newTestControld(t)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 1})
	autoAckCreates(t, f1)
	autoAckCreates(t, f2)

	env := seedEnv(t, st, Environment{Name: "cached", Image: "img:1", Setup: "echo hi"})
	const ref = "rainier-env:cached-0123456789ab"
	env = cacheEnvSnapshot(t, st, env, ref, "vm2")

	seedSession(t, st, Session{ID: "sess_cached", State: StateQueued, Name: "cached1",
		EnvironmentID: env.ID, ResolvedImage: ref, CreatedAt: time.Now().Add(-time.Hour)})

	startRun(t, s)

	eventually(t, 3*time.Second, func() error {
		got := getSession(t, st, "sess_cached")
		if got.State != StateCreating || got.Runner != "vm2" {
			return fmt.Errorf("session = %q on %q, want creating on vm2 (the snapshot holder)", got.State, got.Runner)
		}
		return nil
	})
}

// TestSchedulerFIFOPlacementAndCapacityFrees is the mandated scheduler flow
// test: a 2-slot runner, 3 queued rows, exactly the oldest 2 dispatched and
// left `creating`, the third staying queued until a slot actually frees —
// which a `running` event must NOT trigger (Used and the creating-count
// subtraction move together), but a `dead` event must.
func TestSchedulerFIFOPlacementAndCapacityFrees(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 2,
		Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	rec := autoAckCreates(t, f)

	seedQueued(t, st, "sess_q1", 0)
	seedQueued(t, st, "sess_q2", 1)
	seedQueued(t, st, "sess_q3", 2)

	startRun(t, s)

	eventually(t, 2*time.Second, func() error {
		if n := rec.len(); n != 2 {
			return fmt.Errorf("dispatched %d creates so far, want 2", n)
		}
		return nil
	})
	// Placement (the queued->creating transition) is strictly sequential and
	// FIFO; dispatch to the wire that follows it is concurrent, so the two
	// commands can arrive in either order — what's pinned is *which* two
	// sessions were chosen (the oldest), not the wire order they land in.
	if got := rec.snapshot(); len(got) != 2 || !sameSet(got, []string{"sess_q1", "sess_q2"}) {
		t.Fatalf("dispatched = %v, want exactly {sess_q1, sess_q2} (the oldest two)", got)
	}
	wantState(t, st, "sess_q1", StateCreating)
	wantState(t, st, "sess_q2", StateCreating)

	// No capacity left: sess_q3 waits, and keeps waiting through a mere
	// "running" event — Used catching up to 1 is exactly offset by the
	// creating-count dropping to 1, so no net capacity appears.
	f.setCapacity(1, 2)
	f.event(t, "sess_q1", "running")
	wantState(t, st, "sess_q1", StateRunning)
	time.Sleep(150 * time.Millisecond)
	if got := getSession(t, st, "sess_q3"); got.State != StateQueued {
		t.Fatalf("sess_q3 state = %q, want still queued (a running event must not free a slot)", got.State)
	}
	if n := rec.len(); n != 2 {
		t.Fatalf("dispatched %d creates, want still 2", n)
	}

	// A slot only frees once the session actually terminates.
	f.setCapacity(0, 2)
	f.event(t, "sess_q1", "dead")
	wantState(t, st, "sess_q1", StateDead)

	eventually(t, 2*time.Second, func() error {
		got := getSession(t, st, "sess_q3")
		if got.State != StateCreating {
			return fmt.Errorf("sess_q3 state = %q, want creating", got.State)
		}
		if got.Runner != "vm1" {
			return fmt.Errorf("sess_q3 runner = %q, want vm1", got.Runner)
		}
		return nil
	})

	// A running event frees no slot, but it does clear freeCapacity's
	// double-count: while a row is `creating` AND its container is already in
	// the runner's reported Used, one slot of real headroom is invisible.
	// vm1 now reports used 0/2 with sess_q2 and sess_q3 both `creating`, so
	// free reads 0 and a fourth session waits — until sess_q2 goes running,
	// which drops the creating count to 1 and must WAKE the scheduler. The
	// 2s bound is the assertion: the 10s safety tick would also get there
	// eventually (the burst e2e measured 20s of exactly that), so anything
	// inside this bound is the wake doing its job.
	seedQueued(t, st, "sess_q4", 3)
	time.Sleep(150 * time.Millisecond)
	if got := getSession(t, st, "sess_q4"); got.State != StateQueued {
		t.Fatalf("sess_q4 state = %q, want queued (no headroom until a creating row clears)", got.State)
	}

	f.event(t, "sess_q2", "running")
	eventually(t, 2*time.Second, func() error {
		got := getSession(t, st, "sess_q4")
		if got.State != StateCreating || got.Runner != "vm1" {
			return fmt.Errorf("sess_q4 = %q on %q, want creating on vm1 promptly after the running event", got.State, got.Runner)
		}
		return nil
	})
}

// TestCreateDispatchFailureRequeues covers the dispatch-failure paths, which
// differ in exactly one thing — whether the create was ever delivered:
// ok:false fails the session outright; a connection that dies requeues it
// (with placement cleared); and a timeout on a still-live connection does
// NEITHER, because the runner has the command and may be executing it.
func TestCreateDispatchFailureRequeues(t *testing.T) {
	t.Run("ok false fails the session with the runner's detail", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 2,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		id := "sess_boom"
		seedQueued(t, st, id, 0)

		startRun(t, s)

		cmd := f.nextCmd(t)
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}
		ackCreate(t, f, cmd, false, "boom")

		got := wantState(t, st, id, StateFailed)
		if got.Error != "boom" {
			t.Fatalf("error = %q, want %q", got.Error, "boom")
		}
	})

	// These two call dispatchCreate directly rather than driving it through
	// Run/schedulerLoop: a requeue also fires wakeScheduler, and since vm1
	// is the only (still-connected, still-not-answering) runner, the full
	// loop would immediately re-place and re-dispatch the same row —
	// correct behavior, but it means the row only sits `queued` for a
	// flicker between one OpTimeout and the next re-dispatch, which races
	// any poll trying to observe it. Calling dispatchCreate once, in
	// isolation, pins the outcome itself without that race.
	t.Run("connection death requeues with runner cleared", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 2,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f) // reconcile must finish before we seed, or it can requeue our row itself

		id := "sess_conn_death"
		row := seedSession(t, st, Session{ID: id, State: StateCreating, Runner: "vm1", Name: id, Image: "img:latest"})

		done := make(chan struct{})
		go func() {
			defer close(done)
			s.dispatchCreate(context.Background(), row, "vm1", nil)
		}()

		cmd := f.nextCmd(t)
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}
		// The runner drops off mid-create: nothing on the other end can
		// finish it, and nothing will re-announce it either, so this row must
		// go back on the queue for another runner.
		f.close()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("dispatchCreate did not return after the connection died")
		}
		got := getSession(t, st, id)
		if got.State != StateQueued || got.Runner != "" {
			t.Fatalf("session = %+v, want queued with runner cleared", got)
		}
	})

	// The Important finding this wave closes: a create that times out on a
	// LIVE connection was delivered (a cold image pull routinely outlasts
	// OpTimeout), so requeuing it would place a second copy elsewhere. The
	// row must stay `creating` and be settled by the runner's own news.
	t.Run("timeout on a live connection leaves the row creating", func(t *testing.T) {
		s, st, ts := newTestControld(t, func(c *Config) { c.OpTimeout = 150 * time.Millisecond })
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 2,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		id := "sess_timeout"
		row := seedSession(t, st, Session{ID: id, State: StateCreating, Runner: "vm1", Name: id, Image: "img:latest"})

		done := make(chan struct{})
		go func() {
			defer close(done)
			s.dispatchCreate(context.Background(), row, "vm1", nil)
		}()

		cmd := f.nextCmd(t)
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}
		// Never answer the dispatch: it must time out with the conn still up.

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("dispatchCreate did not return after OpTimeout elapsed")
		}
		got := getSession(t, st, id)
		if got.State != StateCreating || got.Runner != "vm1" {
			t.Fatalf("session = %+v, want still creating on vm1 (the create was delivered)", got)
		}

		// ...and the slow create eventually lands: the runner's own "running"
		// event settles the row without controld ever having requeued it.
		f.event(t, id, "running")
		wantState(t, st, id, StateRunning)
	})
}

// TestDrainQueueStopsWhenNoRunnerHasCapacity pins the "leave the rest
// queued" half of drainQueue directly, without needing a live dispatch: no
// connected runner at all means nothing is even attempted.
func TestDrainQueueStopsWhenNoRunnerHasCapacity(t *testing.T) {
	st := NewMemStore()
	s, err := New(st, Config{RunnerToken: "t", ExternalURL: "http://x:9090", SecretsKey: testSecretsKey})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	seedQueued(t, st, "sess_stuck", 0)
	s.drainQueue(context.Background())

	got := getSession(t, st, "sess_stuck")
	if got.State != StateQueued {
		t.Fatalf("state = %q, want still queued (no connected runner)", got.State)
	}
}
