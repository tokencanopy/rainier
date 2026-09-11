package runnerd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
)

// stallingStopDriver blocks inside a COLD suspend — the window in which the
// entry reads "suspending" and the container is still up — and counts the
// resumes it is asked for.
type stallingStopDriver struct {
	*driver.Fake
	once    sync.Once
	entered chan struct{} // closed when the stop has started
	release chan struct{} // closed by the test to let it finish
	resumes atomic.Int32
}

func newStallingStopDriver() *stallingStopDriver {
	return &stallingStopDriver{Fake: driver.NewFake(4),
		entered: make(chan struct{}), release: make(chan struct{})}
}

func (d *stallingStopDriver) Suspend(ctx context.Context, id string, warm bool) error {
	if !warm {
		d.once.Do(func() { close(d.entered) })
		<-d.release
	}
	return d.Fake.Suspend(ctx, id, warm)
}

func (d *stallingStopDriver) Resume(ctx context.Context, id string) (bool, error) {
	d.resumes.Add(1)
	return d.Fake.Resume(ctx, id)
}

// TestAResumeDuringAnInFlightStopIsRefused: a resume that overtakes a stop
// this runner has already claimed is the one ordering that can leave an entry
// claiming "running" over a container that is being stopped. `resumed` would
// see a non-running state, treat the entry as parked, clear the child's exit
// and bump the boot epoch; the stop would then land and finishColdSuspend's
// compare-and-swap would find the state moved and do nothing. The register
// goroutine reads the hub death that follows as a crash and destroys the
// container — a session reported dead because somebody resumed it.
//
// controld never sends this (it resumes only a suspended_* row), but the local
// dev surface can, so the refusal is a 409 there too.
func TestAResumeDuringAnInFlightStopIsRefused(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	sd := newStallingStopDriver()
	rd := New(sd, "", "", "")
	rd.now = clk.now
	h := &idleHarness{t: t, clk: clk, rd: rd, fd: sd.Fake, id: "sess-idle-1", boot: map[string]uint64{}}
	h.create(h.id)
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.clk.set(30 * time.Minute)

	swept := make(chan []string, 1)
	go func() { swept <- rd.sweepIdle(ctx, 30*time.Minute) }()
	<-sd.entered
	if got := h.entry(h.id).state; got != "suspending" {
		t.Fatalf("entry state mid-stop = %q, want %q", got, "suspending")
	}

	if err := rd.Op(ctx, h.id, "resume", false); !errors.Is(err, errSuspendInFlight) {
		t.Fatalf("resume during an in-flight stop = %v, want %v", err, errSuspendInFlight)
	}
	if got := sd.resumes.Load(); got != 0 {
		t.Fatalf("the driver was asked to resume %d time(s) during the stop; want 0", got)
	}
	if got := h.entry(h.id).state; got != "suspending" {
		t.Fatalf("entry state after the refused resume = %q, want %q", got, "suspending")
	}
	if e := h.entry(h.id); e.childExitedAt.IsZero() {
		t.Fatal("the refused resume cleared the child's exit")
	}

	// The same refusal on the dev HTTP surface, as a 409.
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/sessions/"+h.id+"/resume", "", nil)
	if err != nil {
		t.Fatalf("POST resume: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("POST resume during an in-flight stop = %d, want %d", resp.StatusCode, http.StatusConflict)
	}

	// The stop then lands where it always would have, and the session is
	// resumable afterwards — the refusal parks the resume, it does not break
	// the session.
	close(sd.release)
	if stops := <-swept; len(stops) != 1 || stops[0] != h.id {
		t.Fatalf("sweep stopped %v, want the session", stops)
	}
	if got := h.entry(h.id).state; got != "suspended" {
		t.Fatalf("entry state after the stop = %q, want %q", got, "suspended")
	}
	if got := h.state(); got != driver.StateSuspended {
		t.Fatalf("container state = %v, want %v", got, driver.StateSuspended)
	}
	if err := rd.Op(ctx, h.id, "resume", false); err != nil {
		t.Fatalf("resume after the stop landed: %v", err)
	}
	if got := h.entry(h.id).state; got != "running" {
		t.Fatalf("entry state after the resume = %q, want %q", got, "running")
	}
}

// TestAParkedStopThatCouldNotBeReadIsStillResumable is the other side of that
// refusal, and the reason it asks whether a stop is RUNNING rather than
// whether the state reads "suspending". A stop whose outcome the driver could
// not report deliberately leaves the entry parked on "suspending" — the
// conservative marker that keeps the register goroutine from destroying a
// container this runner cannot speak for — and no sweep ever re-claims it,
// because the idle rule requires "running". A resume is the only way back,
// so a guard keyed on the state alone would strand the session for the life of
// the runner.
func TestAParkedStopThatCouldNotBeReadIsStillResumable(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	ud := &unreadableStopDriver{Fake: driver.NewFake(4), fail: true}
	rd := New(ud, "", "", "")
	rd.now = clk.now
	h := &idleHarness{t: t, clk: clk, rd: rd, fd: ud.Fake, id: "sess-idle-1", boot: map[string]uint64{}}
	h.create(h.id)

	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.clk.set(time.Hour)
	if stops := rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("sweep reported %v stopped; the stop failed", stops)
	}
	if got := h.entry(h.id).state; got != "suspending" {
		t.Fatalf("state after an unreadable stop = %q, want %q", got, "suspending")
	}
	if rd.reg.stopInFlight(h.id) {
		t.Fatal("a stop that has already returned still reads as in flight")
	}

	ud.fail = false
	if err := rd.Op(ctx, h.id, "resume", false); err != nil {
		t.Fatalf("resume of a parked entry whose stop could not be read: %v", err)
	}
	if got := h.entry(h.id).state; got != "running" {
		t.Fatalf("entry state after the resume = %q, want %q", got, "running")
	}
}
