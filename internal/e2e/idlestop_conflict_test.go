package e2e

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
)

// heldColdSuspend is a driver whose FIRST cold suspend does not return until
// the scene lets it. It is the only way to stand inside the window this scene
// is about — a runner that has claimed a stop and not yet landed it — without
// a real docker daemon to be slow for us.
type heldColdSuspend struct {
	*driver.Fake
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newHeldColdSuspend() *heldColdSuspend {
	return &heldColdSuspend{entered: make(chan struct{}), release: make(chan struct{})}
}

// wrap is startRunner's decorator hook: the Server runs on this, the scene
// goes on reading the fake underneath.
func (h *heldColdSuspend) wrap(fake *driver.Fake) driver.Driver {
	h.Fake = fake
	return h
}

func (h *heldColdSuspend) Suspend(ctx context.Context, id string, warm bool) error {
	if !warm {
		h.once.Do(func() { close(h.entered) })
		// ctx as well as the release, so a scene that fails before releasing
		// leaves no goroutine parked here for the rest of the package's run.
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return h.Fake.Suspend(ctx, id, warm)
}

// TestIdleAutoStopRefusesAResumeAsAConflict is the whole path for the ninth
// finding of the idle auto-stop review round, through every real component:
// a real controld on a real listener, a real runnerd dialing it over a real
// websocket, a scripted sessiond standing in for the container, and the real
// REST client a person's CLI uses.
//
// The scene is the one that makes this reachable from controld rather than
// only from the runner's dev surface, and it is ordinary rather than exotic
// because idle auto-stop is unattended: a runner's sweep claims an idle
// session and its `docker stop` is still running when the runner's control
// connection redials — a control plane rollout, a network blip. The runner
// announces that session as `suspended_cold` (announceState renders a
// "suspending" entry that way, deliberately: omitting a session controld
// believes is live is the one direction reconciliation cannot heal from), so
// reconciliation moves the row there — exactly the state a resume is accepted
// in — and the person's `rainier attach`, which resumes on their behalf, is
// dispatched into the window.
//
// What they get must be a CONFLICT: 409, code `conflict`, the handler's
// sentence, the row untouched, and the very same command succeeding once the
// stop lands. Before the fix it was 500 `internal`, "could not resume
// session", about a runner that was healthy and answering all along.
func TestIdleAutoStopRefusesAResumeAsAConflict(t *testing.T) {
	f := newFleet(t)
	held := newHeldColdSuspend()
	n := f.startRunner("vm-a", driver.NewFake(4), held.wrap)
	f.waitRunner("vm-a", true, 30*time.Second)

	created := f.create("finished-agent")
	rows := f.waitSessions(60*time.Second, "the session to run", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})
	if got := rows[created.ID].Runner; got != "vm-a" {
		t.Fatalf("session placed on %q, want vm-a", got)
	}

	// The agent finishes. Nobody is attached, so the session is now holding a
	// slot for nothing — which is the whole reason idle auto-stop exists.
	f.sessiond(created.ID).control(t, relay.ControlEvent{Kind: "child_exited", RC: 0})

	// The sweep, with a timeout short enough that "idle" is true the moment
	// the exit is recorded. Its stop is then held open by the driver.
	sweepCtx, stopSweep := context.WithCancel(context.Background())
	defer stopSweep()
	go n.rd.RunIdleStop(sweepCtx, time.Millisecond)
	select {
	case <-held.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("the idle sweep never reached the driver's cold suspend")
	}

	// The control plane is rolled while that stop is in flight. The runner
	// redials and announces the session as stopped, because that is what it is
	// in the middle of making it, and reconciliation moves the row.
	f.restartControld()
	f.waitRunner("vm-a", true, 30*time.Second)
	f.waitSessions(60*time.Second, "the row to follow the runner's announce", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "suspended_cold"
	})

	// `rainier attach` on a stopped session resumes it. The runner refuses:
	// it is already stopping that sandbox.
	err := f.client().Do(http.MethodPost, "/v0/sessions/"+created.ID+"/resume", nil, nil)
	if err == nil {
		t.Fatal("the resume was accepted while the runner was stopping the sandbox")
	}
	var api *cli.APIError
	if !errors.As(err, &api) {
		t.Fatalf("resume error = %v, want the API's own error envelope", err)
	}
	if api.Status != http.StatusConflict {
		t.Errorf("status = %d, want 409 — the runner is healthy and answering, and the same "+
			"command succeeds a second later", api.Status)
	}
	if api.Code != "conflict" {
		t.Errorf("code = %q, want conflict — this is what the CLI keys its bounded retry on", api.Code)
	}
	if api.Message != "session cannot be resumed right now" {
		t.Errorf("message = %q, want the handler's own sentence", api.Message)
	}

	// And nothing moved: the row still says stopped, so the session is not
	// left claiming to run on a container that is being stopped.
	after := f.list()[created.ID]
	if after.State != "suspended_cold" {
		t.Errorf("state after the refused resume = %q, want suspended_cold", after.State)
	}

	// The stop lands, and the refusal proves to have meant "not yet": the same
	// command, from the same client, brings the session back.
	//
	// Retried against the ROW rather than against the resume's own status,
	// because two known races sit in this window and neither is what the scene
	// is about. The resume can still arrive before `docker stop` has returned
	// (refused again, correctly); and the auto-stop's own `suspended_cold`
	// event — deliberately unfenced, since it carries no placement generation
	// (internal/runnerd/agent.go, and the PR's follow-up 2) — can land AFTER
	// the resume and put the row back to stopped over a container that is now
	// running. Both converge: the event fires once, and the next pass resumes
	// again.
	close(held.release)
	resumed := func() bool {
		if f.list()[created.ID].State == "running" {
			return true
		}
		// A refusal here is expected and retried; the assertion is the row.
		_ = f.client().Do(http.MethodPost, "/v0/sessions/"+created.ID+"/resume", nil, nil)
		return f.list()[created.ID].State == "running"
	}
	waitUntil(t, 60*time.Second, "the session to come back once the stop has landed", resumed)
}
