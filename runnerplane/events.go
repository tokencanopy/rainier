package runnerplane

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const (
	// setupStage is the boot stage a `setup_failed` event names, and the stage
	// a `stage_failed` that names none is read as. A session's boot is a chain
	// — setup, then clone, then init — and every stage composes its failure
	// the same way; see stageFailure.
	setupStage = "setup"
	// unnamedStage stands in when a stage_failed's detail carries no stage in
	// front of it, which only a sender that is not following the contract can
	// produce. The session behind it really has stopped booting, so it still
	// fails — under a name that claims nothing about which stage it was.
	unnamedStage = "boot"
)

// touchRunner refreshes the runner row from any message's piggybacked
// capacity, reporting whether rc is still the registered connection — the
// caller stops reading when it isn't. The identity check and the write both
// happen under the runner's name lock, so a reconnect can neither slip between
// them nor have its own row write overtaken by this one.
func (p *Plane) touchRunner(ctx context.Context, rc *runnerConn, m runner.FromRunner) bool {
	nl := p.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	if !p.isCurrentConn(rc) {
		return false
	}
	err := p.host.FleetRepository().UpsertRunner(ctx, rc.binding.PoolID, control.Runner{
		ID:            rc.binding.RunnerID,
		PoolID:        rc.binding.PoolID,
		CapacityUsed:  m.Used,
		CapacityTotal: m.Total,
		Connected:     true,
		Generation:    rc.gen,
		Capabilities:  rc.caps,
		LastSeenAt:    time.Now(),
	})
	switch {
	case errors.Is(err, control.ErrStale):
		// Another replica (or a redial this process never saw) opened a newer
		// generation for this runner. Registration is not the only place
		// authority can be lost, so the heartbeat is the second fence: this
		// connection is superseded and must stop reading, which ends it and
		// closes the socket the runner will redial on.
		p.logf("runner %s: connection at generation %d is superseded", rc.name, rc.gen)
		return false
	case err != nil:
		p.logf("upsert runner %s: %v", rc.name, err)
	}
	return true
}

// applyRunnerEvent hands a runner's unsolicited state report to the fleet
// service, which owns every session transition an event can cause. This
// function's whole job is translation: runnerd's event vocabulary into the
// control one, and the runner's free text into the composed sentence a
// session's error column carries.
//
// Two arms never reach the service because they are not about the session at
// all — a finished setup is news about the ENVIRONMENT and a rejected
// credential is news about its OWNER — so they go to the host's Aside, which
// owns the vault and the snapshot cache.
//
// The event is stamped with the generation it was PRODUCED under — the one the
// runner echoed from its accept, or the connection's when it echoed none
// (eventGeneration) — never the runner's current one: a message that outlived
// its own socket must be fenced by the service (ErrStale), not applied under
// the generation of the connection that replaced it. touchRunner already drops
// such a message on the way in; this is the second lock on the same door, and
// the session's own placement generation, carried through to the service, is
// the third.
func (p *Plane) applyRunnerEvent(ctx context.Context, rc *runnerConn, m runner.FromRunner) {
	name := rc.name
	if m.Session == "" {
		p.logf("runner %s: event with no session", name)
		return
	}
	ev := control.RunnerEvent{
		WorkspaceID: rc.binding.WorkspaceID,
		PoolID:      rc.binding.PoolID,
		RunnerID:    rc.binding.RunnerID,
		Generation:  eventGeneration(rc, m),
		SessionID:   control.SessionID(m.Session),
		// Carried through untouched: the session's own authority is the
		// store's to check, not this plane's (the service fences it).
		PlacementGeneration: m.PlacementGeneration,
	}
	switch m.State {
	case "running":
		ev.State = control.StateRunning
	case "dead":
		ev.State = control.StateDead
	case "setup_failed":
		// The original name for what is now one stage among three, and one
		// that stays accepted forever: sessiond ships INSIDE the session image
		// while runnerd runs on the host, so a session whose image predates
		// the boot chain still reports its setup failure under this name.
		// m.Detail is already the composed "rc N: <tail>" with no stage in
		// front, so it goes to the stage arm as the stage it always meant.
		ev.State = control.StateFailed
		ev.Detail = stageFailure(setupStage, m.Detail)
	case "stage_failed":
		// One event for every stage of the boot chain. The stage rides at the
		// FRONT of the detail because a runner event has exactly one free-text
		// field: "clone: rc 128: fatal: Authentication failed" splits at the
		// first ": " into the stage and the sentence runnerd composed, which
		// is never parsed further — how a stage failure is described is the
		// runner's half of the contract.
		stage, rest, ok := strings.Cut(m.Detail, ": ")
		if !ok {
			stage, rest = unnamedStage, m.Detail
			p.logf("runner %s: stage_failed for %s named no stage (%q)",
				name, clip(m.Session), clip(m.Detail))
		}
		ev.State = control.StateFailed
		ev.Detail = stageFailure(stage, rest)
	case "child_exited":
		// An OBSERVATION, not a transition. The agent process ended but the
		// session did not: sessiond outlives its child so viewers can still
		// read the scrollback, so the container is up, attachable, and holding
		// its slot. ApplyRunnerEvent ignores State on a child-exit event;
		// Running is the state the row is expected to be in.
		code, err := strconv.Atoi(m.Detail)
		if err != nil {
			// Dropped rather than defaulted: Atoi's zero would land in the
			// column as a CLEAN exit, which is the most misleading value there
			// is. A number we cannot read is better recorded as no number at
			// all.
			p.logf("runner %s: child_exited for %s carried an unreadable code %q; ignoring",
				name, clip(m.Session), clip(m.Detail))
			return
		}
		ev.State = control.StateRunning
		ev.ChildExitCode = &code
	case "setup_done", "credential_rejected":
		p.host.Aside(ctx, rc.binding, ev.Generation, m)
		return
	default:
		p.logf("runner %s: unknown event state %q for %s", name, clip(m.State), clip(m.Session))
		return
	}
	if err := p.host.Fleet().ApplyRunnerEvent(ctx, ev); err != nil {
		// ErrStale and ErrConflict are the races reconciliation and events
		// have always had with each other — an event for a session this runner
		// no longer holds, or one the row has already moved past. They are
		// expected, not errors.
		p.logf("runner %s: event %s for %s not applied: %v",
			name, clip(m.State), clip(m.Session), err)
	}
}

// eventGeneration is the runner generation an event is applied under: the one
// the message itself claims, when it claims one, and otherwise the connection
// it arrived on.
//
// A runner that carries its granted generation is the more precise fence: a
// message can outlive the connection that produced it (it was queued, or the
// socket was replaced while it was in flight), and the claim travels with the
// message where the connection does not. Zero is an old runner, which claims
// nothing and is therefore judged by its socket exactly as before.
func eventGeneration(rc *runnerConn, m runner.FromRunner) uint64 {
	if m.Generation != 0 {
		return m.Generation
	}
	return rc.gen
}

// stageFailure composes what a failed boot stage reads as in a session's error
// column: "<stage> failed: rc N: <tail of the output>". The control plane
// writes the verdict, the runner supplies the evidence — front-loading the rc
// is what keeps the two halves legible as one sentence — and nothing here
// parses the rc back out.
//
// The stage is clipped because it is runner-supplied text being promoted to
// the first word of the plane's own sentence; every stage the contract defines
// (setup, clone, init) passes through untouched.
func stageFailure(stage, detail string) string {
	if detail == "" {
		return clip(stage) + " failed"
	}
	return clip(stage) + " failed: " + detail
}
