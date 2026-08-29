// internal/controld/sched.go
package controld

import (
	"context"
	"errors"
	"log"
	"time"

	"rainier/internal/rwire"
)

// runnerView is one connected runner's placement-relevant state: just
// enough for pickRunner to choose among them.
type runnerView struct {
	Name string
	Free int
}

// pickRunner returns the name of the runner with the most free capacity,
// breaking ties by lexicographically smaller name so placement is
// deterministic (design §4.7). It reports false when rs is empty or no
// runner has any free capacity — a runner with zero or negative Free
// (over-committed, which should not happen but must never be chosen if it
// does) is never a candidate.
func pickRunner(rs []runnerView) (string, bool) {
	best := -1
	for i, r := range rs {
		if r.Free <= 0 {
			continue
		}
		if best == -1 || r.Free > rs[best].Free || (r.Free == rs[best].Free && r.Name < rs[best].Name) {
			best = i
		}
	}
	if best == -1 {
		return "", false
	}
	return rs[best].Name, true
}

// pickForSession chooses the runner for one queued session, layering the two
// environment rules over pickRunner's most-free choice (design §4.6, §4.3):
//
//   - An environment's Placement PINS the session: the candidate set becomes
//     that one runner, and a pin whose runner is full or absent places nothing
//     rather than falling back to the fleet. That is the whole point of the
//     hint — a session that needs the GPU box is not better off elsewhere.
//   - A session whose resolved image IS its environment's cached snapshot
//     prefers the runner holding that snapshot, even over a runner with more
//     free capacity. v0 has no registry, so that runner is the only one where
//     the image exists at all; anywhere else the create would fail on a pull.
//
// env is nil for a scratch session (and for one whose environment has been
// deleted), which is exactly the pre-environments behavior.
func pickForSession(views []runnerView, row Session, env *Environment) (string, bool) {
	if env == nil {
		return pickRunner(views)
	}

	candidates := views
	if env.Placement != "" {
		candidates = nil
		for _, v := range views {
			if v.Name == env.Placement {
				candidates = append(candidates, v)
			}
		}
	}
	if runsCachedSnapshot(row, *env) && env.SnapshotRunner != "" {
		for _, v := range candidates {
			if v.Name == env.SnapshotRunner && v.Free > 0 {
				return v.Name, true
			}
		}
	}
	return pickRunner(candidates)
}

// runsCachedSnapshot reports whether row was resolved to env's cached
// snapshot image — the one case where the setup script must NOT run again
// (the cached image IS the finished setup), and the one that gives the
// session an affinity for the runner holding it.
func runsCachedSnapshot(row Session, env Environment) bool {
	return env.SnapshotRef != "" && row.ResolvedImage == env.SnapshotRef
}

// schedulerLoop wakes on capacity/queue news (the fast path) or a 10s
// safety tick (in case a wake was ever missed or coalesced away) and makes
// one placement pass over the queue each time. It returns when ctx is
// done, which is what lets Run(ctx) host it directly.
func (s *Server) schedulerLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second) // safety net; wakes are the fast path
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.schedWake:
		case <-t.C:
		}
		s.drainQueue(ctx)
	}
}

// drainQueue makes one FIFO placement pass over the queue: oldest session
// first, place it on whichever connected runner currently has the most
// free capacity, and start a goroutine to dispatch its create. It stops the
// instant no runner has any room left, leaving the remainder queued for the
// next wake — a burst that outgrows the fleet waits, it doesn't error.
//
// Placement — the free-capacity math and the queued->creating transition —
// runs sequentially in this one goroutine (schedulerLoop never overlaps
// calls), so two rows can never be handed the same last-open slot; free
// capacity is recomputed fresh from the store before each row, and a
// successful Transition is visible to that recomputation immediately, which
// is what keeps the math honest without any separate in-memory tally. Only
// the create dispatch that follows a successful placement runs
// concurrently, so one slow or wedged runner can't stall placement for the
// rest of the queue.
// A session started from an environment has its environment re-read HERE,
// during placement, rather than carrying a copy from create time: an
// environment edited while its sessions sat in the queue moves future
// placements and leaves the ones already `creating` alone (design §4.6).
func (s *Server) drainQueue(ctx context.Context) {
	rows, err := s.st.OldestQueued(ctx)
	if err != nil {
		log.Printf("controld: scheduler: listing queued sessions: %v", err)
		return
	}
	// One read per environment per pass, not per row: a burst is usually many
	// sessions of the same environment asking the same question.
	envs := map[string]*Environment{}
	for _, row := range rows {
		views, err := s.freeCapacity(ctx)
		if err != nil {
			log.Printf("controld: scheduler: computing free capacity: %v", err)
			return
		}
		if _, any := pickRunner(views); !any {
			return // no capacity anywhere right now; leave the rest queued
		}
		env, ok := s.queuedEnvironment(ctx, envs, row)
		if !ok {
			continue // its environment is unreadable right now; try again next pass
		}
		runner, ok := pickForSession(views, row, env)
		if !ok {
			// The fleet has room, just not where this session's environment
			// pins it. Skipping the row rather than ending the pass is what
			// keeps one blocked pin — a GPU box that is full, or has not joined
			// yet — from holding up every session queued behind it.
			continue
		}
		if err := s.st.Transition(ctx, row.ID, []SessionState{StateQueued}, StateCreating, TransitionOpts{Runner: &runner}); err != nil {
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				continue // canceled or already moved out from under us
			}
			log.Printf("controld: scheduler: placing %s on %s: %v", row.ID, runner, err)
			continue
		}
		go s.dispatchCreate(ctx, row, runner, env)
	}
}

// queuedEnvironment returns the environment row came from, memoized in cache
// for the life of one placement pass. A scratch session, or one whose
// environment has been deleted, resolves to nil: it keeps its own resolved
// image and is placed like any other session.
//
// The bool reports whether the lookup could be made at all. A store that
// cannot answer leaves the row queued for the next pass rather than placing it
// under the wrong rules — an environment read that failed looks exactly like
// "no pin, no cache affinity", which is precisely the placement a pinned
// session must not get.
func (s *Server) queuedEnvironment(ctx context.Context, cache map[string]*Environment, row Session) (*Environment, bool) {
	if row.EnvironmentID == "" {
		return nil, true
	}
	if env, seen := cache[row.EnvironmentID]; seen {
		return env, true
	}
	env, err := s.st.GetEnvironment(ctx, row.EnvironmentID)
	switch {
	case errors.Is(err, ErrNotFound):
		cache[row.EnvironmentID] = nil
		return nil, true
	case err != nil:
		log.Printf("controld: scheduler: get environment %s for %s: %v", row.EnvironmentID, row.ID, err)
		return nil, false
	}
	cache[row.EnvironmentID] = &env
	return &env, true
}

// freeCapacity returns one runnerView per connected runner. Free is
// CapacityTotal - CapacityUsed - the number of sessions this runner is
// currently `creating`: a row just placed here hasn't necessarily shown up
// in the runner's own reported Used yet (that arrives piggybacked on the
// runner's next message, whenever that is), so without subtracting it a
// burst of placements could overshoot every runner's real capacity before a
// single create has landed.
func (s *Server) freeCapacity(ctx context.Context) ([]runnerView, error) {
	runners, err := s.st.ListRunners(ctx)
	if err != nil {
		return nil, err
	}
	views := make([]runnerView, 0, len(runners))
	for _, r := range runners {
		if !r.Connected {
			continue
		}
		creating, err := s.st.SessionsOnRunner(ctx, r.Name, []SessionState{StateCreating})
		if err != nil {
			return nil, err
		}
		views = append(views, runnerView{Name: r.Name, Free: r.CapacityTotal - r.CapacityUsed - len(creating)})
	}
	return views, nil
}

// dispatchCreate sends row's create to runner and settles the outcome:
// ok:false fails the session with the runner's own detail text, and a
// transport failure — no connection, a dead one, a full send queue
// (ErrRunnerUnreachable) — puts the row back on the queue with its placement
// cleared and wakes the scheduler so it, or once it's back another runner,
// gets another chance.
//
// A timeout on a LIVE connection (ErrDispatchTimeout) is deliberately not
// requeued. The command was delivered, and a create that outlasts OpTimeout
// is usually one still pulling a cold image: requeuing it would place a
// second copy of the same session on another runner, and the first copy
// would survive — invisible to the store — until that runner's next announce
// reconciled it away, or, if it landed back on the same runner, leave a row
// stuck `creating` against a container that already exists. So the row is
// left `creating` and settled by whichever arrives first: the runner's
// "running" event, its next announce (adopt if the create landed, requeue if
// it didn't), or a later reconcile.
//
// The caller's own ctx being canceled (process shutdown) is left alone for
// the same reason — the row stays `creating` and the next announce
// reconciles it.
func (s *Server) dispatchCreate(ctx context.Context, row Session, runner string, env *Environment) {
	spec, fail := s.createSpec(ctx, row, env)
	if fail != "" {
		// Nothing was sent, so there is no container anywhere to reconcile
		// against: the session is simply not startable as described.
		log.Printf("controld: create %s on %s: %s", row.ID, runner, fail)
		s.failCreate(ctx, row.ID, fail)
		return
	}
	if !s.pinSetupHash(ctx, row, spec) {
		return
	}
	res, err := s.dispatch(ctx, runner, rwire.ToRunner{
		Type:    "create",
		Session: row.ID,
		Spec:    spec,
	})
	switch {
	case errors.Is(err, ErrDispatchTimeout):
		log.Printf("controld: create %s on %s: no result before the op timeout; leaving it creating for the runner's event or next announce to settle: %v",
			row.ID, runner, err)
	case errors.Is(err, ErrRunnerUnreachable):
		none := ""
		s.transitionQuiet(ctx, row.ID, []SessionState{StateCreating}, StateQueued, TransitionOpts{Runner: &none})
		s.wakeScheduler()
	case err != nil:
		log.Printf("controld: dispatch create %s to %s: %v", row.ID, runner, err)
	case !res.OK:
		s.failCreate(ctx, row.ID, res.Detail)
	}
}

// pinSetupHash records WHICH setup script this create is dispatching, and
// reports whether the dispatch may proceed.
//
// The pin has to happen here rather than at create time because createSpec
// deliberately derives the script from the environment as it stands at
// DISPATCH: a create-time copy could name a script the container never ran.
// Once the command goes out, this hash is the only trace of what was actually
// executed inside it — the environment can be edited a millisecond later, and
// the session row otherwise remembers only the image. Without it, a setup_done
// for a session that ran script v1 would be cached under the hash of v2 and
// hand every later session the wrong toolchain, silently.
//
// A create with no script pins nothing (there is no provenance to record and
// no snapshot to gate). A write that fails FAILS the session rather than
// dispatching anyway: a container whose provenance cannot be recorded must not
// be allowed to run a cacheable setup, and failing loudly costs one session
// where guessing would cost the environment.
func (s *Server) pinSetupHash(ctx context.Context, row Session, spec *rwire.Spec) bool {
	if spec.Setup == "" {
		return true
	}
	if err := s.st.SetSessionSetupHash(ctx, row.ID, SetupHash(spec.Image, spec.Setup)); err != nil {
		log.Printf("controld: create %s: recording the setup it runs: %v", row.ID, err)
		s.failCreate(ctx, row.ID, "could not record the setup this session runs")
		return false
	}
	return true
}

// failCreate settles a create that will never happen: the row goes terminal
// with reason, and the scheduler is woken because a `creating` row leaving
// that state gives its slot back to freeCapacity's math. Nothing else will
// deliver that news — the runner either never got the command or already
// answered — so without the wake the freed slot stays invisible until the
// 10s safety tick.
func (s *Server) failCreate(ctx context.Context, id, reason string) {
	s.transitionQuiet(ctx, id, []SessionState{StateCreating}, StateFailed, TransitionOpts{Error: &reason})
	s.wakeScheduler()
}

// createSpec builds the Spec of row's create command. Everything the
// environment contributes beyond the resolved image — the setup script, its
// timeout, and the decrypted secret environment — is derived HERE, at dispatch
// time, from the environment as it stands now: the session row deliberately
// stores none of it, so no secret value is ever written to the database in a
// second place and no queued session carries a stale copy of a script.
//
// It returns a client-facing failure reason rather than an error, because
// everything that can go wrong here fails the session and the text lands in
// the row's error column, which the API hands straight back to the caller.
// Internal detail is logged instead, and a secret VALUE appears in neither.
func (s *Server) createSpec(ctx context.Context, row Session, env *Environment) (*rwire.Spec, string) {
	spec := &rwire.Spec{
		Name:        row.Name,
		Image:       row.effectiveImage(),
		Cmd:         row.Cmd,
		EgressAllow: row.EgressAllow,
	}
	if env == nil {
		return spec, ""
	}

	// A session running the cached snapshot has its setup already baked into
	// that image; re-running the script would be wrong as well as slow.
	if env.Setup != "" && !runsCachedSnapshot(row, *env) {
		spec.Setup = env.Setup
		spec.SetupTimeoutSec = env.SetupTimeoutSec
		if spec.SetupTimeoutSec <= 0 {
			spec.SetupTimeoutSec = defaultSetupTimeoutSec
		}
	}

	vars, missing, err := s.secretEnv(ctx, *env)
	switch {
	case err != nil:
		log.Printf("controld: create %s: resolving secrets of environment %s: %v", row.ID, env.ID, err)
		return nil, "could not resolve the environment's secrets"
	case missing != "":
		// The create checked this and passed; the secret has been deleted
		// since. Failing loudly beats starting a container whose environment
		// promised a credential it does not have.
		return nil, missingSecretMessage(*env, missing)
	}
	spec.Env = vars
	return spec, ""
}
