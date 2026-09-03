// internal/controld/runners.go
package controld

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/runnerplane"
)

// ErrRunnerUnreachable is what every dispatch that never produced an answer
// wraps: the runner has no control connection, the connection died before
// the result arrived, or OpTimeout elapsed first. Callers (the API in a
// later task) map it to the `runner_unreachable` error code — from the
// client's point of view those three are the same fact, "the runner did not
// answer", and the message text carries which one it was.
var ErrRunnerUnreachable = errors.New("runner unreachable")

// ErrDispatchTimeout is the one no-answer outcome that is NOT evidence the
// command went undelivered: OpTimeout elapsed while the control connection
// was still live, so the runner has the command and may well have executed
// it — controld just didn't hear back in time (a cold image pull routinely
// outlasts a 60s OpTimeout).
//
// It wraps ErrRunnerUnreachable rather than standing alone, deliberately, so
// that every existing errors.Is(err, ErrRunnerUnreachable) call site keeps
// its behavior and only callers that ask specifically see the distinction:
//
//   - api.go's destroy/suspend/resume/snapshot handlers (4 sites) keep
//     answering 502 `runner_unreachable`. That is the honest answer for a
//     timeout too — controld cannot confirm the op either way, and the row
//     is left exactly as the store had it, so the client's retry is against
//     unchanged state. Splitting a second client-visible code here would
//     tell the caller nothing it could act on differently.
//   - sched.go's dispatchCreate is the one caller that must branch: a
//     requeue on a delivered-but-unconfirmed create is what births a
//     duplicate container. See its doc comment.
var ErrDispatchTimeout = fmt.Errorf("timed out with the connection still live: %w", ErrRunnerUnreachable)

const (
	// storeCleanupTimeout bounds the store writes done after a snapshot is
	// built, which must not inherit the connection's context.
	storeCleanupTimeout = 5 * time.Second
	// snapshotRefHashLen is how much of an environment's setup hash goes into
	// its snapshot ref: 12 hex characters, enough that no fleet collides and
	// short enough to read in a `docker images` listing (design §4.3).
	snapshotRefHashLen = 12
)

// runnerHost is the host half of the runner plane: everything the plane
// deliberately does not decide, answered for this one installation. The
// plane owns the socket, the registry, the generations' two fences,
// reconciliation, and the dispatch correlation; what is left here is who a
// connection is, where its generations come from, and the two events and one
// upward request whose answers are host policy (the snapshot cache, the
// vault).
type runnerHost struct{ srv *Server }

var _ runnerplane.Host = runnerHost{}

// Identify authenticates a runner dial. The fleet token is fleet-wide, so it
// names no runner: the binding leaves RunnerID empty and the plane fills it
// from the announce, which is exactly how this installation has always
// identified a runner. It runs before the upgrade, so a bad token is a 401
// rather than a close frame.
func (h runnerHost) Identify(ctx context.Context, r *http.Request, name string) (runnerplane.Binding, error) {
	if !h.srv.runnerTokenOK(r.Header.Get("Authorization")) {
		return runnerplane.Binding{}, errRunnerUnauthorized
	}
	return runnerplane.Binding{
		WorkspaceID: installWorkspace,
		PoolID:      installPool,
		RunnerID:    control.RunnerID(name),
	}, nil
}

// errRunnerUnauthorized refuses a dial. It says nothing about the token it
// was presented with, and nothing reads its text — the plane answers 401.
var errRunnerUnauthorized = errors.New("unauthorized")

// NextGeneration mints the runner's next generation from the STORE, so it
// continues across a restart and no two replicas hand out the same authority.
func (h runnerHost) NextGeneration(ctx context.Context, b runnerplane.Binding) (uint64, error) {
	return h.srv.st.NextRunnerGeneration(ctx, b.PoolID, b.RunnerID)
}

func (h runnerHost) Fleet() control.Fleet                     { return h.srv.fleet }
func (h runnerHost) FleetRepository() control.FleetRepository { return h.srv.st.Fleet() }
func (h runnerHost) Wake(pool control.PoolID)                 { h.srv.fleet.Wake(pool) }

// Aside handles the two events that transition nothing: a finished setup,
// which is news about the environment's snapshot cache, and a rejected
// credential, which is news about the owner's stored token. Both are host
// policy — the image cache and the vault are controld's, not the service's.
func (h runnerHost) Aside(ctx context.Context, b runnerplane.Binding, gen uint64, m runner.FromRunner) {
	h.srv.applyAdapterArm(ctx, string(b.RunnerID), m)
}

// SessionRequest answers one sandbox-initiated request with the session
// owner's authority (srpc.go). The plane sets the answer's id and method and
// sends it back down.
func (h runnerHost) SessionRequest(ctx context.Context, b runnerplane.Binding, id control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope {
	return h.srv.authorizeSessionRequest(ctx, string(b.RunnerID), string(id), env)
}

// ---------------------------------------------------------------------------
// the plane, from the rest of controld
// ---------------------------------------------------------------------------

// sendToRunner queues a command whose result nobody waits for (the attach
// dial-back). It reports only whether the command was queued for delivery,
// under this package's own sentinel: the API maps it to the
// `runner_unreachable` code.
func (s *Server) sendToRunner(runner string, m runner.ToRunner) error {
	if err := s.plane.Send(installPool, control.RunnerID(runner), m); err != nil {
		return fmt.Errorf("%w: %w", err, ErrRunnerUnreachable)
	}
	return nil
}

// runnerConnected reports whether a runner currently holds a control
// connection to this replica.
func (s *Server) runnerConnected(name string) bool {
	return s.plane.Transport().Connected(installPool, control.RunnerID(name))
}

// applyAdapterArm handles the two events that transition nothing: a finished
// setup, which is news about the environment's snapshot cache, and a rejected
// credential, which is news about the owner's stored token. Both are host
// policy — the image cache and the vault are controld's, not the service's —
// and both need the session row, which they read directly (one of the direct
// store reads this composition keeps).
//
// Both keep the exact-placement guard. Without it the fleet-wide runner token
// would let the stale holder of a duplicate publish its own container as an
// environment's cache, or invalidate the credential of a user whose work it is
// no longer running.
func (s *Server) applyAdapterArm(ctx context.Context, name string, m runner.FromRunner) {
	row, err := s.st.Sessions().GetSession(ctx, installWorkspace, control.SessionID(m.Session))
	switch {
	case errors.Is(err, control.ErrNotFound):
		log.Printf("controld: runner %s: event for unknown session %s; ignoring", name, clip(m.Session))
		return
	case err != nil:
		log.Printf("controld: runner %s: event for %s: %v", name, clip(m.Session), err)
		return
	case !placedExactlyOn(row, name, m.State):
		return
	}

	switch m.State {
	case "setup_done":
		// Deliberately no transition: a finished setup is news about the
		// ENVIRONMENT, not about the session, whose state the registration
		// "running" event governs exactly as it does for a scratch session.
		s.cacheEnvironment(ctx, name, row)
	case "credential_rejected":
		// A git operation inside the sandbox was refused by GitHub. The vault
		// mints OPTIMISTICALLY — no GitHub round-trip per mint (design §4.2) —
		// so an observed refusal is the only signal a stored token has been
		// revoked, and this is where the fleet acts on it: the credential
		// flips to needs_refresh, so the next mint refuses with the named
		// action instead of handing out a value known not to work.
		//
		// The event carries nothing, deliberately: WHOSE credential it was is
		// the row's own answer, and a token — or anything derived from one —
		// has no business on this channel.
		if row.CreatorID == "" {
			log.Printf("controld: runner %s: credential_rejected for %s, which has no owner; ignoring", name, row.ID)
			return
		}
		s.rejectCredential(ctx, string(row.CreatorID), githubProvider)
	}
}

// placedExactlyOn reports whether the store places row on the runner now
// reporting about it, logging the mismatch when it doesn't.
//
// applyEvent's own guard is deliberately looser — it accepts a row the store
// places nowhere, because a "running" event can outrun the create's own
// queued→creating transition. The events that END a session (dead, a failed
// boot stage), publish a fleet-wide fact (setup_done), or act on the OWNER's
// behalf (credential_rejected) need the exact match instead: an unplaced row
// is one a requeue cleared and the scheduler may have re-placed elsewhere, so
// a stale holder must not be able to kill the live copy, to have its own
// container's image published as an environment's cache, or to invalidate a
// credential for work it is no longer running.
func placedExactlyOn(row control.Session, runner, state string) bool {
	if string(row.RunnerID) == runner {
		return true
	}
	log.Printf("controld: runner %s reported %s for %s, but the store places it on %q; ignoring",
		runner, clip(state), row.ID, row.RunnerID)
	return false
}

// snapshotRef mints an environment snapshot's image ref:
// rainier-env:<envID>-<first 12 hex of the setup hash> (design §4.3). It is
// content-addressed, so every replica derives the same name from the same
// build inputs and stale refs stay prunable by prefix.
func snapshotRef(envID, setupHash string) string {
	if len(setupHash) > snapshotRefHashLen {
		setupHash = setupHash[:snapshotRefHashLen]
	}
	return "rainier-env:" + envID + "-" + setupHash
}

// cacheEnvironment turns one session's finished setup script into its
// environment's cached snapshot (design §4.3): the runner that ran the script
// is asked to commit the container, the resulting ref is recorded against the
// environment under a guarded write, and the rest of the fleet is told to
// warm it.
//
// The decision to snapshot at all is made HERE, in the connection's reader,
// so that a no-op is complete by the time the event is handled. The work that
// follows must not be: dispatch waits for a result THIS reader is the one to
// deliver, so running it inline would deadlock the connection until OpTimeout
// and stall every other event and result the runner sends meanwhile.
func (s *Server) cacheEnvironment(ctx context.Context, runner string, row control.Session) {
	env, hash, ok := s.snapshotWanted(ctx, runner, row)
	if !ok {
		return
	}
	go s.buildSnapshot(ctx, runner, row, env, hash)
}

// snapshotWanted decides whether row's finished setup is worth snapshotting,
// returning the environment it belongs to and the setup hash to cache it
// under. Everything is read fresh at event time: the environment may have
// been edited, deleted, or cached by a sibling session since this one was
// created.
//
// Five answers are "no", each an ordinary outcome rather than a failure:
//
//   - a scratch session has no environment to cache;
//   - the environment is gone;
//   - it is already cached at exactly this hash — a sibling session won the
//     race, or a cold resume re-ran an idempotent script;
//   - the container is not built from the environment's own image, so the
//     image it would produce is not this environment's cache at all. A
//     session that overrode `image` still runs the setup script, and an
//     environment whose image moved after this session was created is in the
//     same position: publishing either under the environment's hash would
//     hand every later session an image nobody asked for;
//   - the setup this session actually ran is not the setup the environment
//     describes now — its pinned SetupHash says so. This is the edit that
//     lands WHILE the script runs: nothing about the row changes, the image
//     still matches, and only the pin can tell that the container holds the
//     old script. A row with no pin at all (dispatched before the column
//     existed) fails the same way, which is the safe direction.
//
// The last two divide the work between them: the image check catches an
// environment whose image moved (and a session that overrode it), the pin
// catches a script edit — and since the hash is f(image, setup), together
// they leave no edit uncovered.
func (s *Server) snapshotWanted(ctx context.Context, runner string, row control.Session) (control.Environment, string, bool) {
	if row.EnvironmentID == "" {
		log.Printf("controld: runner %s: setup finished for scratch session %s; nothing to cache", runner, row.ID)
		return control.Environment{}, "", false
	}
	env, err := s.st.Environments().GetEnvironment(ctx, installWorkspace, row.EnvironmentID)
	switch {
	case errors.Is(err, control.ErrNotFound):
		log.Printf("controld: runner %s: setup finished for %s, whose environment %s is gone; nothing to cache",
			runner, row.ID, clip(string(row.EnvironmentID)))
		return control.Environment{}, "", false
	case err != nil:
		log.Printf("controld: runner %s: setup finished for %s: reading environment %s: %v",
			runner, row.ID, clip(string(row.EnvironmentID)), err)
		return control.Environment{}, "", false
	}

	// Recomputed rather than read out of env.SetupHash: this hash is both the
	// cache key and the value the guarded store write is checked against, so
	// deriving it from the very two fields the snapshot's content comes from
	// is what keeps the pair honest.
	hash := SetupHash(env.Image, env.Setup)
	switch {
	case env.SnapshotHash == hash:
		log.Printf("controld: environment %s is already cached as %s; %s needs no snapshot",
			env.ID, env.Snapshot.Ref, row.ID)
		return control.Environment{}, "", false
	case row.Spec.Image != env.Image:
		log.Printf("controld: environment %s: not caching %s — it ran the setup over %q, not the environment's %q",
			env.ID, row.ID, clip(row.Spec.Image), clip(env.Image))
		return control.Environment{}, "", false
	case row.SetupHash != hash:
		log.Printf("controld: environment %s: not caching %s — the setup it ran predates an edit to the environment",
			env.ID, row.ID)
		return control.Environment{}, "", false
	}
	return env, hash, true
}

// buildSnapshot asks runner to commit row's container under the environment's
// content-addressed ref, records it, and warms the rest of the fleet. It runs
// in its own goroutine (see cacheEnvironment).
//
// Nothing here needs undoing when a step fails: an environment with no
// snapshot recorded is exactly an environment whose next session runs the
// setup script again — slower, never wrong.
func (s *Server) buildSnapshot(ctx context.Context, runnerName string, row control.Session, env control.Environment, hash string) {
	ref := snapshotRef(string(env.ID), hash)
	res, err := s.transport.Dispatch(ctx, installPool, control.RunnerID(runnerName),
		runner.ToRunner{Type: "snapshot", Session: string(row.ID), Ref: ref})
	switch {
	case err != nil:
		log.Printf("controld: snapshotting %s for environment %s on %s: %v", row.ID, env.ID, runnerName, err)
		return
	case !res.OK:
		log.Printf("controld: snapshotting %s for environment %s on %s: runner reported failure: %s",
			row.ID, env.ID, runnerName, clip(res.Detail))
		return
	case res.Detail != "" && res.Detail != ref:
		// A runner echoes the ref it was given (the driver contract returns an
		// explicit ref verbatim). One that doesn't is a runner bug worth
		// saying out loud — and the ref recorded stays OURS, because the
		// content-addressed name is what every other replica derives
		// independently and what a later create looks the image up by.
		log.Printf("controld: runner %s answered the snapshot of environment %s with ref %q, not the %q it was given; recording ours",
			runnerName, env.ID, clip(res.Detail), ref)
	}

	// Deliberately not under the connection's context: the image exists on the
	// runner now, and a connection dying in this instant must not cost the
	// fleet a rebuild. Bounded so a wedged store cannot leak this goroutine.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeCleanupTimeout)
	defer cancel()

	switch err := s.st.Environments().SetEnvironmentSnapshot(wctx, installWorkspace, env.ID, hash, ref, control.RunnerID(runnerName)); {
	case errors.Is(err, control.ErrStale), errors.Is(err, control.ErrNotFound):
		// The environment was edited (stale) or deleted (not found) while the
		// snapshot was building, so this image is of a setup nobody asked for
		// any more. The guarded write is precisely what keeps it from becoming
		// the cache (design §4.3); the next session rebuilds from the new
		// script.
		log.Printf("controld: environment %s changed while %s was being snapshotted; dropping %s",
			env.ID, row.ID, ref)
		return
	case err != nil:
		log.Printf("controld: recording snapshot %s for environment %s: %v", ref, env.ID, err)
		return
	}
	log.Printf("controld: environment %s cached as %s on %s", env.ID, ref, runnerName)

	// Warm every OTHER connected runner. The holder is excluded: it just built
	// the image, and with no registry in v0 that ref names something only it
	// has, so a prepull there could only fail. Fire-and-forget by design — a
	// prepull is a head start, never a precondition for a create (design §4.3).
	s.plane.Broadcast(installPool, runner.ToRunner{Type: "prepull", Ref: ref}, control.RunnerID(runnerName))
}

// runnerTokenOK compares the presented bearer against the fleet token in
// constant time. The scheme is matched exactly as runnerd sends it.
func (s *Server) runnerTokenOK(authz string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return false
	}
	got := []byte(strings.TrimPrefix(authz, prefix))
	return subtle.ConstantTimeCompare(got, []byte(s.cfg.RunnerToken)) == 1
}

// clip bounds runner-supplied text before it reaches a log line or a
// websocket close reason (the protocol caps those at 123 bytes), keeping the
// result valid UTF-8 even when the cut lands mid-rune.
func clip(s string) string {
	const max = 48
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "") + "..."
}
