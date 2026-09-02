package controlapp

import (
	"context"
	"errors"
	"slices"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// SessionOptions carries the host-supplied dependencies of SessionService.
// Every field is required; NewSessionService refuses a missing dependency with
// control.ErrInvalid. Wake is called only after a queued session is durably
// stored, so the scheduler never learns about a session that did not commit.
type SessionOptions struct {
	Authorizer   control.Authorizer
	Sessions     control.SessionRepository
	Environments control.EnvironmentRepository
	Pools        control.PoolResolver
	Events       control.EventRecorder
	Clock        control.Clock
	IDs          control.IDGenerator
	Wake         func(control.PoolID)
	Fleet        control.FleetRepository
	Transport    control.RunnerTransport
}

// SessionService owns session creation, authorization, lifecycle dispatch, and
// result recording. It is independently constructible so it compiles before
// the aggregate control.Application composer exists.
type SessionService struct {
	auth         control.Authorizer
	sessions     control.SessionRepository
	environments control.EnvironmentRepository
	pools        control.PoolResolver
	events       control.EventRecorder
	clock        control.Clock
	ids          control.IDGenerator
	wake         func(control.PoolID)
	fleet        control.FleetRepository
	transport    control.RunnerTransport
}

// NewSessionService validates that every dependency is present and returns a
// SessionService with private fields only.
func NewSessionService(o SessionOptions) (*SessionService, error) {
	if o.Authorizer == nil || o.Sessions == nil || o.Environments == nil ||
		o.Pools == nil || o.Events == nil || o.Clock == nil || o.IDs == nil || o.Wake == nil ||
		o.Fleet == nil || o.Transport == nil {
		return nil, control.ErrInvalid
	}
	return &SessionService{
		auth:         o.Authorizer,
		sessions:     o.Sessions,
		environments: o.Environments,
		pools:        o.Pools,
		events:       o.Events,
		clock:        o.Clock,
		ids:          o.IDs,
		wake:         o.Wake,
		fleet:        o.Fleet,
		transport:    o.Transport,
	}, nil
}

// CreateSession authorizes the create, replays a matching idempotency key when
// one exists, resolves the environment within the authoritative workspace,
// selects the eligible pool with the most free capacity, stores the queued
// row, records one event, and only then wakes the chosen pool.
func (s *SessionService) CreateSession(ctx context.Context, scope control.Scope, cmd control.CreateSession) (control.Session, error) {
	if err := scope.Validate(); err != nil {
		return control.Session{}, control.ErrInvalid
	}
	if err := cmd.Validate(); err != nil {
		return control.Session{}, control.ErrInvalid
	}
	createResource := control.Resource{
		Kind:        control.ResourceSession,
		WorkspaceID: scope.WorkspaceID,
		CreatorID:   scope.Actor.ID,
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionCreate, createResource); err != nil {
		return control.Session{}, control.ErrDenied
	}

	if cmd.IdempotencyKey != "" {
		existing, err := s.sessions.SessionByIDem(ctx, scope.WorkspaceID, scope.Actor.ID, cmd.IdempotencyKey)
		switch {
		case err == nil:
			return sessionCloneSession(existing), nil
		case errors.Is(err, control.ErrNotFound):
			// Continue to create; this is a fresh key.
		default:
			return control.Session{}, control.ErrUnavailable
		}
	}

	requirements := control.Requirements{}
	var env control.Environment
	if cmd.EnvironmentID != "" {
		var err error
		env, err = s.environments.GetEnvironment(ctx, scope.WorkspaceID, cmd.EnvironmentID)
		if err != nil {
			if errors.Is(err, control.ErrNotFound) {
				return control.Session{}, control.ErrNotFound
			}
			return control.Session{}, control.ErrUnavailable
		}
		requirements = env.Requirements
	}

	poolID, err := s.selectPool(ctx, scope, requirements)
	if err != nil {
		return control.Session{}, err
	}

	id := s.ids.NewSessionID()
	if id == "" {
		return control.Session{}, control.ErrUnavailable
	}

	now := s.clock.Now()
	row := control.Session{
		ID:                  id,
		WorkspaceID:         scope.WorkspaceID,
		CreatorID:           scope.Actor.ID,
		Name:                cmd.Name,
		State:               control.StateQueued,
		EnvironmentID:       cmd.EnvironmentID,
		Spec:                portableSpecFor(cmd, env),
		PoolID:              poolID,
		PlacementGeneration: 1,
		IdempotencyKey:      cmd.IdempotencyKey,
		CreatedAt:           now,
		UpdatedAt:           now,
		LastEventAt:         now,
	}

	stored, err := s.sessions.CreateSession(ctx, scope.WorkspaceID, row)
	if err != nil {
		switch {
		case errors.Is(err, control.ErrConflict):
			return control.Session{}, control.ErrConflict
		case errors.Is(err, control.ErrNotFound):
			return control.Session{}, control.ErrNotFound
		default:
			return control.Session{}, control.ErrUnavailable
		}
	}

	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionCreate, sessionResource(stored)); err != nil {
		return control.Session{}, err
	}
	s.wake(stored.PoolID)
	return sessionCloneSession(stored), nil
}

// selectPool chooses the eligible pool with the greatest free capacity
// (CapacityTotal - CapacityUsed), breaking ties by ascending PoolID. A pool
// with no positive capacity is never chosen; none eligible yields
// control.ErrUnavailable.
func (s *SessionService) selectPool(ctx context.Context, scope control.Scope, req control.Requirements) (control.PoolID, error) {
	pools, err := s.pools.EligiblePools(ctx, scope, req)
	if err != nil {
		return "", control.ErrUnavailable
	}
	copied := slices.Clone(pools)
	best := -1
	for i := range copied {
		free := copied[i].CapacityTotal - copied[i].CapacityUsed
		if free <= 0 {
			continue
		}
		if best == -1 ||
			free > copied[best].CapacityTotal-copied[best].CapacityUsed ||
			(free == copied[best].CapacityTotal-copied[best].CapacityUsed && copied[i].ID < copied[best].ID) {
			best = i
		}
	}
	if best == -1 {
		return "", control.ErrUnavailable
	}
	return copied[best].ID, nil
}

// portableSpecFor resolves the stored execution description of a session by
// the layering rule control.PortableSpec documents: the environment is the
// template and the session's own spec overrides it field by field. A scratch
// session keeps its spec as sent — an empty image asks the host for its
// default. Repos always comes from cmd.Repos so the nil-versus-empty
// distinction is preserved.
func portableSpecFor(cmd control.CreateSession, env control.Environment) control.PortableSpec {
	spec := control.PortableSpec{
		Cmd:   cloneStrings(cmd.Spec.Cmd),
		Repos: cloneRepos(cmd.Repos),
	}
	if cmd.EnvironmentID == "" {
		spec.Image = cmd.Spec.Image
		spec.EgressAllow = cloneStrings(cmd.Spec.EgressAllow)
		return spec
	}
	switch {
	case cmd.Spec.Image != "":
		// An override boots its own image. The snapshot was built from the
		// environment's image and setup, so it cannot stand in for this one.
		spec.Image = cmd.Spec.Image
	case runsCachedSnapshot(env):
		spec.Image = env.Snapshot.Ref
	default:
		spec.Image = env.Image
	}
	spec.EgressAllow = unionHosts(env.EgressAllow, cmd.Spec.EgressAllow)
	return spec
}

// unionHosts returns base plus every host of extra it does not already
// contain, in order, leaving base itself untouched. Two nil inputs stay nil.
func unionHosts(base, extra []string) []string {
	out := slices.Clone(base)
	for _, h := range extra {
		if !slices.Contains(out, h) {
			out = append(out, h)
		}
	}
	return out
}

// runsCachedSnapshot reports whether env's cached snapshot is current and
// therefore the image a session from it should boot.
func runsCachedSnapshot(env control.Environment) bool {
	return env.Snapshot.Ref != "" && env.SnapshotHash == env.SetupHash
}

// GetSession reads the workspace-keyed row, authorizes it, and only then
// returns a copy. A row outside the authoritative workspace is ErrNotFound,
// never ErrDenied.
func (s *SessionService) GetSession(ctx context.Context, scope control.Scope, id control.SessionID) (control.Session, error) {
	if err := scope.Validate(); err != nil {
		return control.Session{}, control.ErrInvalid
	}
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, id)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionGet, sessionResource(row)); err != nil {
		return control.Session{}, control.ErrDenied
	}
	return sessionCloneSession(row), nil
}

// ListSessions authorizes the list against a workspace-scoped empty-ID session
// resource before touching the repository, then returns copied rows. An empty
// page is a non-nil empty slice.
func (s *SessionService) ListSessions(ctx context.Context, scope control.Scope, q control.SessionQuery) (control.SessionPage, error) {
	if err := scope.Validate(); err != nil {
		return control.SessionPage{}, control.ErrInvalid
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionList, control.Resource{
		Kind: control.ResourceSession, WorkspaceID: scope.WorkspaceID,
	}); err != nil {
		return control.SessionPage{}, control.ErrDenied
	}
	rows, next, err := s.sessions.ListSessions(ctx, scope.WorkspaceID, q)
	if err != nil {
		return control.SessionPage{}, control.ErrUnavailable
	}
	out := make([]control.Session, len(rows))
	for i, row := range rows {
		out[i] = sessionCloneSession(row)
	}
	return control.SessionPage{Sessions: out, NextCursor: next}, nil
}

// sessionResource names the session an authorization decision is about.
func sessionResource(row control.Session) control.Resource {
	return control.Resource{
		Kind:        control.ResourceSession,
		WorkspaceID: row.WorkspaceID,
		ID:          string(row.ID),
		CreatorID:   row.CreatorID,
	}
}

// recordEvent records one provider-neutral event after a successful mutation.
// An event-recording failure maps to ErrUnavailable while the persisted row
// remains authoritative.
func recordEvent(ctx context.Context, ids control.IDGenerator, events control.EventRecorder, clock control.Clock, scope control.Scope, action control.Action, res control.Resource) error {
	id := ids.NewEventID()
	if id == "" {
		return control.ErrUnavailable
	}
	if err := events.Record(ctx, control.Event{
		ID:          id,
		WorkspaceID: scope.WorkspaceID,
		ActorID:     scope.Actor.ID,
		Action:      action,
		Resource:    res,
		At:          clock.Now(),
	}); err != nil {
		return control.ErrUnavailable
	}
	return nil
}

// sessionCloneSession returns a deep copy of s so returned sessions can never
// mutate stored slices or pointer fields.
func sessionCloneSession(s control.Session) control.Session {
	out := s
	out.Spec = clonePortableSpec(s.Spec)
	if s.ChildExitCode != nil {
		code := *s.ChildExitCode
		out.ChildExitCode = &code
	}
	return out
}

func clonePortableSpec(s control.PortableSpec) control.PortableSpec {
	out := s
	out.Cmd = cloneStrings(s.Cmd)
	out.EgressAllow = cloneStrings(s.EgressAllow)
	out.Repos = cloneRepos(s.Repos)
	return out
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	return slices.Clone(in)
}

func cloneRepos(in []control.RepoRef) []control.RepoRef {
	if in == nil {
		return nil
	}
	return slices.Clone(in)
}

// dispatch sends msg to the runner that holds row, mapping an absent pool,
// absent runner, missing connection, transport failure, or a false runner
// result to the closed ErrUnavailable sentinel. The runner's own detail text
// never leaves this method.
func (s *SessionService) dispatch(ctx context.Context, row control.Session, msg runner.ToRunner) (runner.FromRunner, error) {
	if row.PoolID == "" || row.RunnerID == "" || !s.transport.Connected(row.PoolID, row.RunnerID) {
		return runner.FromRunner{}, control.ErrUnavailable
	}
	res, err := s.transport.Dispatch(ctx, row.PoolID, row.RunnerID, msg)
	if err != nil {
		return runner.FromRunner{}, control.ErrUnavailable
	}
	if !res.OK {
		return runner.FromRunner{}, control.ErrUnavailable
	}
	return res, nil
}

// authoritative re-reads a row after a runner side effect so callers return
// persisted state rather than a struct the service hoped to commit.
func (s *SessionService) authoritative(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	row, err := s.sessions.GetSession(ctx, ws, id)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, control.ErrUnavailable
	}
	return sessionCloneSession(row), nil
}

// reclaimWorkspace tells the runner holding row to remove its workspace volume
// after an explicit deletion. It is fire-and-forget cleanup: an absent volume
// is a success, and a reclaim failure never converts an already-successful
// destroy into an error.
func (s *SessionService) reclaimWorkspace(ctx context.Context, row control.Session) {
	if row.PoolID == "" || row.RunnerID == "" || !s.transport.Connected(row.PoolID, row.RunnerID) {
		return
	}
	_, _ = s.transport.Dispatch(ctx, row.PoolID, row.RunnerID, runner.ToRunner{Type: "remove_workspace", Session: string(row.ID)})
}

// DeleteSession applies the guarded delete state machine: queued cancels
// outright, creating is refused, a placed session is destroyed on its runner
// then destroyed in the store, a failed session is destroyed the same way, and
// any other terminal state is row-idempotent. Every success wakes the row's
// pool.
func (s *SessionService) DeleteSession(ctx context.Context, scope control.Scope, cmd control.DeleteSession) error {
	if err := scope.Validate(); err != nil {
		return control.ErrInvalid
	}
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, cmd.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.ErrNotFound
		}
		return control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionDelete, sessionResource(row)); err != nil {
		return control.ErrDenied
	}

	switch {
	case row.State.Terminal() && row.State != control.StateFailed:
		s.reclaimWorkspace(ctx, row)
		s.wake(row.PoolID)
		return nil
	case row.State == control.StateCreating:
		return control.ErrConflict
	case row.State == control.StateQueued:
		if err := s.sessions.Transition(ctx, scope.WorkspaceID, cmd.ID, []control.SessionState{control.StateQueued}, control.StateCanceled, control.TransitionOpts{}); err != nil {
			if !errors.Is(err, control.ErrConflict) && !errors.Is(err, control.ErrNotFound) {
				return control.ErrUnavailable
			}
			s.wake(row.PoolID)
			return nil
		}
		if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionDelete, sessionResource(row)); err != nil {
			return err
		}
		s.wake(row.PoolID)
		return nil
	}

	if _, err := s.dispatch(ctx, row, runner.ToRunner{Type: "destroy", Session: string(row.ID)}); err != nil {
		return control.ErrUnavailable
	}
	s.reclaimWorkspace(ctx, row)
	from := control.NonTerminal
	if row.State == control.StateFailed {
		from = []control.SessionState{control.StateFailed}
	}
	if err := s.sessions.Transition(ctx, scope.WorkspaceID, cmd.ID, from, control.StateDestroyed, control.TransitionOpts{}); err != nil {
		if !errors.Is(err, control.ErrConflict) && !errors.Is(err, control.ErrNotFound) {
			return control.ErrUnavailable
		}
		s.wake(row.PoolID)
		return nil
	}
	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionDelete, sessionResource(row)); err != nil {
		return err
	}
	s.wake(row.PoolID)
	return nil
}

// SuspendSession accepts only a running session, dispatches suspend before the
// guarded transition to warm/cold, records the event, and returns the
// authoritative persisted row.
func (s *SessionService) SuspendSession(ctx context.Context, scope control.Scope, cmd control.SuspendSession) (control.Session, error) {
	if err := scope.Validate(); err != nil {
		return control.Session{}, control.ErrInvalid
	}
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, cmd.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionSuspend, sessionResource(row)); err != nil {
		return control.Session{}, control.ErrDenied
	}
	if row.State != control.StateRunning {
		return control.Session{}, control.ErrConflict
	}

	if _, err := s.dispatch(ctx, row, runner.ToRunner{Type: "suspend", Session: string(row.ID), Warm: cmd.Warm}); err != nil {
		return control.Session{}, control.ErrUnavailable
	}

	to := control.StateSuspendedWarm
	if !cmd.Warm {
		to = control.StateSuspendedCold
	}
	if err := s.sessions.Transition(ctx, scope.WorkspaceID, cmd.ID, []control.SessionState{control.StateRunning}, to, control.TransitionOpts{}); err != nil {
		if !errors.Is(err, control.ErrConflict) && !errors.Is(err, control.ErrNotFound) {
			return control.Session{}, control.ErrUnavailable
		}
		s.wake(row.PoolID)
		return s.authoritative(ctx, scope.WorkspaceID, cmd.ID)
	}
	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionSuspend, sessionResource(row)); err != nil {
		return control.Session{}, err
	}
	s.wake(row.PoolID)
	return s.authoritative(ctx, scope.WorkspaceID, cmd.ID)
}

// ResumeSession accepts only a warm/cold suspended session. A cold resume must
// fit on the runner already holding the volume; dispatch precedes the guarded
// transition back to running.
func (s *SessionService) ResumeSession(ctx context.Context, scope control.Scope, cmd control.ResumeSession) (control.Session, error) {
	if err := scope.Validate(); err != nil {
		return control.Session{}, control.ErrInvalid
	}
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, cmd.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionResume, sessionResource(row)); err != nil {
		return control.Session{}, control.ErrDenied
	}
	if row.State != control.StateSuspendedWarm && row.State != control.StateSuspendedCold {
		return control.Session{}, control.ErrConflict
	}

	if row.State == control.StateSuspendedCold {
		free, err := s.coldResumeFree(ctx, row)
		if err != nil {
			return control.Session{}, control.ErrUnavailable
		}
		if free <= 0 {
			return control.Session{}, control.ErrConflict
		}
	}

	if _, err := s.dispatch(ctx, row, runner.ToRunner{Type: "resume", Session: string(row.ID)}); err != nil {
		return control.Session{}, control.ErrUnavailable
	}
	if err := s.sessions.Transition(ctx, scope.WorkspaceID, cmd.ID, []control.SessionState{row.State}, control.StateRunning, control.TransitionOpts{}); err != nil {
		if !errors.Is(err, control.ErrConflict) && !errors.Is(err, control.ErrNotFound) {
			return control.Session{}, control.ErrUnavailable
		}
		s.wake(row.PoolID)
		return s.authoritative(ctx, scope.WorkspaceID, cmd.ID)
	}
	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionResume, sessionResource(row)); err != nil {
		return control.Session{}, err
	}
	s.wake(row.PoolID)
	return s.authoritative(ctx, scope.WorkspaceID, cmd.ID)
}

// coldResumeFree computes free capacity on the runner already holding a cold
// session's volume: CapacityTotal - CapacityUsed - len(creating). A runner the
// pool no longer lists yields zero (no slot).
func (s *SessionService) coldResumeFree(ctx context.Context, row control.Session) (int, error) {
	runners, err := s.fleet.ListRunners(ctx, row.PoolID)
	if err != nil {
		return 0, err
	}
	for _, r := range runners {
		if r.ID != row.RunnerID {
			continue
		}
		creating, err := s.fleet.SessionsOnRunner(ctx, row.PoolID, row.RunnerID, []control.SessionState{control.StateCreating})
		if err != nil {
			return 0, err
		}
		return r.CapacityTotal - r.CapacityUsed - len(creating), nil
	}
	return 0, nil
}

// SnapshotSession allows snapshots only from running, warm-suspended, or
// cold-suspended sessions and returns a portable checkpoint. The opaque
// reference never enters a returned error or an event fact.
func (s *SessionService) SnapshotSession(ctx context.Context, scope control.Scope, cmd control.SnapshotSession) (control.Checkpoint, error) {
	if err := scope.Validate(); err != nil {
		return control.Checkpoint{}, control.ErrInvalid
	}
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, cmd.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Checkpoint{}, control.ErrNotFound
		}
		return control.Checkpoint{}, control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionSnapshot, sessionResource(row)); err != nil {
		return control.Checkpoint{}, control.ErrDenied
	}
	switch row.State {
	case control.StateRunning, control.StateSuspendedWarm, control.StateSuspendedCold:
	default:
		return control.Checkpoint{}, control.ErrConflict
	}

	res, err := s.dispatch(ctx, row, runner.ToRunner{Type: "snapshot", Session: string(row.ID)})
	if err != nil {
		return control.Checkpoint{}, control.ErrUnavailable
	}
	if res.Detail == "" {
		return control.Checkpoint{}, control.ErrUnavailable
	}
	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionSnapshot, sessionResource(row)); err != nil {
		return control.Checkpoint{}, err
	}
	return control.Checkpoint{Ref: res.Detail, Format: "rainier-runner-v0", Capabilities: []string{"workspace"}}, nil
}
