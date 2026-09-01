// Package controlapp implements the portable application behavior behind the
// frozen public control contract. It depends only on the control ports and the
// public runner protocol; HTTP, identity, SQL, Docker, GitHub, provider, and
// billing behavior remain adapters outside the seam.
package controlapp

import (
	"context"
	"errors"
	"slices"

	"github.com/tokencanopy/rainier/control"
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
}

// NewSessionService validates that every dependency is present and returns a
// SessionService with private fields only.
func NewSessionService(o SessionOptions) (*SessionService, error) {
	if o.Authorizer == nil || o.Sessions == nil || o.Environments == nil ||
		o.Pools == nil || o.Events == nil || o.Clock == nil || o.IDs == nil || o.Wake == nil {
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
			return cloneSession(existing), nil
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
	return cloneSession(stored), nil
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

// portableSpecFor resolves the stored execution description of a session. An
// environment session inherits its resolved image (the current snapshot when
// SnapshotHash == SetupHash, else the plain image) and egress list; a scratch
// session keeps its own spec. Repos always comes from cmd.Repos so the
// nil-versus-empty distinction is preserved.
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
	if runsCachedSnapshot(env) {
		spec.Image = env.Snapshot.Ref
	} else {
		spec.Image = env.Image
	}
	spec.EgressAllow = cloneStrings(env.EgressAllow)
	return spec
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
	return cloneSession(row), nil
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
		out[i] = cloneSession(row)
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

// cloneSession returns a deep copy of s so returned sessions can never mutate
// stored slices or pointer fields.
func cloneSession(s control.Session) control.Session {
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
