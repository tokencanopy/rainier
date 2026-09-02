package controlapp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tokencanopy/rainier/control"
)

// EnvironmentOptions carries the host-supplied dependencies of
// EnvironmentService. Every field is required.
type EnvironmentOptions struct {
	Authorizer   control.Authorizer
	Environments control.EnvironmentRepository
	Events       control.EventRecorder
	Clock        control.Clock
	IDs          control.IDGenerator
}

// EnvironmentService owns reusable environment CRUD and setup-checkpoint
// invalidation behind the frozen control.Environments interface.
type EnvironmentService struct {
	auth         control.Authorizer
	environments control.EnvironmentRepository
	events       control.EventRecorder
	clock        control.Clock
	ids          control.IDGenerator
}

// NewEnvironmentService returns ErrInvalid for a missing dependency.
func NewEnvironmentService(o EnvironmentOptions) (*EnvironmentService, error) {
	if o.Authorizer == nil || o.Environments == nil || o.Events == nil || o.Clock == nil || o.IDs == nil {
		return nil, control.ErrInvalid
	}
	return &EnvironmentService{
		auth:         o.Authorizer,
		environments: o.Environments,
		events:       o.Events,
		clock:        o.Clock,
		ids:          o.IDs,
	}, nil
}

// CreateEnvironment validates every scalar bound before generating an ID,
// authorizes the create, copies every input, computes SetupHash, stores,
// records, and returns a copy.
func (s *EnvironmentService) CreateEnvironment(ctx context.Context, scope control.Scope, cmd control.CreateEnvironment) (control.Environment, error) {
	if err := scope.Validate(); err != nil {
		return control.Environment{}, control.ErrInvalid
	}
	if err := validateEnvironment(cmd.Name, cmd.Image, cmd.SetupTimeoutSec, cmd.InitTimeoutSec,
		cmd.Requirements, cmd.SecretRefs, cmd.Connectors); err != nil {
		return control.Environment{}, control.ErrInvalid
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionCreate, control.Resource{
		Kind: control.ResourceEnvironment, WorkspaceID: scope.WorkspaceID,
	}); err != nil {
		return control.Environment{}, control.ErrDenied
	}

	id := s.ids.NewEnvironmentID()
	if id == "" {
		return control.Environment{}, control.ErrUnavailable
	}
	now := s.clock.Now()
	row := control.Environment{
		ID:              id,
		WorkspaceID:     scope.WorkspaceID,
		Name:            cmd.Name,
		Image:           cmd.Image,
		Setup:           cmd.Setup,
		SetupHash:       setupHash(cmd.Image, cmd.Setup),
		Init:            cmd.Init,
		InitTimeoutSec:  cmd.InitTimeoutSec,
		EgressAllow:     cloneStrings(cmd.EgressAllow),
		SecretRefs:      cloneStrings(cmd.SecretRefs),
		Connectors:      cloneConnectors(cmd.Connectors),
		Requirements:    cloneRequirements(cmd.Requirements),
		SetupTimeoutSec: cmd.SetupTimeoutSec,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	stored, err := s.environments.CreateEnvironment(ctx, scope.WorkspaceID, row)
	if err != nil {
		switch {
		case errors.Is(err, control.ErrConflict):
			return control.Environment{}, control.ErrConflict
		case errors.Is(err, control.ErrNotFound):
			return control.Environment{}, control.ErrNotFound
		default:
			return control.Environment{}, control.ErrUnavailable
		}
	}
	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionCreate, environmentResource(stored)); err != nil {
		return control.Environment{}, err
	}
	return sessionCloneEnvironment(stored), nil
}

// GetEnvironment reads by (workspace, id), authorizes ActionGet, and returns a
// copy. A row outside the authoritative workspace is ErrNotFound.
func (s *EnvironmentService) GetEnvironment(ctx context.Context, scope control.Scope, id control.EnvironmentID) (control.Environment, error) {
	if err := scope.Validate(); err != nil {
		return control.Environment{}, control.ErrInvalid
	}
	row, err := s.environments.GetEnvironment(ctx, scope.WorkspaceID, id)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Environment{}, control.ErrNotFound
		}
		return control.Environment{}, control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionGet, environmentResource(row)); err != nil {
		return control.Environment{}, control.ErrDenied
	}
	return sessionCloneEnvironment(row), nil
}

// ListEnvironments authorizes before repository access and normalizes an empty
// page to a non-nil empty slice.
func (s *EnvironmentService) ListEnvironments(ctx context.Context, scope control.Scope, q control.EnvironmentQuery) (control.EnvironmentPage, error) {
	if err := scope.Validate(); err != nil {
		return control.EnvironmentPage{}, control.ErrInvalid
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionList, control.Resource{
		Kind: control.ResourceEnvironment, WorkspaceID: scope.WorkspaceID,
	}); err != nil {
		return control.EnvironmentPage{}, control.ErrDenied
	}
	rows, next, err := s.environments.ListEnvironments(ctx, scope.WorkspaceID, q)
	if err != nil {
		return control.EnvironmentPage{}, control.ErrUnavailable
	}
	out := make([]control.Environment, len(rows))
	for i, row := range rows {
		out[i] = sessionCloneEnvironment(row)
	}
	return control.EnvironmentPage{Environments: out, NextCursor: next}, nil
}

// UpdateEnvironment reads the current row, authorizes ActionUpdate, applies
// only non-nil fields, validates the complete result, recomputes SetupHash,
// and preserves the old Snapshot and SnapshotHash so a hash change leaves the
// snapshot visibly stale rather than erased.
func (s *EnvironmentService) UpdateEnvironment(ctx context.Context, scope control.Scope, cmd control.UpdateEnvironment) (control.Environment, error) {
	if err := scope.Validate(); err != nil {
		return control.Environment{}, control.ErrInvalid
	}
	cur, err := s.environments.GetEnvironment(ctx, scope.WorkspaceID, cmd.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.Environment{}, control.ErrNotFound
		}
		return control.Environment{}, control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionUpdate, environmentResource(cur)); err != nil {
		return control.Environment{}, control.ErrDenied
	}

	next := sessionCloneEnvironment(cur)
	if cmd.Name != nil {
		next.Name = *cmd.Name
	}
	if cmd.Image != nil {
		next.Image = *cmd.Image
	}
	if cmd.Setup != nil {
		next.Setup = *cmd.Setup
	}
	if cmd.Init != nil {
		next.Init = *cmd.Init
	}
	if cmd.InitTimeoutSec != nil {
		next.InitTimeoutSec = *cmd.InitTimeoutSec
	}
	if cmd.EgressAllow != nil {
		next.EgressAllow = cloneStrings(*cmd.EgressAllow)
	}
	if cmd.SecretRefs != nil {
		next.SecretRefs = cloneStrings(*cmd.SecretRefs)
	}
	if cmd.Connectors != nil {
		next.Connectors = cloneConnectors(*cmd.Connectors)
	}
	if cmd.Requirements != nil {
		next.Requirements = cloneRequirements(*cmd.Requirements)
	}
	if cmd.SetupTimeoutSec != nil {
		next.SetupTimeoutSec = *cmd.SetupTimeoutSec
	}

	if err := validateEnvironment(next.Name, next.Image, next.SetupTimeoutSec, next.InitTimeoutSec,
		next.Requirements, next.SecretRefs, next.Connectors); err != nil {
		return control.Environment{}, control.ErrInvalid
	}
	next.SetupHash = setupHash(next.Image, next.Setup)

	stored, err := s.environments.UpdateEnvironment(ctx, scope.WorkspaceID, next)
	if err != nil {
		switch {
		case errors.Is(err, control.ErrConflict):
			return control.Environment{}, control.ErrConflict
		case errors.Is(err, control.ErrNotFound):
			return control.Environment{}, control.ErrNotFound
		default:
			return control.Environment{}, control.ErrUnavailable
		}
	}
	if err := recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionUpdate, environmentResource(stored)); err != nil {
		return control.Environment{}, err
	}
	return sessionCloneEnvironment(stored), nil
}

// DeleteEnvironment reads and authorizes the environment, then refuses the
// delete while any non-terminal session still references it. A repository
// ErrNotFound remains ErrNotFound even if another workspace holds the same
// opaque ID.
func (s *EnvironmentService) DeleteEnvironment(ctx context.Context, scope control.Scope, cmd control.DeleteEnvironment) error {
	if err := scope.Validate(); err != nil {
		return control.ErrInvalid
	}
	row, err := s.environments.GetEnvironment(ctx, scope.WorkspaceID, cmd.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.ErrNotFound
		}
		return control.ErrUnavailable
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionDelete, environmentResource(row)); err != nil {
		return control.ErrDenied
	}

	n, err := s.environments.CountSessionsByEnvironment(ctx, scope.WorkspaceID, cmd.ID, control.NonTerminal)
	if err != nil {
		return control.ErrUnavailable
	}
	if n != 0 {
		return control.ErrConflict
	}
	if err := s.environments.DeleteEnvironment(ctx, scope.WorkspaceID, cmd.ID); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.ErrNotFound
		}
		return control.ErrUnavailable
	}
	return recordEvent(ctx, s.ids, s.events, s.clock, scope, control.ActionDelete, environmentResource(row))
}

// environmentResource names the environment an authorization decision is
// about. Environments carry no creator.
func environmentResource(e control.Environment) control.Resource {
	return control.Resource{
		Kind:        control.ResourceEnvironment,
		WorkspaceID: e.WorkspaceID,
		ID:          string(e.ID),
	}
}

// validateEnvironment applies the exact, provider-neutral bounds shared by
// create and patch.
func validateEnvironment(name, image string, setupTimeout, initTimeout int, req control.Requirements, secretRefs []string, connectors []control.Connector) error {
	if name == "" || image == "" {
		return control.ErrInvalid
	}
	if setupTimeout < 0 || initTimeout < 0 {
		return control.ErrInvalid
	}
	if req.MinCPU < 0 || req.MinMemoryBytes < 0 || req.MinDiskBytes < 0 {
		return control.ErrInvalid
	}
	for _, c := range req.Capabilities {
		if c == "" {
			return control.ErrInvalid
		}
	}
	for _, ref := range secretRefs {
		if ref == "" {
			return control.ErrInvalid
		}
	}
	for _, c := range connectors {
		if c.Type == "" || !json.Valid(c.Raw) {
			return control.ErrInvalid
		}
	}
	return nil
}

// sessionCloneEnvironment returns a deep copy of e so returned environments
// can never mutate stored slices or connector bytes.
func sessionCloneEnvironment(e control.Environment) control.Environment {
	out := e
	out.EgressAllow = cloneStrings(e.EgressAllow)
	out.SecretRefs = cloneStrings(e.SecretRefs)
	out.Connectors = cloneConnectors(e.Connectors)
	out.Requirements = cloneRequirements(e.Requirements)
	out.Snapshot = copyCheckpoint(e.Snapshot)
	return out
}

func cloneConnectors(in []control.Connector) []control.Connector {
	if in == nil {
		return nil
	}
	out := make([]control.Connector, len(in))
	for i, c := range in {
		out[i] = c
		out[i].Raw = cloneBytes(c.Raw)
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneRequirements(r control.Requirements) control.Requirements {
	out := r
	out.Capabilities = cloneStrings(r.Capabilities)
	return out
}

func copyCheckpoint(c control.Checkpoint) control.Checkpoint {
	out := c
	out.Capabilities = cloneStrings(c.Capabilities)
	return out
}
