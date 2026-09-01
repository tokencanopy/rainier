package controlapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// LaunchMaterial is the sensitive, in-memory-only material a create needs
// that the scheduler must never persist: the resolved repositories, the git
// author identity, and the secret environment. It exists only between its
// resolver and the runner.ToRunner it is copied into.
type LaunchMaterial struct {
	Repos          []runner.RepoSpec
	GitAuthorName  string
	GitAuthorEmail string
	Environment    map[string]string
}

// LaunchMaterialResolver is the one real adapter seam this extraction
// introduces: self-hosted and Cloud resolve secret values and source-control
// attribution differently. It returns in-memory material only; values are
// never stored, logged, included in errors, events, or test output.
type LaunchMaterialResolver interface {
	ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (LaunchMaterial, error)
}

// Run hosts the single scheduler loop: it wakes on explicit wake calls and on
// a safety tick that re-drains every pool observed through an accepted
// registration or a wake. It returns when ctx is done.
func (s *FleetService) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.safetyInterval)
	defer ticker.Stop()
	pending := map[control.PoolID]struct{}{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pool := <-s.wake:
			pending[pool] = struct{}{}
		case <-ticker.C:
			// Safety passes cover pools observed through accepted runners.
			for pool := range s.knownPools() {
				pending[pool] = struct{}{}
			}
		}
		for pool := range pending {
			delete(pending, pool)
			s.drainPool(ctx, pool)
		}
	}
}

// runnerView is one connected runner's placement-relevant state.
type runnerView struct {
	id   control.RunnerID
	free int
	caps []string
}

// drainPool makes one FIFO placement pass over pool's queue: oldest session
// first, placed on the connected runner with the most free capacity. It stops
// the instant no runner has any room left, leaving the rest queued. Placement
// runs sequentially here; only the create dispatch that follows a successful
// placement runs concurrently.
func (s *FleetService) drainPool(ctx context.Context, pool control.PoolID) {
	rows, err := s.fleet.OldestQueued(ctx, pool)
	if err != nil {
		return
	}
	envs := map[control.EnvironmentID]*control.Environment{}
	for _, row := range rows {
		views, err := s.freeCapacity(ctx, pool)
		if err != nil {
			return
		}
		if _, any := pickRunner(views); !any {
			return // no capacity anywhere right now; leave the rest queued
		}
		env, ok := s.queuedEnvironment(ctx, envs, row)
		if !ok {
			continue // its environment is unreadable right now; try next pass
		}
		runnerID, ok := pickForEnvironment(views, env)
		if !ok {
			// The fleet has room, just not for this session's requirements.
			// Skipping the row rather than ending the pass keeps one blocked
			// session from holding up compatible ones behind it.
			continue
		}
		rid := runnerID
		if err := s.sessions.Transition(ctx, row.WorkspaceID, row.ID,
			[]control.SessionState{control.StateQueued}, control.StateCreating,
			control.TransitionOpts{RunnerID: &rid}); err != nil {
			if errors.Is(err, control.ErrConflict) || errors.Is(err, control.ErrNotFound) {
				continue // moved on under us
			}
			return
		}
		go s.dispatchCreate(ctx, pool, row, runnerID, env)
	}
}

// freeCapacity returns one runnerView per connected runner, free being
// CapacityTotal - CapacityUsed - the number of sessions still creating there.
func (s *FleetService) freeCapacity(ctx context.Context, pool control.PoolID) ([]runnerView, error) {
	runners, err := s.fleet.ListRunners(ctx, pool)
	if err != nil {
		return nil, err
	}
	views := make([]runnerView, 0, len(runners))
	for _, r := range runners {
		if !r.Connected {
			continue
		}
		creating, err := s.fleet.SessionsOnRunner(ctx, pool, r.ID, []control.SessionState{control.StateCreating})
		if err != nil {
			return nil, err
		}
		views = append(views, runnerView{
			id:   r.ID,
			free: r.CapacityTotal - r.CapacityUsed - len(creating),
			caps: r.Capabilities,
		})
	}
	return views, nil
}

// pickRunner returns the runner with the most free capacity, breaking ties by
// ascending RunnerID. A runner with zero or negative free is never a
// candidate.
func pickRunner(views []runnerView) (control.RunnerID, bool) {
	best := -1
	for i, r := range views {
		if r.free <= 0 {
			continue
		}
		if best == -1 || r.free > views[best].free || (r.free == views[best].free && r.id < views[best].id) {
			best = i
		}
	}
	if best == -1 {
		return "", false
	}
	return views[best].id, true
}

// pickForEnvironment narrows pickRunner to runners holding every portable
// capability the session's environment requires.
func pickForEnvironment(views []runnerView, env *control.Environment) (control.RunnerID, bool) {
	if env == nil {
		return pickRunner(views)
	}
	reqs := env.Requirements.Capabilities
	if len(reqs) == 0 {
		return pickRunner(views)
	}
	candidates := make([]runnerView, 0, len(views))
	for _, v := range views {
		if hasAllCapabilities(v.caps, reqs) {
			candidates = append(candidates, v)
		}
	}
	return pickRunner(candidates)
}

func hasAllCapabilities(caps, reqs []string) bool {
	for _, want := range reqs {
		if !slices.Contains(caps, want) {
			return false
		}
	}
	return true
}

// queuedEnvironment returns the environment row came from, memoized per pass.
// A scratch session, or one whose environment is gone, resolves to nil. The
// bool reports whether the lookup could be made at all.
func (s *FleetService) queuedEnvironment(ctx context.Context, cache map[control.EnvironmentID]*control.Environment, row control.Session) (*control.Environment, bool) {
	if row.EnvironmentID == "" {
		return nil, true
	}
	if env, seen := cache[row.EnvironmentID]; seen {
		return env, true
	}
	env, err := s.environments.GetEnvironment(ctx, row.WorkspaceID, row.EnvironmentID)
	switch {
	case errors.Is(err, control.ErrNotFound):
		cache[row.EnvironmentID] = nil
		return nil, true
	case err != nil:
		return nil, false
	}
	cache[row.EnvironmentID] = &env
	return &env, true
}

// dispatchCreate builds the create spec, pins setup provenance, dispatches,
// and settles the uncertain-delivery outcome without ever duplicating a
// delivered create.
func (s *FleetService) dispatchCreate(ctx context.Context, pool control.PoolID, row control.Session, runnerID control.RunnerID, env *control.Environment) {
	spec, fail := s.createSpec(ctx, row, env)
	if fail != "" {
		s.failCreate(ctx, row, fail)
		return
	}
	if !s.pinSetupHash(ctx, row, spec) {
		return
	}
	res, err := s.transport.Dispatch(ctx, pool, runnerID, runner.ToRunner{
		Type:    "create",
		Session: string(row.ID),
		Spec:    spec,
	})
	switch {
	case err != nil:
		if !s.transport.Connected(pool, runnerID) {
			// The command was never delivered: requeue with placement cleared
			// and wake the scheduler so another runner can take it.
			none := control.RunnerID("")
			s.transitionQuiet(ctx, row.WorkspaceID, row.ID,
				[]control.SessionState{control.StateCreating}, control.StateQueued,
				control.TransitionOpts{RunnerID: &none})
			s.Wake(pool)
		}
		// A failure on a still-live connection leaves the row creating: the
		// create may have been delivered, and it must never be duplicated.
	case !res.OK:
		s.failCreate(ctx, row, res.Detail)
	}
}

// createSpec builds the runner create spec from the session and its current
// environment, resolving sensitive launch material only here and never
// storing it.
func (s *FleetService) createSpec(ctx context.Context, row control.Session, env *control.Environment) (*runner.Spec, string) {
	spec := runner.Spec{
		Name:        row.Name,
		Image:       row.Spec.Image,
		Cmd:         slices.Clone(row.Spec.Cmd),
		EgressAllow: slices.Clone(row.Spec.EgressAllow),
	}
	if env != nil {
		if env.Snapshot.Ref != "" && env.SnapshotHash == env.SetupHash {
			spec.Image = env.Snapshot.Ref
		} else {
			spec.Setup = env.Setup
			spec.SetupTimeoutSec = env.SetupTimeoutSec
		}
		spec.Init = env.Init
		spec.InitTimeoutSec = env.InitTimeoutSec
	}
	material, err := s.launchMaterial.ResolveLaunchMaterial(ctx, row, env)
	if err != nil {
		return nil, "could not resolve launch material"
	}
	spec.Repos = slices.Clone(material.Repos)
	spec.GitAuthorName = material.GitAuthorName
	spec.GitAuthorEmail = material.GitAuthorEmail
	spec.Env = cloneMap(material.Environment)
	return &spec, ""
}

// pinSetupHash records which setup script a create is dispatching, before the
// command goes out. A create with no script pins nothing.
func (s *FleetService) pinSetupHash(ctx context.Context, row control.Session, spec *runner.Spec) bool {
	if spec.Setup == "" {
		return true
	}
	if err := s.sessions.SetSessionSetupHash(ctx, row.WorkspaceID, row.ID, dispatchSetupHash(spec.Image, spec.Setup)); err != nil {
		s.failCreate(ctx, row, "could not record the setup this session runs")
		return false
	}
	return true
}

// failCreate settles a create that will never happen and wakes the scheduler,
// because a creating row leaving its state frees its slot.
func (s *FleetService) failCreate(ctx context.Context, row control.Session, reason string) {
	bounded := boundDetail(reason)
	s.transitionQuiet(ctx, row.WorkspaceID, row.ID,
		[]control.SessionState{control.StateCreating}, control.StateFailed,
		control.TransitionOpts{Error: &bounded})
	s.Wake(row.PoolID)
}

// dispatchSetupHash is the identity of a session's dispatched setup inputs.
func dispatchSetupHash(image, setup string) string {
	h := sha256.Sum256([]byte(image + "\x00" + setup))
	return hex.EncodeToString(h[:])
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
