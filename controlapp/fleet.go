// Package controlapp implements the provider-neutral fleet application
// service behind the frozen control.Fleet interface. One deep FleetService
// module owns fleet truth and within-pool scheduling; injected adapters own
// connections, persistence, eligible-pool policy, sensitive launch material,
// and provider execution.
package controlapp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tokencanopy/rainier/control"
)

// FleetOptions carries the host-supplied ports the fleet service depends on.
// Every port is required; NewFleetService rejects a missing port or a
// non-positive safety interval.
type FleetOptions struct {
	Authorizer     control.Authorizer
	Sessions       control.SessionRepository
	Environments   control.EnvironmentRepository
	Fleet          control.FleetRepository
	Pools          control.PoolResolver
	Transport      control.RunnerTransport
	Events         control.EventRecorder
	Clock          control.Clock
	IDs            control.IDGenerator
	SafetyInterval time.Duration

	// LaunchMaterial resolves sensitive launch material (repositories, git
	// attribution, secret environment) at dispatch time. It is the one real
	// adapter seam this extraction introduces.
	LaunchMaterial LaunchMaterialResolver
}

// FleetService implements control.Fleet and owns runner registration,
// generation-fenced reconciliation, stale-safe event application, and
// within-pool FIFO scheduling.
type FleetService struct {
	auth           control.Authorizer
	sessions       control.SessionRepository
	environments   control.EnvironmentRepository
	fleet          control.FleetRepository
	pools          control.PoolResolver
	transport      control.RunnerTransport
	events         control.EventRecorder
	clock          control.Clock
	ids            control.IDGenerator
	safetyInterval time.Duration

	// launchMaterial resolves sensitive launch material at dispatch time.
	launchMaterial LaunchMaterialResolver

	// wake carries pool IDs that need a placement pass. It is buffered so
	// Wake never blocks a caller; Run drains it.
	wake chan control.PoolID

	// knownMu guards known, the set of pool IDs observed through accepted
	// registrations and wake calls. The safety tick uses it to re-drain pools
	// even when a wake was coalesced away.
	knownMu sync.Mutex
	known   map[control.PoolID]struct{}
}

// NewFleetService builds a FleetService. It requires every port and a
// positive safety interval. No goroutine starts in the constructor.
func NewFleetService(opts FleetOptions) (*FleetService, error) {
	if opts.Authorizer == nil || opts.Sessions == nil || opts.Environments == nil ||
		opts.Fleet == nil || opts.Pools == nil || opts.Transport == nil ||
		opts.Events == nil || opts.Clock == nil || opts.IDs == nil {
		return nil, control.ErrInvalid
	}
	if opts.SafetyInterval <= 0 {
		return nil, control.ErrInvalid
	}
	if opts.LaunchMaterial == nil {
		return nil, control.ErrInvalid
	}
	return &FleetService{
		auth:           opts.Authorizer,
		sessions:       opts.Sessions,
		environments:   opts.Environments,
		fleet:          opts.Fleet,
		pools:          opts.Pools,
		transport:      opts.Transport,
		events:         opts.Events,
		clock:          opts.Clock,
		ids:            opts.IDs,
		safetyInterval: opts.SafetyInterval,
		launchMaterial: opts.LaunchMaterial,
		wake:           make(chan control.PoolID, 64),
		known:          make(map[control.PoolID]struct{}),
	}, nil
}

// Wake requests a placement pass for pool. It never blocks: when the wake
// buffer is full the request is dropped, because the safety tick re-drains
// every known pool anyway.
func (s *FleetService) Wake(pool control.PoolID) {
	if pool == "" {
		return
	}
	s.knownMu.Lock()
	s.known[pool] = struct{}{}
	s.knownMu.Unlock()
	select {
	case s.wake <- pool:
	default:
	}
}

// knownPools returns a copied snapshot of every pool ID observed through an
// accepted registration or a wake call.
func (s *FleetService) knownPools() map[control.PoolID]struct{} {
	s.knownMu.Lock()
	defer s.knownMu.Unlock()
	out := make(map[control.PoolID]struct{}, len(s.known))
	for p := range s.known {
		out[p] = struct{}{}
	}
	return out
}

// RegisterRunner accepts a runner's registration claim. A claim older than
// the store-authoritative generation is refused without touching the store;
// a claim at or above it is converted into a copied Runner marked connected
// and upserted. The claimed session list is deliberately not applied here:
// ReconcileRunner owns that behavior and may run immediately after.
func (s *FleetService) RegisterRunner(ctx context.Context, r control.RunnerRegistration) (control.RunnerRegistrationResult, error) {
	if err := validateRegistration(r); err != nil {
		return control.RunnerRegistrationResult{}, err
	}
	runners, err := s.fleet.ListRunners(ctx, r.PoolID)
	if err != nil {
		return control.RunnerRegistrationResult{}, err
	}
	current := uint64(0)
	found := false
	for _, existing := range runners {
		if existing.ID == r.RunnerID {
			current = existing.Generation
			found = true
			break
		}
	}
	if found && r.Generation < current {
		return control.RunnerRegistrationResult{Accepted: false, Generation: current}, nil
	}

	runner := control.Runner{
		ID:            r.RunnerID,
		PoolID:        r.PoolID,
		CapacityUsed:  r.CapacityUsed,
		CapacityTotal: r.CapacityTotal,
		Connected:     true,
		Generation:    r.Generation,
		Capabilities:  slices.Clone(r.Capabilities),
		LastSeenAt:    s.clock.Now(),
	}
	if err := s.fleet.UpsertRunner(ctx, r.PoolID, runner); err != nil {
		if errors.Is(err, control.ErrStale) {
			// A concurrent higher-generation write won the race; refuse this
			// claim without disturbing it.
			return control.RunnerRegistrationResult{Accepted: false, Generation: r.Generation}, nil
		}
		return control.RunnerRegistrationResult{}, err
	}
	s.Wake(r.PoolID)
	return control.RunnerRegistrationResult{Accepted: true, Generation: r.Generation}, nil
}

// validateRegistration rejects a malformed or contradictory claim before any
// port is touched.
func validateRegistration(r control.RunnerRegistration) error {
	if r.WorkspaceID == "" || r.PoolID == "" || r.RunnerID == "" || r.Generation == 0 ||
		r.CapacityUsed < 0 || r.CapacityTotal < 0 || r.CapacityUsed > r.CapacityTotal {
		return control.ErrInvalid
	}
	seen := make(map[string]struct{}, len(r.Capabilities))
	for _, c := range r.Capabilities {
		if _, dup := seen[c]; dup {
			return control.ErrInvalid
		}
		seen[c] = struct{}{}
	}
	return nil
}

// ListRunners is the ordinary scoped query half of the fleet contract. It
// authorizes first, asks the pool resolver for every pool visible to the
// scope, merges those pools' runners, and returns them copied and sorted by
// (RunnerID, PoolID) with cursor pagination. No provider filter exists.
func (s *FleetService) ListRunners(ctx context.Context, scope control.Scope, q control.RunnerQuery) (control.RunnerPage, error) {
	if err := scope.Validate(); err != nil {
		return control.RunnerPage{}, err
	}
	if err := s.auth.Authorize(ctx, scope, control.ActionList, control.Resource{
		Kind:        control.ResourceRunner,
		WorkspaceID: scope.WorkspaceID,
	}); err != nil {
		return control.RunnerPage{}, control.ErrDenied
	}

	limit := q.Limit
	switch {
	case limit < 0:
		return control.RunnerPage{}, control.ErrInvalid
	case limit == 0:
		limit = 50
	case limit > 100:
		limit = 100
	}

	var afterRunner, afterPool string
	if q.Cursor != "" {
		var err error
		afterRunner, afterPool, err = decodeRunnerCursor(q.Cursor)
		if err != nil {
			return control.RunnerPage{}, control.ErrInvalid
		}
	}

	pools, err := s.pools.EligiblePools(ctx, scope, control.Requirements{})
	if err != nil {
		return control.RunnerPage{}, err
	}

	seenPools := make(map[control.PoolID]struct{}, len(pools))
	var all []control.Runner
	for _, pool := range pools {
		if _, dup := seenPools[pool.ID]; dup {
			continue
		}
		seenPools[pool.ID] = struct{}{}
		runners, err := s.fleet.ListRunners(ctx, pool.ID)
		if err != nil {
			return control.RunnerPage{}, err
		}
		for _, r := range runners {
			r.Capabilities = slices.Clone(r.Capabilities)
			all = append(all, r)
		}
	}

	sort.Slice(all, func(i, j int) bool { return runnerLess(all[i], all[j]) })

	start := 0
	if q.Cursor != "" {
		cursor := control.Runner{ID: control.RunnerID(afterRunner), PoolID: control.PoolID(afterPool)}
		for start < len(all) && !runnerLess(cursor, all[start]) {
			start++
		}
	}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	page := all[start:end]
	if page == nil {
		page = []control.Runner{}
	}
	next := ""
	if end < len(all) {
		next = encodeRunnerCursor(string(all[end-1].ID), string(all[end-1].PoolID))
	}
	return control.RunnerPage{Runners: page, NextCursor: next}, nil
}

// runnerLess reports whether a sorts before b by (RunnerID, PoolID).
func runnerLess(a, b control.Runner) bool {
	if string(a.ID) != string(b.ID) {
		return string(a.ID) < string(b.ID)
	}
	return string(a.PoolID) < string(b.PoolID)
}

// runnerCursor is the opaque pagination cursor: the last (runnerID, poolID)
// pair of a page, base64-url-encoded JSON.
type runnerCursor struct {
	Runner string `json:"r"`
	Pool   string `json:"p"`
}

func encodeRunnerCursor(runnerID, poolID string) string {
	b, _ := json.Marshal(runnerCursor{Runner: runnerID, Pool: poolID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeRunnerCursor(s string) (string, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", "", err
	}
	var c runnerCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return "", "", err
	}
	if c.Runner == "" && c.Pool == "" {
		return "", "", errors.New("controlapp: empty runner cursor")
	}
	return c.Runner, c.Pool, nil
}

// lostAtAnnounce is the safe reason recorded when a runner announces without
// a session the store still believed it held alive.
const lostAtAnnounce = "lost at announce"

// ReconcileRunner makes the store agree with a runner's authoritative
// current-state report. A snapshot older than the store's runner generation
// is fenced without any session read; a newer one first records the new
// authoritative generation; the result's generation is always the
// store-authoritative one, never merely the generation the caller sent.
func (s *FleetService) ReconcileRunner(ctx context.Context, snap control.RunnerSnapshot) (control.ReconcileResult, error) {
	if err := validateSnapshot(snap); err != nil {
		return control.ReconcileResult{}, err
	}
	runners, err := s.fleet.ListRunners(ctx, snap.PoolID)
	if err != nil {
		return control.ReconcileResult{}, err
	}
	var current *control.Runner
	for i := range runners {
		if runners[i].ID == snap.RunnerID {
			current = &runners[i]
			break
		}
	}

	authoritative := snap.Generation
	switch {
	case current != nil && snap.Generation < current.Generation:
		return control.ReconcileResult{Generation: current.Generation, Fenced: true}, nil
	case current != nil && snap.Generation > current.Generation:
		if err := s.upsertSnapshotRunner(ctx, snap, current); err != nil {
			return control.ReconcileResult{}, err
		}
		s.Wake(snap.PoolID)
	case current != nil:
		authoritative = current.Generation
	case current == nil:
		if err := s.upsertSnapshotRunner(ctx, snap, nil); err != nil {
			return control.ReconcileResult{}, err
		}
		s.Wake(snap.PoolID)
	}

	destroy, err := s.reconcileSessions(ctx, snap)
	if err != nil {
		return control.ReconcileResult{}, err
	}
	return control.ReconcileResult{Generation: authoritative, Destroy: destroy}, nil
}

// validateSnapshot rejects a malformed snapshot, including one that names a
// session twice, before any port is touched.
func validateSnapshot(snap control.RunnerSnapshot) error {
	if snap.WorkspaceID == "" || snap.PoolID == "" || snap.RunnerID == "" || snap.Generation == 0 {
		return control.ErrInvalid
	}
	if snap.CapacityUsed < 0 || snap.CapacityTotal < 0 || snap.CapacityUsed > snap.CapacityTotal {
		return control.ErrInvalid
	}
	seen := make(map[control.SessionID]struct{}, len(snap.Sessions))
	for _, s := range snap.Sessions {
		if s.SessionID == "" {
			return control.ErrInvalid
		}
		if _, dup := seen[s.SessionID]; dup {
			return control.ErrInvalid
		}
		seen[s.SessionID] = struct{}{}
	}
	return nil
}

// upsertSnapshotRunner records snap as the runner's authoritative generation,
// preserving capabilities the snapshot does not carry.
func (s *FleetService) upsertSnapshotRunner(ctx context.Context, snap control.RunnerSnapshot, existing *control.Runner) error {
	var caps []string
	if existing != nil {
		caps = slices.Clone(existing.Capabilities)
	}
	return s.fleet.UpsertRunner(ctx, snap.PoolID, control.Runner{
		ID:            snap.RunnerID,
		PoolID:        snap.PoolID,
		CapacityUsed:  snap.CapacityUsed,
		CapacityTotal: snap.CapacityTotal,
		Connected:     true,
		Generation:    snap.Generation,
		Capabilities:  caps,
		LastSeenAt:    s.clock.Now(),
	})
}

// reconcileSessions settles the stored live sessions against the reported set
// and collects orphans for teardown. It is idempotent: repeat calls with the
// same snapshot produce the same Destroy list and no additional mutation.
func (s *FleetService) reconcileSessions(ctx context.Context, snap control.RunnerSnapshot) ([]control.SessionID, error) {
	states := []control.SessionState{
		control.StateCreating, control.StateRunning,
		control.StateSuspendedWarm, control.StateSuspendedCold,
	}
	stored, err := s.fleet.SessionsOnRunner(ctx, snap.PoolID, snap.RunnerID, states)
	if err != nil {
		return nil, err
	}

	storedByID := make(map[control.SessionID]control.Session, len(stored))
	reportedByID := make(map[control.SessionID]control.RunnerSession, len(snap.Sessions))
	for _, row := range stored {
		storedByID[row.ID] = row
	}
	for _, r := range snap.Sessions {
		reportedByID[r.SessionID] = r
	}

	var destroy []control.SessionID

	for _, row := range stored {
		reported, present := reportedByID[row.ID]
		if row.WorkspaceID != snap.WorkspaceID {
			// A session another workspace still owns, held on this runner. When
			// this snapshot reports it, it is an orphan here; either way this
			// snapshot is not authoritative for it, so it is never mutated.
			if present {
				destroy = append(destroy, row.ID)
			}
			continue
		}
		if !present {
			if row.State == control.StateCreating {
				// A create that never landed goes back on the queue.
				empty := control.RunnerID("")
				if err := s.transitionQuiet(ctx, row.WorkspaceID, row.ID,
					[]control.SessionState{control.StateCreating}, control.StateQueued,
					control.TransitionOpts{RunnerID: &empty}); err != nil {
					return nil, err
				}
			} else {
				reason := lostAtAnnounce
				if err := s.transitionQuiet(ctx, row.WorkspaceID, row.ID,
					control.NonTerminal, control.StateDead,
					control.TransitionOpts{Error: &reason}); err != nil {
					return nil, err
				}
			}
			continue
		}
		want, ok := announcedState(reported.State)
		if !ok || want == row.State {
			continue
		}
		if err := s.transitionQuiet(ctx, row.WorkspaceID, row.ID, control.NonTerminal, want, control.TransitionOpts{}); err != nil {
			return nil, err
		}
	}

	for _, reported := range snap.Sessions {
		if _, isStored := storedByID[reported.SessionID]; isStored {
			continue
		}
		row, err := s.sessions.GetSession(ctx, snap.WorkspaceID, reported.SessionID)
		switch {
		case errors.Is(err, control.ErrNotFound):
			destroy = append(destroy, reported.SessionID)
		case err != nil:
			return nil, err
		case row.State.Terminal():
			destroy = append(destroy, reported.SessionID)
		case row.WorkspaceID != snap.WorkspaceID || row.PoolID != snap.PoolID || row.RunnerID != snap.RunnerID:
			// A duplicate held by a stale holder, or a row from another
			// workspace; the runner must tear it down as an orphan.
			destroy = append(destroy, reported.SessionID)
		default:
			// A live session the store still wants on this exact runner but
			// that SessionsOnRunner did not return; leave it alone rather than
			// destroying something the store still wants.
		}
	}

	sort.Slice(destroy, func(i, j int) bool { return string(destroy[i]) < string(destroy[j]) })
	destroy = slices.Compact(destroy)
	return destroy, nil
}

// announcedState maps a reported session state onto the closed vocabulary a
// runner may announce.
func announcedState(s control.SessionState) (control.SessionState, bool) {
	switch s {
	case control.StateRunning, control.StateSuspendedWarm, control.StateSuspendedCold:
		return s, true
	}
	return "", false
}

// transitionQuiet applies a guarded transition, swallowing exactly the two
// races reconciliation and events produce by racing each other: ErrConflict
// (the row moved on) and ErrNotFound (it is gone). Any other error is a real
// store problem the caller decides how to handle.
func (s *FleetService) transitionQuiet(ctx context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	err := s.sessions.Transition(ctx, ws, id, from, to, opts)
	if err == nil || errors.Is(err, control.ErrConflict) || errors.Is(err, control.ErrNotFound) {
		return nil
	}
	return err
}

// maxDetailBytes is the bound on runner-supplied free text before it reaches
// a session error column. It keeps one row of diagnostic text from growing
// without limit while leaving room for a useful failure tail.
const maxDetailBytes = 2048

// eventTransitions is the one guarded-transition table for runner lifecycle
// events: for each target state, the states an event may validly move a
// session from.
var eventTransitions = map[control.SessionState][]control.SessionState{
	control.StateRunning:       {control.StateCreating, control.StateRunning},
	control.StateSuspendedWarm: {control.StateRunning, control.StateSuspendedWarm},
	control.StateSuspendedCold: {control.StateRunning, control.StateSuspendedCold},
	control.StateFailed:        {control.StateCreating, control.StateRunning},
	control.StateDead:          {control.StateCreating, control.StateRunning, control.StateSuspendedWarm, control.StateSuspendedCold},
}

// runnerReportedDead is the safe reason recorded when a runner reports a
// session's container dead.
const runnerReportedDead = "runner reported dead"

var _ control.Fleet = (*FleetService)(nil)

// ApplyRunnerEvent applies one unsolicited runner lifecycle report. Identity
// and generation are fenced before any state logic or event record; a
// mismatch available in this contract returns ErrStale with no effects.
func (s *FleetService) ApplyRunnerEvent(ctx context.Context, event control.RunnerEvent) error {
	if event.WorkspaceID == "" || event.PoolID == "" || event.RunnerID == "" || event.SessionID == "" {
		return control.ErrInvalid
	}
	if event.Generation == 0 {
		return control.ErrInvalid
	}
	if _, ok := eventTransitions[event.State]; !ok {
		return control.ErrInvalid
	}
	if len(event.Detail) > maxDetailBytes || !utf8.ValidString(event.Detail) {
		return control.ErrInvalid
	}

	row, err := s.sessions.GetSession(ctx, event.WorkspaceID, event.SessionID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return control.ErrStale
		}
		return err
	}
	if row.WorkspaceID != event.WorkspaceID || row.PoolID != event.PoolID || row.RunnerID != event.RunnerID {
		return control.ErrStale
	}

	runners, err := s.fleet.ListRunners(ctx, event.PoolID)
	if err != nil {
		return err
	}
	matched := false
	for _, r := range runners {
		if r.ID == event.RunnerID {
			if r.Generation != event.Generation {
				return control.ErrStale
			}
			matched = true
			break
		}
	}
	if !matched {
		return control.ErrStale
	}

	if event.ChildExitCode != nil {
		if err := s.sessions.SetChildExitCode(ctx, event.WorkspaceID, event.SessionID, *event.ChildExitCode); err != nil {
			if errors.Is(err, control.ErrNotFound) {
				return control.ErrStale
			}
			return err
		}
		s.recordLifecycleEvent(ctx, event, row)
		return nil
	}

	target := event.State
	if row.State == target {
		// Already-applied identical event; idempotent success, no record.
		return nil
	}
	var opts control.TransitionOpts
	if target == control.StateFailed {
		detail := boundDetail(event.Detail)
		opts.Error = &detail
	} else if target == control.StateDead {
		reason := runnerReportedDead
		opts.Error = &reason
	}
	err = s.sessions.Transition(ctx, event.WorkspaceID, event.SessionID, eventTransitions[target], target, opts)
	switch {
	case err == nil:
		s.recordLifecycleEvent(ctx, event, row)
		s.Wake(event.PoolID)
		return nil
	case errors.Is(err, control.ErrConflict):
		// A conflict because an identical event already applied is success;
		// any other conflict remains ErrConflict.
		cur, gerr := s.sessions.GetSession(ctx, event.WorkspaceID, event.SessionID)
		if gerr == nil && cur.State == target {
			return nil
		}
		return control.ErrConflict
	case errors.Is(err, control.ErrNotFound):
		return control.ErrStale
	default:
		return err
	}
}

// recordLifecycleEvent records one provider-neutral service fact after an
// accepted mutation. The runner's free text never reaches an Event: it is
// bounded into the session's own error column only.
func (s *FleetService) recordLifecycleEvent(ctx context.Context, event control.RunnerEvent, row control.Session) {
	_ = s.events.Record(ctx, control.Event{
		ID:          s.ids.NewEventID(),
		WorkspaceID: event.WorkspaceID,
		ActorID:     control.ActorID(event.RunnerID),
		Action:      control.ActionUpdate,
		Resource: control.Resource{
			Kind:        control.ResourceSession,
			WorkspaceID: event.WorkspaceID,
			ID:          string(event.SessionID),
			CreatorID:   row.CreatorID,
		},
		At: s.clock.Now(),
	})
}

// boundDetail clips free text to maxDetailBytes valid UTF-8 bytes, cutting on
// a rune boundary.
func boundDetail(s string) string {
	s = strings.ToValidUTF8(s, "")
	if len(s) <= maxDetailBytes {
		return s
	}
	for len(s) > maxDetailBytes {
		_, size := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-size]
	}
	return s
}
