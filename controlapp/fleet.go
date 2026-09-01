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
	"sync"
	"time"

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
	if err := s.auth.Authorize(ctx, scope, control.ActionList, control.Resource{
		Kind:        control.ResourceRunner,
		WorkspaceID: scope.WorkspaceID,
	}); err != nil {
		return control.RunnerPage{}, err
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
