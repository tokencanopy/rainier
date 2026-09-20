package controlapp

import (
	"context"
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
	// EgressAllow lists hosts the resolved material needs reachable — for
	// example the source-control hosts Repos clone from. createSpec unions
	// them into the session's egress list, in order, without duplicates.
	EgressAllow []string
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
	envs := map[queuedEnvKey]*control.Environment{}
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
		runnerID, image, ok := s.placeQueued(ctx, row, env, views)
		if !ok {
			// The fleet has room, just not for this session's requirements.
			// Skipping the row rather than ending the pass keeps one blocked
			// session from holding up compatible ones behind it.
			continue
		}
		rid := runnerID
		opts := control.TransitionOpts{RunnerID: &rid}
		if image != nil {
			// The image this placement resolved travels with the placement,
			// in the same statement as the state, and is what the create
			// dispatched below must carry.
			opts.Image = image
			row.Spec.Image = *image
		}
		if err := s.sessions.Transition(ctx, row.WorkspaceID, row.ID,
			[]control.SessionState{control.StateQueued}, control.StateCreating,
			opts); err != nil {
			if errors.Is(err, control.ErrConflict) || errors.Is(err, control.ErrNotFound) {
				continue // moved on under us
			}
			return
		}
		// The runner's announced capabilities travel with the placement
		// rather than being re-read at dispatch: they are the claims this
		// pass placed ON, and a second read could disagree with the decision
		// already made. createSpec keys its withholding on them.
		go s.dispatchCreate(ctx, pool, row, runnerID, capabilitiesOf(views, runnerID), env)
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

// candidatesFor narrows the fleet to the runners eligible to run a session
// from env at all: every connected runner holding every portable capability
// env requires. Capacity is deliberately not part of it — pickRunner applies
// that — so placement can ask "who could take this?" and "which of those are
// preferable?" over one list, and fall back from the second to the first.
func candidatesFor(views []runnerView, env *control.Environment) []runnerView {
	if env == nil || len(env.Requirements.Capabilities) == 0 {
		return views
	}
	out := make([]runnerView, 0, len(views))
	for _, v := range views {
		if hasAllCapabilities(v.caps, env.Requirements.Capabilities) {
			out = append(out, v)
		}
	}
	return out
}

// admittedBy narrows candidates to the ones a checkpoint location allows: all
// of them when the checkpoint is portable, else the runners it names. A
// location that is neither — the zero value — admits nobody, which is exactly
// "this checkpoint is not usable right now".
func admittedBy(candidates []runnerView, loc control.CheckpointLocation) []runnerView {
	if loc.Portable {
		return candidates
	}
	out := make([]runnerView, 0, len(candidates))
	for _, v := range candidates {
		if slices.Contains(loc.Runners, v.id) {
			out = append(out, v)
		}
	}
	return out
}

// placeQueued decides where a queued row goes and which image that placement
// resolved: nil when the placement resolves none and the row's stored image
// stands.
//
// A session from an environment with a current snapshot prefers a runner the
// host says can boot that checkpoint, and boots the snapshot there. When no
// such runner has room — the holder is full, or away, or the checkpoint is
// nowhere — it falls back to any eligible runner and boots the environment's
// own image, which createSpec then rebuilds with the environment's setup. A
// cache is a head start, never a pin: waiting for one machine to free a slot
// is worse than paying for the setup somewhere that has room now.
//
// A scratch session, a stale snapshot, and a session whose caller overrode
// the image all place exactly as they did before: an override boots the image
// its caller asked for, and no placement may quietly substitute another.
func (s *FleetService) placeQueued(ctx context.Context, row control.Session, env *control.Environment, views []runnerView) (control.RunnerID, *string, bool) {
	candidates := candidatesFor(views, env)
	if env == nil || !runsCachedSnapshot(*env) || overridesEnvironmentImage(row, *env) {
		id, ok := pickRunner(candidates)
		return id, nil, ok
	}
	// An error is not a location: the checkpoint is simply not known to be
	// anywhere, which is the same answer as nowhere. Nothing is logged — the
	// fallback is a normal placement, not an incident.
	loc, err := s.checkpoints.LocateCheckpoint(ctx, row.WorkspaceID, env.Snapshot)
	if err != nil {
		loc = control.CheckpointLocation{}
	}
	if id, ok := pickRunner(admittedBy(candidates, loc)); ok {
		ref := env.Snapshot.Ref
		return id, &ref, true
	}
	id, ok := pickRunner(candidates)
	if !ok {
		return "", nil, false
	}
	image := env.Image
	return id, &image, true
}

// overridesEnvironmentImage reports whether row's stored image is a caller's
// own choice rather than one of the two images a placement may resolve
// between. A requeued row still carrying the ref a previous placement wrote
// is not an override: it is re-resolved from scratch.
func overridesEnvironmentImage(row control.Session, env control.Environment) bool {
	return row.Spec.Image != env.Image && row.Spec.Image != env.Snapshot.Ref
}

// capabilitiesOf returns the capabilities the placement pass saw on id, or
// nil when the view has gone (a runner that disconnected between the pick and
// the dispatch). Nil is the safe answer: a runner whose claims this pass
// cannot state is a runner nothing is withheld from, which dispatches today's
// Spec — the same thing an older runner gets.
func capabilitiesOf(views []runnerView, id control.RunnerID) []string {
	for _, v := range views {
		if v.id == id {
			return v.caps
		}
	}
	return nil
}

func hasAllCapabilities(caps, reqs []string) bool {
	for _, want := range reqs {
		if !slices.Contains(caps, want) {
			return false
		}
	}
	return true
}

// queuedEnvKey is the composite identity of a queued session's environment.
// Environments are workspace-scoped, so a bare EnvironmentID is not enough to
// cache them safely across workspaces sharing one ID.
type queuedEnvKey struct {
	ws control.WorkspaceID
	id control.EnvironmentID
}

// queuedEnvironment returns the environment row came from, memoized per pass.
// A scratch session, or one whose environment is gone, resolves to nil. The
// bool reports whether the lookup could be made at all. The returned
// environment is a deep copy so neither the cache nor the caller aliases the
// store's backing arrays.
func (s *FleetService) queuedEnvironment(ctx context.Context, cache map[queuedEnvKey]*control.Environment, row control.Session) (*control.Environment, bool) {
	if row.EnvironmentID == "" {
		return nil, true
	}
	key := queuedEnvKey{ws: row.WorkspaceID, id: row.EnvironmentID}
	if env, seen := cache[key]; seen {
		return env, true
	}
	env, err := s.environments.GetEnvironment(ctx, row.WorkspaceID, row.EnvironmentID)
	switch {
	case errors.Is(err, control.ErrNotFound):
		cache[key] = nil
		return nil, true
	case err != nil:
		return nil, false
	}
	env = cloneEnvironment(env)
	cache[key] = &env
	return &env, true
}

// cloneEnvironment deep-copies every collection of an environment so the
// cache and the resolver each hold their own arrays.
func cloneEnvironment(e control.Environment) control.Environment {
	e.EgressAllow = slices.Clone(e.EgressAllow)
	e.SecretRefs = slices.Clone(e.SecretRefs)
	e.Connectors = slices.Clone(e.Connectors)
	for i := range e.Connectors {
		e.Connectors[i].Raw = slices.Clone(e.Connectors[i].Raw)
	}
	e.Requirements.Capabilities = slices.Clone(e.Requirements.Capabilities)
	e.Snapshot.Capabilities = slices.Clone(e.Snapshot.Capabilities)
	return e
}

// cloneSession deep-copies the collections of a session so an adapter that
// receives it cannot alias the store's backing arrays.
func cloneSession(s control.Session) control.Session {
	s.Spec.Cmd = slices.Clone(s.Spec.Cmd)
	s.Spec.EgressAllow = slices.Clone(s.Spec.EgressAllow)
	s.Spec.Repos = slices.Clone(s.Spec.Repos)
	return s
}

// dispatchCreate builds the create spec, pins setup provenance, dispatches,
// and settles the uncertain-delivery outcome without ever duplicating a
// delivered create.
func (s *FleetService) dispatchCreate(ctx context.Context, pool control.PoolID, row control.Session, runnerID control.RunnerID, runnerCaps []string, env *control.Environment) {
	// Read BEFORE the spec is built, not after, because a microVM create's
	// bootstrap token is FENCED by this number: the hash recorded against the
	// session has to name the same placement the create carries, or the first
	// exchange is refused as superseded by the very placement that minted it.
	gen, known := s.placedGeneration(ctx, row)
	if !known && slices.Contains(runnerCaps, runner.CapabilityMicrovmV1) {
		// Only for a withholding placement, and only because the token's
		// fence is this number: everywhere else an unknown generation is
		// inert, and failing a create over a transient read would be a
		// regression for every session in the fleet.
		s.failCreate(ctx, row, "could not read this session's placement to mint its bootstrap token")
		return
	}
	spec, fail := s.createSpec(ctx, row, env, runnerCaps, gen)
	if fail != "" {
		s.failCreate(ctx, row, fail)
		return
	}
	if !s.pinSetupHash(ctx, row, spec) {
		return
	}
	res, err := s.transport.Dispatch(ctx, pool, runnerID, runner.ToRunner{
		Type:                "create",
		Session:             string(row.ID),
		Spec:                spec,
		PlacementGeneration: gen,
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

// placedGeneration is the placement generation the sandbox this create starts
// belongs to: the row's AS PLACED, read back from the repository after the
// transition that named this runner opened it. The row this function was
// handed is the one drainPool held BEFORE that transition, so its own copy is
// one behind — and computing the successor here would be this package
// asserting a rule that is the repository's (ports.go: Transition advances
// PlacementGeneration when opts.RunnerID names a runner), which is exactly
// the kind of duplicated arithmetic a stale read is supposed to catch.
//
// A read that cannot be made carries nothing rather than the value it knows
// to be stale: zero is "not carried" on the wire and fences nothing, while a
// wrong number would fence every event this sandbox ever sends.
//
// The bool is the same fact said out loud, and it exists because that
// reasoning stopped being universally true. Zero fences nothing on an EVENT,
// which is what this was written for — but the same number is now also the
// generation a microVM session's bootstrap token is minted against, and
// there a wrong value is not inert: a token recorded at 0 against a row at 3
// is refused on its one and only exchange, as superseded by the very
// placement that minted it. So the caller is told, and refuses the create
// rather than dispatching a session that cannot get its secrets.
func (s *FleetService) placedGeneration(ctx context.Context, row control.Session) (uint64, bool) {
	placed, err := s.sessions.GetSession(ctx, row.WorkspaceID, row.ID)
	if err != nil {
		return 0, false
	}
	return placed.PlacementGeneration, true
}

// createSpec builds the runner create spec from the session and its current
// environment, resolving sensitive launch material only here and never
// storing it.
//
// runnerCaps are the capabilities the placement's runner announced, and gen
// the placement generation the create carries. Together they are the whole of
// what this function needs to decide the one question the microVM bootstrap
// design added to it: whether this session's environment secrets travel in
// Spec.Env, as they always have, or stay behind a single-use token the guest
// exchanges for them after it boots (see §3 of the design note).
func (s *FleetService) createSpec(ctx context.Context, row control.Session, env *control.Environment, runnerCaps []string, gen uint64) (*runner.Spec, string) {
	spec := runner.Spec{
		Name:        row.Name,
		Image:       row.Spec.Image,
		Cmd:         slices.Clone(row.Spec.Cmd),
		EgressAllow: slices.Clone(row.Spec.EgressAllow),
	}
	if env != nil {
		cloned := cloneEnvironment(*env)
		env = &cloned
		// The row's image was resolved at create (portableSpecFor): the
		// snapshot when one was current and the caller did not override the
		// image, else the image itself. Setup runs exactly when the row does
		// not boot the snapshot — an override boots its own image and needs
		// the setup the snapshot would have carried. A hook travels with its
		// bound, only a hook that will run gets one, and a hook whose
		// environment declared no bound gets the host's default.
		if env.Setup != "" && (env.Snapshot.Ref == "" || row.Spec.Image != env.Snapshot.Ref) {
			spec.Setup = env.Setup
			spec.SetupTimeoutSec = boundOr(env.SetupTimeoutSec, s.defaultSetupTimeout)
		}
		if env.Init != "" {
			spec.Init = env.Init
			spec.InitTimeoutSec = boundOr(env.InitTimeoutSec, s.defaultInitTimeout)
		}
	}
	material, err := s.launchMaterial.ResolveLaunchMaterial(ctx, cloneSession(row), env)
	if err != nil {
		return nil, "could not resolve launch material"
	}
	spec.Repos = slices.Clone(material.Repos)
	spec.GitAuthorName = material.GitAuthorName
	spec.GitAuthorEmail = material.GitAuthorEmail
	// The one branch in this function that is about WHERE the session will
	// run rather than what it is. A runner announcing microvm.v1 boots each
	// session in its own VM on a shared regional host, where a driver cannot
	// hand a value to a daemon through an argv and every channel it does have
	// ends in a file; so the values stay here and the create carries their
	// NAMES and a token instead (ADR-0003 §2.7 item 1). Every other runner —
	// which is every runner today — is dispatched exactly what it always was.
	withheld := slices.Contains(runnerCaps, runner.CapabilityMicrovmV1)
	if !withheld {
		spec.Env = cloneMap(material.Environment)
	}
	// The agent home is the creator's, in this workspace: every session they
	// start there mounts the same volume, and that sameness is the whole of
	// "log in once". A session with no creator gets none of this — a home
	// belongs to a person, there is nobody to name, and a variable pointing
	// at a directory that was never mounted, or an egress hole for a login
	// that cannot happen, would be a claim the create cannot keep.
	var providers []AgentProvider
	if row.CreatorID != "" {
		providers = AgentProviders()
		spec.Home = &runner.HomeMount{
			Volume: AgentHomeVolume(row.WorkspaceID, row.CreatorID),
			Path:   HomeMountPath,
		}
		// Agent paths and the manifest are launch invariants: allowing resolved
		// material to replace one would let workspace configuration redirect
		// credential custody or replace the manifest sessiond trusts.
		agentEnv := AgentsEnv(providers)
		for k, v := range spec.Env {
			if _, reserved := agentEnv[k]; !reserved {
				agentEnv[k] = v
			}
		}
		spec.Env = agentEnv
	}
	// The names, and then the token, in that order: the names are derived
	// from the material this function already holds, and they are computed
	// AFTER the agent-home block so that the one rule that block states —
	// agent paths and the manifest are launch invariants a workspace's own
	// configuration may not replace — holds identically on both paths. A
	// secret_ref that happens to be spelled CLAUDE_CONFIG_DIR is dropped for
	// a Docker session and must not be smuggled back in as a name a microVM
	// guest then applies over its own agent home.
	if withheld {
		spec.SecretNames = withholdableNames(material.Environment, spec.Env)
		token, err := s.bootstraps.Mint(ctx, row.WorkspaceID, row.ID, gen)
		if err != nil {
			// Fail closed. The alternative to refusing here is a session
			// dispatched with neither its secrets nor a way to ask for them,
			// which boots, reports healthy, and fails at whatever the first
			// credential-shaped thing it does is.
			return nil, "could not mint this session's bootstrap token"
		}
		spec.BootstrapToken = token
	}
	// Either the values or the token, never both — stated as a check rather
	// than as a comment, because it is the whole security claim of §3 and it
	// is one careless merge away from being false.
	//
	// It is a BACKSTOP and not an independent proof: withholdableNames
	// already excludes every name spec.Env holds, so as this function stands
	// it cannot fire. What it is for is the edit that adds a third writer of
	// spec.Env below the withholding branch, or reorders the two — at which
	// point a failed create is a much better answer than a secret on a
	// shared host's disk.
	if spec.BootstrapToken != "" {
		for _, name := range spec.SecretNames {
			if _, both := spec.Env[name]; both {
				return nil, "could not resolve launch material"
			}
		}
	}
	// The session row stores only the egress its caller or environment
	// declared; the hosts the resolved material needs are the resolver's
	// knowledge and are added here, at dispatch, so the row and the view a
	// human reads off it never claim a host nobody asked for. The providers'
	// hosts — what an agent's login, refresh, and inference reach — join next
	// for the same reason, so a host a human named still reads first.
	spec.EgressAllow = unionHosts(spec.EgressAllow, material.EgressAllow)
	for _, p := range providers {
		spec.EgressAllow = unionHosts(spec.EgressAllow, p.Egress)
	}
	// The developer baseline joins LAST and unconditionally, including for a
	// session with no creator and no environment. This is the seam the whole
	// default-egress policy hangs on: every dispatch in both compositions —
	// self-hosted and hosted — builds its runner spec here, so a host added to
	// the table reaches every session without a second list existing anywhere,
	// and a session's stored egress_allow keeps meaning "what a human asked
	// for" rather than quietly acquiring twelve rows nobody typed.
	//
	// Additive, never replacing: an environment that declares its own hosts
	// keeps every one of them and gains these, and unionHosts dedupes so an
	// environment that already named registry.npmjs.org does not carry it
	// twice. A host set here grants reachability only — the credential a
	// session presents to any of these hosts is still the broker's to issue
	// or withhold.
	spec.EgressAllow = unionHosts(spec.EgressAllow, s.defaultEgress)
	return &spec, ""
}

// unionHosts returns base plus every host of extra it does not already
// contain, in order, leaving base itself untouched. Deduped because a session
// that names a material host explicitly (many do) must not end up with it
// twice, and order is preserved because the resulting list is what a human
// reads back. Two nil inputs stay nil.
func unionHosts(base, extra []string) []string {
	out := slices.Clone(base)
	for _, h := range extra {
		if !slices.Contains(out, h) {
			out = append(out, h)
		}
	}
	return out
}

// pinSetupHash records which setup script a create is dispatching, before the
// command goes out. A create with no script pins nothing.
func (s *FleetService) pinSetupHash(ctx context.Context, row control.Session, spec *runner.Spec) bool {
	if spec.Setup == "" {
		return true
	}
	if err := s.sessions.SetSessionSetupHash(ctx, row.WorkspaceID, row.ID, setupHash(spec.Image, spec.Setup)); err != nil {
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

// boundOr returns the environment's own bound when it declared one, else the
// host's default.
func boundOr(declared, fallback int) int {
	if declared > 0 {
		return declared
	}
	return fallback
}

// withholdableNames are the names a withheld create promises its guest: every
// key of the resolved secret environment that the spec's own configuration
// does not already reserve, sorted.
//
// Sorted because the list is on the wire and in a guest's failure message,
// and a set rendered in map order would make two identical creates look
// different. Nil when there is nothing to promise, so a session with no
// secret_refs puts no `secret_names` on the wire at all and its guest reads
// the honest "none declared" rather than "declared and never arrived".
func withholdableNames(material, reserved map[string]string) []string {
	if len(material) == 0 {
		return nil
	}
	out := make([]string, 0, len(material))
	for name := range material {
		if _, taken := reserved[name]; taken {
			continue
		}
		out = append(out, name)
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	return out
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
