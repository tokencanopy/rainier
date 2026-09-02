package repotest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// The two workspaces and two pools every case is written against. A store
// that treats either pair as the same thing fails the suite.
const (
	Alpha control.WorkspaceID = "ws_alpha"
	Beta  control.WorkspaceID = "ws_beta"
	PoolA control.PoolID      = "pool_a"
	PoolB control.PoolID      = "pool_b"
)

// Stores is what a host hands the suite: its three repository ports over ONE
// backing store (a session created through Sessions must be visible to
// Fleet.SessionsOnRunner), plus the way to make a workspace exist there.
type Stores struct {
	Sessions     control.SessionRepository
	Environments control.EnvironmentRepository
	Fleet        control.FleetRepository
	Provision    func(ctx context.Context, ws control.WorkspaceID) error
}

// Run drives the contract. open is called once per case and must return an
// empty store each time; Run provisions Alpha and Beta before the case body.
func Run(t *testing.T, open func(t *testing.T) Stores) {
	for _, c := range cases() {
		t.Run(c.name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			for _, ws := range []control.WorkspaceID{Alpha, Beta} {
				if err := s.Provision(ctx, ws); err != nil {
					t.Fatalf("provision %s: %v", ws, err)
				}
			}
			c.fn(t, s)
		})
	}
}

type suiteCase struct {
	name string
	fn   func(*testing.T, Stores)
}

// cases is the whole contract, in the order the ports are declared: the
// sessions the application schedules, the environments they start from, and
// the fleet they run on.
func cases() []suiteCase {
	return []suiteCase{
		{"S1 session round trip", caseSessionRoundTrip},
		{"S2 sessions are isolated by workspace", caseSessionWorkspaceIsolation},
		{"S3 an empty workspace is invalid on every session method", caseSessionEmptyWorkspace},
		{"S4 an unknown workspace is not found", caseSessionUnknownWorkspace},
		{"S5 an active name is unique per creator", caseSessionActiveName},
		{"S6 an idempotency key replays its row", caseSessionIdempotency},
		{"S7 session listing order, terminal filter, and cursor", caseSessionListing},
		{"S8 guarded transition and placement generation", casePlacementGeneration},
		{"S9 provenance writes", caseSessionProvenance},
		{"S10 controller generation", caseControllerGeneration},

		{"E1 environment round trip", caseEnvironmentRoundTrip},
		{"E2 an environment name is unique per workspace", caseEnvironmentName},
		{"E3 environments are isolated by workspace", caseEnvironmentIsolation},
		{"E4 environment listing order and cursor", caseEnvironmentListing},
		{"E5 update ignores the snapshot cache", caseEnvironmentUpdateIgnoresCache},
		{"E6 guarded snapshot and its capability", caseEnvironmentSnapshot},
		{"E7 count sessions by environment", caseCountSessionsByEnvironment},
		{"E8 an empty workspace is invalid on every environment method", caseEnvironmentEmptyWorkspace},

		{"F1 runner round trip and order", caseRunnerRoundTrip},
		{"F2 runners are isolated by pool", caseRunnerPoolIsolation},
		{"F3 generation fence", caseGenerationFence},
		{"F4 connected flag", caseRunnerConnected},
		{"F5 sessions on a runner", caseSessionsOnRunner},
		{"F6 oldest queued", caseOldestQueued},
		{"F7 an empty pool is invalid on every fleet method", caseFleetEmptyPool},
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// baseTime is a fixed, monotonic-free instant every case builds its
// timestamps from, so a store that rounds to microseconds and one that keeps
// nanoseconds are compared on the same footing.
func baseTime() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

// mustCreate creates row in ws and returns the stored row.
func mustCreate(t *testing.T, s Stores, ws control.WorkspaceID, row control.Session) control.Session {
	t.Helper()
	if row.CreatedAt.IsZero() {
		row.CreatedAt = baseTime()
		row.UpdatedAt = baseTime()
		row.LastEventAt = baseTime()
	}
	got, err := s.Sessions.CreateSession(context.Background(), ws, row)
	if err != nil {
		t.Fatalf("create %s in %s: %v", row.ID, ws, err)
	}
	return got
}

// mustCreateEnv creates row in ws and returns the stored row.
func mustCreateEnv(t *testing.T, s Stores, ws control.WorkspaceID, row control.Environment) control.Environment {
	t.Helper()
	if row.CreatedAt.IsZero() {
		row.CreatedAt = baseTime()
		row.UpdatedAt = baseTime()
	}
	got, err := s.Environments.CreateEnvironment(context.Background(), ws, row)
	if err != nil {
		t.Fatalf("create environment %s in %s: %v", row.ID, ws, err)
	}
	return got
}

// sameSession fails unless got and want are the same row field for field.
// Times are compared with Equal (a store may return them in another
// location), and the two slices whose nil-ness is load-bearing — Repos and
// ChildExitCode — are compared for presence as well as content.
func sameSession(t *testing.T, what string, got, want control.Session) {
	t.Helper()
	switch {
	case got.ID != want.ID:
		t.Fatalf("%s: ID = %q, want %q", what, got.ID, want.ID)
	case got.WorkspaceID != want.WorkspaceID:
		t.Fatalf("%s: WorkspaceID = %q, want %q", what, got.WorkspaceID, want.WorkspaceID)
	case got.CreatorID != want.CreatorID:
		t.Fatalf("%s: CreatorID = %q, want %q", what, got.CreatorID, want.CreatorID)
	case got.Name != want.Name:
		t.Fatalf("%s: Name = %q, want %q", what, got.Name, want.Name)
	case got.State != want.State:
		t.Fatalf("%s: State = %q, want %q", what, got.State, want.State)
	case got.EnvironmentID != want.EnvironmentID:
		t.Fatalf("%s: EnvironmentID = %q, want %q", what, got.EnvironmentID, want.EnvironmentID)
	case got.Spec.Image != want.Spec.Image:
		t.Fatalf("%s: Spec.Image = %q, want %q", what, got.Spec.Image, want.Spec.Image)
	case !slices.Equal(got.Spec.Cmd, want.Spec.Cmd):
		t.Fatalf("%s: Spec.Cmd = %q, want %q", what, got.Spec.Cmd, want.Spec.Cmd)
	case !slices.Equal(got.Spec.EgressAllow, want.Spec.EgressAllow):
		t.Fatalf("%s: Spec.EgressAllow = %q, want %q", what, got.Spec.EgressAllow, want.Spec.EgressAllow)
	case (got.Spec.Repos == nil) != (want.Spec.Repos == nil):
		t.Fatalf("%s: Spec.Repos nil-ness = %v, want %v", what, got.Spec.Repos == nil, want.Spec.Repos == nil)
	case !slices.Equal(got.Spec.Repos, want.Spec.Repos):
		t.Fatalf("%s: Spec.Repos = %+v, want %+v", what, got.Spec.Repos, want.Spec.Repos)
	case got.SetupHash != want.SetupHash:
		t.Fatalf("%s: SetupHash = %q, want %q", what, got.SetupHash, want.SetupHash)
	case got.PoolID != want.PoolID:
		t.Fatalf("%s: PoolID = %q, want %q", what, got.PoolID, want.PoolID)
	case got.RunnerID != want.RunnerID:
		t.Fatalf("%s: RunnerID = %q, want %q", what, got.RunnerID, want.RunnerID)
	case got.PlacementGeneration != want.PlacementGeneration:
		t.Fatalf("%s: PlacementGeneration = %d, want %d", what, got.PlacementGeneration, want.PlacementGeneration)
	case got.ControllerGeneration != want.ControllerGeneration:
		t.Fatalf("%s: ControllerGeneration = %d, want %d", what, got.ControllerGeneration, want.ControllerGeneration)
	case got.IdempotencyKey != want.IdempotencyKey:
		t.Fatalf("%s: IdempotencyKey = %q, want %q", what, got.IdempotencyKey, want.IdempotencyKey)
	case (got.ChildExitCode == nil) != (want.ChildExitCode == nil):
		t.Fatalf("%s: ChildExitCode = %v, want %v", what, got.ChildExitCode, want.ChildExitCode)
	case got.ChildExitCode != nil && *got.ChildExitCode != *want.ChildExitCode:
		t.Fatalf("%s: ChildExitCode = %d, want %d", what, *got.ChildExitCode, *want.ChildExitCode)
	case got.Error != want.Error:
		t.Fatalf("%s: Error = %q, want %q", what, got.Error, want.Error)
	case !got.CreatedAt.Equal(want.CreatedAt):
		t.Fatalf("%s: CreatedAt = %v, want %v", what, got.CreatedAt, want.CreatedAt)
	case !got.UpdatedAt.Equal(want.UpdatedAt):
		t.Fatalf("%s: UpdatedAt = %v, want %v", what, got.UpdatedAt, want.UpdatedAt)
	case !got.LastEventAt.Equal(want.LastEventAt):
		t.Fatalf("%s: LastEventAt = %v, want %v", what, got.LastEventAt, want.LastEventAt)
	}
}

// sameEnvironment fails unless got and want are the same row field for field.
func sameEnvironment(t *testing.T, what string, got, want control.Environment) {
	t.Helper()
	switch {
	case got.ID != want.ID:
		t.Fatalf("%s: ID = %q, want %q", what, got.ID, want.ID)
	case got.WorkspaceID != want.WorkspaceID:
		t.Fatalf("%s: WorkspaceID = %q, want %q", what, got.WorkspaceID, want.WorkspaceID)
	case got.Name != want.Name:
		t.Fatalf("%s: Name = %q, want %q", what, got.Name, want.Name)
	case got.Image != want.Image:
		t.Fatalf("%s: Image = %q, want %q", what, got.Image, want.Image)
	case got.Setup != want.Setup:
		t.Fatalf("%s: Setup = %q, want %q", what, got.Setup, want.Setup)
	case got.SetupHash != want.SetupHash:
		t.Fatalf("%s: SetupHash = %q, want %q", what, got.SetupHash, want.SetupHash)
	case got.Init != want.Init:
		t.Fatalf("%s: Init = %q, want %q", what, got.Init, want.Init)
	case got.InitTimeoutSec != want.InitTimeoutSec:
		t.Fatalf("%s: InitTimeoutSec = %d, want %d", what, got.InitTimeoutSec, want.InitTimeoutSec)
	case got.SetupTimeoutSec != want.SetupTimeoutSec:
		t.Fatalf("%s: SetupTimeoutSec = %d, want %d", what, got.SetupTimeoutSec, want.SetupTimeoutSec)
	case !slices.Equal(got.EgressAllow, want.EgressAllow):
		t.Fatalf("%s: EgressAllow = %q, want %q", what, got.EgressAllow, want.EgressAllow)
	case !slices.Equal(got.SecretRefs, want.SecretRefs):
		t.Fatalf("%s: SecretRefs = %q, want %q", what, got.SecretRefs, want.SecretRefs)
	case !sameConnectors(got.Connectors, want.Connectors):
		t.Fatalf("%s: Connectors = %+v, want %+v", what, got.Connectors, want.Connectors)
	case !slices.Equal(got.Requirements.Capabilities, want.Requirements.Capabilities):
		t.Fatalf("%s: Requirements.Capabilities = %q, want %q", what, got.Requirements.Capabilities, want.Requirements.Capabilities)
	case got.Requirements.MinCPU != want.Requirements.MinCPU:
		t.Fatalf("%s: Requirements.MinCPU = %d, want %d", what, got.Requirements.MinCPU, want.Requirements.MinCPU)
	case got.Requirements.MinMemoryBytes != want.Requirements.MinMemoryBytes:
		t.Fatalf("%s: Requirements.MinMemoryBytes = %d, want %d", what, got.Requirements.MinMemoryBytes, want.Requirements.MinMemoryBytes)
	case got.Requirements.MinDiskBytes != want.Requirements.MinDiskBytes:
		t.Fatalf("%s: Requirements.MinDiskBytes = %d, want %d", what, got.Requirements.MinDiskBytes, want.Requirements.MinDiskBytes)
	case got.Snapshot.Ref != want.Snapshot.Ref:
		t.Fatalf("%s: Snapshot.Ref = %q, want %q", what, got.Snapshot.Ref, want.Snapshot.Ref)
	case got.SnapshotHash != want.SnapshotHash:
		t.Fatalf("%s: SnapshotHash = %q, want %q", what, got.SnapshotHash, want.SnapshotHash)
	case !got.CreatedAt.Equal(want.CreatedAt):
		t.Fatalf("%s: CreatedAt = %v, want %v", what, got.CreatedAt, want.CreatedAt)
	case !got.UpdatedAt.Equal(want.UpdatedAt):
		t.Fatalf("%s: UpdatedAt = %v, want %v", what, got.UpdatedAt, want.UpdatedAt)
	}
}

// sameConnectors compares connectors by type and by JSON value: a store may
// re-render the bytes (Postgres jsonb does) but may not add, drop, or rewrite
// a member.
func sameConnectors(got, want []control.Connector) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Type != want[i].Type {
			return false
		}
		var g, w any
		if json.Unmarshal(got[i].Raw, &g) != nil || json.Unmarshal(want[i].Raw, &w) != nil {
			return false
		}
		gb, _ := json.Marshal(g)
		wb, _ := json.Marshal(w)
		if string(gb) != string(wb) {
			return false
		}
	}
	return true
}

// sessionIDs is the id sequence of a page, for order assertions.
func sessionIDs(rows []control.Session) []control.SessionID {
	out := make([]control.SessionID, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

// environmentIDs is the id sequence of a page, for order assertions.
func environmentIDs(rows []control.Environment) []control.EnvironmentID {
	out := make([]control.EnvironmentID, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

// pageSessions walks the whole listing at the given limit and returns the
// concatenated rows plus the number of pages it took. It fails if the store
// ever repeats a row or never runs out of cursor.
func pageSessions(t *testing.T, s Stores, ws control.WorkspaceID, q control.SessionQuery) ([]control.Session, int) {
	t.Helper()
	ctx := context.Background()
	var all []control.Session
	seen := map[control.SessionID]bool{}
	pages := 0
	for {
		rows, next, err := s.Sessions.ListSessions(ctx, ws, q)
		if err != nil {
			t.Fatalf("list page %d: %v", pages, err)
		}
		pages++
		for _, r := range rows {
			if seen[r.ID] {
				t.Fatalf("row %s appeared on two pages", r.ID)
			}
			seen[r.ID] = true
		}
		all = append(all, rows...)
		if next == "" {
			return all, pages
		}
		if pages > 20 {
			t.Fatalf("listing never ended: %d pages", pages)
		}
		q.Cursor = next
	}
}

// ---------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------

// caseSessionRoundTrip (S1) pins what a create stores and what it refuses to
// store: the three fields the row's own history owns — the child's exit
// code, the placement generation, and the controller generation — are the
// store's, never the caller's.
func caseSessionRoundTrip(t *testing.T, s Stores) {
	ctx := context.Background()
	code := 3
	in := control.Session{
		ID:            "sess_example",
		CreatorID:     "act_a",
		Name:          "dev",
		State:         control.StateQueued,
		EnvironmentID: "env_a",
		Spec: control.PortableSpec{
			Image:       "img:1",
			Cmd:         []string{"bash", "-lc", "true"},
			EgressAllow: []string{"proxy.example.com", "index.example.test"},
			Repos:       []control.RepoRef{},
		},
		SetupHash:            "h1",
		PoolID:               PoolA,
		RunnerID:             "runner_a",
		PlacementGeneration:  0,
		ControllerGeneration: 9,
		IdempotencyKey:       "idem_1",
		ChildExitCode:        &code,
		Error:                "",
		CreatedAt:            baseTime(),
		UpdatedAt:            baseTime(),
		LastEventAt:          baseTime(),
	}
	created := mustCreate(t, s, Alpha, in)

	want := in
	want.WorkspaceID = Alpha
	want.ChildExitCode = nil     // create never stores one
	want.PlacementGeneration = 1 // zero is stored as one
	want.ControllerGeneration = 0
	sameSession(t, "created row", created, want)

	read, err := s.Sessions.GetSession(ctx, Alpha, "sess_example")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	sameSession(t, "read back", read, created)
	if read.Spec.Repos == nil || len(read.Spec.Repos) != 0 {
		t.Fatalf("an explicitly empty repo override must read back non-nil and empty, got %#v", read.Spec.Repos)
	}

	// The other side of the nil-vs-empty distinction: no override at all.
	bare := mustCreate(t, s, Alpha, control.Session{
		ID: "sess_bare", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA})
	if bare.Spec.Repos != nil {
		t.Fatalf("an absent repo override must read back nil, got %#v", bare.Spec.Repos)
	}
	reread, err := s.Sessions.GetSession(ctx, Alpha, "sess_bare")
	if err != nil {
		t.Fatalf("get sess_bare: %v", err)
	}
	if reread.Spec.Repos != nil {
		t.Fatalf("an absent repo override must stay nil through a read, got %#v", reread.Spec.Repos)
	}
}

// caseSessionWorkspaceIsolation (S2) pins the one answer a cross-workspace
// lookup may give: not found, never a different message and never a row.
func caseSessionWorkspaceIsolation(t *testing.T, s Stores) {
	ctx := context.Background()
	alpha := mustCreate(t, s, Alpha, control.Session{
		ID: "sess_same", CreatorID: "act_a", Name: "alpha-name", State: control.StateQueued, PoolID: PoolA})
	beta := mustCreate(t, s, Beta, control.Session{
		ID: "sess_same", CreatorID: "act_a", Name: "beta-name", State: control.StateQueued, PoolID: PoolB})
	mustCreate(t, s, Beta, control.Session{
		ID: "sess_beta_only", CreatorID: "act_a", Name: "only", State: control.StateQueued, PoolID: PoolB})

	if alpha.WorkspaceID != Alpha || beta.WorkspaceID != Beta {
		t.Fatalf("a created row must carry its own workspace: %q / %q", alpha.WorkspaceID, beta.WorkspaceID)
	}
	got, err := s.Sessions.GetSession(ctx, Beta, "sess_same")
	if err != nil {
		t.Fatalf("get in Beta: %v", err)
	}
	if got.Name != "beta-name" || got.WorkspaceID != Beta {
		t.Fatalf("same id in two workspaces must be two rows: %+v", got)
	}
	if _, err := s.Sessions.GetSession(ctx, Alpha, "sess_beta_only"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("another workspace's session: err = %v, want ErrNotFound", err)
	}

	alphaRows, _, err := s.Sessions.ListSessions(ctx, Alpha, control.SessionQuery{})
	if err != nil {
		t.Fatalf("list Alpha: %v", err)
	}
	if len(alphaRows) != 1 || alphaRows[0].Name != "alpha-name" {
		t.Fatalf("Alpha's listing = %+v, want only its own row", sessionIDs(alphaRows))
	}
	betaRows, _, err := s.Sessions.ListSessions(ctx, Beta, control.SessionQuery{})
	if err != nil {
		t.Fatalf("list Beta: %v", err)
	}
	if len(betaRows) != 2 {
		t.Fatalf("Beta's listing = %+v, want its two rows", sessionIDs(betaRows))
	}
}

// caseSessionEmptyWorkspace (S3) pins that no session method accepts an
// unscoped call. An empty workspace is malformed input, not a missing row.
func caseSessionEmptyWorkspace(t *testing.T, s Stores) {
	ctx := context.Background()
	runner := control.RunnerID("runner_a")
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"CreateSession", func() error {
			_, err := s.Sessions.CreateSession(ctx, "", control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued})
			return err
		}},
		{"GetSession", func() error {
			_, err := s.Sessions.GetSession(ctx, "", "sess_example")
			return err
		}},
		{"SessionByIDem", func() error {
			_, err := s.Sessions.SessionByIDem(ctx, "", "act_a", "idem_1")
			return err
		}},
		{"ListSessions", func() error {
			_, _, err := s.Sessions.ListSessions(ctx, "", control.SessionQuery{})
			return err
		}},
		{"Transition", func() error {
			return s.Sessions.Transition(ctx, "", "sess_example", control.NonTerminal, control.StateDead, control.TransitionOpts{RunnerID: &runner})
		}},
		{"SetSessionSetupHash", func() error {
			return s.Sessions.SetSessionSetupHash(ctx, "", "sess_example", "h1")
		}},
		{"SetChildExitCode", func() error {
			return s.Sessions.SetChildExitCode(ctx, "", "sess_example", 0)
		}},
		{"NextControllerGeneration", func() error {
			_, err := s.Sessions.NextControllerGeneration(ctx, "", "sess_example")
			return err
		}},
	} {
		if err := tc.call(); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("%s with an empty workspace: err = %v, want ErrInvalid", tc.name, err)
		}
	}
}

// caseSessionUnknownWorkspace (S4) pins the difference between "malformed"
// and "nothing there": a workspace the store does not know is a write that
// lands nowhere and a read that finds nothing.
func caseSessionUnknownWorkspace(t *testing.T, s Stores) {
	ctx := context.Background()
	if _, err := s.Sessions.CreateSession(ctx, "ws_nobody", control.Session{
		ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA,
	}); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("create in an unknown workspace: err = %v, want ErrNotFound", err)
	}
	if _, err := s.Sessions.GetSession(ctx, "ws_nobody", "sess_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("get in an unknown workspace: err = %v, want ErrNotFound", err)
	}
}

// caseSessionActiveName (S5) pins the uniqueness the name index carries: one
// live name per creator per workspace, freed the moment the row goes
// terminal, and never shared across a creator or a workspace boundary.
func caseSessionActiveName(t *testing.T, s Stores) {
	ctx := context.Background()
	first := mustCreate(t, s, Alpha, control.Session{
		ID: "sess_one", CreatorID: "act_a", Name: "dev", State: control.StateQueued, PoolID: PoolA})

	if _, err := s.Sessions.CreateSession(ctx, Alpha, control.Session{
		ID: "sess_two", CreatorID: "act_a", Name: "dev", State: control.StateQueued, PoolID: PoolA,
		CreatedAt: baseTime(), UpdatedAt: baseTime(), LastEventAt: baseTime(),
	}); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("a second live dev for act_a: err = %v, want ErrConflict", err)
	}

	// Another creator, and another workspace, hold the same name freely.
	mustCreate(t, s, Alpha, control.Session{
		ID: "sess_other_actor", CreatorID: "act_b", Name: "dev", State: control.StateQueued, PoolID: PoolA})
	mustCreate(t, s, Beta, control.Session{
		ID: "sess_other_ws", CreatorID: "act_a", Name: "dev", State: control.StateQueued, PoolID: PoolB})

	if err := s.Sessions.Transition(ctx, Alpha, first.ID, control.NonTerminal, control.StateDestroyed, control.TransitionOpts{}); err != nil {
		t.Fatalf("destroy the first: %v", err)
	}
	mustCreate(t, s, Alpha, control.Session{
		ID: "sess_three", CreatorID: "act_a", Name: "dev", State: control.StateQueued, PoolID: PoolA})
}

// caseSessionIdempotency (S6) pins the replay: the same key from the same
// creator is answered with the row that key already created, not with an
// error and not with a second row.
func caseSessionIdempotency(t *testing.T, s Stores) {
	ctx := context.Background()
	first := mustCreate(t, s, Alpha, control.Session{
		ID: "sess_one", CreatorID: "act_a", Name: "one", State: control.StateQueued, PoolID: PoolA,
		IdempotencyKey: "idem_1"})

	replay, err := s.Sessions.CreateSession(ctx, Alpha, control.Session{
		ID: "sess_two", CreatorID: "act_a", Name: "two", State: control.StateQueued, PoolID: PoolA,
		IdempotencyKey: "idem_1",
		CreatedAt:      baseTime(), UpdatedAt: baseTime(), LastEventAt: baseTime()})
	if err != nil {
		t.Fatalf("replayed key: %v, want the first row and no error", err)
	}
	if replay.ID != first.ID || replay.Name != "one" {
		t.Fatalf("replay returned %+v, want the row idem_1 already created (%s)", replay, first.ID)
	}

	// The key is the creator's, not the workspace's: another creator using it
	// is a new session.
	other := mustCreate(t, s, Alpha, control.Session{
		ID: "sess_other", CreatorID: "act_b", Name: "other", State: control.StateQueued, PoolID: PoolA,
		IdempotencyKey: "idem_1"})
	if other.ID == first.ID {
		t.Fatalf("another creator's use of the same key must be a new row")
	}

	for _, tc := range []struct {
		creator control.ActorID
		want    control.SessionID
	}{{"act_a", "sess_one"}, {"act_b", "sess_other"}} {
		got, err := s.Sessions.SessionByIDem(ctx, Alpha, tc.creator, "idem_1")
		if err != nil || got.ID != tc.want {
			t.Fatalf("SessionByIDem(%s) = %+v, %v; want %s", tc.creator, got.ID, err, tc.want)
		}
	}
	if _, err := s.Sessions.SessionByIDem(ctx, Alpha, "act_a", "idem_nosuch"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("unknown key: err = %v, want ErrNotFound", err)
	}
	if _, err := s.Sessions.SessionByIDem(ctx, Alpha, "act_a", ""); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("empty key: err = %v, want ErrNotFound", err)
	}
}

// caseSessionListing (S7) pins the page: what it hides, what order it comes
// back in, and that its cursor resumes exactly where the page stopped.
func caseSessionListing(t *testing.T, s Stores) {
	ctx := context.Background()
	ids := []control.SessionID{"sess_e1", "sess_e2", "sess_e3", "sess_e4", "sess_e5"}
	for i, id := range ids {
		at := baseTime().Add(time.Duration(i) * time.Second)
		state := control.StateQueued
		if id == "sess_e3" {
			state = control.StateDead
		}
		mustCreate(t, s, Alpha, control.Session{
			ID: id, CreatorID: "act_a", State: state, PoolID: PoolA,
			CreatedAt: at, UpdatedAt: at, LastEventAt: at})
	}
	mustCreate(t, s, Beta, control.Session{
		ID: "sess_beta", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolB})

	live := []control.SessionID{"sess_e5", "sess_e4", "sess_e2", "sess_e1"}
	rows, _, err := s.Sessions.ListSessions(ctx, Alpha, control.SessionQuery{})
	if err != nil {
		t.Fatalf("default list: %v", err)
	}
	if !slices.Equal(sessionIDs(rows), live) {
		t.Fatalf("default list = %v, want %v (newest first, terminal and Beta hidden)", sessionIDs(rows), live)
	}

	all := []control.SessionID{"sess_e5", "sess_e4", "sess_e3", "sess_e2", "sess_e1"}
	rows, _, err = s.Sessions.ListSessions(ctx, Alpha, control.SessionQuery{IncludeTerminal: true})
	if err != nil {
		t.Fatalf("IncludeTerminal list: %v", err)
	}
	if !slices.Equal(sessionIDs(rows), all) {
		t.Fatalf("IncludeTerminal list = %v, want %v", sessionIDs(rows), all)
	}

	page, next, err := s.Sessions.ListSessions(ctx, Alpha, control.SessionQuery{Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if !slices.Equal(sessionIDs(page), live[:2]) || next == "" {
		t.Fatalf("first page = %v (next %q), want %v and a cursor", sessionIDs(page), next, live[:2])
	}
	paged, pages := pageSessions(t, s, Alpha, control.SessionQuery{Limit: 2})
	if !slices.Equal(sessionIDs(paged), live) {
		t.Fatalf("paged listing = %v, want %v", sessionIDs(paged), live)
	}
	if pages < 2 {
		t.Fatalf("a 4-row listing at limit 2 took %d page(s)", pages)
	}

	if _, _, err := s.Sessions.ListSessions(ctx, Alpha, control.SessionQuery{Cursor: "not-a-cursor!"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("garbage cursor: err = %v, want ErrInvalid", err)
	}
}

// casePlacementGeneration (S8) pins the guarded transition and the rule that
// makes a placement generation mean something: it advances exactly when a
// transition names a runner to run on, and never otherwise.
func casePlacementGeneration(t *testing.T, s Stores) {
	ctx := context.Background()
	row := mustCreate(t, s, Alpha, control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA})
	if row.PlacementGeneration != 1 {
		t.Fatalf("created generation = %d, want 1", row.PlacementGeneration)
	}

	// A from-list the row's state is not in loses, and changes nothing.
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, []control.SessionState{control.StateRunning}, control.StateCreating, control.TransitionOpts{}); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("transition from the wrong state: err = %v, want ErrConflict", err)
	}
	unchanged, err := s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	sameSession(t, "after a losing transition", unchanged, row)

	placed := control.RunnerID("runner_a")
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, []control.SessionState{control.StateQueued}, control.StateCreating, control.TransitionOpts{RunnerID: &placed}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunnerID != placed || got.PlacementGeneration != 2 {
		t.Fatalf("after placement: runner %q gen %d, want runner_a gen 2", got.RunnerID, got.PlacementGeneration)
	}
	if !got.UpdatedAt.After(row.UpdatedAt) || !got.LastEventAt.After(row.LastEventAt) {
		t.Fatalf("a transition must move both clocks: updated %v last_event %v, from %v",
			got.UpdatedAt, got.LastEventAt, row.UpdatedAt)
	}

	cleared := control.RunnerID("")
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, []control.SessionState{control.StateCreating}, control.StateQueued, control.TransitionOpts{RunnerID: &cleared}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunnerID != "" || got.PlacementGeneration != 2 {
		t.Fatalf("after requeue: runner %q gen %d, want no runner and gen still 2", got.RunnerID, got.PlacementGeneration)
	}

	// A transition that names no runner leaves the placement exactly as it
	// found it — the runner column and the generation both.
	reason := "lost"
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, control.NonTerminal, control.StateDead, control.TransitionOpts{Error: &reason}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Error != "lost" || got.RunnerID != "" || got.PlacementGeneration != 2 {
		t.Fatalf("after an error-only transition: %+v", got)
	}

	if err := s.Sessions.Transition(ctx, Alpha, "sess_nosuch", control.NonTerminal, control.StateDead, control.TransitionOpts{}); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("transition of an unknown id: err = %v, want ErrNotFound", err)
	}
}

// caseSessionProvenance (S9) pins the two unguarded observation writes: they
// move their own column and the edit clock, and leave the lifecycle clock
// alone, because neither is a lifecycle event.
func caseSessionProvenance(t *testing.T, s Stores) {
	ctx := context.Background()
	row := mustCreate(t, s, Alpha, control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA})

	if err := s.Sessions.SetSessionSetupHash(ctx, Alpha, row.ID, "h1"); err != nil {
		t.Fatal(err)
	}
	got, err := s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SetupHash != "h1" {
		t.Fatalf("setup hash = %q, want h1", got.SetupHash)
	}
	if !got.UpdatedAt.After(row.UpdatedAt) {
		t.Fatalf("a setup-hash write must bump updated_at: %v vs %v", got.UpdatedAt, row.UpdatedAt)
	}
	if !got.LastEventAt.Equal(row.LastEventAt) {
		t.Fatalf("a setup-hash write must leave last_event_at alone: %v vs %v", got.LastEventAt, row.LastEventAt)
	}

	if err := s.Sessions.SetChildExitCode(ctx, Alpha, row.ID, 7); err != nil {
		t.Fatal(err)
	}
	exited, err := s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if exited.ChildExitCode == nil || *exited.ChildExitCode != 7 {
		t.Fatalf("child exit code = %v, want 7", exited.ChildExitCode)
	}
	if !exited.UpdatedAt.After(got.UpdatedAt) {
		t.Fatalf("an exit-code write must bump updated_at: %v vs %v", exited.UpdatedAt, got.UpdatedAt)
	}
	if !exited.LastEventAt.Equal(row.LastEventAt) {
		t.Fatalf("an exit-code write must leave last_event_at alone: %v vs %v", exited.LastEventAt, row.LastEventAt)
	}

	// The pointer is the caller's own: writing through it must not reach back
	// into the store.
	*exited.ChildExitCode = 99
	again, err := s.Sessions.GetSession(ctx, Alpha, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ChildExitCode == nil || *again.ChildExitCode != 7 {
		t.Fatalf("the stored exit code aliased the caller's pointer: %v", again.ChildExitCode)
	}

	if err := s.Sessions.SetSessionSetupHash(ctx, Alpha, "sess_nosuch", "h1"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("setup hash on an unknown id: err = %v, want ErrNotFound", err)
	}
	if err := s.Sessions.SetChildExitCode(ctx, Alpha, "sess_nosuch", 0); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("exit code on an unknown id: err = %v, want ErrNotFound", err)
	}
}

// caseControllerGeneration (S10) pins the durable lease: the counter is the
// row's, it only ever goes up, and a same-named row in another workspace has
// a counter of its own.
func caseControllerGeneration(t *testing.T, s Stores) {
	ctx := context.Background()
	mustCreate(t, s, Alpha, control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA})
	mustCreate(t, s, Beta, control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolB})

	for want := uint64(1); want <= 3; want++ {
		got, err := s.Sessions.NextControllerGeneration(ctx, Alpha, "sess_example")
		if err != nil {
			t.Fatalf("next generation %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("generation = %d, want %d", got, want)
		}
	}
	row, err := s.Sessions.GetSession(ctx, Alpha, "sess_example")
	if err != nil {
		t.Fatal(err)
	}
	if row.ControllerGeneration != 3 {
		t.Fatalf("row's controller generation = %d, want 3", row.ControllerGeneration)
	}

	betaRow, err := s.Sessions.GetSession(ctx, Beta, "sess_example")
	if err != nil {
		t.Fatal(err)
	}
	if betaRow.ControllerGeneration != 0 {
		t.Fatalf("Beta's same-id row moved with Alpha's: %d, want 0", betaRow.ControllerGeneration)
	}
	if got, err := s.Sessions.NextControllerGeneration(ctx, Beta, "sess_example"); err != nil || got != 1 {
		t.Fatalf("Beta's first generation = %d, %v; want 1", got, err)
	}

	if _, err := s.Sessions.NextControllerGeneration(ctx, Alpha, "sess_nosuch"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("generation for an unknown id: err = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// environments
// ---------------------------------------------------------------------------

const (
	connectorAJSON = `{"type":"github","repo":"acme/app","base_branch":"main"}`
	connectorBJSON = `{"type":"tunnel","target":"http://service.agents.localhost:8080"}`
)

func fixtureEnvironment(id control.EnvironmentID, name string) control.Environment {
	return control.Environment{
		ID:             id,
		Name:           name,
		Image:          "img:1",
		Setup:          "make deps",
		SetupHash:      "h1",
		Init:           "make dev-server &",
		InitTimeoutSec: 120,
		EgressAllow:    []string{"index.example.test"},
		SecretRefs:     []string{"BUILD_TOKEN"},
		Connectors: []control.Connector{
			{Type: "github", Raw: json.RawMessage(connectorAJSON)},
			{Type: "tunnel", Raw: json.RawMessage(connectorBJSON)},
		},
		Requirements: control.Requirements{
			Capabilities: []string{"placement:runner_a", "gpu"},
			MinCPU:       2,
		},
		SetupTimeoutSec: 600,
		CreatedAt:       baseTime(),
		UpdatedAt:       baseTime(),
	}
}

// caseEnvironmentRoundTrip (E1) pins what a create stores: everything the
// caller gave, including the setup hash — the repository computes nothing —
// and nothing of the snapshot cache, which only SetEnvironmentSnapshot
// writes.
func caseEnvironmentRoundTrip(t *testing.T, s Stores) {
	ctx := context.Background()
	in := fixtureEnvironment("env_a", "dev")
	created := mustCreateEnv(t, s, Alpha, in)

	want := in
	want.WorkspaceID = Alpha
	sameEnvironment(t, "created row", created, want)
	if created.Snapshot.Ref != "" || created.Snapshot.Format != "" || created.SnapshotHash != "" {
		t.Fatalf("a fresh environment has no snapshot: %+v / %q", created.Snapshot, created.SnapshotHash)
	}
	if created.SetupHash != "h1" {
		t.Fatalf("SetupHash = %q, want the caller's h1 — the repository computes none", created.SetupHash)
	}

	read, err := s.Environments.GetEnvironment(ctx, Alpha, "env_a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	sameEnvironment(t, "read back", read, created)
}

// caseEnvironmentName (E2) pins the name index: unique inside a workspace,
// free across one, and enforced on a rename as well as a create.
func caseEnvironmentName(t *testing.T, s Stores) {
	ctx := context.Background()
	mustCreateEnv(t, s, Alpha, fixtureEnvironment("env_a", "dev"))

	if _, err := s.Environments.CreateEnvironment(ctx, Alpha, fixtureEnvironment("env_b", "dev")); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("duplicate name in one workspace: err = %v, want ErrConflict", err)
	}
	mustCreateEnv(t, s, Beta, fixtureEnvironment("env_a", "dev"))

	other := mustCreateEnv(t, s, Alpha, fixtureEnvironment("env_c", "other"))
	other.Name = "dev"
	if _, err := s.Environments.UpdateEnvironment(ctx, Alpha, other); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("rename onto a held name: err = %v, want ErrConflict", err)
	}
	if _, err := s.Environments.UpdateEnvironment(ctx, Alpha, fixtureEnvironment("env_nosuch", "ghost")); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("update of an unknown id: err = %v, want ErrNotFound", err)
	}
}

// caseEnvironmentIsolation (E3) pins that an environment id from another
// workspace is not a key here — on a read or on a delete.
func caseEnvironmentIsolation(t *testing.T, s Stores) {
	ctx := context.Background()
	alpha := mustCreateEnv(t, s, Alpha, fixtureEnvironment("env_a", "dev"))
	mustCreateEnv(t, s, Beta, fixtureEnvironment("env_b", "dev"))

	if _, err := s.Environments.GetEnvironment(ctx, Beta, alpha.ID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Alpha's environment read from Beta: err = %v, want ErrNotFound", err)
	}
	if err := s.Environments.DeleteEnvironment(ctx, Beta, alpha.ID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Alpha's environment deleted from Beta: err = %v, want ErrNotFound", err)
	}
	if _, err := s.Environments.GetEnvironment(ctx, Alpha, alpha.ID); err != nil {
		t.Fatalf("Alpha's environment must survive Beta's delete: %v", err)
	}
	if err := s.Environments.DeleteEnvironment(ctx, Alpha, alpha.ID); err != nil {
		t.Fatalf("delete in its own workspace: %v", err)
	}
	if err := s.Environments.DeleteEnvironment(ctx, Alpha, alpha.ID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("delete twice: err = %v, want ErrNotFound", err)
	}
}

// caseEnvironmentListing (E4) pins the page: name order, a cursor that
// resumes, and an unlimited query that is the whole workspace.
func caseEnvironmentListing(t *testing.T, s Stores) {
	ctx := context.Background()
	for id, name := range map[control.EnvironmentID]string{"env_c": "c", "env_a": "a", "env_b": "b"} {
		mustCreateEnv(t, s, Alpha, fixtureEnvironment(id, name))
	}
	mustCreateEnv(t, s, Beta, fixtureEnvironment("env_beta", "a"))

	want := []control.EnvironmentID{"env_a", "env_b", "env_c"}
	rows, next, err := s.Environments.ListEnvironments(ctx, Alpha, control.EnvironmentQuery{})
	if err != nil {
		t.Fatalf("unlimited list: %v", err)
	}
	if !slices.Equal(environmentIDs(rows), want) || next != "" {
		t.Fatalf("unlimited list = %v (next %q), want %v and no cursor", environmentIDs(rows), next, want)
	}

	page, next, err := s.Environments.ListEnvironments(ctx, Alpha, control.EnvironmentQuery{Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if !slices.Equal(environmentIDs(page), want[:2]) || next == "" {
		t.Fatalf("first page = %v (next %q), want %v and a cursor", environmentIDs(page), next, want[:2])
	}
	rest, _, err := s.Environments.ListEnvironments(ctx, Alpha, control.EnvironmentQuery{Limit: 2, Cursor: next})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if !slices.Equal(environmentIDs(rest), want[2:]) {
		t.Fatalf("second page = %v, want %v", environmentIDs(rest), want[2:])
	}
}

// caseEnvironmentUpdateIgnoresCache (E5) pins who owns the snapshot columns.
// An update may move the setup hash — that is how a cache goes stale — but
// it may not write, clear, or hijack the cache itself.
func caseEnvironmentUpdateIgnoresCache(t *testing.T, s Stores) {
	ctx := context.Background()
	env := mustCreateEnv(t, s, Alpha, fixtureEnvironment("env_a", "dev"))
	if err := s.Environments.SetEnvironmentSnapshot(ctx, Alpha, env.ID, "h1", "snap:1", "runner_a"); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	cached, err := s.Environments.GetEnvironment(ctx, Alpha, env.ID)
	if err != nil {
		t.Fatal(err)
	}

	upd := cached
	upd.SetupHash = "h2"
	upd.Setup = "make deps && make build"
	upd.Snapshot = control.Checkpoint{Ref: "hijacked", Format: "made-up"}
	upd.SnapshotHash = "deadbeef"
	moved, err := s.Environments.UpdateEnvironment(ctx, Alpha, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if moved.SetupHash != "h2" || moved.Setup != "make deps && make build" {
		t.Fatalf("update must store the caller's setup and hash: %+v", moved)
	}
	if moved.Snapshot.Ref != cached.Snapshot.Ref || moved.SnapshotHash != cached.SnapshotHash {
		t.Fatalf("update must leave the cache alone: %+v / %q, want %+v / %q",
			moved.Snapshot, moved.SnapshotHash, cached.Snapshot, cached.SnapshotHash)
	}
	read, err := s.Environments.GetEnvironment(ctx, Alpha, env.ID)
	if err != nil {
		t.Fatal(err)
	}
	if read.Snapshot.Ref != "snap:1" || read.SnapshotHash != "h1" {
		t.Fatalf("the stale cache must still be there and still be visibly stale: %+v / %q", read.Snapshot, read.SnapshotHash)
	}
}

// caseEnvironmentSnapshot (E6) pins the compare-and-set that keeps a snapshot
// honest, and the affinity capability a CURRENT snapshot lends the
// environment: emitted while the hash still matches, gone the moment it does
// not.
func caseEnvironmentSnapshot(t *testing.T, s Stores) {
	ctx := context.Background()
	env := mustCreateEnv(t, s, Alpha, fixtureEnvironment("env_a", "dev"))
	stored := slices.Clone(env.Requirements.Capabilities)

	if err := s.Environments.SetEnvironmentSnapshot(ctx, Alpha, env.ID, "h1", "snap:1", "runner_a"); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	cached, err := s.Environments.GetEnvironment(ctx, Alpha, env.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cached.Snapshot.Ref != "snap:1" || cached.Snapshot.Format == "" || cached.SnapshotHash != "h1" {
		t.Fatalf("snapshot not recorded: %+v / %q", cached.Snapshot, cached.SnapshotHash)
	}
	wantCaps := append(slices.Clone(stored), "snapshot:runner_a")
	if !slices.Equal(cached.Requirements.Capabilities, wantCaps) {
		t.Fatalf("capabilities = %q, want the stored ones then %q", cached.Requirements.Capabilities, "snapshot:runner_a")
	}

	if err := s.Environments.SetEnvironmentSnapshot(ctx, Alpha, env.ID, "h9", "snap:2", "runner_b"); !errors.Is(err, control.ErrStale) {
		t.Fatalf("snapshot against a hash the environment no longer has: err = %v, want ErrStale", err)
	}
	after, err := s.Environments.GetEnvironment(ctx, Alpha, env.ID)
	if err != nil {
		t.Fatal(err)
	}
	sameEnvironment(t, "after a losing snapshot", after, cached)

	// Once the setup hash moves on, the snapshot is stale and stops holding
	// the environment to the runner that built it.
	upd := after
	upd.SetupHash = "h2"
	if _, err := s.Environments.UpdateEnvironment(ctx, Alpha, upd); err != nil {
		t.Fatalf("update: %v", err)
	}
	moved, err := s.Environments.GetEnvironment(ctx, Alpha, env.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(moved.Requirements.Capabilities, stored) {
		t.Fatalf("a stale snapshot must stop pinning: capabilities = %q, want %q", moved.Requirements.Capabilities, stored)
	}

	if err := s.Environments.SetEnvironmentSnapshot(ctx, Alpha, "env_nosuch", "h1", "snap:3", "runner_a"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("snapshot for an unknown environment: err = %v, want ErrNotFound", err)
	}
}

// caseCountSessionsByEnvironment (E7) pins the delete guard's number: every
// session on the environment in THIS workspace, filtered by state, and never
// a session another workspace put on the same id.
func caseCountSessionsByEnvironment(t *testing.T, s Stores) {
	ctx := context.Background()
	mustCreateEnv(t, s, Alpha, fixtureEnvironment("env_a", "dev"))
	mustCreateEnv(t, s, Beta, fixtureEnvironment("env_a", "dev"))

	for _, tc := range []struct {
		id    control.SessionID
		state control.SessionState
	}{
		{"sess_one", control.StateQueued},
		{"sess_two", control.StateQueued},
		{"sess_three", control.StateDestroyed},
	} {
		mustCreate(t, s, Alpha, control.Session{
			ID: tc.id, CreatorID: "act_a", State: tc.state, PoolID: PoolA, EnvironmentID: "env_a"})
	}
	mustCreate(t, s, Beta, control.Session{
		ID: "sess_beta", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolB, EnvironmentID: "env_a"})

	for _, tc := range []struct {
		ws     control.WorkspaceID
		states []control.SessionState
		want   int
	}{
		{Alpha, nil, 3},
		{Alpha, []control.SessionState{control.StateQueued}, 2},
		{Beta, nil, 1},
	} {
		got, err := s.Environments.CountSessionsByEnvironment(ctx, tc.ws, "env_a", tc.states)
		if err != nil {
			t.Fatalf("count(%s, %v): %v", tc.ws, tc.states, err)
		}
		if got != tc.want {
			t.Fatalf("count(%s, %v) = %d, want %d", tc.ws, tc.states, got, tc.want)
		}
	}
}

// caseEnvironmentEmptyWorkspace (E8) pins that no environment method accepts
// an unscoped call.
func caseEnvironmentEmptyWorkspace(t *testing.T, s Stores) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"CreateEnvironment", func() error {
			_, err := s.Environments.CreateEnvironment(ctx, "", fixtureEnvironment("env_a", "dev"))
			return err
		}},
		{"GetEnvironment", func() error {
			_, err := s.Environments.GetEnvironment(ctx, "", "env_a")
			return err
		}},
		{"ListEnvironments", func() error {
			_, _, err := s.Environments.ListEnvironments(ctx, "", control.EnvironmentQuery{})
			return err
		}},
		{"UpdateEnvironment", func() error {
			_, err := s.Environments.UpdateEnvironment(ctx, "", fixtureEnvironment("env_a", "dev"))
			return err
		}},
		{"DeleteEnvironment", func() error {
			return s.Environments.DeleteEnvironment(ctx, "", "env_a")
		}},
		{"CountSessionsByEnvironment", func() error {
			_, err := s.Environments.CountSessionsByEnvironment(ctx, "", "env_a", nil)
			return err
		}},
		{"SetEnvironmentSnapshot", func() error {
			return s.Environments.SetEnvironmentSnapshot(ctx, "", "env_a", "h1", "snap:1", "runner_a")
		}},
	} {
		if err := tc.call(); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("%s with an empty workspace: err = %v, want ErrInvalid", tc.name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// fleet
// ---------------------------------------------------------------------------

// caseRunnerRoundTrip (F1) pins the runner row and the order it comes back
// in: every field the fleet keeps, capabilities in the order they were
// given, and a pool that has none answering with an empty list.
func caseRunnerRoundTrip(t *testing.T, s Stores) {
	ctx := context.Background()
	caps := []string{"placement:runner_a", "gpu"}
	for _, id := range []control.RunnerID{"runner_b", "runner_a"} {
		if err := s.Fleet.UpsertRunner(ctx, PoolA, control.Runner{
			ID: id, PoolID: PoolA, CapacityUsed: 1, CapacityTotal: 4, Connected: true,
			Generation: 1, Capabilities: caps, LastSeenAt: baseTime(),
		}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}

	rows, err := s.Fleet.ListRunners(ctx, PoolA)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 2 || rows[0].ID != "runner_a" || rows[1].ID != "runner_b" {
		t.Fatalf("listing = %+v, want [runner_a runner_b] in id order", rows)
	}
	got := rows[0]
	switch {
	case got.PoolID != PoolA:
		t.Fatalf("PoolID = %q, want %q", got.PoolID, PoolA)
	case got.CapacityUsed != 1 || got.CapacityTotal != 4:
		t.Fatalf("capacity = %d/%d, want 1/4", got.CapacityUsed, got.CapacityTotal)
	case !got.Connected:
		t.Fatalf("connected = false, want true")
	case got.Generation != 1:
		t.Fatalf("generation = %d, want 1", got.Generation)
	case !slices.Equal(got.Capabilities, caps):
		t.Fatalf("capabilities = %q, want %q in the given order", got.Capabilities, caps)
	case !got.LastSeenAt.Equal(baseTime()):
		t.Fatalf("last seen = %v, want %v", got.LastSeenAt, baseTime())
	}

	empty, err := s.Fleet.ListRunners(ctx, PoolB)
	if err != nil {
		t.Fatalf("list an empty pool: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("a pool with no runners = %+v, want empty", empty)
	}
}

// caseRunnerPoolIsolation (F2) pins that a runner id is a key only inside its
// pool: the same name in two pools is two rows, and a write to one is not a
// write to the other.
func caseRunnerPoolIsolation(t *testing.T, s Stores) {
	ctx := context.Background()
	for _, tc := range []struct {
		pool  control.PoolID
		total int
	}{{PoolA, 4}, {PoolB, 8}} {
		if err := s.Fleet.UpsertRunner(ctx, tc.pool, control.Runner{
			ID: "runner_a", PoolID: tc.pool, CapacityTotal: tc.total, Connected: true,
			Generation: 1, LastSeenAt: baseTime(),
		}); err != nil {
			t.Fatalf("upsert in %s: %v", tc.pool, err)
		}
	}

	if err := s.Fleet.SetRunnerConnected(ctx, PoolB, "runner_a", false); err != nil {
		t.Fatalf("disconnect in PoolB: %v", err)
	}
	for _, tc := range []struct {
		pool      control.PoolID
		total     int
		connected bool
	}{{PoolA, 4, true}, {PoolB, 8, false}} {
		rows, err := s.Fleet.ListRunners(ctx, tc.pool)
		if err != nil || len(rows) != 1 {
			t.Fatalf("list %s: %v %+v", tc.pool, err, rows)
		}
		if rows[0].CapacityTotal != tc.total || rows[0].Connected != tc.connected {
			t.Fatalf("%s's runner_a = total %d connected %v, want %d / %v",
				tc.pool, rows[0].CapacityTotal, rows[0].Connected, tc.total, tc.connected)
		}
	}
}

// caseGenerationFence (F3) pins the fence: a write from a superseded
// connection changes nothing, and the current generation may write again.
func caseGenerationFence(t *testing.T, s Stores) {
	ctx := context.Background()
	first := control.Runner{ID: "runner_a", PoolID: PoolA, CapacityTotal: 4, Connected: true, Generation: 2, Capabilities: []string{"gpu"}}
	if err := s.Fleet.UpsertRunner(ctx, PoolA, first); err != nil {
		t.Fatalf("upsert gen 2: %v", err)
	}
	stale := first
	stale.Generation, stale.CapacityTotal = 1, 99
	if err := s.Fleet.UpsertRunner(ctx, PoolA, stale); !errors.Is(err, control.ErrStale) {
		t.Fatalf("upsert gen 1 over 2: err = %v, want ErrStale", err)
	}
	rows, err := s.Fleet.ListRunners(ctx, PoolA)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 || rows[0].CapacityTotal != 4 {
		t.Fatalf("after a stale upsert: rows = %+v, err = %v; want the gen-2 row untouched", rows, err)
	}
	for _, gen := range []uint64{2, 3} {
		next := first
		next.Generation = gen
		if err := s.Fleet.UpsertRunner(ctx, PoolA, next); err != nil {
			t.Fatalf("upsert gen %d: %v", gen, err)
		}
	}
}

// caseRunnerConnected (F4) pins the connectivity flag: a runner the fleet
// has never seen cannot be marked anything, and a known one's flag moves
// with its clock.
func caseRunnerConnected(t *testing.T, s Stores) {
	ctx := context.Background()
	if err := s.Fleet.SetRunnerConnected(ctx, PoolA, "runner_nosuch", true); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("connect an unknown runner: err = %v, want ErrNotFound", err)
	}
	if err := s.Fleet.UpsertRunner(ctx, PoolA, control.Runner{
		ID: "runner_a", PoolID: PoolA, CapacityTotal: 4, Connected: true, Generation: 1, LastSeenAt: baseTime(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Fleet.SetRunnerConnected(ctx, PoolA, "runner_a", false); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	rows, err := s.Fleet.ListRunners(ctx, PoolA)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: %v %+v", err, rows)
	}
	if rows[0].Connected {
		t.Fatalf("connected = true, want false")
	}
	if !rows[0].LastSeenAt.After(baseTime()) {
		t.Fatalf("last seen = %v, want it moved past %v", rows[0].LastSeenAt, baseTime())
	}
}

// caseSessionsOnRunner (F5) pins the capacity/reconciliation read: the
// sessions on ONE runner of ONE pool, filtered by state, with an empty
// filter meaning every state.
func caseSessionsOnRunner(t *testing.T, s Stores) {
	ctx := context.Background()
	for _, tc := range []struct {
		id     control.SessionID
		pool   control.PoolID
		runner control.RunnerID
		state  control.SessionState
	}{
		{"sess_one", PoolA, "runner_a", control.StateRunning},
		{"sess_two", PoolA, "runner_a", control.StateRunning},
		{"sess_three", PoolA, "runner_a", control.StateCreating},
		{"sess_four", PoolB, "runner_a", control.StateRunning},
		{"sess_five", PoolA, "runner_b", control.StateRunning},
	} {
		mustCreate(t, s, Alpha, control.Session{
			ID: tc.id, CreatorID: "act_a", State: tc.state, PoolID: tc.pool, RunnerID: tc.runner})
	}

	for _, tc := range []struct {
		pool   control.PoolID
		runner control.RunnerID
		states []control.SessionState
		want   int
	}{
		{PoolA, "runner_a", []control.SessionState{control.StateRunning}, 2},
		{PoolA, "runner_a", []control.SessionState{control.StateRunning, control.StateCreating}, 3},
		{PoolB, "runner_a", nil, 1},
	} {
		rows, err := s.Fleet.SessionsOnRunner(ctx, tc.pool, tc.runner, tc.states)
		if err != nil {
			t.Fatalf("on-runner(%s, %s, %v): %v", tc.pool, tc.runner, tc.states, err)
		}
		if len(rows) != tc.want {
			t.Fatalf("on-runner(%s, %s, %v) = %v, want %d rows", tc.pool, tc.runner, tc.states, sessionIDs(rows), tc.want)
		}
		for _, r := range rows {
			if r.PoolID != tc.pool || r.RunnerID != tc.runner {
				t.Fatalf("on-runner returned a row from elsewhere: %+v", r)
			}
		}
	}
}

// caseOldestQueued (F6) pins the placement pass's read: this pool's queue,
// oldest first, and nothing of any other pool's.
func caseOldestQueued(t *testing.T, s Stores) {
	ctx := context.Background()
	want := []control.SessionID{"sess_one", "sess_two", "sess_three"}
	for i, id := range want {
		at := baseTime().Add(time.Duration(i) * time.Second)
		mustCreate(t, s, Alpha, control.Session{
			ID: id, CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA,
			CreatedAt: at, UpdatedAt: at, LastEventAt: at})
	}
	mustCreate(t, s, Alpha, control.Session{
		ID: "sess_beta_pool", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolB})
	mustCreate(t, s, Alpha, control.Session{
		ID: "sess_running", CreatorID: "act_a", State: control.StateRunning, PoolID: PoolA,
		CreatedAt: baseTime(), UpdatedAt: baseTime(), LastEventAt: baseTime()})

	rows, err := s.Fleet.OldestQueued(ctx, PoolA)
	if err != nil {
		t.Fatalf("oldest queued: %v", err)
	}
	if !slices.Equal(sessionIDs(rows), want) {
		t.Fatalf("PoolA's queue = %v, want %v oldest first", sessionIDs(rows), want)
	}
	rows, err = s.Fleet.OldestQueued(ctx, PoolB)
	if err != nil {
		t.Fatalf("oldest queued in PoolB: %v", err)
	}
	if !slices.Equal(sessionIDs(rows), []control.SessionID{"sess_beta_pool"}) {
		t.Fatalf("PoolB's queue = %v, want only its own row", sessionIDs(rows))
	}
}

// caseFleetEmptyPool (F7) pins that no fleet method accepts an unscoped call.
func caseFleetEmptyPool(t *testing.T, s Stores) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"UpsertRunner", func() error {
			return s.Fleet.UpsertRunner(ctx, "", control.Runner{ID: "runner_a", Generation: 1})
		}},
		{"SetRunnerConnected", func() error {
			return s.Fleet.SetRunnerConnected(ctx, "", "runner_a", true)
		}},
		{"ListRunners", func() error {
			_, err := s.Fleet.ListRunners(ctx, "")
			return err
		}},
		{"SessionsOnRunner", func() error {
			_, err := s.Fleet.SessionsOnRunner(ctx, "", "runner_a", nil)
			return err
		}},
		{"OldestQueued", func() error {
			_, err := s.Fleet.OldestQueued(ctx, "")
			return err
		}},
	} {
		if err := tc.call(); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("%s with an empty pool: err = %v, want ErrInvalid", tc.name, err)
		}
	}
}
