package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

func TestSessionConversionRoundTrip(t *testing.T) {
	code := 3
	in := Session{
		ID: "sess_example", OwnerID: "usr_example", Name: "investigate",
		Image: "", ResolvedImage: "registry.example.invalid/snap@sha256:0000",
		Cmd: []string{"claude"}, EgressAllow: []string{"example.com"},
		State: StateRunning, Runner: "runner-a", IdempotencyKey: "idem_example",
		EnvironmentID: "env_example", SetupHash: "abc", Repos: []RepoRef{{Repo: "acme/app"}},
		ChildExitCode: &code,
	}
	c := sessionToControl(in)
	if c.WorkspaceID != installWorkspace || c.PoolID != installPool || c.CreatorID != "usr_example" ||
		c.RunnerID != "runner-a" || c.PlacementGeneration != 1 ||
		c.Spec.Image != "registry.example.invalid/snap@sha256:0000" {
		t.Fatalf("toControl = %+v", c)
	}
	back := sessionFromControl(c)
	back.CreatedAt, back.UpdatedAt, back.LastEventAt = in.CreatedAt, in.UpdatedAt, in.LastEventAt
	if !reflect.DeepEqual(back, in) {
		t.Fatalf("round trip drifted:\n got %+v\nwant %+v", back, in)
	}
	c.Spec.Cmd[0] = "mutated"
	if in.Cmd[0] == "mutated" {
		t.Fatal("toControl aliased the store's slice")
	}
}

// A scratch session has no environment, so its image round-trips through
// Image rather than ResolvedImage — the two columns are how the store tells
// "what the caller asked for" from "what resolution settled on".
func TestSessionConversionScratchKeepsCallerImage(t *testing.T) {
	in := Session{ID: "sess_example", OwnerID: "usr_example", Image: "alpine:example", State: StateQueued}
	c := sessionToControl(in)
	if c.Spec.Image != "alpine:example" || c.EnvironmentID != "" {
		t.Fatalf("toControl = %+v", c)
	}
	back := sessionFromControl(c)
	if back.Image != "alpine:example" || back.ResolvedImage != "" {
		t.Fatalf("scratch image = %q / resolved %q", back.Image, back.ResolvedImage)
	}
	if back.Cmd != nil || back.EgressAllow != nil || back.Repos != nil {
		t.Fatalf("nil slices must stay nil, got %+v", back)
	}
}

func TestRepositoriesRefuseOtherWorkspaces(t *testing.T) {
	st := NewMemStore()
	st.CreateSession(context.Background(), Session{ID: "sess_example", OwnerID: "usr_example", State: StateQueued})
	sessions := storeSessions{st: st}
	if _, err := sessions.GetSession(context.Background(), "ws_other", "sess_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace: got %v, want ErrNotFound", err)
	}
	if err := sessions.Transition(context.Background(), "ws_other", "sess_example", control.NonTerminal, control.StateCanceled, control.TransitionOpts{}); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace transition: got %v", err)
	}
	if row, _ := st.GetSession(context.Background(), "sess_example"); row.State != StateQueued {
		t.Fatal("a refused transition mutated the store")
	}

	// The same refusal on the other two repositories and on the pool key:
	// isolation is a property of every adapter method, not of sessions.
	envs := storeEnvironments{st: st}
	if _, err := envs.GetEnvironment(context.Background(), "ws_other", "env_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace environment: got %v", err)
	}
	if err := envs.DeleteEnvironment(context.Background(), "ws_other", "env_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace environment delete: got %v", err)
	}
	fleet := &storeFleet{st: st, gens: &runnerGenerations{}}
	if _, err := fleet.ListRunners(context.Background(), "pool_other"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other pool: got %v", err)
	}
	if err := fleet.UpsertRunner(context.Background(), "pool_other", control.Runner{ID: "runner-a"}); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other pool upsert: got %v", err)
	}
	if runners, _ := st.ListRunners(context.Background()); len(runners) != 0 {
		t.Fatal("a refused upsert wrote a runner")
	}
}

func TestEnvironmentPlacementRoundTripsAsCapability(t *testing.T) {
	e := Environment{ID: "env_example", Name: "std", Image: "img", Placement: "runner-a",
		SnapshotRef: "snap:1", SnapshotRunner: "runner-b", SnapshotHash: "h", SetupHash: "h"}
	c := environmentToControl(e)
	if !reflect.DeepEqual(c.Requirements.Capabilities, []string{"placement:runner-a", "snapshot:runner-b"}) {
		t.Fatalf("capabilities = %v", c.Requirements.Capabilities)
	}
	if c.Snapshot.Ref != "snap:1" || c.SnapshotHash != "h" {
		t.Fatalf("snapshot = %+v / %q", c.Snapshot, c.SnapshotHash)
	}
	back := environmentFromControl(c)
	if back.Placement != "runner-a" {
		t.Fatalf("placement = %q", back.Placement)
	}
	if back.SnapshotRef != "" || back.SnapshotRunner != "" || back.SnapshotHash != "" {
		t.Fatalf("fromControl must never write snapshot columns, got %+v", back)
	}
	stale := e
	stale.SetupHash = "changed"
	if caps := environmentToControl(stale).Requirements.Capabilities; len(caps) != 1 || caps[0] != "placement:runner-a" {
		t.Fatalf("a stale snapshot must not pin placement, got %v", caps)
	}
	// No pin and no snapshot at all: the capability list stays nil rather
	// than becoming an empty slice, so a Requirements comparison is stable.
	bare := environmentToControl(Environment{ID: "env_example", Name: "std", Image: "img"})
	if bare.Requirements.Capabilities != nil || !reflect.DeepEqual(bare.Snapshot, control.Checkpoint{}) {
		t.Fatalf("bare environment = %+v", bare)
	}
}

// The environment body — everything that is not placement or snapshot —
// survives the trip in both directions, connectors included.
func TestEnvironmentBodyRoundTrips(t *testing.T) {
	in := Environment{
		ID: "env_example", Name: "std", Image: "img", Setup: "make setup",
		SetupHash: "h", Init: "make init", InitTimeoutSec: 30, SetupTimeoutSec: 600,
		EgressAllow: []string{"example.com"}, SecretRefs: []string{"API_TOKEN"},
		Connectors: []Connector{{Type: "github", Raw: json.RawMessage(`{"type":"github","repo":"acme/app"}`)}},
	}
	back := environmentFromControl(environmentToControl(in))
	if !reflect.DeepEqual(back, in) {
		t.Fatalf("round trip drifted:\n got %+v\nwant %+v", back, in)
	}
}

func TestCreateSessionIdempotentReplayReturnsExisting(t *testing.T) {
	ctx := context.Background()
	sessions := storeSessions{st: NewMemStore()}

	first, err := sessions.CreateSession(ctx, installWorkspace, control.Session{
		ID: "sess_first", CreatorID: "usr_example", Name: "investigate",
		State: control.StateQueued, IdempotencyKey: "idem_example",
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if first.ID != "sess_first" || first.WorkspaceID != installWorkspace {
		t.Fatalf("first = %+v", first)
	}

	// A replayed key is not a conflict: the contract says it returns the row
	// the key already created, so the caller sees its own earlier answer.
	second, err := sessions.CreateSession(ctx, installWorkspace, control.Session{
		ID: "sess_second", CreatorID: "usr_example", Name: "investigate-again",
		State: control.StateQueued, IdempotencyKey: "idem_example",
	})
	if err != nil {
		t.Fatalf("replay: got %v, want nil", err)
	}
	if second.ID != "sess_first" || second.Name != "investigate" {
		t.Fatalf("replay returned %+v, want the first row", second)
	}

	// A name already held by a live session of the same creator is still a
	// conflict, and it is reported as one.
	if _, err := sessions.CreateSession(ctx, installWorkspace, control.Session{
		ID: "sess_third", CreatorID: "usr_example", Name: "investigate", State: control.StateQueued,
	}); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("duplicate name: got %v, want ErrConflict", err)
	}

	if _, err := sessions.SessionByIDem(ctx, installWorkspace, "usr_example", "idem_absent"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("unknown key: got %v, want ErrNotFound", err)
	}
}

func TestStoreErrMapsSentinels(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"nil stays nil", nil, nil},
		{"not found", ErrNotFound, control.ErrNotFound},
		{"wrapped not found", fmt.Errorf("get session: %w", ErrNotFound), control.ErrNotFound},
		{"conflict", ErrConflict, control.ErrConflict},
		{"wrapped conflict", fmt.Errorf("transition: %w", ErrConflict), control.ErrConflict},
		{"anything else is unavailable", errors.New("pq: connection refused"), control.ErrUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := storeErr(tc.in)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	// The mapped error is the bare sentinel: a store message may carry SQL,
	// a DSN, or a row's contents, none of which may reach a control error.
	got := storeErr(errors.New("pq: relation \"sessions\" does not exist"))
	if got.Error() != control.ErrUnavailable.Error() {
		t.Fatalf("store text leaked into %q", got.Error())
	}
}

func TestRunnerGenerationsAreMonotonicPerName(t *testing.T) {
	g := &runnerGenerations{}
	if got := g.current("runner-c"); got != 0 {
		t.Fatalf("never connected: got %d, want 0", got)
	}
	if got := g.next("runner-a"); got != 1 {
		t.Fatalf("first: got %d, want 1", got)
	}
	if got := g.next("runner-a"); got != 2 {
		t.Fatalf("second: got %d, want 2", got)
	}
	if got := g.next("runner-b"); got != 1 {
		t.Fatalf("another name starts over: got %d, want 1", got)
	}
	if got := g.current("runner-a"); got != 2 {
		t.Fatalf("current: got %d, want 2", got)
	}
	if got := g.current("runner-c"); got != 0 {
		t.Fatalf("still never connected: got %d, want 0", got)
	}
}

// ListRunners is where a runner's process-local generation and its two
// synthesized capabilities are attached — neither has a column in O8.
func TestListRunnersSynthesizesGenerationAndCapabilities(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	st.UpsertRunner(ctx, Runner{Name: "runner-a", CapacityUsed: 1, CapacityTotal: 4, Connected: true})
	st.UpsertRunner(ctx, Runner{Name: "runner-b", CapacityTotal: 2})
	gens := &runnerGenerations{}
	gens.next("runner-a")
	gens.next("runner-a")

	runners, err := (&storeFleet{st: st, gens: gens}).ListRunners(ctx, installPool)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(runners) != 2 || runners[0].ID != "runner-a" || runners[1].ID != "runner-b" {
		t.Fatalf("runners = %+v", runners)
	}
	if runners[0].PoolID != installPool || runners[0].Generation != 2 || runners[0].CapacityUsed != 1 || runners[0].CapacityTotal != 4 || !runners[0].Connected {
		t.Fatalf("runner-a = %+v", runners[0])
	}
	if !reflect.DeepEqual(runners[0].Capabilities, []string{"placement:runner-a", "snapshot:runner-a"}) {
		t.Fatalf("capabilities = %v", runners[0].Capabilities)
	}
	if runners[1].Generation != 0 {
		t.Fatalf("a runner that never connected in this process = generation %d", runners[1].Generation)
	}
}

// UpsertRunner writes only the four columns the store has; generation and
// capabilities are process-local in O8 and are deliberately dropped.
func TestUpsertRunnerIgnoresGenerationAndCapabilities(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	fleet := &storeFleet{st: st, gens: &runnerGenerations{}}
	if err := fleet.UpsertRunner(ctx, installPool, control.Runner{
		ID: "runner-a", PoolID: installPool, CapacityUsed: 2, CapacityTotal: 8,
		Connected: true, Generation: 7, Capabilities: []string{"gpu"},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rows, err := st.ListRunners(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %+v err = %v", rows, err)
	}
	if rows[0].Name != "runner-a" || rows[0].CapacityUsed != 2 || rows[0].CapacityTotal != 8 || !rows[0].Connected {
		t.Fatalf("row = %+v", rows[0])
	}
	// The synthesized pair comes back regardless of what was written.
	back, err := fleet.ListRunners(ctx, installPool)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !reflect.DeepEqual(back[0].Capabilities, []string{"placement:runner-a", "snapshot:runner-a"}) {
		t.Fatalf("capabilities = %v", back[0].Capabilities)
	}
}

func TestFleetReadsConvertEveryRow(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	st.CreateSession(ctx, Session{ID: "sess_placed", OwnerID: "usr_example", State: StateRunning, Runner: "runner-a"})
	st.CreateSession(ctx, Session{ID: "sess_queued", OwnerID: "usr_example", State: StateQueued})
	fleet := &storeFleet{st: st, gens: &runnerGenerations{}}

	placed, err := fleet.SessionsOnRunner(ctx, installPool, "runner-a", []control.SessionState{control.StateRunning})
	if err != nil || len(placed) != 1 {
		t.Fatalf("placed = %+v err = %v", placed, err)
	}
	if placed[0].ID != "sess_placed" || placed[0].WorkspaceID != installWorkspace || placed[0].PoolID != installPool || placed[0].RunnerID != "runner-a" {
		t.Fatalf("placed[0] = %+v", placed[0])
	}

	queued, err := fleet.OldestQueued(ctx, installPool)
	if err != nil || len(queued) != 1 {
		t.Fatalf("queued = %+v err = %v", queued, err)
	}
	if queued[0].ID != "sess_queued" || queued[0].PoolID != installPool {
		t.Fatalf("queued[0] = %+v", queued[0])
	}
}

// An unreadable cursor is the caller's mistake, not a broken store, so it is
// ErrInvalid rather than ErrUnavailable.
func TestListSessionsInvalidCursorIsInvalid(t *testing.T) {
	ctx := context.Background()
	sessions := storeSessions{st: NewMemStore()}
	if _, _, err := sessions.ListSessions(ctx, installWorkspace, control.SessionQuery{Cursor: "not-a-cursor"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

// The store reports both a superseded setup hash and a vanished environment
// as ErrConflict; the contract calls a snapshot built from edited setup
// stale, so this is the one method that does not map ErrConflict straight
// through.
func TestSetEnvironmentSnapshotMapsConflictToStale(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	envs := storeEnvironments{st: st}
	stored, err := st.CreateEnvironment(ctx, Environment{ID: "env_example", Name: "std", Image: "img", Setup: "make setup"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := envs.SetEnvironmentSnapshot(ctx, installWorkspace, "env_example", stored.SetupHash, "snap:1", "runner-a"); err != nil {
		t.Fatalf("current hash: %v", err)
	}
	row, _ := st.GetEnvironment(ctx, "env_example")
	if row.SnapshotRef != "snap:1" || row.SnapshotRunner != "runner-a" || row.SnapshotHash != stored.SetupHash {
		t.Fatalf("snapshot columns = %+v", row)
	}

	if err := envs.SetEnvironmentSnapshot(ctx, installWorkspace, "env_example", "superseded", "snap:2", "runner-a"); !errors.Is(err, control.ErrStale) {
		t.Fatalf("stale hash: got %v, want ErrStale", err)
	}
	if err := envs.SetEnvironmentSnapshot(ctx, installWorkspace, "env_absent", "h", "snap:2", "runner-a"); !errors.Is(err, control.ErrStale) {
		t.Fatalf("vanished environment: got %v, want ErrStale", err)
	}
	if err := envs.SetEnvironmentSnapshot(ctx, "ws_other", "env_example", stored.SetupHash, "snap:2", "runner-a"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace: got %v, want ErrNotFound", err)
	}
}

// UpdateEnvironment carries the three snapshot columns forward itself:
// control.Environment's Snapshot fields are the store's to write, and a
// round trip through the control model must not clear them.
func TestUpdateEnvironmentPreservesSnapshotColumns(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	envs := storeEnvironments{st: st}
	stored, err := st.CreateEnvironment(ctx, Environment{ID: "env_example", Name: "std", Image: "img"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := st.SetEnvironmentSnapshot(ctx, "env_example", stored.SetupHash, "snap:1", "runner-a"); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	current, err := envs.GetEnvironment(ctx, installWorkspace, "env_example")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	current.Name = "renamed"
	updated, err := envs.UpdateEnvironment(ctx, installWorkspace, current)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Name != "renamed" {
		t.Fatalf("name = %q", updated.Name)
	}
	row, _ := st.GetEnvironment(ctx, "env_example")
	if row.SnapshotRef != "snap:1" || row.SnapshotRunner != "runner-a" || row.SnapshotHash != stored.SetupHash {
		t.Fatalf("snapshot columns lost: %+v", row)
	}
	if updated.Snapshot.Ref != "snap:1" {
		t.Fatalf("returned environment lost its snapshot: %+v", updated.Snapshot)
	}
}
