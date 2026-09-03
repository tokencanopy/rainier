package controld

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
)

func TestEligiblePoolsSumsConnectedRunners(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	pools := installationPools{st: st}
	scope := userScope(User{ID: "usr_example", Role: "member"})

	// An installation with no runners at all still has its pool: a session
	// with nowhere to go is queued waiting for a runner, not refused for
	// having no eligible pool.
	got, err := pools.EligiblePools(ctx, scope, control.Requirements{})
	if err != nil {
		t.Fatalf("empty fleet: %v", err)
	}
	if len(got) != 1 || got[0].ID != installPool || got[0].CapacityTotal != 0 || got[0].CapacityUsed != 0 {
		t.Fatalf("empty fleet pools = %+v", got)
	}

	// Seeded as a runner registers itself: its own capabilities on its own
	// row, which is where the pool's list is now unioned from.
	seedRunner := func(name string, used, total int, connected bool) {
		t.Helper()
		if err := st.Fleet().UpsertRunner(ctx, installPool, control.Runner{
			ID: control.RunnerID(name), PoolID: installPool,
			CapacityUsed: used, CapacityTotal: total, Connected: connected,
			Capabilities: runnerCapabilities(name),
		}); err != nil {
			t.Fatalf("seed runner %s: %v", name, err)
		}
	}
	seedRunner("runner-a", 1, 4, true)
	seedRunner("runner-b", 2, 2, true)
	seedRunner("runner-c", 5, 9, false)

	got, err = pools.EligiblePools(ctx, scope, control.Requirements{})
	if err != nil {
		t.Fatalf("eligible: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("pools = %+v, want exactly one", got)
	}
	// The disconnected runner contributes neither capacity nor capabilities.
	if got[0].CapacityUsed != 3 || got[0].CapacityTotal != 6 {
		t.Fatalf("capacity = %d/%d, want 3/6", got[0].CapacityUsed, got[0].CapacityTotal)
	}
	want := []string{"placement:runner-a", "placement:runner-b"}
	if strings.Join(got[0].Capabilities, ",") != strings.Join(want, ",") {
		t.Fatalf("capabilities = %v, want %v", got[0].Capabilities, want)
	}

	if _, err := pools.EligiblePools(ctx, control.Scope{WorkspaceID: "ws_other"}, control.Requirements{}); err == nil {
		t.Fatal("another workspace must not resolve this installation's pool")
	}
}

func TestIDGeneratorMintsDistinctPrefixedIDs(t *testing.T) {
	var g idGenerator
	seen := make(map[string]bool, 300)
	for i := 0; i < 100; i++ {
		for _, minted := range []struct{ id, prefix string }{
			{string(g.NewSessionID()), "sess_"},
			{string(g.NewEnvironmentID()), "env_"},
			{string(g.NewEventID()), "evt_"},
		} {
			if !strings.HasPrefix(minted.id, minted.prefix) {
				t.Fatalf("%q does not start with %q", minted.id, minted.prefix)
			}
			if len(minted.id) != len(minted.prefix)+32 {
				t.Fatalf("%q is not %s + 32 hex chars", minted.id, minted.prefix)
			}
			if seen[minted.id] {
				t.Fatalf("minted %q twice", minted.id)
			}
			seen[minted.id] = true
		}
	}
	if len(seen) != 300 {
		t.Fatalf("minted %d distinct ids, want 300", len(seen))
	}
}

// The recorder is the one place an application event meets a log, so it is
// also the place a session name, image, or error could leak into one. It
// logs three opaque fields and nothing else.
func TestLogRecorderLogsOnlyActionKindAndID(t *testing.T) {
	var buf bytes.Buffer
	out, flags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(out)
		log.SetFlags(flags)
	})

	err := logRecorder{}.Record(context.Background(), control.Event{
		ID:          "evt_example",
		WorkspaceID: installWorkspace,
		ActorID:     "usr_example",
		Action:      control.ActionCreate,
		Resource: control.Resource{
			Kind: control.ResourceSession, WorkspaceID: installWorkspace,
			ID: "sess_example", CreatorID: "usr_example",
		},
		At:    time.Now(),
		Usage: control.Usage{AgentTokenCount: 42},
	})
	if err != nil {
		t.Fatalf("Record must never fail: %v", err)
	}
	line := strings.TrimSpace(buf.String())
	if line != "controld: event create session sess_example" {
		t.Fatalf("logged %q", line)
	}
}

func TestSystemClockReportsWallTime(t *testing.T) {
	before := time.Now()
	got := systemClock{}.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %v, outside [%v, %v]", got, before, after)
	}
}

// A self-hosted installation has no transactions to open: the unit of work is
// the closure itself, run on the context it was handed, and fn's error is the
// answer unchanged. That is what the port permits a host without transactions
// to do, and Task 2 replaces it with the store's real one.
func TestDirectUnitOfWorkRunsTheClosureOnTheSameContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), userContextKey{}, User{ID: "usr_example"})
	var inner context.Context
	if err := (directUnitOfWork{}).Run(ctx, func(c context.Context) error {
		inner = c
		return nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if inner != ctx {
		t.Fatal("Run handed fn a context of its own; a host without transactions carries nothing")
	}
	boom := errors.New("safe context only")
	if err := (directUnitOfWork{}).Run(ctx, func(context.Context) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want fn's own error unchanged", err)
	}
}

// A self-hosted snapshot is a container image on the runner that built it, so
// the locator's whole answer is that one runner: nowhere before a snapshot
// exists, the holder once it does, and nowhere for a ref no environment of
// this workspace carries.
func TestPinnedCheckpointsNamesTheHolderAndNobodyElse(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	envs := st.Environments()
	loc := pinnedCheckpoints{st: st}

	env, err := envs.CreateEnvironment(ctx, installWorkspace, control.Environment{
		ID: "env_example", Name: "dev", Image: "registry.example.invalid/base@sha256:0000",
		Setup: "make deps", SetupHash: "h1",
	})
	if err != nil {
		t.Fatalf("create environment: %v", err)
	}

	// No snapshot yet: nowhere, and the caller boots without one.
	got, err := loc.LocateCheckpoint(ctx, installWorkspace, SnapshotCheckpoint("snap:1"))
	if err != nil {
		t.Fatalf("locate before the snapshot: %v", err)
	}
	if got.Portable || len(got.Runners) != 0 {
		t.Fatalf("location before the snapshot = %+v, want nowhere", got)
	}

	if err := envs.SetEnvironmentSnapshot(ctx, installWorkspace, env.ID, "h1", "snap:1", "runner_a"); err != nil {
		t.Fatalf("set snapshot: %v", err)
	}

	got, err = loc.LocateCheckpoint(ctx, installWorkspace, SnapshotCheckpoint("snap:1"))
	if err != nil {
		t.Fatalf("locate: %v", err)
	}
	if got.Portable {
		t.Fatal("a runner-built image is not portable")
	}
	if len(got.Runners) != 1 || got.Runners[0] != "runner_a" {
		t.Fatalf("runners = %v, want exactly the holder", got.Runners)
	}

	// A ref nothing in this workspace holds, an empty ref, and another
	// workspace's view are all nowhere rather than an error.
	for name, cp := range map[string]control.Checkpoint{
		"unknown ref": SnapshotCheckpoint("snap:other"),
		"empty ref":   {},
	} {
		got, err := loc.LocateCheckpoint(ctx, installWorkspace, cp)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Portable || len(got.Runners) != 0 {
			t.Fatalf("%s: location = %+v, want nowhere", name, got)
		}
	}
	got, err = loc.LocateCheckpoint(ctx, "ws_other", SnapshotCheckpoint("snap:1"))
	if err != nil {
		t.Fatalf("another workspace: %v", err)
	}
	if got.Portable || len(got.Runners) != 0 {
		t.Fatalf("another workspace saw %+v, want nowhere", got)
	}
}
