package controld

import (
	"bytes"
	"context"
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

	st.UpsertRunner(ctx, Runner{Name: "runner-a", CapacityUsed: 1, CapacityTotal: 4, Connected: true})
	st.UpsertRunner(ctx, Runner{Name: "runner-b", CapacityUsed: 2, CapacityTotal: 2, Connected: true})
	st.UpsertRunner(ctx, Runner{Name: "runner-c", CapacityUsed: 5, CapacityTotal: 9, Connected: false})

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
	want := []string{"placement:runner-a", "snapshot:runner-a", "placement:runner-b", "snapshot:runner-b"}
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
