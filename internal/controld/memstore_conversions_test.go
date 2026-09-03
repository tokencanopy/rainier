// internal/controld/memstore_conversions_test.go
package controld

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// The conversions in memstore.go, which lower the store's own control rows
// onto the old single-tenant surface and lift them back. They are the last
// users of the twin types, and they go with them in Task 5; until then this
// is where their round trips are pinned (they moved here when adapt_store.go
// was deleted).

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
