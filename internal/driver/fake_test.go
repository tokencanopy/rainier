// internal/driver/fake_test.go
package driver

import (
	"context"
	"reflect"
	"testing"
)

func TestFakeSatisfiesContract(t *testing.T) {
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewFake(4)
		return d, func() {}
	})
}

// TestFakeRecordsWorkspaceVolumeAndEnv pins the bookkeeping the fake owes the
// packages that test against it (runnerd, controld): a session's workspace
// volume appears on Create under the same `rainier-ws-<session>` name the
// docker driver uses, survives a cold park, and goes away on Destroy — plus
// the Spec.Env it was handed, so callers can assert what they injected without
// a docker daemon.
func TestFakeRecordsWorkspaceVolumeAndEnv(t *testing.T) {
	f := NewFake(4)
	ctx := context.Background()

	h, err := f.Create(ctx, Spec{
		SessionID: "sess-a",
		Env:       map[string]string{"FOO": "bar"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !f.hasVolume("rainier-ws-sess-a") {
		t.Fatalf("Create recorded no workspace volume: %v", f.Volumes())
	}
	if got := f.envFor(h.ID); !reflect.DeepEqual(got, map[string]string{"FOO": "bar"}) {
		t.Errorf("recorded env = %v, want FOO=bar", got)
	}

	// Cold park keeps the volume: `docker stop` does not touch volumes, and a
	// fake that dropped it here would let a runnerd test pass against a
	// behavior docker doesn't have.
	if err := f.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if !f.hasVolume("rainier-ws-sess-a") {
		t.Error("cold park dropped the workspace volume")
	}
	if err := f.Resume(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if !f.hasVolume("rainier-ws-sess-a") {
		t.Error("resume dropped the workspace volume")
	}

	if err := f.Destroy(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if f.hasVolume("rainier-ws-sess-a") {
		t.Errorf("Destroy left the workspace volume behind: %v", f.Volumes())
	}
}

// TestFakeDestroyContainerKeepsTheWorkspace is the fake's half of the crash
// path. runnerd's crash tests run against this driver and assert on nothing
// but what it records, so if the fake dropped the volume here those tests
// would go green against exactly the bug they exist to catch: a container
// death taking a user's workspace with it.
func TestFakeDestroyContainerKeepsTheWorkspace(t *testing.T) {
	f := NewFake(4)
	ctx := context.Background()

	h, err := f.Create(ctx, Spec{SessionID: "sess-crash"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.DestroyContainer(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if g, _ := f.Inspect(ctx, h.ID); g.State != StateGone {
		t.Fatalf("state after DestroyContainer = %s, want gone", g.State)
	}
	if !f.hasVolume("rainier-ws-sess-crash") {
		t.Fatalf("DestroyContainer dropped the workspace volume: %v", f.Volumes())
	}
	// The container is gone, so its capacity slot is back — that is the whole
	// reason the crash path destroys anything at all.
	if used, _, _ := f.Capacity(ctx); used != 0 {
		t.Fatalf("used after DestroyContainer = %d, want 0", used)
	}

	if err := f.RemoveWorkspace(ctx, "sess-crash"); err != nil {
		t.Fatal(err)
	}
	if f.hasVolume("rainier-ws-sess-crash") {
		t.Fatalf("RemoveWorkspace left the volume behind: %v", f.Volumes())
	}
}

// TestFakeRecordedEnvIsACopy: the fake must not alias the caller's map, or a
// test that mutates its own Spec.Env after Create would silently rewrite what
// the driver "received".
func TestFakeRecordedEnvIsACopy(t *testing.T) {
	f := NewFake(2)
	env := map[string]string{"FOO": "bar"}
	h, err := f.Create(context.Background(), Spec{SessionID: "sess-b", Env: env})
	if err != nil {
		t.Fatal(err)
	}
	env["FOO"] = "mutated"
	if got := f.envFor(h.ID)["FOO"]; got != "bar" {
		t.Errorf("recorded env FOO = %q after the caller mutated its map; want the value as passed", got)
	}
}

// TestFakeNoSessionIDNoVolume mirrors the docker driver: with no session id
// there is no name to key a workspace on, so no volume is recorded rather than
// one shared `rainier-ws-` for every id-less session.
func TestFakeNoSessionIDNoVolume(t *testing.T) {
	f := NewFake(2)
	if _, err := f.Create(context.Background(), Spec{}); err != nil {
		t.Fatal(err)
	}
	if names := f.Volumes(); len(names) != 0 {
		t.Errorf("id-less create recorded volumes %v, want none", names)
	}
}

// TestFakeRecordsPrepulls pins the bookkeeping runnerd's agent tests depend
// on: Prepull is otherwise invisible (nothing about the fake's state changes),
// so recording the refs is the only way a caller can prove the command reached
// the driver at all.
func TestFakeRecordsPrepulls(t *testing.T) {
	f := NewFake(2)
	ctx := context.Background()
	if err := f.Prepull(ctx, "rainier-env:e1-aaa"); err != nil {
		t.Fatal(err)
	}
	if err := f.Prepull(ctx, "rainier-env:e2-bbb"); err != nil {
		t.Fatal(err)
	}
	want := []string{"rainier-env:e1-aaa", "rainier-env:e2-bbb"}
	if got := f.Pulls(); !reflect.DeepEqual(got, want) {
		t.Errorf("Pulls() = %v, want %v (in call order)", got, want)
	}

	// Same guard as the docker driver's: a ref-less prepull is an upstream
	// bug, and a fake that accepted it would let a runnerd test pass against
	// a call production rejects.
	if err := f.Prepull(ctx, ""); err == nil {
		t.Error("Prepull with an empty ref = nil, want an error")
	}
	if got := f.Pulls(); len(got) != 2 {
		t.Errorf("a rejected prepull was recorded: %v", got)
	}
}

// TestFakeRecordsSnapshotStrips is the fake's half of the strip guarantee.
// The docker driver's stripping is only visible in a committed image's config;
// above the driver there is nothing to look at, so the fake's record IS the
// observable that runnerd's and the e2e suite's tests assert on — that the
// keys which must never reach a committed image were named on the call.
func TestFakeRecordsSnapshotStrips(t *testing.T) {
	f := NewFake(2)
	ctx := context.Background()
	h, err := f.Create(ctx, Spec{SessionID: "sess-s", Env: map[string]string{"TOKEN": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Snapshot(ctx, h.ID, "rainier-env:e1-aaa", []string{"TOKEN", "RAINIER_SETUP_B64"}); err != nil {
		t.Fatal(err)
	}
	// Recorded on the generated-ref path too: what a caller asked to be
	// stripped matters whoever named the tag.
	if _, err := f.Snapshot(ctx, h.ID, "", nil); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"TOKEN", "RAINIER_SETUP_B64"}, nil}
	if got := f.Strips(); !reflect.DeepEqual(got, want) {
		t.Errorf("Strips() = %v, want %v (in call order)", got, want)
	}

	// A snapshot that never happened must not be recorded — otherwise a
	// caller could read the record as proof of a commit that failed.
	if _, err := f.Snapshot(ctx, "no-such-handle", "ref", []string{"TOKEN"}); err == nil {
		t.Error("Snapshot of an unknown handle = nil, want an error")
	}
	if got := f.Strips(); len(got) != 2 {
		t.Errorf("a failed snapshot was recorded: %v", got)
	}
}

// TestFakeStripsIsACopy: like Pulls, the accessor must not hand out the fake's
// own slices — a caller sorting or appending to what it got would rewrite the
// driver's record.
func TestFakeStripsIsACopy(t *testing.T) {
	f := NewFake(2)
	ctx := context.Background()
	h, _ := f.Create(ctx, Spec{SessionID: "sess-t"})
	if _, err := f.Snapshot(ctx, h.ID, "ref", []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	got := f.Strips()
	got[0][0] = "mutated"
	if again := f.Strips(); again[0][0] != "A" {
		t.Errorf("Strips()[0][0] = %q after the caller mutated its copy; want %q", again[0][0], "A")
	}
}

// TestFakePullsIsACopy: the accessor must not hand out the fake's own slice,
// or a caller appending to what it got would rewrite the driver's record.
func TestFakePullsIsACopy(t *testing.T) {
	f := NewFake(2)
	if err := f.Prepull(context.Background(), "img:1"); err != nil {
		t.Fatal(err)
	}
	got := f.Pulls()
	got[0] = "mutated"
	if again := f.Pulls(); again[0] != "img:1" {
		t.Errorf("Pulls()[0] = %q after the caller mutated its copy; want %q", again[0], "img:1")
	}
}
