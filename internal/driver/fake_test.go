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
		t.Fatalf("Create recorded no workspace volume: %v", f.volumeNames())
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
		t.Errorf("Destroy left the workspace volume behind: %v", f.volumeNames())
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
	if names := f.volumeNames(); len(names) != 0 {
		t.Errorf("id-less create recorded volumes %v, want none", names)
	}
}
