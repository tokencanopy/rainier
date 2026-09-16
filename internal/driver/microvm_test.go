// internal/driver/microvm_test.go
package driver

import (
	"context"
	"reflect"
	"testing"
)

func TestMicrovmSatisfiesContract(t *testing.T) {
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewMicrovm(MicrovmOpts{
			TotalSlots: 4,
			Engine:     NewSimulatedEngine(),
		})
		return d, func() {}
	})
}

func TestMicrovmRecordsWorkspaceVolumeAndEnv(t *testing.T) {
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		Engine:     NewSimulatedEngine(),
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "mvm-sess-a",
		Env:       map[string]string{"FOO": "bar"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !m.HasWorkspace("mvm-sess-a") {
		t.Fatalf("Create recorded no workspace volume: %v", m.Volumes())
	}

	// Cold park keeps the volume
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	if !m.HasWorkspace("mvm-sess-a") {
		t.Error("cold park dropped the workspace volume")
	}
	if _, err := m.Resume(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if !m.HasWorkspace("mvm-sess-a") {
		t.Error("resume dropped the workspace volume")
	}

	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if m.HasWorkspace("mvm-sess-a") {
		t.Errorf("Destroy left the workspace volume behind: %v", m.Volumes())
	}
}

func TestMicrovmDestroyContainerKeepsWorkspace(t *testing.T) {
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		Engine:     NewSimulatedEngine(),
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "mvm-sess-crash"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.DestroyContainer(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateGone {
		t.Fatalf("state after DestroyContainer = %s, want gone", g.State)
	}
	if !m.HasWorkspace("mvm-sess-crash") {
		t.Fatalf("DestroyContainer dropped the workspace volume: %v", m.Volumes())
	}
	if used, _, _ := m.Capacity(ctx); used != 0 {
		t.Fatalf("used after DestroyContainer = %d, want 0", used)
	}

	if err := m.RemoveWorkspace(ctx, "mvm-sess-crash"); err != nil {
		t.Fatal(err)
	}
	if m.HasWorkspace("mvm-sess-crash") {
		t.Fatalf("RemoveWorkspace left the volume behind: %v", m.Volumes())
	}
}

func TestMicrovmRecordsPrepulls(t *testing.T) {
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 2,
		Engine:     NewSimulatedEngine(),
	})
	ctx := context.Background()
	if err := m.Prepull(ctx, "rainier-env:e1-aaa"); err != nil {
		t.Fatal(err)
	}
	if err := m.Prepull(ctx, "rainier-env:e2-bbb"); err != nil {
		t.Fatal(err)
	}
	want := []string{"rainier-env:e1-aaa", "rainier-env:e2-bbb"}
	if got := m.Pulls(); !reflect.DeepEqual(got, want) {
		t.Errorf("Pulls() = %v, want %v (in call order)", got, want)
	}
	if err := m.Prepull(ctx, ""); err == nil {
		t.Error("Prepull with an empty ref = nil, want an error")
	}
}

func TestMicrovmRecordsSnapshotStrips(t *testing.T) {
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 2,
		Engine:     NewSimulatedEngine(),
	})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "mvm-sess-s", Env: map[string]string{"TOKEN": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Snapshot(ctx, h.ID, "rainier-env:e1-aaa", []string{"TOKEN", "RAINIER_SETUP_B64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Snapshot(ctx, h.ID, "", nil); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"TOKEN", "RAINIER_SETUP_B64"}, nil}
	if got := m.Strips(); !reflect.DeepEqual(got, want) {
		t.Errorf("Strips() = %v, want %v (in call order)", got, want)
	}
}
