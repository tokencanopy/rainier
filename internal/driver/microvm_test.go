// internal/driver/microvm_test.go
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestMicrovmSatisfiesContract(t *testing.T) {
	RunContract(t, func(t *testing.T) (Driver, func()) {
		tempDir, err := os.MkdirTemp("", "rainier-mvm-contract-*")
		if err != nil {
			t.Fatal(err)
		}
		d := NewMicrovm(MicrovmOpts{
			TotalSlots: 4,
			StateDir:   tempDir,
			Engine:     NewSimulatedEngine(),
		})
		cleanup := func() {
			_ = os.RemoveAll(tempDir)
		}
		return d, cleanup
	})
}

func TestMicrovmRestartRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-mvm-recover-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sim := NewSimulatedEngine()
	m1 := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		StateDir:   tempDir,
		Engine:     sim,
	})
	ctx := context.Background()

	h1, err := m1.Create(ctx, Spec{SessionID: "sess-recover-1"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := m1.Create(ctx, Spec{SessionID: "sess-recover-2"})
	if err != nil {
		t.Fatal(err)
	}

	// Cold park the second session
	if err := m1.Suspend(ctx, h2.ID, false); err != nil {
		t.Fatal(err)
	}

	// Simulate runner restart: create a new driver instance pointing to the same StateDir
	m2 := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		StateDir:   tempDir,
		Engine:     sim,
	})

	// List should discover both sessions
	listed, err := m2.List(ctx)
	if err != nil {
		t.Fatalf("List on recovered driver: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("recovered %d sessions, want 2", len(listed))
	}

	foundMap := make(map[string]State)
	for _, l := range listed {
		foundMap[l.SessionID] = l.Handle.State
	}

	if st := foundMap["sess-recover-1"]; st != StateRunning {
		t.Errorf("recovered sess-recover-1 state = %s, want running", st)
	}
	if st := foundMap["sess-recover-2"]; st != StateSuspended {
		t.Errorf("recovered sess-recover-2 state = %s, want suspended", st)
	}

	// Inspecting the handles on the new driver succeeds
	if g1, err := m2.Inspect(ctx, h1.ID); err != nil || g1.State != StateRunning {
		t.Errorf("m2.Inspect(h1) = %+v, %v; want running", g1, err)
	}
	if g2, err := m2.Inspect(ctx, h2.ID); err != nil || g2.State != StateSuspended {
		t.Errorf("m2.Inspect(h2) = %+v, %v; want suspended", g2, err)
	}
}

func TestMicrovmWorkspaceFilesSurviveColdPark(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-mvm-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		StateDir:   tempDir,
		Engine:     NewSimulatedEngine(),
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{
		SessionID: "sess-persist",
	})
	if err != nil {
		t.Fatal(err)
	}

	wsDir := m.workspaceDir("sess-persist")
	if wsDir == "" {
		t.Fatal("empty workspace directory")
	}

	// Write real files into the persistent workspace
	testFile := filepath.Join(wsDir, "work.txt")
	testData := []byte("uncommitted agent work that must survive cold park")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// Cold park stops the VM
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateSuspended {
		t.Fatalf("state after cold suspend = %s, want suspended", g.State)
	}

	// File still exists while parked
	if data, err := os.ReadFile(testFile); err != nil || string(data) != string(testData) {
		t.Fatalf("file corrupted or missing during cold park: %v", err)
	}

	// Resume restarts the microVM
	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatalf("resume after cold park: %v", err)
	}
	if !restarted {
		t.Error("cold resume did not report restarted = true")
	}
	if g, _ := m.Inspect(ctx, h.ID); g.State != StateRunning {
		t.Fatalf("state after resume = %s, want running", g.State)
	}

	// File survived and is intact
	if data, err := os.ReadFile(testFile); err != nil || string(data) != string(testData) {
		t.Fatalf("file missing or changed after resume: %v", err)
	}

	// Destroy removes the workspace directory from disk
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := os.Stat(wsDir); !os.IsNotExist(err) {
		t.Errorf("workspace directory %s still exists after Destroy", wsDir)
	}
}

func TestMicrovmGuestEnvTranslation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-mvm-env-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sim := NewSimulatedEngine()
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		StateDir:   tempDir,
		Engine:     sim,
	})
	ctx := context.Background()

	spec := Spec{
		SessionID:       "sess-trans",
		DialURL:         "ws://172.18.0.1:8080",
		ProxyURL:        "http://172.18.0.1:3128",
		Setup:           "npm install -g pnpm",
		SetupTimeoutSec: 600,
		Repos: []RepoSpec{
			{Owner: "tokencanopy", Name: "rainier", BaseBranch: "main", SessionBranch: "rainier/feat", Dir: "rainier"},
		},
		Init:           "pnpm test",
		InitTimeoutSec: 180,
		GitAuthorName:  "Test Author",
		GitAuthorEmail: "author@example.invalid",
		Env:            map[string]string{"APP_ENV": "production"},
	}

	h, err := m.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	cfg, ok := sim.configs[h.ID]
	if !ok {
		t.Fatal("simulated engine did not capture VMMConfig")
	}

	// Verify required guest env keys were populated
	checks := map[string]string{
		"RAINIER_DIAL":             "ws://172.18.0.1:8080",
		"RAINIER_SESSION":          "sess-trans",
		"RAINIER_GIT_AUTHOR_NAME":  "Test Author",
		"RAINIER_GIT_AUTHOR_EMAIL": "author@example.invalid",
		"APP_ENV":                  "production",
		"RAINIER_SETUP_TIMEOUT":    "600",
		"RAINIER_INIT_TIMEOUT":     "180",
	}

	for k, want := range checks {
		if got := cfg.Env[k]; got != want {
			t.Errorf("cfg.Env[%q] = %q, want %q", k, got, want)
		}
	}

	if cfg.Env["HTTP_PROXY"] == "" || cfg.Env["NO_PROXY"] == "" {
		t.Errorf("proxy variables were not injected into guest env: %+v", cfg.Env)
	}
	if cfg.Env["RAINIER_SETUP_B64"] == "" {
		t.Errorf("RAINIER_SETUP_B64 was not injected")
	}
	if cfg.Env["RAINIER_REPOS_B64"] == "" {
		t.Errorf("RAINIER_REPOS_B64 was not injected")
	}
	if cfg.Env["RAINIER_INIT_B64"] == "" {
		t.Errorf("RAINIER_INIT_B64 was not injected")
	}
}

func TestMicrovmDestroyContainerEngineFailure(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-mvm-stoperr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sim := NewSimulatedEngine()
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		StateDir:   tempDir,
		Engine:     sim,
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-stopfail"})
	if err != nil {
		t.Fatal(err)
	}

	// Configure engine to fail on Stop
	wantErr := errors.New("hypervisor process hung")
	sim.failOnStop[h.ID] = wantErr

	err = m.DestroyContainer(ctx, h.ID)
	if err == nil {
		t.Fatal("DestroyContainer should have failed when engine.Stop fails")
	}

	// Instance must be preserved so capacity accounting is not corrupted
	if g, _ := m.Inspect(ctx, h.ID); g.State == StateGone {
		t.Error("DestroyContainer prematurely deleted instance when engine.Stop failed")
	}
	if used, _, _ := m.Capacity(ctx); used != 1 {
		t.Errorf("capacity after failed DestroyContainer = %d, want 1", used)
	}

	// Clear failure and destroy cleanly
	delete(sim.failOnStop, h.ID)
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("clean destroy: %v", err)
	}
}

func TestMicrovmFirecrackerFailsClosed(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-fc-fail-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	fc := NewFirecrackerEngine("/nonexistent/bin/firecracker", tempDir)
	err = fc.Launch(context.Background(), VMMConfig{ID: "mvm-fail"})
	if err == nil {
		t.Fatal("FirecrackerEngine.Launch must fail closed when binary is missing")
	}
}

func TestMicrovmFirecrackerClientConfiguration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-fc-mock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sockPath := filepath.Join(tempDir, "mock.sock")
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var mu sync.Mutex
	calls := make(map[string]bool)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls[r.Method+" "+r.URL.Path] = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	go server.Serve(listener)
	defer server.Close()

	fc := newFirecrackerClient(sockPath)
	ctx := context.Background()

	// Exercise each API endpoint
	if err := fc.putJSON(ctx, "/machine-config", map[string]any{"vcpu_count": 2}); err != nil {
		t.Fatal(err)
	}
	if err := fc.putJSON(ctx, "/boot-source", map[string]any{"kernel_image_path": "/vmlinux"}); err != nil {
		t.Fatal(err)
	}
	if err := fc.putJSON(ctx, "/drives/rootfs", map[string]any{"path_on_host": "/rootfs"}); err != nil {
		t.Fatal(err)
	}
	if err := fc.putJSON(ctx, "/actions", map[string]any{"action_type": "InstanceStart"}); err != nil {
		t.Fatal(err)
	}
	if err := fc.patchJSON(ctx, "/vm", map[string]any{"state": "Paused"}); err != nil {
		t.Fatal(err)
	}
	if err := fc.patchJSON(ctx, "/vm", map[string]any{"state": "Resumed"}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	expectedCalls := []string{
		"PUT /machine-config",
		"PUT /boot-source",
		"PUT /drives/rootfs",
		"PUT /actions",
		"PATCH /vm",
	}
	for _, call := range expectedCalls {
		if !calls[call] {
			t.Errorf("Firecracker API client missed call %s", call)
		}
	}
}

func TestMicrovmStateReconciliation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-mvm-reconcile-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sim := NewSimulatedEngine()
	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 4,
		StateDir:   tempDir,
		Engine:     sim,
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-reconcile"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Destroy(ctx, h.ID)

	// Simulate unexpected hypervisor process crash
	sim.mu.Lock()
	sim.states[h.ID] = VMMStateStopped
	sim.mu.Unlock()

	// Inspect should reconcile with the hypervisor and reflect that it is no longer running
	h2, err := m.Inspect(ctx, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if h2.State == StateRunning {
		t.Errorf("Inspect failed to reconcile stopped engine state: got %s, want suspended", h2.State)
	}

	// List should also reflect the reconciled state
	listed, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Handle.State == StateRunning {
		t.Errorf("List failed to reflect stopped engine state: %+v", listed)
	}
}

func TestMicrovmRecordsPrepulls(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rainier-mvm-pulls-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 2,
		StateDir:   tempDir,
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
	tempDir, err := os.MkdirTemp("", "rainier-mvm-strips-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m := NewMicrovm(MicrovmOpts{
		TotalSlots: 2,
		StateDir:   tempDir,
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

var _ = io.Discard
var _ = json.Marshal
var _ = time.Second
