package driver

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type coldIdentityEngine struct {
	*SimulatedEngine
	identity               guestHostIdentity
	failIdentity, failStop bool
}

func (e *coldIdentityEngine) guestIdentity(VMMConfig, int) (guestHostIdentity, error) {
	if e.failIdentity {
		return guestHostIdentity{}, errors.New("synthetic identity refusal")
	}
	return e.identity, nil
}
func (e *coldIdentityEngine) Stop(ctx context.Context, id string) error {
	if e.failStop {
		return errors.New("synthetic stop refusal")
	}
	return e.SimulatedEngine.Stop(ctx, id)
}
func TestColdResumeRefreshesVerifiedHostIdentity(t *testing.T) {
	for _, outcome := range []string{"verified", "identity_refused", "stop_refused"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			dir := shortTempDir(t)
			engine := &coldIdentityEngine{SimulatedEngine: NewSimulatedEngineWithDir(dir), identity: guestHostIdentity{BootID: "boot_test", StartTime: 1, NamespaceDevice: 1, NamespaceInode: 1}}
			m, _ := testMicrovm(t, MicrovmOpts{StateDir: dir, Engine: engine, GuestReconnect: true})
			m.SetHost(&stubMicrovmHost{})
			h, err := m.Create(ctx, Spec{SessionID: "session_test", GuestReconnect: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Suspend(ctx, h.ID, false); err != nil {
				t.Fatal(err)
			}
			engine.identity.StartTime = 2
			engine.identity.NamespaceInode = 2
			engine.failIdentity = outcome != "verified"
			engine.failStop = outcome == "stop_refused"
			_, err = m.Resume(ctx, h.ID)
			if outcome == "verified" {
				if err != nil {
					t.Fatal(err)
				}
				if m.instances[h.ID].Identity != engine.identity {
					t.Fatal("cold boot retained old process identity")
				}
			} else {
				if err == nil {
					t.Fatal("unverified cold boot accepted")
				}
				if outcome == "stop_refused" {
					if m.instances[h.ID].slot == nil || m.instances[h.ID].Identity != (guestHostIdentity{}) {
						t.Fatal("possibly live VM lost resource ownership or kept trusted identity")
					}
				} else if state, _ := engine.State(ctx, h.ID); state == VMMStateRunning {
					t.Fatal("unverified VM was not stopped")
				}
			}
			engine.failStop = false
			_ = m.Destroy(ctx, h.ID)
		})
	}
}

func TestResumeStatusDurablyFencesDelayedLaunch(t *testing.T) {
	ctx := context.Background()
	m, engine := testMicrovm(t, MicrovmOpts{})
	m.SetHost(&stubMicrovmHost{})
	h, err := m.Create(ctx, Spec{SessionID: "session_test", PlacementGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	state, generation, err := m.ResumeStatus(ctx, h.ID, 2)
	if err != nil || state != "suspended_cold" || generation != 2 {
		t.Fatal("unreceived resume was not durably fenced")
	}
	if _, err = m.ResumePlacement(ctx, h.ID, 2); err == nil {
		t.Fatal("delayed canceled claim launched a VM")
	}
	stopTestMicrovmRunner(t, m)
	recovered, _ := testMicrovm(t, MicrovmOpts{StateDir: m.opts.StateDir, BaseRootfs: m.opts.BaseRootfs, Engine: engine, Net: m.opts.Net, Format: m.opts.Format})
	listed, err := recovered.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].PlacementGeneration != 2 {
		t.Fatal("resume fence did not survive runner restart")
	}
	if _, err = recovered.ResumePlacement(ctx, h.ID, 2); err == nil {
		t.Fatal("restart allowed canceled claim to launch")
	}
	_ = recovered.Destroy(ctx, h.ID)
}

func TestColdResumeDeletionWaitsForLaunchOwnership(t *testing.T) {
	engine := &parkedLaunchEngine{SimulatedEngine: NewSimulatedEngine(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	m, _ := testMicrovm(t, MicrovmOpts{Engine: engine})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()
	h, err := m.Create(ctx, Spec{SessionID: "session_test", PlacementGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatal(err)
	}
	engine.park()
	done := make(chan error, 1)
	go func() { _, err := m.ResumePlacement(ctx, h.ID, 2); done <- err }()
	select {
	case <-engine.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("launch not reached")
	}
	// Deletion cannot declare the old stopped VM gone while a new launch is
	// still using its disks and allocated namespace.
	bounded, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = m.Destroy(bounded, h.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("destroy during launch = %v, want bounded wait", err)
	}
	workspace, _ := m.workspaceDiskPath("session_test")
	if _, err := os.Stat(workspace); err != nil {
		t.Error("workspace removed during launch")
	}
	close(engine.release)
	if err := <-done; err != nil {
		t.Errorf("launch failed: %v", err)
	}
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if state, _ := engine.State(ctx, h.ID); state == VMMStateRunning {
		t.Fatal("VM survived deletion")
	}
}
