package runnerd

import (
	"context"
	"testing"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/protocol/runner"
)

type observedResumeDriver struct {
	*driver.Fake
	before func()
}

func (d *observedResumeDriver) Resume(ctx context.Context, id string) (bool, error) {
	d.before()
	return d.Fake.Resume(ctx, id)
}
func TestResumeCommandFencesBootBeforeDriverLaunch(t *testing.T) {
	h := newIdleHarness(t)
	if err := h.rd.Op(context.Background(), h.id, "suspend", false); err != nil {
		t.Fatal(err)
	}
	h.rd.reg.mu.Lock()
	row := h.rd.reg.items[h.id]
	row.placementGen = 1
	row.guestEpoch = 9
	row.guestReconnect = true
	oldBoot := row.boot
	h.rd.reg.mu.Unlock()
	var claimedBoot uint64
	h.rd.drv = &observedResumeDriver{Fake: h.fd, before: func() {
		current, _ := h.rd.reg.snapshot(h.id)
		if current.placementGen != 2 || current.guestEpoch != 0 || current.boot == oldBoot || current.state != "resuming" {
			t.Fatal("new guest can launch before fresh boot authority")
		}
		claimedBoot = current.boot
	}}
	var answer runner.FromRunner
	h.rd.execute(context.Background(), runner.ToRunner{Type: "resume", Session: h.id, PlacementGeneration: 2}, func(m runner.FromRunner) { answer = m }, AgentConfig{}, &agentSessionState{})
	current, _ := h.rd.reg.snapshot(h.id)
	if !answer.OK || current.boot != claimedBoot || current.state != "running" || current.resumePending {
		t.Fatal("resume failed to publish exact claimed boot")
	}
	h.rd.execute(context.Background(), runner.ToRunner{Type: "resume", Session: h.id, PlacementGeneration: 2}, func(m runner.FromRunner) { answer = m }, AgentConfig{}, &agentSessionState{})
	if answer.OK {
		t.Fatal("replayed cold resume was accepted")
	}
}

func TestResumeStatusRequiresExactSettledPlacement(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		pending     bool
		want        string
	}{
		{"running", "running", false, "running"},
		{"cold", "suspended", false, "suspended_cold"},
		{"launching", "resuming", true, "resuming"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newIdleHarness(t)
			h.rd.reg.mu.Lock()
			row := h.rd.reg.items[h.id]
			row.state = tc.state
			row.placementGen = 2
			row.resumePending = tc.pending
			h.rd.reg.mu.Unlock()
			var reply runner.FromRunner
			h.rd.execute(context.Background(), runner.ToRunner{Type: "resume_status", Session: h.id, PlacementGeneration: 2}, func(m runner.FromRunner) { reply = m }, AgentConfig{}, &agentSessionState{})
			if !reply.OK || reply.State != tc.want || reply.PlacementGeneration != 2 {
				t.Fatal("resume status omitted exact pending or settled ownership")
			}
			h.rd.execute(context.Background(), runner.ToRunner{Type: "resume_status", Session: h.id, PlacementGeneration: 1}, func(m runner.FromRunner) { reply = m }, AgentConfig{}, &agentSessionState{})
			if reply.OK {
				t.Fatal("stale status query was accepted")
			}
		})
	}
}

// A refused command (for example, idle stop still running) never reached the
// driver's resume call. Reconciliation must fence that command before retry.
func TestResumeStatusFencesUnreceivedLegacyResume(t *testing.T) {
	h := newIdleHarness(t)
	if err := h.rd.Op(context.Background(), h.id, "suspend", false); err != nil {
		t.Fatal(err)
	}
	h.rd.reg.mu.Lock()
	h.rd.reg.items[h.id].placementGen = 1
	h.rd.reg.mu.Unlock()
	reply := h.rd.resumeStatus(context.Background(), runner.ToRunner{Session: h.id, PlacementGeneration: 2})
	if !reply.OK || reply.State != "suspended_cold" || reply.PlacementGeneration != 2 {
		t.Fatalf("unreceived resume cannot settle: %+v", reply)
	}
	if err := h.rd.opAtPlacement(context.Background(), h.id, "resume", false, 2); err == nil {
		t.Fatal("delayed canceled command restarted the sandbox")
	}
	if err := h.rd.opAtPlacement(context.Background(), h.id, "resume", false, 3); err != nil {
		t.Fatalf("next claim cannot resume: %v", err)
	}
}

type noRestartResumeDriver struct{ *driver.Fake }

func (d noRestartResumeDriver) Resume(context.Context, string) (bool, error) { return false, nil }
func TestColdResumeRequiresActualRestart(t *testing.T) {
	h := newIdleHarness(t)
	if err := h.rd.Op(context.Background(), h.id, "suspend", false); err != nil {
		t.Fatal(err)
	}
	h.rd.reg.mu.Lock()
	h.rd.reg.items[h.id].placementGen = 1
	h.rd.reg.mu.Unlock()
	h.rd.drv = noRestartResumeDriver{h.fd}
	if err := h.rd.opAtPlacement(context.Background(), h.id, "resume", false, 2); err == nil {
		t.Fatal("cold resume reported success without a restarted sandbox")
	}
	row, _ := h.rd.reg.snapshot(h.id)
	if row.resumePending || row.state == "running" {
		t.Fatal("refused cold resume published running or kept in-flight claim")
	}
}
func TestColdResumeCompletionPreservesDeletionOwnership(t *testing.T) {
	h := newIdleHarness(t)
	if err := h.rd.Op(context.Background(), h.id, "suspend", false); err != nil {
		t.Fatal(err)
	}
	h.rd.reg.mu.Lock()
	h.rd.reg.items[h.id].placementGen = 1
	h.rd.reg.mu.Unlock()
	h.rd.drv = &observedResumeDriver{Fake: h.fd, before: func() {
		// Delete sets this before waiting for the driver-owned resume to finish.
		h.rd.reg.setState(h.id, "destroying")
	}}
	_ = h.rd.opAtPlacement(context.Background(), h.id, "resume", false, 2)
	row, _ := h.rd.reg.snapshot(h.id)
	if row.state != "destroying" {
		t.Fatalf("BUG: late resume changed deletion-owned state to %s", row.state)
	}
}
