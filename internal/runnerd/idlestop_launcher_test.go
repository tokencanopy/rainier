package runnerd

import (
	"os"
	"regexp"
	"testing"
)

// TestTheLauncherPassesAnIdleStopKnob: --idle-stop is on by default (30m), and
// the hosted launcher is where a fleet's runners are actually started. Without
// a knob there, the only way to change the timeout — or to stand the feature
// down on a fleet whose controld has not been rolled yet, which is the one
// case that REQUIRES standing it down — is to edit the script on the box.
//
// Pinned as text rather than by running the script, because what matters is
// that the flag reaches runnerd with an operator-settable value and a default
// that matches runnerd's own; scripts/fleet-up.sh's own smoke test (in
// internal/driver) stops at the image-build boundary and never gets this far.
func TestTheLauncherPassesAnIdleStopKnob(t *testing.T) {
	b, err := os.ReadFile("../../scripts/fleet-up.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`--idle-stop\s+"\$\{IDLE_STOP:-30m\}"`).Match(b) {
		t.Error(`scripts/fleet-up.sh does not pass --idle-stop "${IDLE_STOP:-30m}" to runnerd: ` +
			"the hosted runner takes the 30m default with no way to change or disable it short of editing the script")
	}
	// And the reason the knob has to exist is written where the operator
	// reaching for it will be.
	if !regexp.MustCompile(`IDLE_STOP=0`).Match(b) {
		t.Error("scripts/fleet-up.sh does not say that IDLE_STOP=0 is what a fleet whose controld has not been rolled must pass")
	}
}

// TestTheReadmeStatesTheDeployOrder: the auto-stop event is additive, so a
// runner rolled ahead of its control plane has it dropped on the unknown-state
// arm and the session row reads `running` over a stopped container — a session
// the user cannot attach to and cannot resume until the runner reconnects.
// Nothing in the runner can detect an old control plane, so the order is the
// mitigation, and an operational mitigation that is not written down is not
// one.
func TestTheReadmeStatesTheDeployOrder(t *testing.T) {
	b, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []*regexp.Regexp{
		regexp.MustCompile(`(?i)roll .*controld.* before the runners`),
		regexp.MustCompile(`--idle-stop 0`),
	} {
		if !want.Match(b) {
			t.Errorf("README.md's runner section does not state %s", want)
		}
	}
}
