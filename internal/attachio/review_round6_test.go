package attachio

import (
	"testing"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

func msg(typ, mode string, gen uint64) terminal.ServerMessage {
	return terminal.ServerMessage{Type: typ, Mode: mode, Generation: terminal.GenOf(gen)}
}

// ---------------------------------------------------------------------------
// FIX (1) — NeverClaim is --view; askedView is "asked for view mode", which a
// reconnect also does. The take-control key must survive a reconnect.
// ---------------------------------------------------------------------------

func TestFix1WhoMayClaim(t *testing.T) {
	cases := []struct {
		name  string
		opts  Options
		first terminal.ServerMessage
		want  bool // a Ctrl-\ press sends a claim
	}{
		{"plain attach that came back a viewer (reconnect)",
			Options{Control: true, Mode: terminal.ModeView}, msg(terminal.TypeAttached, terminal.ModeView, 7), true},
		{"--view", Options{Control: true, Mode: terminal.ModeView, NeverClaim: true},
			msg(terminal.TypeAttached, terminal.ModeView, 7), false},
		{"--view after a stale", Options{Control: true, Mode: terminal.ModeView, NeverClaim: true},
			msg(terminal.TypeStale, "", 7), false},
		{"--view after somebody else took control",
			Options{Control: true, Mode: terminal.ModeView, NeverClaim: true},
			msg(terminal.TypeControlChanged, terminal.ModeView, 9), false},
		{"plain attach holding control", Options{Control: true, Mode: terminal.ModeControl},
			msg(terminal.TypeAttached, terminal.ModeControl, 7), false},
		{"plain attach displaced mid-attach", Options{Control: true, Mode: terminal.ModeControl},
			msg(terminal.TypeControlChanged, terminal.ModeView, 9), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			own := newOwnership(tc.opts)
			own.observe(tc.first)
			m, ok := own.claim()
			if ok != tc.want {
				t.Fatalf("after %s/%s/%d: claim sent = %t, want %t",
					tc.first.Type, tc.first.Mode, tc.first.Generation.Value(), ok, tc.want)
			}
			if ok && m.Expected.Value() != tc.first.Generation.Value() {
				t.Fatalf("claim carried expected=%d, want %d", m.Expected.Value(), tc.first.Generation.Value())
			}
			// And the key itself is live either way once a server has answered
			// (a --view attach swallows it rather than forwarding it).
			if !own.keyActive() {
				t.Fatal("the take-control key is not active after an answer")
			}
		})
	}
}

// TestFix1ViewNeverClaimsViaTake pins the other claim route --view has to
// close: --take's single claim. (attachFlags refuses both flags together, so
// this is defence in depth rather than a reachable CLI state.)
func TestFix1TakeUnderNeverClaim(t *testing.T) {
	own := newOwnership(Options{Control: true, Mode: terminal.ModeView, NeverClaim: true, Take: true})
	own.observe(msg(terminal.TypeAttached, terminal.ModeView, 3))
	if own.takeOnce() {
		// takeOnce only reports that --take wants to fire; claim() is what
		// decides whether anything goes on the wire.
		if _, ok := own.claim(); ok {
			t.Fatal("--view + --take sent a claim")
		}
	}
	if _, ok := own.claim(); ok {
		t.Fatal("--view sent a claim")
	}
}

// TestFix1AReconnectedViewerIsNotToldItIsAViewer is the HALF of the
// askedView/neverClaim split that the fix did not carry through: observe's
// notice suppression still reads askedView, which a reconnect sets too, so the
// device that was superseded while it was away is silently demoted.
func TestFix1AReconnectedViewerIsNotToldItIsAViewer(t *testing.T) {
	// What --view gets: silence, correctly. It asked to watch.
	view := newOwnership(Options{Control: true, Mode: terminal.ModeView, NeverClaim: true})
	if n := view.observe(msg(terminal.TypeAttached, terminal.ModeView, 7)); n != "" {
		t.Fatalf("--view was told %q", n)
	}
	// What a plain attach reconnecting after it was superseded gets. It did
	// not ask to watch: reconnectOwnership asked for view mode on its behalf
	// so that a blip does not snatch control back.
	back := newOwnership(Options{Control: true, Mode: terminal.ModeView, Expected: 0})
	notice := back.observe(msg(terminal.TypeAttached, terminal.ModeView, 7))
	if notice == "" {
		t.Errorf("FIX1/gap: a plain attach that reconnected as a viewer is told nothing — "+
			"no %q line — so the user whose control was taken while they were away "+
			"sees no sign of it. observe() suppresses on askedView, which the reconnect sets; "+
			"only claim() was switched to neverClaim.", NoticeViewing)
	}
}

// TestFix3ClientSideFirstWordIsADisplacement is the client half of the
// attachplane probe: when the plane's first word to an attach is a
// control_changed (the moved-but-never-told case), what does the person see?
func TestFix3ClientSideFirstWordIsADisplacement(t *testing.T) {
	t.Run("plain attach", func(t *testing.T) {
		own := newOwnership(Options{Control: true, Mode: terminal.ModeControl})
		first := own.observe(msg(terminal.TypeControlChanged, terminal.ModeView, 2))
		second := own.observe(msg(terminal.TypeAttached, terminal.ModeView, 2))
		t.Logf("plain attach displaced before its opening answer: %q then %q", first, second)
		if first != "" && second != "" && first == second {
			t.Logf("FIX3/client: the same notice %q is printed twice", first)
		}
	})
	t.Run("--view attach", func(t *testing.T) {
		own := newOwnership(Options{Control: true, Mode: terminal.ModeView, NeverClaim: true})
		first := own.observe(msg(terminal.TypeControlChanged, terminal.ModeView, 2))
		second := own.observe(msg(terminal.TypeAttached, terminal.ModeView, 2))
		t.Logf("--view attach displaced before its opening answer: %q then %q", first, second)
		if second == NoticeTaken {
			t.Logf("FIX3/client: a --view attach is told %q — askedView's suppression only "+
				"covers the FIRST answer, and the courtesy notice consumed it", second)
		}
	})
}
