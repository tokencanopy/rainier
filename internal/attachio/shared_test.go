package attachio

import (
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// What a person is TOLD under a shared attachment policy. The client sends
// exactly what it sent before — the policy reaches it as a fact off the session
// view, not as a protocol change — so these tests are about copy, which is the
// only thing that differs.

func sharedOpts(others int) Options {
	return Options{Control: true, Mode: terminal.ModeControl, Shared: true, OtherTypers: others}
}

// TestSharedOpeningNoticeIsSilenceOrOneLine is the acceptance rule: the first
// typer is told nothing, and a typer joining others is told once, in one line,
// with the key that gets out.
func TestSharedOpeningNoticeIsSilenceOrOneLine(t *testing.T) {
	for _, tc := range []struct {
		name   string
		others int
		want   string
	}{
		{"the first typer is silent", 0, ""},
		{"one other typer is singular", 1,
			"[1 other terminal attached; everyone may type. Ctrl-] detaches.]"},
		{"several are plural", 3,
			"[3 other terminals attached; everyone may type. Ctrl-] detaches.]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			own := newOwnership(sharedOpts(tc.others))
			got := own.observe(msg(terminal.TypeAttached, terminal.ModeControl, 4))
			if got != tc.want {
				t.Fatalf("notice = %q, want %q", got, tc.want)
			}
			// And none of the exclusive policy's copy appears under it.
			if strings.Contains(got, "control") || strings.Contains(got, "Ctrl-\\") {
				t.Fatalf("notice %q carries take-over copy", got)
			}
		})
	}
}

// TestSharedPolicyNeverPrintsTheTakeOverCopy walks every ownership message a
// shared-policy attach can receive and pins that none of them produces a
// sentence about another device or a key that cannot succeed.
func TestSharedPolicyNeverPrintsTheTakeOverCopy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   Options
		msgs   []terminal.ServerMessage
		want   string // the LAST notice
		silent bool   // no notice at any step
	}{
		{"a viewer is told it may not type, with no key",
			Options{Control: true, Mode: terminal.ModeControl, Shared: true},
			[]terminal.ServerMessage{msg(terminal.TypeAttached, terminal.ModeView, 2)},
			NoticeViewOnly, false},
		{"--view is told nothing at all",
			Options{Control: true, Mode: terminal.ModeView, NeverClaim: true, Shared: true},
			[]terminal.ServerMessage{msg(terminal.TypeAttached, terminal.ModeView, 2)},
			"", true},
		{"its own release says it may no longer type",
			sharedOpts(0),
			[]terminal.ServerMessage{
				msg(terminal.TypeAttached, terminal.ModeControl, 2),
				msg(terminal.TypeControlChanged, terminal.ModeView, 2),
			}, NoticeViewOnly, false},
		{"a revocation is the same sentence",
			sharedOpts(1),
			[]terminal.ServerMessage{
				msg(terminal.TypeAttached, terminal.ModeControl, 2),
				msg(terminal.TypeControlChanged, terminal.ModeView, 3),
			}, NoticeViewOnly, false},
		{"becoming a typer again says so",
			Options{Control: true, Mode: terminal.ModeControl, Shared: true},
			[]terminal.ServerMessage{
				msg(terminal.TypeAttached, terminal.ModeView, 2),
				msg(terminal.TypeControlChanged, terminal.ModeControl, 2),
			}, NoticeHaveControl, false},
		{"a refused claim does not offer to try again",
			Options{Control: true, Mode: terminal.ModeControl, Shared: true},
			[]terminal.ServerMessage{
				msg(terminal.TypeAttached, terminal.ModeView, 2),
				msg(terminal.TypeStale, "", 2),
			}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			own := newOwnership(tc.opts)
			var last string
			for _, m := range tc.msgs {
				got := own.observe(m)
				if tc.silent && got != "" {
					t.Fatalf("notice %q where none was wanted", got)
				}
				for _, forbidden := range []string{NoticeViewing, NoticeTaken, NoticeStale} {
					if got == forbidden {
						t.Fatalf("a shared-policy attach printed the exclusive copy %q", got)
					}
				}
				last = got
			}
			if last != tc.want {
				t.Fatalf("last notice = %q, want %q", last, tc.want)
			}
		})
	}
}

// TestAnExclusivePolicyKeepsTodaysCopy is the other side of the switch: the
// same messages, Shared unset — which is also what an older server that reports
// no policy leaves — and every sentence is the one that shipped.
func TestAnExclusivePolicyKeepsTodaysCopy(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		m    terminal.ServerMessage
		want string
	}{
		{"the first typer is silent", Options{Control: true, Mode: terminal.ModeControl},
			msg(terminal.TypeAttached, terminal.ModeControl, 1), ""},
		{"a viewer is offered the key", Options{Control: true, Mode: terminal.ModeControl},
			msg(terminal.TypeAttached, terminal.ModeView, 1), NoticeViewing},
		{"a refused claim is offered another try", Options{Control: true, Mode: terminal.ModeControl},
			msg(terminal.TypeStale, "", 1), NoticeStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := newOwnership(tc.opts).observe(tc.m); got != tc.want {
				t.Fatalf("notice = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSharedPolicyChangesNothingAboutWhatIsSENT: the policy is copy. A
// shared-policy typer still stamps its generation, still swallows nothing extra,
// and still sends no claim of its own; a shared-policy viewer still sends no
// input. Anything else would be a wire change, which this is not.
func TestSharedPolicyChangesNothingAboutWhatIsSENT(t *testing.T) {
	typer := newOwnership(sharedOpts(2))
	typer.observe(msg(terminal.TypeAttached, terminal.ModeControl, 9))
	if !typer.mayType() {
		t.Fatal("a shared-policy typer was not allowed to type")
	}
	if got := typer.stamp(); got != 9 {
		t.Fatalf("stamp = %d, want the generation it was told, 9", got)
	}
	if !typer.keyActive() {
		t.Fatal("the take-control key stopped being intercepted under a shared policy")
	}
	if _, ok := typer.claim(); ok {
		t.Fatal("a client that may already type sent a claim")
	}

	viewer := newOwnership(Options{Control: true, Mode: terminal.ModeControl, Shared: true})
	viewer.observe(msg(terminal.TypeAttached, terminal.ModeView, 9))
	if viewer.mayType() {
		t.Fatal("a shared-policy viewer was allowed to type")
	}
	// It may still press the key — the plane answers it "you are still a
	// viewer" without touching anything — because taking that away silently
	// would be the one thing a person cannot tell from a broken connection.
	if _, ok := viewer.claim(); !ok {
		t.Fatal("a shared-policy viewer's take-control key sent nothing")
	}
}

// TestTakeIsANoOpForAnAttachThatCanAlreadyType is `--take` under a shared
// policy: accepted, and it does nothing beyond attaching as a typer.
func TestTakeIsANoOpForAnAttachThatCanAlreadyType(t *testing.T) {
	own := newOwnership(Options{Control: true, Mode: terminal.ModeControl, Shared: true, Take: true})
	own.observe(msg(terminal.TypeAttached, terminal.ModeControl, 3))
	if own.takeOnce() {
		t.Fatal("--take spent its claim on an attach that could already type")
	}
}
