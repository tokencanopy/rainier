package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/attachio"
	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// What `rainier` says under each attachment policy. The policy reaches this CLI
// as one additive object on the session view — the terminal protocol carries
// none — so these tests drive it from that object.

// TestInfoInputRowUnderBothPolicies pins the row `info` prints about who may
// type. Under a shared policy there is no controller to name, so the row is the
// rule and the count; under an exclusive one, and against a server that reports
// no policy at all, it is today's row word for word.
func TestInfoInputRowUnderBothPolicies(t *testing.T) {
	cfg := cli.Config{
		Current: "ctx",
		Contexts: map[string]cli.Context{"ctx": {
			Server:                "https://rainier.example.invalid",
			Token:                 "tok_example",
			LastControlSession:    "sess_example",
			LastControlGeneration: "5",
		}},
	}
	held := controllerView{Generation: "5", Held: true}

	for _, tc := range []struct {
		name  string
		s     session
		label string
		value string
	}{
		{"shared, alone", session{ID: "sess_example", Controller: held,
			Input: inputView{Policy: "shared", Attached: 1}}, "Input", "shared (1 attached)"},
		{"shared, several", session{ID: "sess_example", Controller: held,
			Input: inputView{Policy: "shared", Attached: 3}}, "Input", "shared (3 attached)"},
		{"shared, nobody attached", session{ID: "sess_example",
			Input: inputView{Policy: "shared"}}, "Input", "shared (0 attached)"},
		{"exclusive, this device", session{ID: "sess_example", Controller: held,
			Input: inputView{Policy: "exclusive", Attached: 1}}, "Controller", "this device"},
		{"exclusive, nobody", session{ID: "sess_example",
			Input: inputView{Policy: "exclusive"}}, "Controller", "none"},
		{"an older server reporting no policy", session{ID: "sess_example", Controller: held},
			"Controller", "this device"},
		{"a policy word this build does not know", session{ID: "sess_example", Controller: held,
			Input: inputView{Policy: "collaborative"}}, "Controller", "this device"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			label, value := inputRow(cfg, tc.s)
			if label != tc.label || value != tc.value {
				t.Fatalf("inputRow = %q: %q, want %q: %q", label, value, tc.label, tc.value)
			}
		})
	}
}

// TestInfoPrintsTheInputRowAligned keeps the row in the output `info` actually
// produces, in both shapes, and keeps the column it lives in.
func TestInfoPrintsTheInputRowAligned(t *testing.T) {
	var shared bytes.Buffer
	printInfo(&shared, cli.Config{}, session{
		ID: "sess_example", Name: "box1", State: "running",
		Input: inputView{Policy: "shared", Attached: 2},
	})
	if !strings.Contains(shared.String(), "Input:        shared (2 attached)") {
		t.Fatalf("info printed:\n%s", shared.String())
	}
	if strings.Contains(shared.String(), "Controller:") {
		t.Fatalf("a shared-policy session named a controller:\n%s", shared.String())
	}

	var exclusive bytes.Buffer
	printInfo(&exclusive, cli.Config{}, session{
		ID: "sess_example", Name: "box1", State: "running",
		Controller: controllerView{Generation: "3", Held: true},
		Input:      inputView{Policy: "exclusive", Attached: 1},
	})
	if !strings.Contains(exclusive.String(), "Controller:   another device") {
		t.Fatalf("info printed:\n%s", exclusive.String())
	}
}

// TestWithServerPolicyFoldsTheViewIntoTheAttach: what a person is told is
// decided from the view the CLI already read, and nothing else about the attach
// changes — an attach against an exclusive or an older server asks for exactly
// what it asked for before.
func TestWithServerPolicyFoldsTheViewIntoTheAttach(t *testing.T) {
	base := defaultOwnership()
	for _, tc := range []struct {
		name string
		in   inputView
		want attachio.Options
	}{
		{"shared, with peers", inputView{Policy: "shared", Attached: 2},
			attachio.Options{Control: true, Mode: terminal.ModeControl, Shared: true, OtherTypers: 2}},
		{"shared, alone", inputView{Policy: "shared"},
			attachio.Options{Control: true, Mode: terminal.ModeControl, Shared: true}},
		{"exclusive", inputView{Policy: "exclusive", Attached: 1},
			attachio.Options{Control: true, Mode: terminal.ModeControl}},
		{"an older server", inputView{}, attachio.Options{Control: true, Mode: terminal.ModeControl}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withServerPolicy(base, session{Input: tc.in}); got != tc.want {
				t.Fatalf("withServerPolicy = %+v, want %+v", got, tc.want)
			}
		})
	}
	// --view and --take are the user's instructions and survive either policy.
	view := attachio.Options{Control: true, Mode: terminal.ModeView, NeverClaim: true}
	got := withServerPolicy(view, session{Input: inputView{Policy: "shared", Attached: 4}})
	if got.Mode != terminal.ModeView || !got.NeverClaim || !got.Shared || got.OtherTypers != 4 {
		t.Fatalf("--view under a shared policy asks for %+v", got)
	}
	take := attachio.Options{Control: true, Mode: terminal.ModeControl, Take: true}
	if got := withServerPolicy(take, session{Input: inputView{Policy: "shared"}}); !got.Take {
		t.Fatalf("--take was dropped under a shared policy: %+v", got)
	}
}

// TestTheOpeningCountIsNotReassertedOnAReconnect: the count is the opening
// attach's, and only the opening attach's. Each reconnect builds a fresh
// ownership, so a count carried forward would re-announce "N other terminals
// attached" hours later, from a number read before the first attach, about
// devices that may all have gone — a false statement about other people's
// terminals rather than merely an imprecise one.
func TestTheOpeningCountIsNotReassertedOnAReconnect(t *testing.T) {
	opening := withServerPolicy(defaultOwnership(), session{
		Input: inputView{Policy: "shared", Attached: 3}})
	if opening.OtherTypers != 3 {
		t.Fatalf("the opening attach asks for %+v", opening)
	}
	next := reconnectOwnership(opening, attachio.Outcome{
		Mode: terminal.ModeControl, Generation: 7})
	if next.OtherTypers != 0 {
		t.Fatalf("a reconnect carried a count of %d forward", next.OtherTypers)
	}
	// The policy itself is kept: the copy a reconnect prints, if it prints any,
	// must still be the right policy's.
	if !next.Shared {
		t.Fatal("a reconnect forgot the server's policy")
	}
	if got := attachio.SharedNotice(next.OtherTypers); got != "" {
		t.Fatalf("a reconnect would print %q", got)
	}
}

// TestAnAutoAttachLearnsThePolicyFromTheCreate: `new` and `agent login` attach
// to a session they just created, so the policy they print copy under comes off
// the create's own response rather than defaulting to exclusive.
func TestAnAutoAttachLearnsThePolicyFromTheCreate(t *testing.T) {
	created := session{ID: "sess_example", Input: inputView{Policy: "shared"}}
	own := withServerPolicy(defaultOwnership(), created)
	if !own.Shared || own.OtherTypers != 0 {
		t.Fatalf("an auto-attach asks for %+v, want shared with no peers", own)
	}
	// And the copy that follows from it: nothing on the way in, and no offer of
	// a key that cannot succeed if this attach is later revoked.
	if got := attachio.SharedNotice(own.OtherTypers); got != "" {
		t.Fatalf("a freshly created session's attach would print %q", got)
	}
}

// TestPrepareAttachReturnsTheViewItRead: the policy and the count reach the
// attach without a second round trip, which is the whole reason prepareAttach
// hands its row back.
func TestPrepareAttachReturnsTheViewItRead(t *testing.T) {
	var gets int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gets++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"session": map[string]any{
			"id": "sess_attach", "state": "running",
			"input": map[string]any{"policy": "shared", "attached": 2},
		}})
	}))
	defer ts.Close()

	row, err := prepareAttach(&cli.Client{Base: ts.URL}, "sess_attach", false)
	if err != nil {
		t.Fatalf("prepareAttach: %v", err)
	}
	if !row.Input.sharedInput() || row.Input.Attached != 2 {
		t.Fatalf("prepareAttach returned %+v", row.Input)
	}
	if gets != 1 {
		t.Fatalf("the session was fetched %d times, want once", gets)
	}
}

// TestAttachHelpDescribesBothPolicies: an installed CLI passes --take, so the
// flag stays and its help says what it is worth — and the help cannot keep
// stating "one device at a time may type" as the rule, because that is now one
// of two rules a server may be running.
func TestAttachHelpDescribesBothPolicies(t *testing.T) {
	out, err := captureStdout(t, func() error {
		if !printCommandHelp("attach") {
			t.Fatal("no help for attach")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"every attached terminal may type",
		"no effect\n            where every attached terminal may already type",
		"most recent resize from any terminal that may type",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("attach help does not mention %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "One device at a time may type; everyone else watches.") {
		t.Fatalf("attach help still states exclusivity as the rule:\n%s", out)
	}
}
