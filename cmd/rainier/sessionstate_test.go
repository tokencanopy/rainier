package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/cli"
)

// A session has three facts and they move independently. These cases are the
// combinations that used to collapse into one word — and the ones that used
// to produce the wrong word (docs/cli-v0-contract.md §3.1).
func TestThreeDimensions(t *testing.T) {
	cases := []struct {
		name       string
		in         session
		lifecycle  string
		process    string
		connection string
	}{
		{
			"queued", session{State: "queued"},
			lifecycleStarting, processNone, connectionUnavailable,
		},
		{
			"creating", session{State: "creating", Reachable: true},
			lifecycleStarting, processNone, connectionAvailable,
		},
		{
			"running with a live child", session{State: "running", Reachable: true},
			lifecycleRunning, processRunning, connectionAvailable,
		},
		{
			// The regression this model exists for. The sandbox is up, the
			// agent inside it finished. Two facts, and the session is NOT
			// over: it holds its filesystem and it is still attachable.
			"running with a child that exited cleanly",
			session{State: "running", Reachable: true, ChildExitCode: intPtr(0)},
			lifecycleRunning, processExited, connectionAvailable,
		},
		{
			"running with a child that was killed",
			session{State: "running", Reachable: true, ChildExitCode: intPtr(137)},
			lifecycleRunning, processExited, connectionAvailable,
		},
		{
			// -1 is the code that used to render as "running (exited -1)".
			"running with a child that has no exit status",
			session{State: "running", Reachable: true, ChildExitCode: intPtr(-1)},
			lifecycleRunning, processExited, connectionAvailable,
		},
		{
			// A runner that dropped its link. The session is still Running:
			// the entitlement and the work are intact, and the connection is
			// the thing that is missing.
			"running but not reachable",
			session{State: "running"},
			lifecycleRunning, processRunning, connectionUnavailable,
		},
		{
			"suspended warm", session{State: "suspended_warm"},
			lifecycleStopped, "paused", connectionUnavailable,
		},
		{
			"suspended cold", session{State: "suspended_cold"},
			lifecycleStopped, processNone, connectionUnavailable,
		},
		{
			"suspended with a child that exited",
			session{State: "suspended_cold", ChildExitCode: intPtr(0)},
			lifecycleStopped, processExited, connectionUnavailable,
		},
		{
			"failed", session{State: "failed", Reachable: true},
			lifecycleFailed, processNone, connectionAvailable,
		},
		{
			"dead", session{State: "dead"},
			lifecycleFailed, processNone, connectionUnavailable,
		},
		{
			// Terminal records get their accurate word, not a euphemism: a
			// session somebody cancelled and one somebody deleted are
			// different events with different causes.
			"canceled", session{State: "canceled"},
			lifecycleCanceled, processNone, connectionUnavailable,
		},
		{
			"destroyed", session{State: "destroyed"},
			lifecycleDeleted, processNone, connectionUnavailable,
		},
		{
			// A state this build has never heard of stays unknown. It must
			// NOT be translated into another dimension's vocabulary — an
			// unrecognized lifecycle says nothing about the connection, and
			// this session is reachable.
			"an unknown future state",
			session{State: "hibernating", Reachable: true},
			lifecycleUnknown, processNone, connectionAvailable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lifecycleOf(tc.in); got != tc.lifecycle {
				t.Errorf("lifecycle = %q, want %q", got, tc.lifecycle)
			}
			if got := processOf(tc.in); got != tc.process {
				t.Errorf("process = %q, want %q", got, tc.process)
			}
			if got := connectionOf(tc.in); got != tc.connection {
				t.Errorf("connection = %q, want %q", got, tc.connection)
			}
		})
	}
}

// An unknown state carries the server's own word through, so somebody looking
// at a state this build does not know can quote it.
func TestUnknownLifecycleShowsTheServersWord(t *testing.T) {
	got := displayLifecycle(session{State: "hibernating"})
	if !strings.Contains(got, "Unknown") || !strings.Contains(got, "hibernating") {
		t.Fatalf("displayLifecycle = %q, want it to say unknown and name the state", got)
	}
	// And it is sanitized like every other server string.
	if got := displayLifecycle(session{State: "hib\x1b[2Jernating"}); strings.ContainsRune(got, 0x1b) {
		t.Errorf("an unknown state let a terminal escape through: %q", got)
	}
}

// Action eligibility comes from the API's own transition rules, never from
// the display word. These expectations were read off controlapp:
// SuspendSession accepts StateRunning only; ResumeSession accepts the two
// suspended states only; AttachmentService.attachable accepts running, and
// failed only while its runner is connected; DeleteSession refuses creating
// and is a no-op on an already-destroyed row.
func TestActionEligibilityFollowsRawStates(t *testing.T) {
	cases := []struct {
		name                   string
		in                     session
		attach, stop, deletion string
	}{
		{"running", session{State: "running", Reachable: true}, eligibleYes, eligibleYes, eligibleYes},
		{
			// The correction: a running session whose child exited is still a
			// running session to the server, so both actions remain open.
			"running with a child that exited",
			session{State: "running", Reachable: true, ChildExitCode: intPtr(0)},
			eligibleYes, eligibleYes, eligibleYes,
		},
		{
			// Stop is NOT claimed for a starting session. queued and creating
			// share the label Starting, and SuspendSession refuses both.
			"queued", session{State: "queued"}, eligibleYes, eligibleNo, eligibleYes,
		},
		{
			// creating additionally cannot be deleted: DeleteSession answers
			// it with a conflict because a dispatch may be in flight.
			"creating", session{State: "creating"}, eligibleYes, eligibleNo, eligibleNo,
		},
		{"suspended warm", session{State: "suspended_warm"}, eligibleYes, eligibleNo, eligibleYes},
		{"suspended cold", session{State: "suspended_cold"}, eligibleYes, eligibleNo, eligibleYes},
		{
			// A failed session keeps a diagnostic terminal only while its
			// runner is connected. This is the one action whose answer
			// legitimately depends on the connection fact, because the
			// endpoint itself says so.
			"failed and reachable", session{State: "failed", Reachable: true},
			eligibleYes, eligibleNo, eligibleYes,
		},
		{"failed and unreachable", session{State: "failed"}, eligibleNo, eligibleNo, eligibleYes},
		{"dead", session{State: "dead"}, eligibleNo, eligibleNo, eligibleYes},
		{"canceled", session{State: "canceled"}, eligibleNo, eligibleNo, eligibleYes},
		{"destroyed", session{State: "destroyed"}, eligibleNo, eligibleNo, eligibleNo},
		{
			// Conservative means making no claim, not inventing one.
			"an unknown future state", session{State: "hibernating", Reachable: true},
			eligibleUnknown, eligibleUnknown, eligibleUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canAttach(tc.in); got != tc.attach {
				t.Errorf("canAttach = %q, want %q", got, tc.attach)
			}
			if got := canStop(tc.in); got != tc.stop {
				t.Errorf("canStop = %q, want %q", got, tc.stop)
			}
			if got := canDelete(tc.in); got != tc.deletion {
				t.Errorf("canDelete = %q, want %q", got, tc.deletion)
			}
		})
	}
}

// The human table shows three dimensions in three columns. The specific thing
// it must never do again is fold the exit code into the state cell.
func TestListColumnsSeparateTheThreeDimensions(t *testing.T) {
	rows := []session{
		{ID: "sess_a", Name: "live", State: "running", Reachable: true, CreatedAt: "2026-09-08T00:00:00Z"},
		{ID: "sess_b", Name: "done", State: "running", Reachable: true, ChildExitCode: intPtr(0), CreatedAt: "2026-09-08T00:00:00Z"},
		{ID: "sess_c", Name: "gone", State: "running", CreatedAt: "2026-09-08T00:00:00Z"},
	}
	var out bytes.Buffer
	printSessions(&out, cli.Config{}, rows, false)

	header := strings.Fields(strings.SplitN(out.String(), "\n", 2)[0])
	want := []string{"NAME", "STATE", "PROCESS", "CONNECTION", "AGE"}
	if len(header) != len(want) {
		t.Fatalf("header = %v, want %v", header, want)
	}
	for i := range want {
		if header[i] != want[i] {
			t.Fatalf("header = %v, want %v", header, want)
		}
	}
	if strings.Contains(out.String(), "exited -1") || strings.Contains(out.String(), "running (exited") {
		t.Errorf("the exit code is back inside the state cell:\n%s", out.String())
	}
	for _, want := range []string{"Running", "Exited (0)", "Available", "Unavailable"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("table is missing %q:\n%s", want, out.String())
		}
	}
	// All three sessions are Running: the process and the connection differ,
	// and neither may change the lifecycle column.
	if n := strings.Count(out.String(), "Running"); n < 3 {
		t.Errorf("a process or connection difference changed the state column:\n%s", out.String())
	}
}

// info prints the three dimensions on labelled lines and the server's own
// state beside them.
func TestInfoSeparatesTheThreeDimensions(t *testing.T) {
	var out bytes.Buffer
	printInfo(&out, cli.Config{}, session{
		ID: "sess_a", Name: "box", State: "running", Reachable: false,
		ChildExitCode: intPtr(3), CreatedAt: "2026-09-08T00:00:00Z",
	})
	for _, want := range []string{
		"Session:      Running",
		"Process:      Exited (3)",
		"Connection:   Unavailable",
		"API state:    running",
		"Attach:       yes",
		"Stop:         yes",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("info is missing %q:\n%s", want, out.String())
		}
	}
}

// A refusal names the API rule behind it, so a person can tell "not yet" from
// "never".
func TestInfoExplainsRefusals(t *testing.T) {
	var out bytes.Buffer
	printInfo(&out, cli.Config{}, session{ID: "sess_a", Name: "box", State: "queued"})
	if !strings.Contains(out.String(), "Stop:         no (only a running session can be stopped)") {
		t.Errorf("info does not explain why a starting session cannot be stopped:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Attach:       yes (waits for it to start)") {
		t.Errorf("info does not explain the attach wait:\n%s", out.String())
	}
}

// --json keeps the canonical API facts verbatim. `state` is the server's own
// string and is never replaced by a derived word; reachable and
// child_exit_code are the boolean and the nullable integer the API sent.
func TestSessionJSONKeepsCanonicalFacts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		in       session
		wantExit any
	}{
		{"a child that exited", session{
			ID: "sess_a", Name: "box", State: "running", Reachable: false, ChildExitCode: intPtr(0),
		}, float64(0)},
		{"no child yet", session{ID: "sess_b", Name: "new", State: "queued"}, nil},
		{"an unknown state", session{ID: "sess_c", Name: "odd", State: "hibernating", Reachable: true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeSessionJSON(&out, cli.Config{}, tc.in); err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
				t.Fatalf("not JSON: %v\n%s", err, out.String())
			}
			// The raw state, verbatim. Not "running" for a queued session and
			// not "unknown" for a state this build does not know.
			if doc["state"] != tc.in.State {
				t.Errorf("state = %v, want the raw API state %q", doc["state"], tc.in.State)
			}
			if doc["reachable"] != tc.in.Reachable {
				t.Errorf("reachable = %v, want %v", doc["reachable"], tc.in.Reachable)
			}
			exit, present := doc["child_exit_code"]
			if !present {
				t.Errorf("child_exit_code key is missing; it must be present and null when absent")
			}
			if exit != tc.wantExit {
				t.Errorf("child_exit_code = %#v, want %#v", exit, tc.wantExit)
			}
			// The derived fields are additive and separately named.
			for _, key := range []string{"lifecycle", "process", "connection", "actions"} {
				if _, ok := doc[key]; !ok {
					t.Errorf("derived key %q is missing:\n%s", key, out.String())
				}
			}
			if doc["lifecycle"] == doc["state"] && tc.in.State != "running" {
				t.Errorf("lifecycle and state collapsed together: %v", doc["lifecycle"])
			}
			actions, _ := doc["actions"].(map[string]any)
			if actions["attach"] != canAttach(tc.in) || actions["stop"] != canStop(tc.in) {
				t.Errorf("actions = %v, want the raw-state verdicts", actions)
			}
		})
	}
}

// The listing and the detail read produce the same document shape, from the
// same builder, so a script cannot find one shape in a list and another in a
// detail read.
func TestListAndInfoJSONAgree(t *testing.T) {
	row := session{ID: "sess_a", Name: "box", State: "running", Reachable: true, ChildExitCode: intPtr(2)}

	var one, many bytes.Buffer
	if err := writeSessionJSON(&one, cli.Config{}, row); err != nil {
		t.Fatal(err)
	}
	if err := writeSessionsJSON(&many, cli.Config{}, []session{row}); err != nil {
		t.Fatal(err)
	}
	var detail map[string]any
	var listing struct {
		Sessions []map[string]any `json:"sessions"`
	}
	if err := json.Unmarshal(one.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(many.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Sessions) != 1 {
		t.Fatalf("listing has %d sessions", len(listing.Sessions))
	}
	delete(detail, "schema")
	delete(detail, "version")
	entry := listing.Sessions[0]
	if len(detail) != len(entry) {
		t.Fatalf("detail has %d keys, listing entry has %d", len(detail), len(entry))
	}
	for key, want := range detail {
		got := entry[key]
		if key == "actions" {
			continue // compared structurally above
		}
		if got != want {
			t.Errorf("key %q: listing has %#v, detail has %#v", key, got, want)
		}
	}
}

// Terminal records stay out of the default list and appear under --all.
func TestTerminalRecordsAreNotActive(t *testing.T) {
	for _, state := range []string{"canceled", "destroyed"} {
		if activeSession(session{State: state}) {
			t.Errorf("%s is listed as active", state)
		}
	}
	for _, state := range []string{"queued", "creating", "running", "suspended_warm", "suspended_cold", "failed", "dead", "hibernating"} {
		if !activeSession(session{State: state}) {
			t.Errorf("%s is not listed as active", state)
		}
	}
}
