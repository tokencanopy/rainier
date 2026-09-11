package terminal_test

import (
	"encoding/json"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// TestExecMessagesAreAdditive is the compatibility claim of the exec message
// set, stated as bytes: every field exec adds is omitempty, so a peer that
// sets none of them writes exactly the bytes it wrote before exec existed.
//
// It is deliberately the SAME assertion TestUnnegotiatedMessagesAreByteIdentical
// makes for conditional ownership, repeated after a second round of additive
// fields, because "additive" is a property of the current struct rather than
// of the commit that added a field.
func TestExecMessagesAreAdditive(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  any
		want string
	}{
		{"stdin", terminal.ClientMessage{Type: "stdin", Data: []byte("hi")},
			`{"type":"stdin","data":"aGk="}`},
		{"resize", terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24},
			`{"type":"resize","cols":80,"rows":24}`},
		{"claim", terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(7)},
			`{"type":"claim","expected":"7"}`},
		{"snapshot", terminal.ServerMessage{Type: "snapshot", Seq: 9, Data: []byte("hi"), Cols: 80, Rows: 24},
			`{"type":"snapshot","seq":9,"data":"aGk=","cols":80,"rows":24}`},
		{"output", terminal.ServerMessage{Type: "output", Seq: 9, Data: []byte("hi")},
			`{"type":"output","seq":9,"data":"aGk="}`},
		{"exit", terminal.ServerMessage{Type: "exit", ExitCode: 7},
			`{"type":"exit","exitCode":7}`},
		{"attached", terminal.ServerMessage{Type: terminal.TypeAttached,
			Mode: terminal.ModeControl, Generation: terminal.GenOf(4)},
			`{"type":"attached","mode":"control","gen":"4"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.msg)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != tc.want {
				t.Fatalf("%s = %s\nwant %s", tc.name, raw, tc.want)
			}
		})
	}
}

// TestExecWireShapes pins the JSON of each new message. These bytes are the
// contract between three separately released parties — the CLI, the plane and
// the sandbox — so a field rename here is a silent protocol break rather than
// a compile error.
func TestExecWireShapes(t *testing.T) {
	start, err := json.Marshal(terminal.ClientMessage{
		Type: terminal.TypeExecStart,
		Exec: &runner.ExecSpec{Argv: []string{"git", "status"}, Cwd: "/workspace/repo"},
	})
	if err != nil {
		t.Fatal(err)
	}
	const wantStart = `{"type":"exec_start","exec":{"argv":["git","status"],"cwd":"/workspace/repo"}}`
	if string(start) != wantStart {
		t.Fatalf("exec_start = %s\nwant %s", start, wantStart)
	}

	eof, err := json.Marshal(terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})
	if err != nil {
		t.Fatal(err)
	}
	if string(eof) != `{"type":"exec_stdin_eof"}` {
		t.Fatalf("exec_stdin_eof = %s", eof)
	}

	sig, err := json.Marshal(terminal.ClientMessage{Type: terminal.TypeExecSignal, Signal: terminal.SignalINT})
	if err != nil {
		t.Fatal(err)
	}
	if string(sig) != `{"type":"exec_signal","signal":"INT"}` {
		t.Fatalf("exec_signal = %s", sig)
	}

	for _, tc := range []struct {
		name string
		msg  terminal.ServerMessage
		want string
	}{
		{"started", terminal.ServerMessage{Type: terminal.TypeExecStarted},
			`{"type":"exec_started"}`},
		{"started detached", terminal.ServerMessage{Type: terminal.TypeExecStarted, PID: 4321},
			`{"type":"exec_started","pid":4321}`},
		{"stdout", terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("hi")},
			`{"type":"exec_stdout","data":"aGk="}`},
		{"stderr", terminal.ServerMessage{Type: terminal.TypeExecStderr, Data: []byte("hi")},
			`{"type":"exec_stderr","data":"aGk="}`},
		{"exit", terminal.ServerMessage{Type: terminal.TypeExecExit, ExitCode: 7},
			`{"type":"exec_exit","exitCode":7}`},
		{"exit 0", terminal.ServerMessage{Type: terminal.TypeExecExit},
			`{"type":"exec_exit"}`},
		{"signalled", terminal.ServerMessage{Type: terminal.TypeExecExit, Signal: "TERM"},
			`{"type":"exec_exit","signal":"TERM"}`},
		{"error", terminal.ServerMessage{Type: terminal.TypeExecError, Reason: terminal.ReasonCwdRefused},
			`{"type":"exec_error","reason":"cwd_refused"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.msg)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != tc.want {
				t.Fatalf("%s = %s\nwant %s", tc.name, raw, tc.want)
			}
		})
	}
}

// TestExecExitZeroIsAbsentAndReadsBackAsZero is the one place the reused
// `exitCode` tag could bite: omitempty drops a zero, so an exec that
// succeeded sends no exit code at all. That is correct only because the
// absent value decodes to 0, which is what a successful command's status IS
// — and because a SIGNALLED exit is distinguished by Signal rather than by
// a sentinel exit code that a real command could also produce.
func TestExecExitZeroIsAbsentAndReadsBackAsZero(t *testing.T) {
	raw, err := json.Marshal(terminal.ServerMessage{Type: terminal.TypeExecExit})
	if err != nil {
		t.Fatal(err)
	}
	var back terminal.ServerMessage
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.ExitCode != 0 || back.Signal != "" {
		t.Fatalf("decoded exec_exit = code %d signal %q, want 0 and no signal", back.ExitCode, back.Signal)
	}
}

// TestExecVocabularyIsClosed pins the words themselves. The CLI maps each one
// to an exit code and a sentence, so a rename here is a CLI that reports the
// wrong outcome rather than a build failure.
func TestExecVocabularyIsClosed(t *testing.T) {
	got := []string{
		terminal.TypeExecStart, terminal.TypeExecStdinEOF, terminal.TypeExecSignal,
		terminal.TypeExecStarted, terminal.TypeExecStdout, terminal.TypeExecStderr,
		terminal.TypeExecExit, terminal.TypeExecError,
		terminal.SignalTERM, terminal.SignalINT,
	}
	want := []string{
		"exec_start", "exec_stdin_eof", "exec_signal",
		"exec_started", "exec_stdout", "exec_stderr",
		"exec_exit", "exec_error",
		"TERM", "INT",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("vocabulary word %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestExecReasonsAreClosedBothWays iterates the CODE's table rather than the
// test's own list.
//
// Iterating `want` meant a NEW reason word was never noticed, which is how
// no_answer and stdin_overrun drifted out of the design's published
// vocabulary — the doc lists seven and the code has ten. Checking both
// directions means a word added here fails until it is written down.
func TestExecReasonsAreClosedBothWays(t *testing.T) {
	want := map[string]bool{
		"unsupported":       true,
		"not_found":         true,
		"not_executable":    true,
		"cwd_refused":       true,
		"env_refused":       true,
		"log_refused":       true,
		"too_many_execs":    true,
		"too_many_detached": true,
		"no_answer":         true,
		"stdin_overrun":     true,
	}
	got := map[string]bool{}
	for _, r := range terminal.ExecReasons() {
		if got[r] {
			t.Fatalf("reason %q appears twice in the vocabulary", r)
		}
		got[r] = true
		if !want[r] {
			t.Fatalf("the code has a reason word %q this test does not know. "+
				"Add it HERE, to docs/design/rainier-exec.md's vocabulary list and to "+
				"docs/cli-v0-contract.md §3.9 — a word a caller can receive and no "+
				"document names is a word nobody can act on.", r)
		}
	}
	for r := range want {
		if !got[r] {
			t.Fatalf("reason %q is documented and no longer in the vocabulary", r)
		}
	}
}

// TestExecReasonsIsACopy: the vocabulary is a fact about this protocol, not a
// variable a consumer may edit out from under every other consumer.
func TestExecReasonsIsACopy(t *testing.T) {
	first := terminal.ExecReasons()
	first[0] = "tampered"
	if terminal.ExecReasons()[0] == "tampered" {
		t.Fatal("ExecReasons hands out the same backing array every time")
	}
}
