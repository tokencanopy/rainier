package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/internal/execio"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// parsing
// ---------------------------------------------------------------------------

// TestParseExecArgs pins the invocation. The session comes FIRST, before the
// flags, and the command comes after `--`; both are contract rather than an
// accident of the parser.
func TestParseExecArgs(t *testing.T) {
	spec, ref, jsonOut, err := parseExecArgs([]string{"box1", "--", "git", "status"})
	if err != nil {
		t.Fatal(err)
	}
	if ref != "box1" || jsonOut {
		t.Fatalf("ref=%q json=%v", ref, jsonOut)
	}
	if len(spec.Argv) != 2 || spec.Argv[0] != "git" || spec.Argv[1] != "status" {
		t.Fatalf("argv = %v", spec.Argv)
	}

	spec, _, jsonOut, err = parseExecArgs([]string{"box1", "--tty", "--cwd", "app",
		"--env", "CI=1", "--env", "LEVEL=2", "--json", "--", "make", "-j4"})
	if err != nil {
		t.Fatal(err)
	}
	if !spec.TTY || spec.Cwd != "app" || !jsonOut {
		t.Fatalf("spec = %+v json=%v", spec, jsonOut)
	}
	if spec.Env["CI"] != "1" || spec.Env["LEVEL"] != "2" || len(spec.Env) != 2 {
		t.Fatalf("env = %v", spec.Env)
	}
	if spec.Cols <= 0 || spec.Rows <= 0 {
		t.Fatalf("a --tty exec opened at %dx%d; a pty has to be some size", spec.Cols, spec.Rows)
	}
	if len(spec.Argv) != 2 || spec.Argv[1] != "-j4" {
		t.Fatalf("argv = %v; an argument that looks like a flag must survive --", spec.Argv)
	}

	// A command whose own flags would otherwise be eaten.
	spec, _, _, err = parseExecArgs([]string{"box1", "--", "ls", "-la", "--color=never"})
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Argv) != 3 || spec.Argv[2] != "--color=never" {
		t.Fatalf("argv = %v", spec.Argv)
	}

	// Detach.
	spec, _, _, err = parseExecArgs([]string{"box1", "--detach", "--log", "run.log",
		"--", "claude", "--continue"})
	if err != nil {
		t.Fatal(err)
	}
	if !spec.Detach || spec.LogPath != "run.log" {
		t.Fatalf("spec = %+v", spec)
	}
}

// TestParseExecArgsRefusals: every one of these exits 2, because the
// invocation itself is wrong and a 2 is never worth retrying.
func TestParseExecArgsRefusals(t *testing.T) {
	for name, args := range map[string][]string{
		"nothing at all":        {},
		"no command":            {"box1"},
		"no command after --":   {"box1", "--tty"},
		"a flag before the ref": {"--tty", "box1", "--", "true"},
		"detach with no log":    {"box1", "--detach", "--", "true"},
		"log without detach":    {"box1", "--log", "run.log", "--", "true"},
		"detach with a tty":     {"box1", "--detach", "--tty", "--log", "l", "--", "true"},
		"env with no =":         {"box1", "--env", "JUSTNAME", "--", "true"},
		"env with no name":      {"box1", "--env", "=value", "--", "true"},
		"env given twice":       {"box1", "--env", "CI=1", "--env", "CI=2", "--", "true"},
		"an unknown flag":       {"box1", "--nope", "--", "true"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := parseExecArgs(args)
			if err == nil {
				t.Fatal("accepted")
			}
			if got := exitCodeFor(err); got != 2 {
				t.Fatalf("%v exits %d, want 2 (an invalid invocation)", err, got)
			}
		})
	}
}

// TestExecURLIsItsOwnRoute. A query parameter on attach would be IGNORED by a
// plane that predates it, which would open an ordinary controller attachment
// — a take-over — with the command never running. A route answers 404.
func TestExecURLIsItsOwnRoute(t *testing.T) {
	for in, want := range map[string]string{
		"https://rainier.example.invalid":  "wss://rainier.example.invalid/v0/sessions/sess_example/exec",
		"http://127.0.0.1:8080":            "ws://127.0.0.1:8080/v0/sessions/sess_example/exec",
		"https://rainier.example.invalid/": "wss://rainier.example.invalid/v0/sessions/sess_example/exec",
	} {
		if got := execURLFor(in, "sess_example"); got != want {
			t.Fatalf("execURLFor(%q) = %q, want %q", in, got, want)
		}
	}
	if strings.Contains(execURLFor("https://x.invalid", "sess_example"), "attach") {
		t.Fatal("the exec URL is an attach URL")
	}
}

// ---------------------------------------------------------------------------
// refusals
// ---------------------------------------------------------------------------

// TestExecDialFailureSentences: each refusal is a different action for the
// caller, which is why they are not one message with a status code in it.
func TestExecDialFailureSentences(t *testing.T) {
	for name, tc := range map[string]struct {
		err  *execio.DialError
		want string
	}{
		"an old plane": {&execio.DialError{Status: http.StatusNotFound},
			"this Rainier does not support exec (the control plane is older than the CLI)"},
		"no such session": {&execio.DialError{Status: http.StatusNotFound, Code: "not_found"},
			"no session box1"},
		"an old runner": {&execio.DialError{Status: http.StatusNotImplemented,
			Code: "exec_unsupported"}, "cannot run commands in it"},
		"not running": {&execio.DialError{Status: http.StatusConflict,
			Code: "session_not_running", State: "suspended_warm"}, "is suspended_warm, not running"},
		"unreachable": {&execio.DialError{Status: http.StatusServiceUnavailable,
			Code: "runner_unreachable"}, "is not connected"},
		"forbidden": {&execio.DialError{Status: http.StatusForbidden, Code: "forbidden"},
			"not authorized to run commands"},
	} {
		t.Run(name, func(t *testing.T) {
			err := execDialFailure(tc.err, "box1")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want it to contain %q", err, tc.want)
			}
			if got := exitCodeFor(err); got != 1 {
				t.Fatalf("a refusal exits %d, want 1", got)
			}
		})
	}
}

// TestExecDialFailureNamesTheResumeCommand, because a script that means to
// resume writes two facts in the order it chose — exec never resumes a
// session on its own, which would make `exec s -- git status` cost minutes
// and change what the caller is billed for.
func TestExecDialFailureNamesTheResumeCommand(t *testing.T) {
	err := execDialFailure(&execio.DialError{Status: http.StatusConflict,
		Code: "session_not_running", State: "suspended_cold"}, "box1")
	if !strings.Contains(err.Error(), "rainier resume box1") {
		t.Fatalf("the 409 sentence does not say what to do: %v", err)
	}
}

// ---------------------------------------------------------------------------
// exit codes and streams
// ---------------------------------------------------------------------------

// TestExecExitErrorCarriesTheCommandsStatus out to main, which exits with it.
func TestExecExitErrorCarriesTheCommandsStatus(t *testing.T) {
	for _, code := range []int{1, 2, 7, 125, 126, 127, 137, 255} {
		if got := exitCodeFor(execExitError{code: code}); got != code {
			t.Fatalf("exitCodeFor(execExitError{%d}) = %d", code, got)
		}
	}
	// And it is not confused with a usage error, which is always 2.
	if got := exitCodeFor(usagef("bad")); got != 2 {
		t.Fatalf("a usage error exits %d, want 2", got)
	}
}

// TestACommandsOwnFailurePrintsNothing: it has already said whatever it had
// to say on its own streams, and a line of Rainier's would corrupt the output
// of `rainier exec s -- cat f`.
func TestACommandsOwnFailurePrintsNothing(t *testing.T) {
	if !silentError(execExitError{code: 7}) {
		t.Fatal("a command that exited 7 would have had a Rainier sentence printed over it")
	}
	// A refusal Rainier owns is NOT silent: it is the only thing the caller
	// will ever be told about why the command did not run.
	for _, reason := range []string{
		terminal.ReasonNotFound, terminal.ReasonNotExecutable,
		terminal.ReasonCwdRefused, terminal.ReasonEnvRefused, terminal.ReasonLogRefused,
	} {
		err := execExitError{code: 126, reason: reason}
		if silentError(err) {
			t.Fatalf("a %s refusal said nothing at all", reason)
		}
		if err.Error() == "" {
			t.Fatalf("a %s refusal has no sentence", reason)
		}
	}
}

// TestRainiersOwnSentencesNeverReachStdout is §6.1 for this command, checked
// rather than asserted: `rainier exec s -- cat f > out` has to produce
// exactly f, so everything Rainier says goes to stderr.
func TestRainiersOwnSentencesNeverReachStdout(t *testing.T) {
	res := execio.Result{Started: true}
	stdout, stderr, err := captureBoth(t, func() error {
		return reportExec(testConfigForExec(), "sess_example",
			runner.ExecSpec{Argv: []string{"x"}}, res, false)
	})
	if err == nil {
		t.Fatal("a run with no exit status reported success")
	}
	if stdout != "" {
		t.Fatalf("Rainier wrote %q to stdout", stdout)
	}
	if !strings.Contains(stderr, "exit status") {
		t.Fatalf("the 125 sentence did not reach stderr: %q", stderr)
	}
	if code, _ := execio.ExitCodeFor(res); code != execio.ExitNoStatus {
		t.Fatalf("a run with no status exits %d, want 125", code)
	}
}

// TestDetachedExecPrintsOnlyThePid: it is the caller's only handle on the
// process, so it is the one thing on stdout, where a script can read it.
func TestDetachedExecPrintsOnlyThePid(t *testing.T) {
	res := execio.Result{Started: true, Detached: true, PID: 4321}
	stdout, stderr, err := captureBoth(t, func() error {
		return reportExec(testConfigForExec(), "sess_example",
			runner.ExecSpec{Argv: []string{"claude"}, Detach: true, LogPath: "run.log"}, res, false)
	})
	if err != nil {
		t.Fatalf("a detached exec reported %v", err)
	}
	if strings.TrimSpace(stdout) != "4321" {
		t.Fatalf("stdout = %q, want just the pid", stdout)
	}
	if stderr != "" {
		t.Fatalf("a detached exec wrote %q to stderr", stderr)
	}
}

// ---------------------------------------------------------------------------
// --json
// ---------------------------------------------------------------------------

// TestExecJSONIsOneDocument, and it never carries output: bounding it would
// truncate, and not bounding it would put a build log in a JSON string.
func TestExecJSONIsOneDocument(t *testing.T) {
	code := 7
	started := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	res := execio.Result{Started: true, ExitCode: &code,
		StartedAt: started, ExitedAt: started.Add(4021 * time.Millisecond),
		Queued: 118 * time.Millisecond, Duration: 4021 * time.Millisecond}

	stdout, _, _ := captureBoth(t, func() error {
		return reportExec(testConfigForExec(), "sess_example",
			runner.ExecSpec{Argv: []string{"make"}}, res, true)
	})

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 1 {
		t.Fatalf("--json wrote %d lines to stdout, want exactly one document", len(lines))
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &doc); err != nil {
		t.Fatalf("--json wrote %q: %v", lines[0], err)
	}
	if doc["schema"] != "rainier.v0.exec" || doc["version"] != float64(1) ||
		doc["session"] != "sess_example" {
		t.Fatalf("document = %v", doc)
	}
	if doc["exit_code"] != float64(7) {
		t.Fatalf("exit_code = %v", doc["exit_code"])
	}
	// Both keys present, exactly one null, so a consumer cannot mistake
	// "absent" for "older CLI".
	signal, ok := doc["signal"]
	if !ok || signal != nil {
		t.Fatalf("signal = %v (present=%v), want an explicit null", signal, ok)
	}
	if doc["queued_ms"] != float64(118) || doc["duration_ms"] != float64(4021) {
		t.Fatalf("timings = %v / %v", doc["queued_ms"], doc["duration_ms"])
	}
	if doc["started_at"] != "2026-09-11T10:00:00Z" {
		t.Fatalf("started_at = %v", doc["started_at"])
	}
	for _, forbidden := range []string{"stdout", "stderr", "output"} {
		if _, has := doc[forbidden]; has {
			t.Fatalf("--json carried the command's %s", forbidden)
		}
	}
}

// TestExecJSONSignalIsExclusiveWithTheCode: exactly one is non-null, because
// a command may legitimately exit 137 and a caller who needs certainty reads
// these two fields rather than the exit code.
func TestExecJSONSignalIsExclusiveWithTheCode(t *testing.T) {
	stdout, _, _ := captureBoth(t, func() error {
		return reportExec(testConfigForExec(), "sess_example", runner.ExecSpec{Argv: []string{"x"}},
			execio.Result{Started: true, Signal: "KILL"}, true)
	})
	var doc map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["signal"] != "KILL" || doc["exit_code"] != nil {
		t.Fatalf("document = %v, want signal KILL and a null exit_code", doc)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// testConfigForExec is a config with no server and no credentials: the tests
// above never reach one, and a stray request would fail loudly rather than
// touching anything real.
func testConfigForExec() cli.Config { return cli.Config{} }
