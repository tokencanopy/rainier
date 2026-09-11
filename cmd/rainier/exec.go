package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/internal/execio"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// `rainier exec <session> [flags] -- <command> [args...]` runs one command
// inside a live session's sandbox, as the session's own user, and exits with
// the command's exit status.
//
// It is Advanced (contract §2.2): dispatched, documented under `rainier help
// all`, absent from the twenty-line default help. It is automation's command,
// and the first-run journey is new → attach → stop.
//
// The stream rule is the whole reason a script can use it: the command's
// stdout is rainier's stdout, byte for byte, with nothing added and no
// trailing newline invented; the command's stderr is rainier's stderr; and
// everything RAINIER says — a refusal, the state in a 409, the version
// sentence for a 404 — goes to stderr, always, so that
// `rainier exec s -- cat f > out` produces exactly f.

const execUsage = `usage: rainier exec <session> [flags] -- <command> [args...]

Run one command inside a live session's sandbox and exit with its status.

Flags:
  --tty          allocate a terminal; stdout and stderr MERGE, because a pty
                 has one stream
  --cwd DIR      run in DIR, which must be inside /workspace (default /workspace)
  --env K=V      set one environment variable (repeatable)
  --detach       leave the command running after this CLI exits; requires
                 --log, prints the pid, and exits 0
  --log PATH     where a --detach'd command's output goes, inside /workspace
  --json         write one result document to stdout; the command's own
                 output moves to stderr

The command is exec'd directly: there is no shell, so no globs, no $VAR and
no &&. Name one if you want one: rainier exec s -- sh -c 'cd src && make'.

A detached command is stopped the way any process is — rainier exec s --
kill <pid> — and dies with its session when it is stopped or deleted.`

// execEnvFlag collects a repeatable --env K=V in the order it was typed.
type execEnvFlag []string

func (e *execEnvFlag) String() string { return strings.Join(*e, ",") }

func (e *execEnvFlag) Set(v string) error {
	if !strings.Contains(v, "=") {
		return fmt.Errorf("--env must be K=V, got %q", safeField(v))
	}
	*e = append(*e, v)
	return nil
}

// runExec is the command. It returns an error for everything that is not the
// command's own status, and an execExitError — which exitCodeFor answers
// with the code it carries — for everything that is.
func runExec(args []string) error {
	spec, ref, jsonOut, err := parseExecArgs(args)
	if err != nil {
		return err
	}

	cfg, _, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}

	o := execio.Options{Spec: spec, Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr}
	if jsonOut {
		// §6.2: a --json document is the only thing on stdout, so the
		// command's own bytes move. Both streams go to stderr as they
		// arrive, and stdout carries the document after the command exits.
		o.Stdout, o.Stderr = os.Stderr, os.Stderr
	}
	o.Header = http.Header{"Authorization": {"Bearer " + cli.NewClient(cfg).Token}}
	if active, ok := cfg.Active(); ok && active.Workspace != "" {
		// The exec stream is scoped like every other request on a hosted
		// context: the edge routes it by the same header.
		o.Header.Set("Rainier-Workspace", active.Workspace)
	}

	res, err := execio.Run(context.Background(), execURLFor(cfg.ServerURL, id), o)
	if err != nil {
		return execDialFailure(err, ref)
	}
	return reportExec(cfg, id, spec, res, jsonOut)
}

// parseExecArgs splits `<session> [flags] -- <command>`.
//
// The session comes FIRST, before the flags, and that is a contract rather
// than an accident of the parser: `--env K=V` takes a value, so a scan
// looking for "the first thing that is not a flag" would take K=V for the
// session name on `rainier exec --env FOO=bar box1 -- make`. Requiring the
// order makes every invocation unambiguous and the error for the other one
// exact.
func parseExecArgs(args []string) (runner.ExecSpec, string, bool, error) {
	if len(args) == 0 {
		return runner.ExecSpec{}, "", false, usagef("%s", execUsage)
	}
	ref := args[0]
	if strings.HasPrefix(ref, "-") {
		return runner.ExecSpec{}, "", false, usagef(
			"the session comes first: rainier exec <session> [flags] -- <command>")
	}

	// ContinueOnError, not the ExitOnError the older commands use: a bad flag
	// here has to come back as a usageError so that §6.1's exit 2 is what a
	// script sees, and so that the usage above is printed on STDERR rather
	// than mixed into a stream the caller is redirecting.
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	tty := fs.Bool("tty", false, "allocate a terminal (stdout and stderr merge)")
	cwd := fs.String("cwd", "", "run in this directory, inside /workspace")
	detach := fs.Bool("detach", false, "leave the command running after this CLI exits")
	logPath := fs.String("log", "", "where a detached command's output goes, inside /workspace")
	jsonOut := fs.Bool("json", false, "write one result document to stdout")
	var env execEnvFlag
	fs.Var(&env, "env", "set one environment variable, K=V (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return runner.ExecSpec{}, "", false, usagef("%v\n\n%s", err, execUsage)
	}

	argv := fs.Args()
	if len(argv) == 0 {
		return runner.ExecSpec{}, "", false, usagef(
			"rainier exec needs a command: rainier exec %s -- <command> [args...]", safeField(ref))
	}

	spec := runner.ExecSpec{Argv: argv, Cwd: *cwd, TTY: *tty,
		Detach: *detach, LogPath: *logPath}
	if spec.TTY {
		spec.Cols, spec.Rows = terminalSizeOrDefault()
	}
	var err error
	if spec.Env, err = execEnvMap(env); err != nil {
		return runner.ExecSpec{}, "", false, err
	}
	switch {
	case spec.Detach && spec.LogPath == "":
		return runner.ExecSpec{}, "", false, usagef(
			"--detach needs --log PATH: a detached command's output has to go somewhere, " +
				"and it must be inside /workspace")
	case !spec.Detach && spec.LogPath != "":
		return runner.ExecSpec{}, "", false, usagef(
			"--log only applies to --detach; without it the command's output is already yours")
	case spec.Detach && spec.TTY:
		return runner.ExecSpec{}, "", false, usagef(
			"--detach and --tty are not compatible: a detached command has no terminal to attach one to")
	}
	return spec, ref, *jsonOut, nil
}

// execEnvMap turns the repeated --env flags into the map the wire carries,
// refusing a name given twice rather than silently keeping one of them.
func execEnvMap(flags execEnvFlag) (map[string]string, error) {
	if len(flags) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(flags))
	for _, kv := range flags {
		name, value, _ := strings.Cut(kv, "=")
		if name == "" {
			return nil, usagef("--env needs a name: %q", safeField(kv))
		}
		if _, dup := out[name]; dup {
			return nil, usagef("--env %s was given twice", safeField(name))
		}
		out[name] = value
	}
	return out, nil
}

// execURLFor is the exec route. It is its OWN route rather than a parameter
// on attach, so a control plane older than this CLI answers 404 — which is
// the honest answer, and the one execDialFailure turns into a sentence.
func execURLFor(serverURL, id string) string {
	ws := serverURL
	switch {
	case strings.HasPrefix(ws, "https://"):
		ws = "wss://" + strings.TrimPrefix(ws, "https://")
	case strings.HasPrefix(ws, "http://"):
		ws = "ws://" + strings.TrimPrefix(ws, "http://")
	}
	return strings.TrimRight(ws, "/") + "/v0/sessions/" + id + "/exec"
}

// execDialFailure turns a refused upgrade into the sentence that says what to
// do about it. Each one is a different action for the caller, which is why
// they are not one message with a status code in it.
func execDialFailure(err error, ref string) error {
	var de *execio.DialError
	if !errors.As(err, &de) {
		return err
	}
	switch de.Status {
	case http.StatusNotFound:
		if de.Code == "not_found" {
			return fmt.Errorf("no session %s", safeField(ref))
		}
		// A 404 with no envelope is a route this server does not have.
		return errors.New(
			"this Rainier does not support exec (the control plane is older than the CLI)")
	case http.StatusNotImplemented:
		return errors.New("the runner holding this session cannot run commands in it; " +
			"it is older than exec. Sessions created after the runner is updated can be exec'd into.")
	case http.StatusConflict:
		return fmt.Errorf("session %s is %s, not running.\nStart it first: rainier resume %s",
			safeField(ref), safeField(de.State), safeField(ref))
	case http.StatusServiceUnavailable:
		return fmt.Errorf("the runner holding session %s is not connected; try again shortly",
			safeField(ref))
	case http.StatusForbidden:
		return fmt.Errorf("not authorized to run commands in session %s", safeField(ref))
	default:
		return de
	}
}

// reportExec is everything this CLI says about a finished exec, and the exit
// code it leaves behind.
func reportExec(cfg cli.Config, id string, spec runner.ExecSpec, res execio.Result, jsonOut bool) error {
	if res.Detached && res.Started {
		// The pid is the caller's only handle on a detached run, so it is the
		// one thing on stdout — where a script can read it.
		if jsonOut {
			return execJSON(id, spec, res)
		}
		fmt.Fprintln(os.Stdout, res.PID)
		return nil
	}
	// The document is written for a REFUSAL too, which is what its `error`
	// field is for: a caller that asked for machine-readable output and got a
	// cwd it could fix should not have to parse a sentence off stderr to
	// learn that. Only a run that never reached the sandbox at all — a 404, a
	// 409 — has nothing to describe, and that is reported as an error before
	// this is ever called.
	if jsonOut && (res.Started || res.Reason != "") {
		if err := execJSON(id, spec, res); err != nil {
			return err
		}
	}

	code, decided := execio.ExitCodeFor(res)
	if !decided {
		return execRefusal(res)
	}
	if code == execio.ExitNoStatus && res.Reason == "" && !res.Interrupted {
		// The socket died mid-run. Which WAY it died is worth a sentence: a
		// session somebody suspended or deleted underneath the command is a
		// different thing from a sandbox that crashed, and the caller can act
		// on the difference.
		fmt.Fprintln(os.Stderr, "rainier: "+execEndedSentence(cfg, id))
	}
	if code == 0 {
		return nil
	}
	return execExitError{code: code, reason: res.Reason}
}

// execRefusal is the sentence for an exec that never started and named a
// reason Rainier owns rather than the command's.
func execRefusal(res execio.Result) error {
	switch res.Reason {
	case terminal.ReasonUnsupported:
		return errors.New("this session's sandbox does not support exec; it was created " +
			"before exec shipped. A session created now can be exec'd into.")
	case terminal.ReasonTooManyExecs:
		return errors.New("this session is already running as many commands as it may; " +
			"wait for one to finish, or stop a detached one with " +
			"rainier exec <session> -- kill <pid>")
	case terminal.ReasonNoAnswer:
		return errors.New("the session's sandbox did not answer in time; try again")
	case terminal.ReasonStdinOverrun:
		return errors.New("the command stopped reading its input while more was still " +
			"arriving, and the session will not buffer more of it.\n" +
			"Run it again writing to a file instead: rainier exec <session> -- sh -c 'cat > f'")
	}
	if res.Interrupted {
		return errors.New("interrupted")
	}
	return execio.NoStatusError()
}

// execEndedSentence names WHICH end it was. The session is re-read, briefly
// and best-effort, because "the connection ended" and "somebody stopped the
// session out from under you" are different things to a person reading a
// build log — and the second one is an action somebody took.
func execEndedSentence(cfg cli.Config, id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	row, err := getSessionContext(ctx, cli.NewClient(cfg), id)
	if err != nil {
		var apiErr *cli.APIError
		if errors.As(err, &apiErr) && apiErr.Code == "not_found" {
			return "the session was deleted before the command reported an exit status"
		}
		return execio.NoStatusError().Error()
	}
	switch row.State {
	case "suspended_warm", "suspended_cold":
		return "the session was stopped before the command reported an exit status"
	case "destroyed", "canceled":
		return "the session was deleted before the command reported an exit status"
	case "dead", "failed":
		return "the session's sandbox ended before the command reported an exit status"
	}
	return execio.NoStatusError().Error()
}

// execJSON writes the one document --json promises. It never carries output:
// bounding it would truncate, and not bounding it would put a build log in a
// JSON string. A caller who wants the command's stdout AND machine-readable
// timings runs without --json and reads the exit code, which is the
// machine-readable fact that matters.
func execJSON(id string, spec runner.ExecSpec, res execio.Result) error {
	doc := map[string]any{
		"schema":  "rainier.v0.exec",
		"version": 1,
		"session": id,
		"tty":     spec.TTY,
	}
	doc["exit_code"] = nil
	doc["signal"] = nil
	switch {
	case res.Signal != "":
		doc["signal"] = res.Signal
	case res.ExitCode != nil:
		doc["exit_code"] = *res.ExitCode
	}
	if res.Detached {
		doc["detached"] = true
		doc["pid"] = res.PID
	}
	if !res.StartedAt.IsZero() {
		doc["started_at"] = res.StartedAt.UTC().Format(time.RFC3339)
		doc["queued_ms"] = res.Queued.Milliseconds()
	}
	if !res.ExitedAt.IsZero() {
		doc["exited_at"] = res.ExitedAt.UTC().Format(time.RFC3339)
		doc["duration_ms"] = res.Duration.Milliseconds()
	}
	if res.Reason != "" {
		doc["error"] = res.Reason
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(raw))
	return nil
}

// execExitError carries a command's own status out to main, which exits with
// it. It is not a failure of Rainier's and prints nothing: the command has
// already said whatever it had to say on its own streams.
type execExitError struct {
	code   int
	reason string
}

func (e execExitError) Error() string {
	switch e.reason {
	case terminal.ReasonNotFound:
		return "command not found in this session"
	case terminal.ReasonNotExecutable:
		return "the command is not executable in this session"
	case terminal.ReasonCwdRefused:
		return "--cwd must name a directory inside /workspace"
	case terminal.ReasonEnvRefused:
		return "an --env name this session does not accept; see rainier help exec"
	case terminal.ReasonLogRefused:
		return "--log must name a path inside /workspace that can be written"
	}
	return ""
}

// silent reports that this error has already said everything it is going to.
// A command that exited 7 has printed whatever it printed; adding a line of
// Rainier's own would corrupt the output of `rainier exec s -- cat f`.
func (e execExitError) silent() bool { return e.reason == "" }

// terminalSizeOrDefault is the size a --tty exec opens with: this terminal's,
// or a plain 80x24 when there is not one. A pty has to be SOME size, and a
// zero-sized one is a terminal no program draws on.
func terminalSizeOrDefault() (cols, rows int) {
	if cols, rows, err := term.GetSize(int(os.Stdin.Fd())); err == nil && cols > 0 && rows > 0 {
		return cols, rows
	}
	return 80, 24
}
