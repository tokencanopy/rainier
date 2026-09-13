package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
)

// info, stop and delete: the three commands that act on one session by name.
// They share this file because they share the shape — resolve a selector,
// read the authoritative row, act, and report what the SERVER says happened
// rather than what was asked for.

// ---------------------------------------------------------------------------
// rainier info
// ---------------------------------------------------------------------------

// runInfo prints one authoritative view of one session
// (docs/cli-v0-contract.md §3.5). It exists because `ls` deliberately shows
// three columns: everything a person occasionally needs about one session had
// to go somewhere, and a second command is better than a wider table.
//
// What it will not print is as deliberate as what it will: no raw provider
// error, no credential, no terminal contents, no internal database detail.
// The session's own error string is server prose and is sanitized before it
// is shown.
func runInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print one machine-readable session document")
	fs.Parse(reorderArgs(fs, args))
	ref, err := requireSelector(fs, "info")
	if err != nil {
		return err
	}

	cfg, c, id, err := resolveClientAndIDIncludingTerminal(ref)
	if err != nil {
		return err
	}
	row, err := getSession(c, id)
	if err != nil {
		return err
	}
	if *asJSON {
		return writeSessionJSON(os.Stdout, cfg, row)
	}
	printInfo(os.Stdout, cfg, row)
	return nil
}

// printInfo renders one session across its three dimensions
// (docs/cli-v0-contract.md §3.1). Every value that came off the wire goes
// through safeField: this is a session another person may have named, and a
// terminal escape in a name would let them forge the lines around it.
//
// Session, Process and Connection are three labelled lines rather than one
// clever sentence. A person debugging a session needs to know which of the
// three is the problem, and the old single cell — "running (exited 0)" — put
// two of them in one word and left the third out entirely.
func printInfo(w io.Writer, cfg cli.Config, s session) {
	fmt.Fprintf(w, "ID:           %s\n", safeField(s.ID))
	fmt.Fprintf(w, "Name:         %s\n", safeField(dashIfEmpty(s.Name)))
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Session:      %s\n", displayLifecycle(s))
	fmt.Fprintf(w, "Process:      %s\n", displayProcess(s))
	fmt.Fprintf(w, "Connection:   %s\n", displayConnection(s))
	// The server's own word, always, beside the three derived ones. It is
	// what a bug report quotes and what a future state shows through.
	fmt.Fprintf(w, "API state:    %s\n", safeField(s.State))
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Created:      %s\n", safeField(dashIfEmpty(s.CreatedAt)))
	fmt.Fprintf(w, "Last event:   %s\n", safeField(dashIfEmpty(s.LastEventAt)))
	fmt.Fprintf(w, "Updated:      %s\n", safeField(dashIfEmpty(s.UpdatedAt)))
	fmt.Fprintf(w, "Environment:  %s\n", safeField(dashIfEmpty(s.Environment)))
	// Who may type. Under an exclusive policy that is three answers and no
	// fourth — the API says whether somebody holds control, never who, so
	// everybody that is not this device is "another device" — and under a
	// shared one it is the rule and the count, because there is no controller
	// to name. The column stays aligned either way.
	inputLabel, inputValue := inputRow(cfg, s)
	fmt.Fprintf(w, "%-13s %s\n", inputLabel+":", inputValue)
	if agent := sessionAgent(s); agent != "" {
		fmt.Fprintf(w, "Agent:        %s\n", safeField(agent))
	}
	// Eligibility is derived from the raw state's own transition rules, never
	// from the display word above it (contract §3.3).
	fmt.Fprintf(w, "Attach:       %s\n", eligibilityText(canAttach(s), attachNote(s)))
	fmt.Fprintf(w, "Stop:         %s\n", eligibilityText(canStop(s), stopNote(s)))
	fmt.Fprintf(w, "Delete:       %s\n", eligibilityText(canDelete(s), deleteNote(s)))
	if detail := diagnosticDetail(s); detail != "" {
		fmt.Fprintf(w, "Waiting on:   %s\n", diagnosticText(cfg, detail))
	}
	if s.Error != "" {
		fmt.Fprintf(w, "Failure:      %s\n", "session failed; use diagnostic attach to inspect its output")
	}
}

// eligibilityText renders one action's answer, with the reason when there is
// one worth giving. "no" on its own invites the question this note answers.
func eligibilityText(verdict, note string) string {
	if note == "" {
		return verdict
	}
	return verdict + " (" + note + ")"
}

// The notes name the API rule behind each answer, so a person can tell "not
// yet" from "never".
func attachNote(s session) string {
	switch s.State {
	case "queued", "creating":
		return "waits for it to start"
	case "suspended_warm", "suspended_cold":
		return "resumes it first"
	case "failed":
		if s.Reachable {
			return "diagnostic terminal, while its runner stays connected"
		}
		return "its runner is no longer connected"
	case "dead", "canceled", "destroyed":
		return "the sandbox is gone"
	}
	if canAttach(s) == eligibleUnknown {
		return "this build does not know this state; the server decides"
	}
	return ""
}

func stopNote(s session) string {
	switch canStop(s) {
	case eligibleYes:
		return ""
	case eligibleUnknown:
		return "this build does not know this state; the server decides"
	}
	switch s.State {
	case "queued", "creating":
		return "only a running session can be stopped"
	case "suspended_warm", "suspended_cold":
		return "already stopped"
	default:
		return "the sandbox is gone"
	}
}

func deleteNote(s session) string {
	switch canDelete(s) {
	case eligibleYes:
		return ""
	case eligibleUnknown:
		return "this build does not know this state; the server decides"
	}
	if s.State == "creating" {
		return "wait for it to finish starting"
	}
	return "already deleted"
}

// writeSessionJSON emits one session document.
//
// The canonical API facts lead and are carried VERBATIM: `state` is the
// server's own string and never a derived word, `reachable` and
// `child_exit_code` are the booleans and the nullable integer the API sent.
// A consumer that wants exactly what the control plane said can read those
// three and ignore everything else.
//
// The derived fields are additive and separately named — `lifecycle`,
// `process`, `connection` — so nothing here overwrites a fact with an
// interpretation of it (contract §6.2).
func writeSessionJSON(w io.Writer, cfg cli.Config, s session) error {
	return writeJSON(w, schemaSession, sessionDocument(cfg, s))
}

func sessionDocument(cfg cli.Config, s session) map[string]any {
	doc := map[string]any{
		// Canonical API facts.
		"id":              s.ID,
		"name":            s.Name,
		"state":           s.State,
		"reachable":       s.Reachable,
		"child_exit_code": nil,
		"created_at":      s.CreatedAt,
		"updated_at":      s.UpdatedAt,
		"last_event_at":   s.LastEventAt,
		"environment":     s.Environment,
		"runner":          s.Runner,
		// Derived, additive, one key per dimension.
		"lifecycle":  lifecycleOf(s),
		"process":    processOf(s),
		"connection": connectionOf(s),
		"actions": map[string]any{
			"attach": canAttach(s),
			"stop":   canStop(s),
			"delete": canDelete(s),
		},
	}
	if s.ChildExitCode != nil {
		doc["child_exit_code"] = *s.ChildExitCode
	}
	if agent := sessionAgent(s); agent != "" {
		doc["agent"] = agent
	}
	if s.QueueReason != "" {
		doc["queue_reason"] = diagnosticText(cfg, s.QueueReason)
	}
	if s.Error != "" {
		doc["failure"] = "session failed; use diagnostic attach to inspect its output"
	}
	for key, value := range doc {
		if text, ok := value.(string); ok {
			doc[key] = redactSecrets(cfg, text)
		}
	}
	return doc
}

// sessionAgent names the coding agent a session runs, when that is knowable.
// It is not, today: matching the session's command back to a provider needs
// the launch catalog the server does not publish (contract §5.3), and this
// returns "" until it does. It is a function rather than an inline blank so
// that landing the catalog is one edit here.
func sessionAgent(session) string { return "" }

// ---------------------------------------------------------------------------
// rainier stop
// ---------------------------------------------------------------------------

// stopSettle bounds how long stop waits for the server's post-stop state. The
// suspend call itself is synchronous — the runner is dispatched to and must
// answer before the transition — so this budget covers the store catching up,
// not the work.
const stopSettle = 20 * time.Second
const stopPoll = 250 * time.Millisecond

// runStop stops a session and releases its compute capacity
// (docs/cli-v0-contract.md §3.7).
//
// It is the cold stop, always. Warm and cold are a capacity decision the
// control plane makes sense of and a person does not, and offering the choice
// here would mean explaining what "warm" costs — a held slot on a runner, in
// a product sold by the slot. `stop` means the session is not consuming
// anything until it is attached again.
//
// The one thing it must never do is say "stopped" about a session that isn't.
// State is what a person is trusting this command with: an unpersisted stop
// is lost work. So it reads the authoritative row afterwards and reports
// failure unless the server says the session is stopped.
func runStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print one machine-readable result document")
	// Accepted and ignored: `suspend --cold` was already the safe stop, so a
	// script that passes it means exactly what this command does.
	cold := fs.Bool("cold", false, "")
	warm := fs.Bool("warm", false, "")
	fs.Parse(reorderArgs(fs, args))
	_ = cold
	// Checked before the positional: `rainier stop --warm` is wrong about what
	// the command does, and saying "usage: stop <session>" would send the
	// reader looking for a missing argument instead.
	if *warm {
		return usagef("rainier stop always persists the session and releases its capacity; there is no warm stop")
	}
	ref, err := requireSelector(fs, "stop")
	if err != nil {
		return err
	}

	_, c, id, err := resolveClientAndIDIncludingTerminal(ref)
	if err != nil {
		return err
	}
	return stopSession(context.Background(), c, id, *asJSON, os.Stdout)
}

// stoppedRaw reports whether the server's own state says this session is
// stopped. It reads the RAW state: suspended_warm and suspended_cold are both
// "stopped" to a person, and both are states ResumeSession accepts, so both
// satisfy a stop that has already happened.
func stoppedRaw(s session) bool {
	return s.State == "suspended_warm" || s.State == "suspended_cold"
}

func alreadyStoppedMessage(row session) string {
	if row.State == "suspended_warm" {
		return "session was already stopped; capacity remains reserved by a warm suspension"
	}
	return "session was already stopped"
}

func stopSession(ctx context.Context, c *cli.Client, id string, asJSON bool, out io.Writer) error {
	row, err := getSessionContext(ctx, c, id)
	if err != nil {
		return err
	}
	// Idempotent: already stopped is the outcome the caller asked for, and a
	// second `stop` in a script must not be a failure.
	if stoppedRaw(row) {
		return reportStop(out, asJSON, id, row.State, alreadyStoppedMessage(row))
	}
	// SuspendSession accepts control.StateRunning and nothing else. Saying so
	// here — before spending a round trip on a refusal — is the difference
	// between "only a running session can be stopped" and a bare conflict.
	// A running session whose child has exited is stoppable: the server's rule
	// is about the sandbox, not the process inside it.
	if verdict := canStop(row); verdict == eligibleNo {
		return fmt.Errorf("session %s cannot be stopped: %s (its state is %s)",
			safeField(sessionLabel(row)), stopNote(row), safeField(row.State))
	}

	warm := false
	var resp sessionEnvelope
	err = c.DoContext(ctx, http.MethodPost, "/v0/sessions/"+id+"/suspend", suspendRequest{Warm: &warm}, &resp)
	if err != nil {
		// A conflict can mean another client stopped it first. Re-read before
		// reporting a failure that may not be one; anything else is real.
		var apiErr *cli.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "conflict" {
			return err
		}
		current, readErr := getSessionContext(ctx, c, id)
		if readErr != nil || !stoppedRaw(current) {
			return err
		}
		return reportStop(out, asJSON, id, current.State, alreadyStoppedMessage(current))
	}

	final, err := awaitStopped(ctx, c, id, resp.Session)
	if err != nil {
		return err
	}
	if !stoppedRaw(final) {
		// Never report success on a state that is not stopped: a suspend that
		// left the session running or failed did not persist it, and saying
		// otherwise is how a person loses work they thought was safe.
		return fmt.Errorf("stop did not complete: session %s is %s (API state %s). Its state has not been released; run `rainier info %s`",
			safeField(id), displayLifecycle(final), safeField(final.State), safeField(id))
	}
	if final.State == "suspended_warm" {
		return reportStop(out, asJSON, id, final.State, "session stopped; capacity remains reserved by a warm suspension")
	}
	return reportStop(out, asJSON, id, final.State, "session stopped and its resources released")
}

// awaitStopped converges on the authoritative post-stop row. The suspend
// response is usually already it; the poll is for the store that has not
// caught up yet, and it is bounded so a stuck server becomes an error rather
// than a hang.
func awaitStopped(ctx context.Context, c *cli.Client, id string, first session) (session, error) {
	if stoppedRaw(first) {
		return first, nil
	}
	ctx, cancel := context.WithTimeout(ctx, stopSettle)
	defer cancel()
	latest := first
	for {
		row, err := getSessionContext(ctx, c, id)
		if err != nil {
			if latest.ID != "" {
				return latest, nil // report what we last saw; the caller judges it
			}
			return session{}, err
		}
		latest = row
		if stoppedRaw(row) {
			return row, nil
		}
		select {
		case <-ctx.Done():
			return latest, nil
		case <-time.After(stopPoll):
		}
	}
}

// reportStop answers a completed stop. state is the SERVER's own word, not a
// display one: a mutation document records what the API said happened.
func reportStop(out io.Writer, asJSON bool, id, state, detail string) error {
	if asJSON {
		return writeJSON(out, schemaMutation, mutationDocument("stop", id, true, state, detail))
	}
	fmt.Fprintf(out, "%s: %s\n", id, detail)
	return nil
}

// runSuspend is the compatibility alias (contract §2.1).
func runSuspend(args []string) error {
	deprecated("suspend", "stop")
	return runStop(args)
}

// ---------------------------------------------------------------------------
// rainier delete
// ---------------------------------------------------------------------------

// runDelete destroys a session permanently (docs/cli-v0-contract.md §3.8).
//
// The confirmation is the whole design. Deletion is not recoverable and the
// thing being deleted is hours of an agent's work, so an interactive caller
// is asked and told what "delete" costs before they answer. A caller with no
// terminal is refused rather than prompted, because a prompt written into a
// pipe is a program that hangs forever on an answer that can never come.
func runDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	yes := fs.Bool("yes", false, "confirm the deletion without prompting (required when there is no terminal)")
	asJSON := fs.Bool("json", false, "print one machine-readable result document")
	fs.Parse(reorderArgs(fs, args))
	ref, err := requireSelector(fs, "delete")
	if err != nil {
		return err
	}
	return deleteSession(ref, *yes, *asJSON, os.Stdout)
}

func deleteSession(ref string, yes, asJSON bool, out io.Writer) error {
	_, c, id, err := resolveClientAndIDIncludingTerminal(ref)
	if err != nil {
		return err
	}

	if !yes {
		if !interactiveTerminal() {
			return usagef("deleting %s destroys it permanently and cannot be undone; pass --yes to confirm", id)
		}
		// The prompt is a diagnostic, not output: under --json the document on
		// stdout has to be the only thing there.
		fmt.Fprintf(os.Stderr, "Delete %s permanently? Its container, its terminal and any work not pushed are destroyed and cannot be recovered.\n", safeField(id))
		ok, err := confirm("continue? [y/N] ")
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("deletion canceled")
		}
	}

	status, err := c.DoStatus(context.Background(), http.MethodDelete, "/v0/sessions/"+id, nil, nil)
	if err != nil {
		var apiErr *cli.APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			// Already gone is the outcome that was asked for.
			return reportDelete(out, asJSON, id, false, "session was already gone")
		}
		return commandError{message: "delete was not confirmed; inspect rainier info " + safeField(id) + "; retry with rainier delete " + safeField(id) + " --yes", cause: err}
	}
	if status == http.StatusAccepted {
		// Explicitly not "deleted": the server has taken the request and the
		// destruction is still happening.
		return reportDelete(out, asJSON, id, true, "deletion accepted; the session is being destroyed")
	}
	return reportDelete(out, asJSON, id, false, "deleted permanently")
}

func reportDelete(out io.Writer, asJSON bool, id string, async bool, detail string) error {
	if asJSON {
		doc := mutationDocument("delete", id, true, "", detail)
		doc["async"] = async
		return writeJSON(out, schemaMutation, doc)
	}
	fmt.Fprintf(out, "%s: %s\n", id, detail)
	return nil
}

// runRm is the compatibility alias (contract §2.1). It keeps `rm`'s existing
// non-interactive behavior on purpose: `rm` has never prompted, scripts call
// it, and changing that would break them for no gain — the safe command is
// the new one.
func runRm(args []string) error {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	fs.Parse(reorderArgs(fs, args))
	ref, err := requireSelector(fs, "rm")
	if err != nil {
		return err
	}
	deprecated("rm", "delete")
	_, c, id, err := resolveClientAndIDIncludingTerminal(ref)
	if err != nil {
		return err
	}
	if err := c.Do(http.MethodDelete, "/v0/sessions/"+id, nil, nil); err != nil {
		return err
	}
	fmt.Println("removed", id)
	return nil
}
