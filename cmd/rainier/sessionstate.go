package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/tokencanopy/rainier/internal/cli"
)

// A session has three independent facts, and this file keeps them apart
// (docs/cli-v0-contract.md §3.1).
//
// The control plane reports them separately because they ARE separate:
//
//   - state is the session's lifecycle — is this sandbox coming up, up,
//     stopped, or broken.
//   - child_exit_code is the process inside it — has the command the session
//     runs finished, and with what status.
//   - reachable is the connection to it — can the control plane talk to the
//     runner holding it right now.
//
// Collapsing them loses information the user needs and the API already has.
// A session whose agent exited is still a running sandbox: it holds its slot,
// its filesystem is intact, and it is still attachable — so calling it
// "finished" tells somebody their session is over when it is not. A runner
// that dropped its connection makes a session unreachable, not failed; the
// entitlement and the work are still there, and treating the two alike would
// have somebody delete a session over a network blip.
//
// So the CLI renders three columns and derives capability from the RAW state,
// never from the display word.

// Lifecycle is the display vocabulary for a session's state. These are keys:
// lowercase and stable, for --json and for comparison. Human output
// capitalizes them.
const (
	lifecycleStarting = "starting"
	lifecycleRunning  = "running"
	lifecycleStopped  = "stopped"
	lifecycleFailed   = "failed"
	// Terminal records. They are normally absent from the active list, and a
	// person who asks for one by name is owed the accurate word rather than a
	// euphemism: a session somebody cancelled and a session somebody deleted
	// are different events with different causes.
	lifecycleCanceled = "canceled"
	lifecycleDeleted  = "deleted"
	// lifecycleUnknown is a state this build has never heard of. It is
	// reported as unknown and carried through raw, never translated into some
	// other dimension's vocabulary — an unrecognized lifecycle is not a
	// statement about the connection.
	lifecycleUnknown = "unknown"
)

// Process is the display vocabulary for the child process.
const (
	// processRunning means the child exists. It does NOT mean the child is
	// doing anything: an agent sitting at a prompt waiting for input and an
	// agent mid-compile are the same fact here, and the CLI must not imply
	// otherwise.
	processRunning = "running"
	processExited  = "exited"
	// processNone is a session with no child to report on — one that has not
	// started yet, or one whose sandbox is gone.
	processNone = "none"
)

// Connection is the display vocabulary for reachability.
const (
	connectionAvailable   = "available"
	connectionUnavailable = "unavailable"
)

// lifecycleOf maps the control plane's session state onto the display
// vocabulary. It reads child_exit_code nowhere and reachable nowhere: those
// are the other two dimensions, and a lifecycle that moved when a child
// exited would be the collapse this file exists to prevent.
func lifecycleOf(s session) string {
	switch s.State {
	case "queued", "creating":
		return lifecycleStarting
	case "running":
		// Regardless of child_exit_code. The sandbox is up.
		return lifecycleRunning
	case "suspended_warm", "suspended_cold":
		return lifecycleStopped
	case "failed", "dead":
		return lifecycleFailed
	case "canceled":
		return lifecycleCanceled
	case "destroyed":
		return lifecycleDeleted
	default:
		return lifecycleUnknown
	}
}

// processOf reports the child process. The exit code is the only evidence the
// API gives: present means the child ran and finished, absent means there is
// one running or there is not one yet, and the session's own state is what
// separates those two.
func processOf(s session) string {
	if s.ChildExitCode != nil {
		return processExited
	}
	switch s.State {
	case "running":
		return processRunning
	case "suspended_warm":
		return "paused"
	default:
		return processNone
	}
}

func connectionOf(s session) string {
	if s.Reachable {
		return connectionAvailable
	}
	return connectionUnavailable
}

// displayLifecycle is the lifecycle as a person reads it. An unknown state
// carries the server's own word through rather than hiding it: somebody
// looking at a state this build does not know needs to be able to quote it.
func displayLifecycle(s session) string {
	life := lifecycleOf(s)
	if life == lifecycleUnknown {
		return "Unknown (" + safeField(s.State) + ")"
	}
	return title(life)
}

// displayProcess renders the process column, naming the exit status when
// there is one — that number is the whole reason a person looks.
func displayProcess(s session) string {
	switch processOf(s) {
	case processExited:
		return fmt.Sprintf("Exited (%d)", *s.ChildExitCode)
	case processRunning:
		return "Running"
	case "paused":
		return "Paused"
	default:
		return "-"
	}
}

func displayConnection(s session) string { return title(connectionOf(s)) }

// title capitalizes a display key for human output. It shares
// capitalizeFirst with the agent labels so there is one rune-safe
// implementation rather than two byte-slicing ones.
func title(word string) string { return capitalizeFirst(word) }

// ---------------------------------------------------------------------------
// action eligibility, from the raw API's own transition rules
// ---------------------------------------------------------------------------

// Eligibility is what the CLI can say about one action. It is three-valued
// because a state this build does not know admits no honest yes or no: the
// CLI reports `unknown`, attempts nothing on its own judgement, and lets the
// server be the authority.
const (
	eligibleYes     = "yes"
	eligibleNo      = "no"
	eligibleUnknown = "unknown"
)

// canAttach reports whether `rainier attach` has a path to a terminal.
//
// It is wider than the attach endpoint alone, and deliberately so: the CLI
// composes resume-then-attach for a stopped session and waits out a starting
// one, so those are things attach really can do. Each entry corresponds to a
// real API path:
//
//   - running: AttachTerminal accepts it outright.
//   - queued, creating: not attachable yet; attach waits, exactly as `new`
//     does after a create.
//   - suspended_warm, suspended_cold: ResumeSession accepts these two and
//     nothing else, then the attach follows.
//   - failed: AttachTerminal admits a failed session ONLY while it still has
//     a connected runner (controlapp/attachments.go attachable). Reachability
//     is therefore part of the answer here — the one place the connection
//     fact legitimately bears on an action, because the endpoint itself says
//     so.
//
// Everything else is refused by the endpoint.
func canAttach(s session) string {
	switch s.State {
	case "running", "queued", "creating", "suspended_warm", "suspended_cold":
		return eligibleYes
	case "failed":
		if s.Reachable {
			return eligibleYes
		}
		return eligibleNo
	case "dead", "canceled", "destroyed":
		return eligibleNo
	default:
		return eligibleUnknown
	}
}

// canStop reports whether `rainier stop` can run.
//
// SuspendSession accepts control.StateRunning and NOTHING else — a queued or
// creating session is refused with a conflict. That is why this reads the raw
// state and not the lifecycle word: `queued` and `creating` display as
// Starting alongside nothing else, and claiming Stop for them because they
// share a label would promise an operation the server rejects.
//
// A running session whose child has exited is still stoppable, because the
// server's rule is about the session and not about the process inside it.
func canStop(s session) string {
	switch s.State {
	case "running":
		return eligibleYes
	case "queued", "creating", "suspended_warm", "suspended_cold",
		"failed", "dead", "canceled", "destroyed":
		return eligibleNo
	default:
		return eligibleUnknown
	}
}

// canDelete mirrors DeleteSession's own state machine: a creating session is
// refused with a conflict because there is nothing to destroy yet and a
// dispatch may already be in flight; an already-destroyed row has nothing
// left; a failed session especially can be deleted, because it may still own
// a live container and deleting it is the cleanup.
func canDelete(s session) string {
	switch s.State {
	case "running", "queued", "suspended_warm", "suspended_cold",
		"failed", "dead", "canceled":
		return eligibleYes
	case "creating":
		return eligibleNo
	case "destroyed":
		return eligibleNo
	default:
		return eligibleUnknown
	}
}

// activeSession reports whether a session belongs in the default list: the
// ones a developer can still act on. Terminal records — canceled, destroyed —
// are history and appear under --all.
func activeSession(s session) bool {
	switch s.State {
	case "canceled", "destroyed":
		return false
	default:
		return true
	}
}

// diagnosticDetail is the extra sentence a state does not explain by itself:
// why a queued session is still queued. The exit code is NOT folded in here
// any more — it has a column of its own, which is the entire point.
func diagnosticDetail(s session) string {
	if s.QueueReason != "" {
		return s.QueueReason
	}
	return ""
}

// exitCodeText renders a nullable exit code for a place that has room for one
// value only.
func exitCodeText(s session) string {
	if s.ChildExitCode == nil {
		return "-"
	}
	return strconv.Itoa(*s.ChildExitCode)
}

// ---------------------------------------------------------------------------
// the `current` selector (contract §3.2)
// ---------------------------------------------------------------------------

// currentSelector is the literal word a session command accepts in place of
// an id or a name.
const currentSelector = "current"

var errNoCurrentSession = errors.New(
	"no current session in this context yet; `rainier new` and `rainier attach` set it. Run `rainier ls` to see what exists")

// rememberCurrentSession records id as the context's current session. It is
// called after a successful create and after a successful attach, and its
// failures are reported to the caller rather than swallowed: a `current` that
// silently did not move is worse than one that says it could not be written,
// because the next command would then act on the previous session.
func rememberCurrentSession(id string, original ...cli.Config) error {
	if id == "" {
		return nil
	}
	return cli.UpdateConfig(func(latest *cli.Config) error {
		name := latest.ActiveName()
		if len(original) > 0 {
			name = original[0].ActiveName()
		}
		ctx, ok := latest.Contexts[name]
		if !ok {
			return nil // nothing to attach the memory to; the command itself still succeeded
		}
		if len(original) > 0 {
			before, ok := original[0].Active()
			if !ok || ctx.Server != before.Server || ctx.Workspace != before.Workspace || ctx.OwnerID != before.OwnerID {
				return errors.New("session context changed")
			}
		}
		ctx.CurrentSession = id
		latest.UpdateContext(name, ctx)
		return nil
	})
}

// currentSessionID reads the context's remembered session id.
func currentSessionID(cfg cli.Config) (string, error) {
	ctx, ok := cfg.Active()
	if !ok || ctx.CurrentSession == "" {
		return "", errNoCurrentSession
	}
	return ctx.CurrentSession, nil
}

// staleCurrentSession dresses a not-found on the remembered id as what it
// actually is. "no session sess_abc123" after typing `current` reads as a
// bug; "the session `current` pointed at is gone" is the truth and says what
// to do next.
func staleCurrentSession(id string, err error) error {
	var apiErr *cli.APIError
	if errors.As(err, &apiErr) && apiErr.Status == 404 {
		return fmt.Errorf("the session `current` pointed at (%s) no longer exists; run `rainier ls`, then name one", id)
	}
	return err
}
