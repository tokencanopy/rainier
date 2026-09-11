package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// version is optionally supplied by a release build with -ldflags '-X main.version=...'.
// Source builds must never impersonate a published release.
var version string

// scripts/build.sh supplies these from the source worktree. Go's automatic
// VCS discovery can instead identify an enclosing checkout for nested worktrees.
var sourceRevision, sourceDirty string

func buildVersion() string {
	if version != "" {
		return version
	}
	if sourceRevision != "" {
		revision := sourceRevision
		if len(revision) > 12 {
			revision = revision[:12]
		}
		if sourceDirty == "true" {
			revision += ", modified"
		}
		return "dev (" + revision + ")"
	}
	return "dev"
}

// printUsage is the default help, and the constraint on it is a product one
// rather than a stylistic one: a new developer has to be able to read the
// whole primary interface off one screen (docs/cli-v0-contract.md §1). It
// fits in 24 lines, it names only commands a hosted developer needs, and it
// shows the happy path in the order it is walked.
//
// Everything else this CLI can do still dispatches; it lives under
// `rainier help all`, where somebody looking for it will find it and nobody
// meeting rainier for the first time has to wade through it.
//
// The writer is a parameter because the same text is two different things:
// help somebody asked for is output and belongs on stdout, and help printed
// because an invocation was wrong is a diagnostic and belongs on stderr
// (contract §6.1).
func printUsage(w io.Writer) {
	fmt.Fprintln(w, `usage: rainier <command> [flags]

Account:
  login             Sign in to your Rainier workspace
  logout            Remove this machine's credentials for it
  status            Are you ready to code?  [--verbose] [--json]

Sessions:
  new               Create a session and attach to it
                    [--name N] [--agent claude|codex] [--detach] [-- CMD ...]
  ls                List your sessions  [--all] [--verbose] [--json]
  info <session>    Everything about one session  [--json]
  attach <session>  Reopen its terminal; Ctrl-] detaches and leaves it running,
                    Ctrl-\ takes control when another device has it
  stop <session>    Stop it, keeping its files; billing is unchanged
  delete <session>  Destroy it permanently  [--yes]

Coding agents:
  agent login <claude|codex>     Sign the agent in, once, for every session
  agent status [--json]          What is signed in
  agent logout <claude|codex>    Sign it out everywhere  [--yes]

Also: rainier help [command] | rainier help all | rainier version
<session> is a session name, a sess_ id, or the word "current".`)
}

// Help is intercepted before command handlers so even nested help is read-only.
// Stop at --: a session command such as `new -- echo --help` belongs to the child.
func handleHelpVersion(command string, args []string) bool {
	if command == "version" || command == "--version" {
		if len(args) != 0 {
			fmt.Fprintln(os.Stderr, "usage: rainier version")
			os.Exit(2)
		}
		fmt.Println("rainier " + buildVersion())
		return true
	}
	if command == "help" || command == "--help" || command == "-h" {
		if len(args) == 0 {
			printUsage(os.Stdout)
			return true
		}
		if !printCommandHelp(args[0]) {
			fmt.Fprintln(os.Stderr, "unknown help command; run rainier help")
			os.Exit(2)
		}
		return true
	}
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if arg == "--help" || arg == "-h" {
			if !printCommandHelp(command) {
				fmt.Fprintln(os.Stderr, "unknown help command; run rainier help")
				os.Exit(2)
			}
			return true
		}
	}
	return false
}

// printCommandHelp prints one command's detail. It answers for the public
// surface, for every compatibility alias, and for every Advanced command,
// because `rainier <anything> --help` has to work whether or not the command
// is one the default help advertises.
func printCommandHelp(command string) bool {
	var help string
	switch command {
	case "all":
		printManual()
		return true

	// --- the public surface -------------------------------------------------
	case "login":
		help = `usage: rainier login
       rainier login --cloud EDGE_URL [--device-name NAME] [--context NAME]
       rainier login [--from-gh | --token TOKEN | --client-id ID]
                     [--server URL] [--refresh github] [--context NAME]

With no arguments, login signs in again to the server you are already using —
it is always safe to rerun — or, on a machine with no context yet, to this
build's hosted default when it has one. Hosted login opens a browser.

Signing in proves who you are. It does not make a workspace ready: when
compute still needs a plan, a payment or provisioning, login says so and
prints the web address to continue at. Run rainier status afterwards.

The remaining forms are self-hosted (rainier help all): --cloud names a
hosted edge explicitly, and the GitHub flags log in to a self-hosted controld,
whose vault also holds the credential your sessions use for git.`
	case "logout":
		help = `usage: rainier logout [--context NAME]

Remove this machine's access and refresh credentials for the current server.
Nothing else changes: your remote sessions keep running, your Claude and
Codex logins stay in the workspace, and your GitHub connection stays
authorized. Rerunning it is not an error. Sign back in with rainier login.`
	case "status":
		help = `usage: rainier status [--verbose] [--json]

Seven lines saying whether you can start coding: signed in, workspace,
compute, default environment, GitHub, and each coding agent. Every line comes
from the server, never from this machine's configuration — a stored token is
not a ready workspace.

When something needs doing on the web, status prints the address the server
named. Exit 0 when everything required is ready, 1 when it is not.
--verbose adds the full readiness diagnostics; --json prints one document.`
	case "new":
		help = `usage: rainier new [--name N] [--agent claude|codex] [--detach]
                   [--env ENV] [--image IMG] [--egress host,host]
                   [-- CMD ARGS...]

Create a session and attach to its terminal; Ctrl-] detaches without stopping
it. With no --env the session starts from your workspace's default
environment.

A session runs a shell, a coding agent, or any command you name after "--".
--agent starts the named agent; it cannot be combined with an explicit
command, since both answer the same question. Git inside the session is the
source of truth for your repositories.

--detach creates without attaching; use rainier attach <session> later.
--env, --image and --egress are advanced (rainier help all).`
	case "ls":
		help = `usage: rainier ls [--all] [--verbose] [--json]

Your sessions, across the three things that can be true of one at once:

  STATE       the sandbox: Starting, Running, Stopped, Failed
  PROCESS     what you told it to run: Running, or Exited (code)
  CONNECTION  whether it can be reached right now: Available, Unavailable

They are separate because they fail separately. A session whose agent exited
is still Running — it holds its filesystem and you can still attach to it. A
session you cannot reach is Unavailable, not Failed; the work is still there
and the runner may come back.

By default you see the sessions you can still act on. --all adds cancelled and
deleted history. --verbose adds the id, environment, runner, any queue reason,
and the server's own API state.`
	case "info":
		help = `usage: rainier info <session> [--json]

One session in full: its session state, its process, its connection, the
server's own API state beside them, when it was created and last changed, its
environment, and whether it can be attached, stopped or deleted right now.
Those three answers come from the API's own transition rules, so "Stop: no"
means the server would refuse it, not that a word looked wrong.

A failed session's reason is included, sanitized. --json carries the raw API
facts — state, reachable, child_exit_code — beside the derived ones.`
	case "attach":
		help = `usage: rainier attach <session> [--since N] [--view | --take]

Open the session's current terminal screen. Ctrl-] detaches and leaves the
session running. A stopped session is resumed first and waited for.

One device at a time may type; everyone else watches. Attaching takes
control when nobody has it, which is every single-device attach, and
otherwise attaches as a viewer and says so. Ctrl-\ takes control, and tells
the device that had it. Ctrl-] releases control as it detaches, so the next
attach needs no key at all.

  --view    watch without ever claiming control; Ctrl-\ does nothing
  --take    take control on attach, even if another device has it

Reconnecting after a disconnect resumes control only if nobody took it in
the meantime; if somebody did, it comes back as a viewer and says so.

A running sandbox stays attachable after its child exits. A failed session
is attachable while its runner is reachable. --since requests diagnostic
replay: --since 0 replays the whole event log — a failed
setup's complete output — and --since N resumes after sequence N.

Transient disconnects and hosted lease renewals reconnect from the last
rendered sequence. Hosted credentials refresh automatically.`
	case "stop":
		help = `usage: rainier stop <session> [--json]

Stop a running session. Its filesystem is persisted, so rainier attach brings
it back where you left it, and the session resources it was holding — the
runner slot, its memory — are released for your other sessions to use.

This does not change what you pay. Your Dedicated subscription stays active
and continues to bill monthly whether your sessions are running or stopped;
stopping frees capacity inside the compute you already have. Cancelling the
subscription is a separate action, on the web.

Only a running session can be stopped. Stopping an already-stopped one is not
an error. If the stop or its persistence fails, stop says so and does not
report success — your session's state is never discarded quietly.

To destroy a session for good, use rainier delete.`
	case "delete":
		help = `usage: rainier delete <session> [--yes] [--json]

Destroy the session permanently. Its container, its terminal, and any work
you have not pushed are gone and cannot be recovered.

On a terminal it asks first. In a script it requires --yes and refuses
otherwise rather than waiting for an answer that cannot arrive. A session
that is already gone is reported as such, not as a failure.`
	case "agent":
		help = agentUsage
	case "version":
		help = "usage: rainier version\n\nPrint this CLI's build version."
	case "help":
		help = "usage: rainier help [command]\n\nrainier help lists the primary commands; rainier help <command> details one;\nrainier help all is the full manual, including advanced and self-hosted use."

	// --- compatibility aliases (contract §2.1) ------------------------------
	case "doctor":
		help = `usage: rainier doctor

Compatibility alias for rainier status --verbose. Prefer rainier status.`
	case "suspend":
		help = `usage: rainier suspend <session>

Compatibility alias for rainier stop. Prefer rainier stop. The warm/cold
distinction is gone from the interface: stop always persists the session and
releases its capacity, which is what --cold used to mean.`
	case "rm":
		help = `usage: rainier rm <session>

Compatibility alias for rainier delete, kept with its original behavior: it
destroys the session without prompting and needs no --yes, because scripts
call it that way. Prefer rainier delete, which asks first.`

	// --- advanced (contract §2.2) -------------------------------------------
	case "env":
		help = envUsage
	case "secret":
		help = secretUsage
	case "context":
		help = `usage: rainier context list | use <name> | current | remove <name>

Advanced. A context is one server and its credentials. Every command uses the
current context. list shows saved contexts; use switches; current prints the
selection; remove deletes local credentials for that context. Login creates a
context. Hosted v0 needs exactly one, and login manages it for you.`
	case "workspace":
		help = `usage: rainier workspace use <id>

Advanced. Select a workspace within the current hosted context. A hosted login
chooses automatically when there is one workspace, which is the hosted v0
case. This selection scopes subsequent requests; it does not create a
workspace.`
	case "resume":
		help = `usage: rainier resume <session> [--json]

Advanced. Resume a stopped session without attaching, for automation that
wants the two halves separately. Interactive users run rainier attach, which
resumes a stopped session on its own.`
	case "snapshot":
		help = `usage: rainier snapshot <session>

Advanced. Create a checkpoint of the session and print its reference.`
	case "push", "pull":
		help = "usage: rainier push <local-dir> <session>:<path>"
		if command == "pull" {
			help = "usage: rainier pull <session>:<path> <local-dir>"
		}
		help += "\n\nAdvanced. Transfer a directory once, bounded to 256 MiB. Remote paths stay\ninside /workspace; neither direction follows symlinks outside the tree being\nmoved."
	case "creds":
		help = `usage: rainier creds

Advanced, self-hosted only. Show the server vault's GitHub credential:
provider, status, scopes and verification/use times. If it says needs_refresh
or lacks repo scope, run rainier login --refresh github. A hosted rainier has
no vault — GitHub is a connection you authorize in the browser, and
rainier status reports it.`
	case "connection":
		help = connectionUsage + "\n\nProviders: " + strings.Join(connectionProviders, ", ") + "."
	default:
		return false
	}
	// Help somebody asked for is output, not a diagnostic (contract §6.1).
	fmt.Fprintln(os.Stdout, help)
	return true
}
