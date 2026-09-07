package main

import (
	"fmt"
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

func printUsage() {
	fmt.Fprintln(os.Stderr, `usage: rainier <command> [flags]

Start and reconnect:
  login        Sign in to your server or hosted workspace
  doctor       Check basic session and coding-agent readiness
  new          Create a session and attach
  ls           List sessions
  attach       Reattach; resume a suspended session

Manage sessions:
  suspend, resume, snapshot, rm
  diff         Show repository changes
  push, pull   Transfer a directory to or from a session

Configure:
  env          Manage reusable session environments
  agent        Log coding agents in or out; inspect status
  secret       Manage write-only environment secrets
  creds        Inspect GitHub credential status (self-hosted)
  connection   Inspect a hosted GitHub connection; share it with a workspace
  context, workspace   Select your server and workspace
  version      Show CLI build version

Examples: rainier new --env dev --name box1
          rainier attach box1
Help: rainier help <command> | rainier <command> --help
Full guide: rainier help all`)
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
			printUsage()
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

func printCommandHelp(command string) bool {
	var help string
	switch command {
	case "all":
		printManual()
		return true
	case "login":
		help = `usage: rainier login [--from-gh | --token TOKEN | --client-id ID]
                     [--server URL] [--refresh github] [--context NAME]
       rainier login --cloud EDGE_URL [--device-name NAME] [--context NAME]

Use the URL supplied by your administrator. Hosted login opens a browser;
self-hosted login uses GitHub. --from-gh borrows your gh CLI token.
GitHub credentials are vaulted for session git access; repo scope is needed.
Use rainier creds to inspect status and login --refresh github to refresh it.
Contexts store one server and its credentials. After login, run rainier doctor.`
	case "new":
		help = `usage: rainier new [--name N] [--env ENV] [--image IMG]
                   [--egress host,host] [--detach] [--idempotency-key KEY]
                   [-- CMD ARGS...]

Create a session and attach to its terminal; Ctrl-] detaches without stopping it.
--env accepts an environment name or id and inherits its image, setup, egress,
and secrets. --image and --egress override it for this session only.
Prerequisites: login and a connected runner with capacity (rainier doctor).
An environment image must already include the coding agent you want to run.
--detach creates without attaching; use rainier attach <id|name> later.`
	case "attach":
		help = `usage: rainier attach <id|name> [--since N]

Open the current terminal screen; resume a suspended session when necessary.
Ctrl-] detaches and keeps the session. Transient established-stream disconnects
and hosted lease renewals reconnect from the last rendered sequence. Hosted
credentials refresh automatically; revoked access still requires login.
--since 0 replays the full event log;
--since N resumes after N. Run rainier doctor if the session keeps waiting.
Use a sess_ id directly, or a session name. Ambiguous names require an id.`
	case "doctor":
		help = `usage: rainier doctor

Read-only readiness report, bounded to about 15 seconds including token refresh.
Checks active config, server authentication, workspace, runners, environments,
and coding-agent status using existing API routes. Normal token refresh may save
rotated credentials. No provisioning or automatic environment/agent setup.
Exit 0: basic session prerequisites pass; 1: a required check failed.
Environment or agent warnings mean coding-agent readiness is incomplete;
basic sessions may still work. Backend version is unknown unless advertised.`
	case "env":
		help = envUsage
	case "agent":
		help = agentUsage + "\n\nProviders: " + strings.Join(agentProviderNames(), ", ") + "."
	case "secret":
		help = secretUsage
	case "context":
		help = `usage: rainier context list | use <name> | current | remove <name>

A context is one server and its credentials. Every command uses the current
context. list shows saved contexts; use switches; current prints the selection;
remove deletes local credentials for that context. Login creates a context.`
	case "workspace":
		help = `usage: rainier workspace use <id>

Select a workspace within the current hosted context. A hosted login chooses
automatically when there is one workspace; otherwise it lists available ids.
This selection scopes subsequent requests; it does not create a workspace.`
	case "push", "pull":
		help = "usage: rainier push <local-dir> <id|name>:<path>"
		if command == "pull" {
			help = "usage: rainier pull <id|name>:<path> <local-dir>"
		}
		help += "\n\nTransfer a directory once, bounded to 256 MiB. Remote paths stay inside\n/workspace; neither direction follows symlinks outside the tree being moved."
	case "ls":
		help = "usage: rainier ls [--all]\n\nList sessions; --all includes terminal states. Shows runner and queue state."
	case "suspend":
		help = "usage: rainier suspend <id|name> [--cold]\n\nSuspend a session (warm by default); --cold snapshots it to release memory."
	case "resume":
		help = "usage: rainier resume <id|name>\n\nResume a suspended session. Use attach to open its terminal afterwards."
	case "snapshot":
		help = "usage: rainier snapshot <id|name>\n\nCreate a checkpoint of the session and print its reference."
	case "rm":
		help = "usage: rainier rm <id|name>\n\nDestroy the session. Detach with Ctrl-] to keep a session instead."
	case "diff":
		help = "usage: rainier diff <id|name>\n\nShow git --stat for each cloned repository against its original base branch."
	case "creds":
		help = "usage: rainier creds\n\nShow GitHub credential provider, status, scopes and verification/use times.\nIf needs_refresh or repo scope is missing, run rainier login --refresh github.\nThis reads a self-hosted server's vault. On a hosted rainier, GitHub is a\nconnection you authorize in the browser instead: see rainier help connection."
	case "connection":
		help = connectionUsage + "\n\nProviders: " + strings.Join(connectionProviders, ", ") + "."
	default:
		return false
	}
	fmt.Fprintln(os.Stderr, help)
	return true
}
