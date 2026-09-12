// Command rainier is the client CLI for controld and for Rainier Cloud.
//
// Its public surface is small on purpose and is specified in
// docs/cli-v0-contract.md: sign in, check readiness, authenticate a coding
// agent, and create and manage sessions. Everything else — self-hosted login,
// environments, secrets, contexts, transfers, snapshots, administration —
// still dispatches, but lives under `rainier help all` so that a first-time
// reader sees the product and not the toolbox.
//
// Subcommand dispatch follows runnerctl's style: stdlib `flag` per
// subcommand, no cobra.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/internal/attachio"
	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// devicePollTimeout bounds a single poll request to GitHub's device-flow
// token endpoint — without it, a stalled or blackholed request could park
// the whole `login --client-id` flow indefinitely instead of just failing
// that one poll and trying again on the next interval tick.
const devicePollTimeout = 15 * time.Second

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stderr)
		os.Exit(2)
	}
	cmd, rest := os.Args[1], os.Args[2:]
	if handleHelpVersion(cmd, rest) {
		return
	}

	var err error
	switch cmd {
	// --- the public surface (docs/cli-v0-contract.md §2) ---
	case "login":
		err = runLogin(rest)
	case "logout":
		err = runLogout(rest)
	case "status":
		err = runStatus(rest)
	case "new":
		err = runNew(rest)
	case "ls":
		err = runLs(rest)
	case "info":
		err = runInfo(rest)
	case "attach":
		err = runAttach(rest)
	case "stop":
		err = runStop(rest)
	case "delete":
		err = runDelete(rest)
	case "agent":
		err = runAgent(rest)

	// --- compatibility aliases (§2.1): hidden, temporary, and each one a
	// thin call into the command that replaced it, so they cannot drift ---
	case "doctor":
		err = runDoctor(rest)
	case "suspend":
		err = runSuspend(rest)
	case "rm":
		err = runRm(rest)

	// --- advanced (§2.2): dispatched, documented under `rainier help all` ---
	case "resume":
		err = runResume(rest)
	case "snapshot":
		err = runSnapshot(rest)
	case "push":
		err = runPush(rest)
	case "pull":
		err = runPull(rest)
	case "creds":
		err = runCreds(rest)
	case "connection":
		err = runConnection(rest)
	case "secret":
		err = runSecret(rest)
	case "env":
		err = runEnv(rest)
	case "context":
		err = runContext(rest)
	case "workspace":
		err = runWorkspace(rest)
	case "exec":
		err = runExec(rest)

	default:
		// help, --help, -h and version never reach here: handleHelpVersion
		// above answers all of them, so a second arm for them would be an
		// unreachable copy that the next person edits by mistake.
		fmt.Fprintf(os.Stderr, "rainier: unknown command %q\n", cmd)
		printUsage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		// One exit for every command: redacted on stderr, with the server's
		// stable code and request id when it has them, and 2 rather than 1
		// when the invocation itself was wrong (contract §6.1, §6.3).
		//
		// The one error that prints NOTHING is a command `rainier exec` ran
		// that exited non-zero on its own: it has already said whatever it
		// had to say on its own streams, and a line of Rainier's would
		// corrupt the output of `rainier exec s -- cat f`.
		cfg, _ := cli.Load()
		if !silentError(err) {
			reportError(cfg, os.Stderr, err)
		}
		os.Exit(exitCodeFor(err))
	}
}

// printManual is `rainier help all`: the reference for everything the default
// help leaves out. Its job is to be complete, not short, and its structure is
// the one thing that keeps it from leaking back into the first-run
// experience — everything past the primary surface is under an Advanced
// heading, so a reader always knows whether what they are looking at is part
// of the product they bought or part of the toolbox underneath it.
func printManual() {
	fmt.Fprintln(os.Stdout, `rainier — the full command reference

PRIMARY COMMANDS
  login                      sign in (rerunnable; no arguments needed)
  logout                     remove this machine's credentials
  status [--verbose] [--json]   is this workspace ready to code?
  new [--name N] [--agent claude|codex] [--detach] [-- CMD ARGS...]
  ls [--all] [--verbose] [--json]
  info <session> [--json]
  attach <session> [--since N]
  stop <session> [--json]
  delete <session> [--yes] [--json]
  agent login <claude|codex> | status [--json] | logout <claude|codex> [--yes]
  help [command] | version

SESSION SELECTORS
  A <session> is one of three things:
    sess_...   an exact id, used verbatim
    a name     resolved against your sessions; a name matching more than one
               is refused, with every match's id listed, rather than guessed
    current    the session you last created or attached, in this context

  "current" remembers the ID, never the name, so a name freed by a delete and
  reused by the next new cannot silently retarget it. A real session named
  "current" is reachable only by its id (ls --verbose prints ids).

SESSION STATES
  starting     queued or booting
  running      sandbox up; the child may have exited
  stopped      suspended; attach brings it back
  failed       failed or dead; info shows the API state
  canceled / deleted   terminal records (ls --all)
  unknown      an unrecognized API state

  Process exit and runner reachability are separate facts.
  info shows recent activity as the server-reported last event timestamp.

MACHINE-READABLE OUTPUT
  --json is supported by status, ls, info, agent status, and by new --detach,
  stop, resume and delete. Every document carries "schema" and "version" fields.
  Output never changes shape because stdout is a pipe: --json is the only way
  to ask for JSON. Human output goes to stdout, diagnostics and errors to
  stderr. Exit 0 success, 1 operational or server failure, 2 bad invocation.

COMPATIBILITY ALIASES (temporary; prefer the command each names)
  doctor       = status --verbose
  suspend      = stop
  rm           = delete, keeping its original no-prompt behavior for scripts
  agent ls     = agent status

ADVANCED — self-hosted login
  login --cloud EDGE_URL [--device-name NAME] [--context NAME]
  login [--from-gh | --token GH_TOKEN | --client-id ID] [--server URL]
        [--refresh github] [--context NAME]

  --cloud names a hosted edge explicitly and runs the browser login. The
  GitHub forms log in to a self-hosted controld and store your GitHub token in
  that server's credential vault, so its sessions can clone, pull and push as
  you. "creds" shows what is stored — provider, status, scopes, last verified
  and used. A status of needs_refresh means git saw that token rejected: run

    rainier login --refresh github

  to log in again with a fresh token and clear it. Use the same command when
  "creds" shows scopes without "repo": such a token can prove who you are but
  cannot do git.

ADVANCED — hosted GitHub connection
  connection ls | share <provider> [--workspace ID]
             | unshare <provider> [--workspace ID] | reconnect <provider>

  On a hosted rainier there is no vault: you connect your GitHub account in
  the browser and the cell brokers a credential into each session. A new
  connection reaches no workspace, so "connection share github" is what lets
  your workspace use it and "connection unshare github" removes it.
  "connection reconnect github" replaces the browser authorization and
  restores the previous access mode and workspace selection.

  The API replaces the whole workspace list rather than adding to it, and
  offers no conditional write, so editing one connection from two places at
  once can lose an edit. Neither command ever prints a credential: the CLI
  never has one to print.

  In hosted v0 these are web actions. rainier status reports GitHub readiness
  and the address to continue at; these commands remain for the flows that
  still depend on them.

ADVANCED — contexts and workspaces
  context list | use <name> | current | remove <name>
  workspace use <id>

  A context is one server and the credentials for it, so one config can hold
  a self-hosted controld and any number of hosted edges. Every command talks
  to whichever is current. A hosted context is scoped to one workspace: a
  login with a single workspace picks it, which is the hosted v0 case.

ADVANCED — environments and secrets
  env create <name> [flags] | ls | show <ref> | update <ref> [flags] | rm <ref>
  secret set <NAME> [--value V] | ls | rm <NAME>

  An environment is an image, a setup script, an egress allowlist, secrets and
  connectors, reused by every session started from it. "new --env NAME" starts
  from one; --image and --egress override it for that one session. In hosted
  v0 the workspace's default environment is maintained for you and "new" needs
  no --env at all.

  "secret set" reads the value from stdin when --value is omitted, so it never
  lands in your shell history:  cat token.txt | rainier secret set GH_TOKEN
  Values are write-only: the API never gives one back, and "secret ls" shows
  names and timestamps only.

ADVANCED — running a command in a session
  exec <session> [--tty] [--cwd DIR] [--env K=V]... [--json] -- CMD [ARGS...]
  exec <session> --detach --log PATH -- CMD [ARGS...]

  Runs one command inside a live session's sandbox, as the session's user,
  and exits with the command's exit status. There is no shell: argv is
  exec'd directly, so name one if you want one (-- sh -c 'cd src && make').
  --tty allocates a terminal and MERGES stdout with stderr, because a pty
  has one stream. --detach leaves the command running after this CLI exits,
  prints its pid and exits 0; stop it the way you stop any process —
  exec <session> -- kill <pid> — and it dies with its session.

  An exec does not take the terminal's controller lease and its output never
  reaches the session's scrollback: attach --since 0 never replays it.

ADVANCED — transfer and snapshots
  push <local-dir> <session>:<path>
  pull <session>:<path> <local-dir>
  snapshot <session>
  resume <session>

  push and pull move a directory between your machine and a session's
  workspace: one-shot, bounded to 256 MiB, always inside /workspace, and
  never following a symlink out of the tree being moved. snapshot checkpoints
  a session and prints its reference. resume is stop's counterpart for
  automation; interactive users just attach.

ADVANCED — diagnostics
  attach --since N   0 replays the whole event log — a failed setup's full
                     output, or a day of scrollback — and N resumes after
                     sequence N, the number a disconnect line prints. It also
                     requests diagnostic replay even for terminal lifecycle states.
  status --verbose   the full readiness report: config, authentication,
                     workspace, runners, environments and agents.
  ls --verbose       ids, environments, runners, reachability, and the
                     server's own state for each session.

REPOSITORIES
  Git inside the session is the source of truth for repository and worktree
  state. Rainier has no repository-diff command: run git in the session.`)
}

// ---------------------------------------------------------------------------
// wire shapes (mirrors internal/controld/api.go's client-facing JSON —
// cmd/rainier only decodes the fields it displays or acts on)
// ---------------------------------------------------------------------------

type session struct {
	ID          string   `json:"id"`
	OwnerID     string   `json:"owner_id"`
	Name        string   `json:"name"`
	Image       string   `json:"image"`
	Cmd         []string `json:"cmd"`
	EgressAllow []string `json:"egress_allow"`
	State       string   `json:"state"`
	Runner      string   `json:"runner"`
	Reachable   bool     `json:"reachable"`
	Error       string   `json:"error"`
	// Environment is the name of the environment this session came from, ""
	// for a scratch session. QueueReason, when set, is why a queued session is
	// still queued — controld derives it per request, so it is never stale.
	Environment string `json:"environment"`
	QueueReason string `json:"queue_reason"`
	// ChildExitCode is the exit status of the session's agent process, null
	// until it has one. A pointer because exit 0 is an answer: a session whose
	// agent finished cleanly must not render the same as one still working.
	ChildExitCode *int   `json:"child_exit_code"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
	LastEventAt   string `json:"last_event_at"`
	// Controller is who may type: the generation in force and whether
	// anybody currently holds it. It names nobody — the API does not
	// disclose another client's identity — so "this device" is something the
	// CLI works out from the generation it was last granted, not something
	// the server tells it. Absent from an older server, which reads as
	// "nobody has control", and is the truth there: nothing was ever
	// conditional.
	Controller controllerView `json:"controller"`
	// Input is the RULE the server decides who may type by, and how many
	// terminals currently may. It is where this CLI learns the attachment
	// policy: the terminal protocol carries none, and an attach reads what it
	// is told about itself.
	Input inputView `json:"input"`
}

// controllerView is the session view's additive controller object. The
// generation is a decimal STRING on the wire, so it is one here: a uint64
// past 2^53 would be silently wrong as a JSON number.
type controllerView struct {
	Generation string `json:"generation"`
	Held       bool   `json:"held"`
}

// inputView is the session view's additive input object: the server's
// attachment policy and how many attached terminals may type under it.
//
// An absent or unrecognised policy is EXCLUSIVE as far as this CLI is
// concerned, which is what an older server means by omitting the object and
// what every build of this CLI has always assumed. A word this build does not
// know is treated the same way, because the copy that assumes exclusivity is
// the copy that offers a key — and offering a key that does nothing is a
// smaller wrong than withholding the one that works.
type inputView struct {
	Policy   string `json:"policy"`
	Attached int    `json:"attached"`
}

// sharedInput reports whether the server told us every attached terminal may
// type.
func (v inputView) sharedInput() bool { return v.Policy == "shared" }

type sessionEnvelope struct {
	Session session `json:"session"`
}

type sessionsEnvelope struct {
	Sessions   []session `json:"sessions"`
	NextCursor string    `json:"next_cursor"`
}

type snapshotResponse struct {
	Ref string `json:"ref"`
}

// userView mirrors controld's own: the caller's identity, as returned by
// both POST /v0/auth/github and GET /v0/me. ID is the caller's own user id —
// the same string their sessions carry as owner_id, which is what makes
// owner-preference possible (see resolveSessionID).
type userView struct {
	ID    string `json:"id"`
	Login string `json:"login"`
	Role  string `json:"role"`
}

type authResponse struct {
	Token string   `json:"token"`
	User  userView `json:"user"`
	// Scopes is what GitHub reported the token can do; Warning is set when
	// something about it will bite later (v0: no `repo` scope). Neither
	// carries the token itself — this API never gives one back.
	Scopes  string `json:"scopes"`
	Warning string `json:"warning"`
}

// credential mirrors one element of GET /v0/credentials. As with `secret`,
// there is no value field here for the same reason there is none
// server-side: the API never returns one.
type credential struct {
	Provider       string `json:"provider"`
	Status         string `json:"status"`
	Scopes         string `json:"scopes"`
	ObtainedAt     string `json:"obtained_at"`
	LastVerifiedAt string `json:"last_verified_at"`
	LastUsedAt     string `json:"last_used_at"`
}

type credentialsEnvelope struct {
	Credentials []credential `json:"credentials"`
}

type createSessionRequest struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
	Environment string   `json:"environment,omitempty"`
	// Repos overrides the repositories the environment's connectors declare.
	// A POINTER to the slice, because the server draws a distinction a plain
	// `[]repoRequest` with omitempty cannot express: absent inherits the
	// environment's repositories, an explicit empty array clones nothing.
	// `agent login` is the one caller that needs the second — a login session
	// has no business holding anybody's source.
	Repos *[]repoRequest `json:"repos,omitempty"`
}

// repoRequest is one entry of that array, mirroring v0wire.RepoRequest.
type repoRequest struct {
	Repo       string  `json:"repo"`
	BaseBranch *string `json:"base_branch,omitempty"`
}

// agent mirrors one element of GET /v0/agents: which coding agent, whether
// this person has logged it in, when custody last saw it move, and the
// workspaces the login reaches. Like `credential` and `secret` there is no
// field for a value, because the API has none either — a listing that could
// carry a credential is the thing this whole feature exists to avoid.
//
// Since is a string rather than a time: the server renders it null for a
// provider nobody has logged in, and a JSON null decodes into a string as the
// empty one, which is exactly what the table renders as "-".
type agent struct {
	Provider   string   `json:"provider"`
	Status     string   `json:"status"`
	Since      string   `json:"since"`
	Version    uint64   `json:"version"`
	Workspaces []string `json:"workspaces"`
}

type agentsEnvelope struct {
	Agents []agent `json:"agents"`
}

type suspendRequest struct {
	Warm *bool `json:"warm,omitempty"`
}

// secret mirrors one element of GET /v0/secrets. There is no value field
// here for the same reason there is none server-side: the API never returns
// one.
type secret struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type secretsEnvelope struct {
	Secrets []secret `json:"secrets"`
}

type putSecretRequest struct {
	Value string `json:"value"`
}

// environment mirrors controld's environment view. Connectors are kept as
// raw JSON on purpose: this CLI passes an operator's connector objects
// through untouched in both directions, so a key the server would reject
// never becomes a key the CLI silently drops. (The server preserves the JSON
// value, not necessarily the byte sequence — Postgres jsonb re-renders
// whitespace and member order.)
type environment struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Default marks the workspace's default environment. No server sends it
	// yet; decoding it here is what lets `new` and `agent login` stop
	// guessing the day one does, without this CLI ever carrying a name or an
	// image of its own (docs/cli-v0-contract.md §5.3).
	Default         bool              `json:"default"`
	Image           string            `json:"image"`
	Setup           string            `json:"setup"`
	SetupHash       string            `json:"setup_hash"`
	Init            string            `json:"init"`
	InitTimeoutSec  int               `json:"init_timeout_sec"`
	EgressAllow     []string          `json:"egress_allow"`
	SecretRefs      []string          `json:"secret_refs"`
	Connectors      []json.RawMessage `json:"connectors"`
	Placement       string            `json:"placement"`
	Capabilities    []string          `json:"capabilities"`
	SetupTimeoutSec int               `json:"setup_timeout_sec"`
	SnapshotRef     string            `json:"snapshot_ref"`
	SnapshotRunner  string            `json:"snapshot_runner"`
	SnapshotHash    string            `json:"snapshot_hash"`
	CreatedAt       string            `json:"created_at"`
	UpdatedAt       string            `json:"updated_at"`
}

type environmentEnvelope struct {
	Environment environment `json:"environment"`
}

type environmentsEnvelope struct {
	Environments []environment `json:"environments"`
}

type createEnvironmentRequest struct {
	Name            string          `json:"name"`
	Image           string          `json:"image"`
	Setup           string          `json:"setup,omitempty"`
	Init            string          `json:"init,omitempty"`
	InitTimeoutSec  int             `json:"init_timeout_sec,omitempty"`
	EgressAllow     []string        `json:"egress_allow,omitempty"`
	SecretRefs      []string        `json:"secret_refs,omitempty"`
	Connectors      json.RawMessage `json:"connectors,omitempty"`
	Placement       string          `json:"placement,omitempty"`
	Capabilities    []string        `json:"capabilities,omitempty"`
	SetupTimeoutSec int             `json:"setup_timeout_sec,omitempty"`
}

// ---------------------------------------------------------------------------
// login
// ---------------------------------------------------------------------------

// refreshableProviders is every provider `login --refresh` knows. The vault
// is keyed by (user, provider) and v0 stores GitHub only; naming an unknown
// one is refused rather than quietly refreshing GitHub, since a caller who
// typed "gitlab" did not mean "github".
var refreshableProviders = []string{"github"}

func runLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	fromGH := fs.Bool("from-gh", false, "obtain a GitHub token via `gh auth token`")
	token := fs.String("token", "", "a GitHub access token to use directly")
	clientID := fs.String("client-id", "", "GitHub OAuth App client id — runs the device flow")
	server := fs.String("server", "", "controld server URL")
	refresh := fs.String("refresh", "", "replace the stored credential for this `provider` (github) — use it when `rainier creds` says needs_refresh")
	cloud := fs.String("cloud", "", "hosted edge `URL` — runs the browser login instead of the GitHub one")
	deviceName := fs.String("device-name", "", "how this device is `named` in the hosted login attempt (default: the hostname)")
	contextName := fs.String("context", "", "config context to write (default: \"default\" for a GitHub login, the edge host for --cloud)")
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() != 0 {
		return usagef("usage: rainier login [flags]")
	}

	sources := 0
	if *fromGH {
		sources++
	}
	if *token != "" {
		sources++
	}
	if *clientID != "" {
		sources++
	}
	if sources > 1 {
		return usagef("choose one of --from-gh, --token, or --client-id")
	}
	if *refresh != "" && !slices.Contains(refreshableProviders, *refresh) {
		return usagef("--refresh %s: unknown provider; rainier stores credentials for: %s",
			*refresh, strings.Join(refreshableProviders, ", "))
	}

	// The hosted login is a different exchange with a different server: it
	// shares only the word "login", so combining it with the GitHub flags is
	// refused rather than silently honoring one of them.
	if *cloud != "" {
		if *fromGH || *token != "" || *clientID != "" || *refresh != "" || *server != "" {
			return usagef("login --cloud names the hosted edge and runs the browser login; " +
				"it takes none of --from-gh, --token, --client-id, --refresh or --server")
		}
		return finishLogin(runCloudLogin(*cloud, *deviceName, *contextName))
	}

	// A bare `rainier login` is the first line of the happy path and has to
	// work without anyone learning a flag. It re-authenticates against the
	// server this machine is already using — which is what makes login safe to
	// rerun after a logout, an expired refresh token, or a revoked device —
	// and falls back to this build's hosted default on a machine with no
	// context yet (docs/cli-v0-contract.md §4.1).
	if bareLogin(*fromGH, *token, *clientID, *server, *refresh) {
		if target, name, ok := reauthenticationTarget(*contextName); ok {
			return finishLogin(runCloudLogin(target, *deviceName, name))
		}
		// The hosted default is for a machine with nothing configured. A
		// machine whose context names a self-hosted controld already has a
		// server, and signing that person into a different one — silently
		// creating a second context — is not what "log me in again" meant.
		if fallback := hostedDefaultServer(); fallback != "" && !hasConfiguredServer(*contextName) {
			return finishLogin(runCloudLogin(fallback, *deviceName, *contextName))
		}
		return bareLoginHasNoTarget(*contextName)
	}

	cfg, _ := cli.Load() // a missing/unreadable config is not fatal here: --server can still supply everything
	// A GitHub login writes the "default" context unless one is named: it is
	// the self-hosted half of the config, and it must not be affected by
	// whichever hosted context happens to be current.
	target := *contextName
	if target == "" {
		target = cli.DefaultContext
	}
	serverURL := *server
	if serverURL == "" {
		serverURL = cfg.Contexts[target].Server
	}
	if serverURL == "" {
		fmt.Fprintln(os.Stderr, "rainier login: --server URL is required (no server configured yet)")
		os.Exit(2)
	}

	var ghToken string
	switch {
	case *fromGH:
		out, err := exec.Command("gh", "auth", "token").Output()
		if err != nil {
			return fmt.Errorf("gh auth token: %w", err)
		}
		ghToken = strings.TrimSpace(string(out))
	case *token != "":
		ghToken = *token
	case *clientID != "":
		t, err := githubDeviceFlow(*clientID)
		if err != nil {
			return err
		}
		ghToken = t
	default:
		fmt.Fprintln(os.Stderr, "rainier login: specify one of:")
		fmt.Fprintln(os.Stderr, "  --from-gh          use the token from `gh auth token`")
		fmt.Fprintln(os.Stderr, "  --token GH_TOKEN   use a GitHub access token directly")
		fmt.Fprintln(os.Stderr, "  --client-id ID     run the GitHub device flow with this OAuth App client id")
		os.Exit(2)
	}

	// A refresh is the same exchange: the server upserts the credential on
	// every login, so re-logging in IS how a needs_refresh row is cleared.
	// There is no second endpoint to call and nothing extra to send.
	c := &cli.Client{Base: serverURL}
	var resp authResponse
	if err := c.Do(http.MethodPost, "/v0/auth/github", map[string]string{"access_token": ghToken}, &resp); err != nil {
		return err
	}

	// Login is where this CLI learns who it is. The identity in the exchange
	// response is the same one GET /v0/me answers with — id, login, role —
	// and its id is what resolveSessionID compares against a session's
	// owner_id, so caching it here is what makes owner-preference work from
	// the first command after a login rather than only after a `new`.
	//
	// An older controld that does not send an id leaves whatever is already
	// cached alone: a refresh of a GitHub credential is no reason to forget
	// who the caller is.
	if err := cli.UpdateConfig(func(latest *cli.Config) error {
		ctx := latest.Contexts[target]
		if ctx.Server != serverURL || (resp.User.ID != "" && ctx.OwnerID != resp.User.ID) {
			ctx.CurrentSession = ""
		}
		ctx.Kind, ctx.RefreshToken, ctx.AccessExpiresAt, ctx.Workspace = "", "", "", ""
		ctx.Server, ctx.Token = serverURL, resp.Token
		if resp.User.ID != "" {
			ctx.OwnerID = resp.User.ID
		}
		latest.SetContext(target, ctx)
		return nil
	}); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}

	if *refresh != "" {
		fmt.Printf("refreshed %s credential for %s (%s)\n", *refresh, resp.User.Login, resp.User.Role)
	} else {
		fmt.Printf("logged in as %s (%s)\n", resp.User.Login, resp.User.Role)
	}
	if resp.Scopes != "" {
		fmt.Printf("github scopes: %s\n", resp.Scopes)
	}
	// The server warns rather than fails when the token can't do git; print
	// it on stdout beside the rest of the login summary, since it is about
	// this login's outcome and not a CLI-level error.
	if resp.Warning != "" {
		fmt.Printf("warning: %s\n", resp.Warning)
	}
	return nil
}

// bareLogin reports whether this invocation named no way to authenticate. It
// is the shape `rainier login` takes in the happy path, and the shape that
// gets to resolve a server on its own.
func bareLogin(fromGH bool, token, clientID, server, refresh string) bool {
	return !fromGH && token == "" && clientID == "" && server == "" && refresh == ""
}

// reauthenticationTarget names the hosted server a bare login should sign in
// to again: the one whose context is current, or the one --context named.
//
// Only a hosted context qualifies. A self-hosted context authenticates with a
// GitHub token this CLI cannot obtain by itself, so a bare login there falls
// through to the usage that says which flag supplies one.
func reauthenticationTarget(contextName string) (server, name string, ok bool) {
	cfg, err := cli.Load()
	if err != nil {
		return "", "", false
	}
	name = contextName
	if name == "" {
		name = cfg.ActiveName()
	}
	// A logged-out context still names its server and still records that it
	// is hosted, and signing back in to it is exactly what a bare login means
	// after `rainier logout`.
	ctx, exists := cfg.Contexts[name]
	if !exists || ctx.Server == "" || !ctx.Hosted() {
		return "", "", false
	}
	return ctx.Server, name, true
}

// hasConfiguredServer reports whether the named (or current) context already
// names a server. It is the guard that keeps a build's hosted default from
// hijacking a self-hosted machine's bare login.
func hasConfiguredServer(contextName string) bool {
	cfg, err := cli.Load()
	if err != nil {
		return false
	}
	name := contextName
	if name == "" {
		name = cfg.ActiveName()
	}
	ctx, ok := cfg.Contexts[name]
	return ok && ctx.Server != ""
}

// bareLoginHasNoTarget explains why `rainier login` on its own could not
// resolve a server, and the two cases are genuinely different.
//
// A self-hosted context HAS a server; what a bare login cannot do there is
// obtain a GitHub token, which is the credential that login exchanges. Telling
// that person "no server configured" would be false and would send them
// looking for the wrong thing.
//
// With no context at all there is nothing to sign in to, and this build
// compiled in no hosted default. That is deliberate: inventing a hostname
// would send somebody's credentials at a server nobody has stood up.
func bareLoginHasNoTarget(contextName string) error {
	cfg, err := cli.Load()
	name := contextName
	if name == "" && err == nil {
		name = cfg.ActiveName()
	}
	if err == nil {
		if ctx, ok := cfg.Contexts[name]; ok && ctx.Server != "" && !ctx.Hosted() {
			return usagef("rainier login: context %s is a self-hosted server, and a bare login has no way to obtain a GitHub token for it.\n"+
				"Name one:  rainier login --from-gh   (or --token GH_TOKEN, or --client-id ID)\n"+
				"See rainier help login for the whole self-hosted form.", name)
		}
	}
	return usagef("rainier login: no server configured on this machine, and this build has no hosted default.\n" +
		"Sign in to a hosted rainier with:   rainier login --cloud EDGE_URL\n" +
		"Or to a self-hosted controld with:  rainier login --server URL --from-gh   (rainier help login)")
}

// finishLogin is what a successful login says next, and the reason it exists
// is a claim the CLI must never make: authentication is not readiness.
//
// A person whose account is fine and whose workspace has no compute has
// logged in successfully and cannot start a session. Printing "logged in" and
// stopping would leave them to discover that at `rainier new`. So login ends
// by reporting the authoritative workspace state and, when the server named
// one, the address to continue at — and it does not turn a not-ready
// workspace into a failed login, because the login did work.
func finishLogin(err error) error {
	if err != nil {
		return err
	}
	cfg, cfgErr := cli.Load()
	if cfgErr != nil {
		return nil
	}
	if _, ok := cfg.Active(); !ok {
		return nil
	}
	fmt.Println()
	rows, ready := collectStatus(context.Background(), cfg)
	printStatus(os.Stdout, rows)
	if !ready {
		fmt.Fprintln(os.Stderr, "this workspace is not ready yet; the lines above say what is outstanding")
	}
	return nil
}

// ---------------------------------------------------------------------------
// creds
// ---------------------------------------------------------------------------

// runCreds renders GET /v0/credentials: what the server's vault holds for
// the caller, and nothing about anyone else. There is no value in the
// response and none in this table by construction — a credential is
// write-only at that API exactly like a secret.
//
// The vault is a self-hosted idea. A hosted edge neither serves that route nor
// forwards it, so this command can only ever 404 against one, and a bare
// "resource not found" reads as "you have no credential" — the opposite of
// useful for a person whose GitHub connection is fine. Say which command owns
// the question instead, before spending the round trip.
func runCreds(args []string) error {
	fs := flag.NewFlagSet("creds", flag.ExitOnError)
	fs.Parse(args)

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	if ctx, ok := cfg.Active(); ok && ctx.Hosted() {
		return errors.New("this is a hosted rainier, which has no credential vault: GitHub is a connection you " +
			"authorize in the browser, and `rainier connection ls` is what shows it")
	}
	c := cli.NewClient(cfg)

	var resp credentialsEnvelope
	if err := c.Do(http.MethodGet, "/v0/credentials", nil, &resp); err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "PROVIDER\tSTATUS\tSCOPES\tLAST_VERIFIED\tLAST_USED")
	for _, cr := range resp.Credentials {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			cr.Provider, cr.Status, dashIfEmpty(cr.Scopes), formatAge(cr.LastVerifiedAt), formatAge(cr.LastUsedAt))
	}
	return w.Flush()
}

// deviceCodeResponse and accessTokenResponse are GitHub's device-flow wire
// shapes (https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps#device-flow).
type deviceCodeResponse struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type accessTokenResponse struct {
	AccessToken string `json:"access_token"`
	Error       string `json:"error"`
}

// githubDeviceFlow runs GitHub's OAuth device flow end to end: request a
// device/user code pair, print it for the human to enter at
// verification_uri, then poll for the access token at the interval GitHub
// names (backing off further on slow_down) until it's granted or the code
// expires.
func githubDeviceFlow(clientID string) (string, error) {
	dc, err := requestDeviceCode(clientID)
	if err != nil {
		return "", err
	}
	fmt.Printf("First, go to %s and enter this code: %s\n", dc.VerificationURI, dc.UserCode)
	fmt.Println("Waiting for authorization…")

	interval := time.Duration(dc.Interval) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)

	for {
		time.Sleep(interval)
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("github device flow: code expired before authorization completed")
		}
		at, err := pollAccessToken(clientID, dc.DeviceCode)
		if err != nil {
			return "", err
		}
		switch at.Error {
		case "":
			if at.AccessToken != "" {
				return at.AccessToken, nil
			}
		case "authorization_pending":
			// keep polling at the same interval
		case "slow_down":
			interval += 5 * time.Second
		default:
			return "", fmt.Errorf("github device flow: %s", at.Error)
		}
	}
}

func requestDeviceCode(clientID string) (deviceCodeResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), devicePollTimeout)
	defer cancel()
	// `repo` is what lets a session clone, pull and push on the user's
	// behalf; `read:user` is what the login exchange itself needs. Both are
	// requested up front because the alternative — asking for read:user now
	// and repo at the first clone — means a device-flow prompt in the middle
	// of a session, which is the one moment there is nobody at the terminal.
	form := url.Values{"client_id": {clientID}, "scope": {"repo read:user"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://github.com/login/device/code", strings.NewReader(form.Encode()))
	if err != nil {
		return deviceCodeResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return deviceCodeResponse{}, err
	}
	defer resp.Body.Close()
	var dc deviceCodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&dc); err != nil {
		return deviceCodeResponse{}, fmt.Errorf("decoding device code response: %w", err)
	}
	return dc, nil
}

// pollAccessToken makes one poll of GitHub's device-flow token endpoint,
// bounded by devicePollTimeout so a single stalled request can't park the
// whole login flow — githubDeviceFlow's own loop is what retries on the
// next interval tick.
func pollAccessToken(clientID, deviceCode string) (accessTokenResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), devicePollTimeout)
	defer cancel()
	form := url.Values{
		"client_id":   {clientID},
		"device_code": {deviceCode},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://github.com/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return accessTokenResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return accessTokenResponse{}, err
	}
	defer resp.Body.Close()
	var at accessTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&at); err != nil {
		return accessTokenResponse{}, fmt.Errorf("decoding access token response: %w", err)
	}
	return at, nil
}

// ---------------------------------------------------------------------------
// hosted login (--cloud), contexts and workspaces
// ---------------------------------------------------------------------------

// The hosted login is passwordless and happens in a browser: this CLI starts
// an attempt, prints (and, when it can, opens) the URL the human finishes it
// at, and polls one endpoint until the browser half is done. It never sees an
// email code and never handles a password — it holds a poll token, and gets a
// token pair back. Everything after that is the same CLI against a different
// server, with one addition: the workspace the context is scoped to.

const (
	// minPollInterval and maxPollInterval bound whatever poll_interval_seconds
	// the edge asks for. The floor keeps a misconfigured (or hostile) edge from
	// turning this loop into a hot one; the ceiling keeps a large value from
	// leaving a human staring at a finished browser tab.
	minPollInterval = 2 * time.Second
	maxPollInterval = 10 * time.Second
	// loginAttemptFallbackTTL bounds the poll loop when the attempt names no
	// expiry this CLI can parse. The edge's own expires_at is authoritative
	// whenever it is readable.
	loginAttemptFallbackTTL = 15 * time.Minute
	// edgeRequestTimeout bounds one request of the login exchange, so a
	// blackholed poll fails that poll instead of parking the whole login.
	edgeRequestTimeout = 30 * time.Second
)

// loginAttempt is POST /v0/auth/login-attempts' response: the attempt's id,
// the path (on the same server) the human opens, the token this CLI polls
// with, and how long it may keep doing so.
type loginAttempt struct {
	ID                  string `json:"id"`
	BrowserPath         string `json:"browser_path"`
	PollToken           string `json:"poll_token"`
	ExpiresAt           string `json:"expires_at"`
	PollIntervalSeconds int    `json:"poll_interval_seconds"`
}

// workspaceView is one row of GET /v0/workspaces — the workspaces this
// account may act in, and the caller's role in each.
type workspaceView struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
}

type workspacesEnvelope struct {
	Workspaces []workspaceView `json:"workspaces"`
	NextCursor string          `json:"next_cursor"`
}

func runCloudLogin(edgeURL, deviceName, contextName string) error {
	return runCloudLoginContext(context.Background(), edgeURL, deviceName, contextName, nil)
}

// runCloudLoginSleep is runCloudLogin with the wait between polls injected,
// the way attachWithRetrySleep is: the flow's shape — how many polls, at what
// interval — is exactly what a test needs to pin, and it must not cost the
// test the real seconds.
func runCloudLoginSleep(edgeURL, deviceName, contextName string, sleep func(time.Duration)) error {
	return runCloudLoginContext(context.Background(), edgeURL, deviceName, contextName, sleep)
}

// runCloudLoginContext is the hosted browser login bound to its caller's
// lifetime. Reconnect uses it after a terminal DELETE, so an interrupt must
// return through reconnect's stage-aware recovery instead of stranding the
// command inside an uncancelable poll.
func runCloudLoginContext(ctx context.Context, edgeURL, deviceName, contextName string, sleep func(time.Duration)) error {
	base := strings.TrimRight(edgeURL, "/")
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return usagef("login --cloud requires an http(s) server URL without credentials, query, or fragment")
	}
	if deviceName == "" {
		deviceName = defaultDeviceName()
	}
	// A context named after the edge host is what makes two hosted accounts,
	// or a hosted edge beside a self-hosted controld, nameable without the
	// user inventing anything.
	if contextName == "" {
		contextName = u.Host
	}

	var attempt loginAttempt
	if _, err := edgePostContext(ctx, base+"/v0/auth/login-attempts",
		map[string]string{"device_name": deviceName}, &attempt); err != nil {
		return fmt.Errorf("starting the login: %w", err)
	}
	if attempt.ID == "" || attempt.BrowserPath == "" || attempt.PollToken == "" {
		return fmt.Errorf("starting the login: the server's login attempt is incomplete")
	}

	browseURL := base + attempt.BrowserPath
	fmt.Printf("Open this URL to finish signing in as %s:\n\n  %s\n\n", deviceName, browseURL)
	openBrowser(browseURL)
	fmt.Println("Waiting for the browser…")

	pair, err := pollLoginAttemptContext(ctx, base, attempt, sleep)
	if err != nil {
		return err
	}

	if err := cli.UpdateConfigContext(ctx, func(cfg *cli.Config) error {
		ctx := cfg.Contexts[contextName]
		if ctx.Server != base {
			ctx = cli.Context{}
		}
		ctx.OwnerID, ctx.CurrentSession = "", ""
		ctx.Server, ctx.Token = base, pair.AccessToken
		ctx.RefreshToken, ctx.AccessExpiresAt = pair.RefreshToken, pair.AccessExpiresAt
		// Recorded so a later logout, which deletes both tokens, still leaves
		// something that says how to sign back in here.
		ctx.Kind = cli.KindHosted
		cfg.SetContext(contextName, ctx)
		return nil
	}); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}
	fmt.Printf("logged in to %s (context %s)\n", u.Host, contextName)

	return selectWorkspaceContext(ctx, contextName)
}

// selectWorkspace finishes a hosted login: one workspace is the answer and
// becomes the context's scope; several are printed for the user to choose
// from, because picking for them would silently attach every later command to
// the wrong team; none is said plainly, since the next step is in the browser.
func selectWorkspace(contextName string) error {
	return selectWorkspaceContext(context.Background(), contextName)
}

func selectWorkspaceContext(ctx context.Context, contextName string) error {
	cfg, err := cli.Load()
	if err != nil {
		return err
	}
	if !cfg.Use(contextName) {
		return cli.ErrLoginAgain
	}
	spaces, err := listWorkspacesContext(ctx, cli.NewClient(cfg))
	if err != nil {
		return fmt.Errorf("listing workspaces: %w", err)
	}
	switch len(spaces) {
	case 0:
		fmt.Println("no workspaces yet — create one in the browser, then run `rainier workspace use <id>`")
		return nil
	case 1:
		return setWorkspaceContext(ctx, contextName, spaces[0])
	default:
		fmt.Println("workspaces:")
		printWorkspaces(spaces)
		fmt.Println("run `rainier workspace use <id>` to choose one")
		return nil
	}
}

// setWorkspace stores w as contextName's scope. The config is re-read first:
// the listing that produced w may have refreshed the hosted token pair, and
// writing back a copy loaded before that would strand the context on a
// refresh token the edge has already spent.
func setWorkspace(contextName string, w workspaceView) error {
	return setWorkspaceContext(context.Background(), contextName, w)
}

func setWorkspaceContext(ctx context.Context, contextName string, w workspaceView) error {
	if err := cli.UpdateConfigContext(ctx, func(cfg *cli.Config) error {
		ctx, ok := cfg.Contexts[contextName]
		if !ok {
			return cli.ErrLoginAgain
		}
		ctx.Workspace = w.ID
		cfg.UpdateContext(contextName, ctx)
		return nil
	}); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}
	fmt.Printf("workspace: %s (%s)\n", w.ID, w.Name)
	return nil
}

func listWorkspaces(c *cli.Client) ([]workspaceView, error) {
	return listWorkspacesContext(context.Background(), c)
}

func listWorkspacesContext(ctx context.Context, c *cli.Client) ([]workspaceView, error) {
	var spaces []workspaceView
	path := "/v0/workspaces"
	seen := map[string]bool{}
	for {
		var resp workspacesEnvelope
		requestCtx, cancel := context.WithTimeout(ctx, edgeRequestTimeout)
		err := c.DoContext(requestCtx, http.MethodGet, path, nil, &resp)
		cancel()
		if err != nil {
			return nil, err
		}
		spaces = append(spaces, resp.Workspaces...)
		if resp.NextCursor == "" {
			return spaces, nil
		}
		if seen[resp.NextCursor] {
			return nil, errors.New("workspace listing repeated a pagination cursor")
		}
		seen[resp.NextCursor] = true
		path = "/v0/workspaces?cursor=" + url.QueryEscape(resp.NextCursor)
	}
}

func printWorkspaces(spaces []workspaceView) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tROLE")
	for _, s := range spaces {
		fmt.Fprintf(w, "%s\t%s\t%s\n", s.ID, s.Name, s.Role)
	}
	w.Flush()
}

// pollLoginAttempt polls the exchange until the browser half completes. 202
// means the human is still working; 200 carries the token pair; 410 means the
// attempt lapsed, which is also what the deadline means locally.
func pollLoginAttempt(base string, attempt loginAttempt, sleep func(time.Duration)) (cli.TokenPair, error) {
	return pollLoginAttemptContext(context.Background(), base, attempt, sleep)
}

func pollLoginAttemptContext(ctx context.Context, base string, attempt loginAttempt, sleep func(time.Duration)) (cli.TokenPair, error) {
	interval := time.Duration(attempt.PollIntervalSeconds) * time.Second
	interval = min(max(interval, minPollInterval), maxPollInterval)
	deadline := time.Now().Add(loginAttemptFallbackTTL)
	if t, err := time.Parse(time.RFC3339, attempt.ExpiresAt); err == nil {
		deadline = t
	}

	path := base + "/v0/auth/login-attempts/" + url.PathEscape(attempt.ID) + "/exchange"
	body := map[string]string{"poll_token": attempt.PollToken}
	for {
		var pair cli.TokenPair
		status, err := edgePostContext(ctx, path, body, &pair)
		switch {
		case err == nil && status == http.StatusOK && pair.AccessToken != "":
			return pair, nil
		case err == nil:
			// 202: the browser half is not finished yet.
		default:
			var apiErr *cli.APIError
			if errors.As(err, &apiErr) && (apiErr.Status == http.StatusGone || apiErr.Code == "login_expired") {
				return cli.TokenPair{}, errLoginExpired(base)
			}
			return cli.TokenPair{}, err
		}
		if !time.Now().Before(deadline) {
			return cli.TokenPair{}, errLoginExpired(base)
		}
		if sleep != nil {
			sleep(interval)
			if err := ctx.Err(); err != nil {
				return cli.TokenPair{}, err
			}
		} else {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return cli.TokenPair{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
}

func errLoginExpired(base string) error {
	return fmt.Errorf("this login expired before the browser finished; run `rainier login --cloud %s` again", base)
}

// edgePost performs one JSON request of the hosted login exchange and reports
// the status alongside the error, which the poll loop needs: on this wire 202
// and 200 are both successes that mean different things. A non-2xx is decoded
// as the API's error envelope, so a 410 arrives as a *cli.APIError the caller
// can recognize without reading prose.
func edgePost(fullURL string, in, out any) (int, error) {
	return edgePostContext(context.Background(), fullURL, in, out)
}

func edgePostContext(parent context.Context, fullURL string, in, out any) (int, error) {
	ctx, cancel := context.WithTimeout(parent, edgeRequestTimeout)
	defer cancel()

	b, err := json.Marshal(in)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Request-Id", cli.RandHex(8))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, edgeBodyLimit))
	if err != nil {
		return resp.StatusCode, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil && len(bytes.TrimSpace(data)) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return resp.StatusCode, fmt.Errorf("decoding the response: %w", err)
			}
		}
		return resp.StatusCode, nil
	}

	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &env); err != nil || env.Error.Code == "" {
		return resp.StatusCode, fmt.Errorf("unexpected response: %d", resp.StatusCode)
	}
	return resp.StatusCode, &cli.APIError{Code: env.Error.Code, Message: env.Error.Message, Status: resp.StatusCode}
}

// edgeBodyLimit caps how much of a login-exchange response is ever read: the
// bodies on this wire are a handful of fields, and the far side is untrusted
// until the login succeeds.
const edgeBodyLimit = 64 << 10

// defaultDeviceName names this machine in the hosted login attempt — it is
// what the human sees in the browser ("approve a login from …") and later in
// their session list. The hostname is the one identifier that is both stable
// and already known to them.
func defaultDeviceName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "rainier-cli"
}

// openBrowser opens url in the desktop browser, best effort. Printing the URL
// is the contract; opening it is a convenience, so every failure here — no
// opener on a headless box, an opener that exits nonzero — is silent.
// RAINIER_NO_BROWSER=1 turns it off outright.
func openBrowser(target string) {
	if os.Getenv("RAINIER_NO_BROWSER") != "" {
		return
	}
	opener := "xdg-open"
	if runtime.GOOS == "darwin" {
		opener = "open"
	}
	path, err := exec.LookPath(opener)
	if err != nil {
		return
	}
	cmd := exec.Command(path, target)
	if err := cmd.Start(); err != nil {
		return
	}
	go cmd.Wait() //nolint:errcheck // reap the opener; its outcome is not ours
}

// ---------------------------------------------------------------------------
// context
// ---------------------------------------------------------------------------

// runContext is the switch between servers: one config holds a self-hosted
// controld and any number of hosted edges, and exactly one of them is current.
func runContext(args []string) error {
	fs := flag.NewFlagSet("context", flag.ExitOnError)
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: rainier context list | use <name> | current | remove <name>")
	}

	cfg, err := cli.Load()
	if err != nil {
		return err
	}

	switch rest[0] {
	case "list":
		if len(cfg.Contexts) == 0 {
			fmt.Println("no contexts yet: run `rainier login --server URL` or `rainier login --cloud EDGE_URL`")
			return nil
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "CURRENT\tNAME\tSERVER\tWORKSPACE")
		current := cfg.ActiveName()
		for _, name := range cfg.Names() {
			ctx := cfg.Contexts[name]
			marker := ""
			if name == current {
				marker = "*"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", marker, name, ctx.Server, dashIfEmpty(ctx.Workspace))
		}
		return w.Flush()

	case "current":
		if _, ok := cfg.Active(); !ok {
			return fmt.Errorf("no current context: run `rainier login --server URL` or `rainier login --cloud EDGE_URL`")
		}
		fmt.Println(cfg.ActiveName())
		return nil

	case "use":
		if len(rest) != 2 {
			return fmt.Errorf("usage: rainier context use <name>")
		}
		if err := cli.UpdateConfig(func(latest *cli.Config) error {
			if !latest.Use(rest[1]) {
				return fmt.Errorf("no context named %s; `rainier context list` shows the ones there are", rest[1])
			}
			cfg = *latest
			return nil
		}); err != nil {
			return fmt.Errorf("saving config: %w", err)
		}
		fmt.Printf("context: %s (%s)\n", rest[1], cfg.ServerURL)
		return nil

	case "remove":
		if len(rest) != 2 {
			return fmt.Errorf("usage: rainier context remove <name>")
		}
		if err := cli.UpdateConfig(func(latest *cli.Config) error {
			if !latest.RemoveContext(rest[1]) {
				return fmt.Errorf("no context named %s; `rainier context list` shows the ones there are", rest[1])
			}
			return nil
		}); err != nil {
			return fmt.Errorf("saving config: %w", err)
		}
		fmt.Printf("removed context %s\n", rest[1])
		return nil

	default:
		return fmt.Errorf("rainier context: unknown subcommand %q; use list, use, current or remove", rest[0])
	}
}

// ---------------------------------------------------------------------------
// workspace
// ---------------------------------------------------------------------------

// runWorkspace picks which hosted workspace the current context acts in. The
// id is checked against the account's own listing here, so a typo fails now
// rather than as an unexplainable refusal on the next command.
func runWorkspace(args []string) error {
	fs := flag.NewFlagSet("workspace", flag.ExitOnError)
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) != 2 || rest[0] != "use" {
		return fmt.Errorf("usage: rainier workspace use <id>")
	}
	id := rest[1]

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	name := cfg.ActiveName()
	ctx, _ := cfg.Active()
	if !ctx.Hosted() {
		return fmt.Errorf("workspace use: context %s is not a hosted one; workspaces come from `rainier login --cloud EDGE_URL`", name)
	}

	spaces, err := listWorkspaces(cli.NewClient(cfg))
	if err != nil {
		return fmt.Errorf("listing workspaces: %w", err)
	}
	i := slices.IndexFunc(spaces, func(w workspaceView) bool { return w.ID == id })
	if i < 0 {
		ids := make([]string, 0, len(spaces))
		for _, w := range spaces {
			ids = append(ids, w.ID)
		}
		if len(ids) == 0 {
			return fmt.Errorf("no workspace %s: this account has no workspaces yet", id)
		}
		return fmt.Errorf("no workspace %s in this account; yours are: %s", id, strings.Join(ids, ", "))
	}
	return setWorkspace(name, spaces[i])
}

// ---------------------------------------------------------------------------
// new
// ---------------------------------------------------------------------------

func runNew(args []string) error {
	fs := flag.NewFlagSet("new", flag.ExitOnError)
	name := fs.String("name", "", "session name")
	agentName := fs.String("agent", "", "coding `agent` to start in the session (claude, codex)")
	env := fs.String("env", "", "environment to start from (name or id)")
	image := fs.String("image", "", "container image (overrides the environment's)")
	egress := fs.String("egress", "", "comma-separated egress allowlist (overrides the environment's)")
	detach := fs.Bool("detach", false, "create without attaching")
	asJSON := fs.Bool("json", false, "with --detach, print one machine-readable result document")
	idempotencyKey := fs.String("idempotency-key", "", "stable create retry key (developer tooling)")
	fs.Parse(reorderArgs(fs, args))
	cmdArgs := fs.Args() // whatever followed "--"

	// Two answers to "what does this session run" is a mistake, not a
	// precedence puzzle. Refusing costs one retype; picking a winner costs a
	// person a session that quietly did the other thing (contract §3.3).
	if *agentName != "" && *image != "" {
		return usagef("--agent requires the environment image; use -- CMD with --image")
	}
	if *agentName != "" && len(cmdArgs) > 0 {
		return usagef("--agent %s and an explicit command both say what this session runs; pass one of them", *agentName)
	}
	if *asJSON && !*detach {
		return usagef("--json describes a created session; use it with --detach (an attached session's output is the terminal's)")
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)
	ctx := context.Background()

	// The workspace's default environment when none was named. The server
	// owns which one that is; when it names none and its catalog cannot
	// answer, the session is a scratch session exactly as it always was
	// (contract §5.2).
	// --image is the advanced escape hatch that says "run exactly this image".
	// Folding a default environment's setup script and secrets in underneath
	// one would be a surprise, and a setup script written for another image is
	// a session that fails at boot — so an explicit image opts out of the
	// default the same way an explicit --env opts into a specific one.
	var resolvedEnv environment
	environmentName := *env
	if environmentName == "" && *image == "" {
		resolved, resolveErr := resolveDefaultEnvironment(ctx, c)
		switch {
		case resolveErr == nil:
			resolvedEnv, environmentName = resolved, resolved.ID
		case !errors.Is(resolveErr, errNoDefaultEnvironment):
			return resolveErr
		}
	}

	// --env and the two override flags compose: the environment supplies
	// everything the flags don't, and controld resolves the pair (design §4.3).
	body := createSessionRequest{Name: *name, Image: *image, Environment: environmentName}
	switch {
	case len(cmdArgs) > 0:
		body.Cmd = cmdArgs
	case *agentName != "":
		// The environment has to be settled first: which agents can start is
		// a property of the environment's image, so there is no launch argv
		// to ask for until we know which environment the session will run in.
		if environmentName == "" {
			return fmt.Errorf("--agent %s needs an environment whose image carries that agent, and this workspace publishes no default one; name it with --env", safeField(*agentName))
		}
		if resolvedEnv.ID == "" {
			named, err := environmentByRef(ctx, c, environmentName)
			if err != nil {
				return err
			}
			resolvedEnv = named
		}
		launch, err := agentLaunchCommand(ctx, c, resolvedEnv, *agentName)
		if err != nil {
			return err
		}
		body.Environment = resolvedEnv.ID
		body.Cmd = launch
	}
	if *egress != "" {
		body.EgressAllow = strings.Split(*egress, ",")
	}

	created, err := createSession(c, body, *idempotencyKey)
	if err != nil {
		var apiErr *cli.APIError
		if errors.As(err, &apiErr) && apiErr.Code == "workspace_not_ready" {
			return newSessionError(err, workspaceNotReadyDestination(ctx, cfg, c))
		}
		return err
	}
	// The id is the durable handle, so it is printed before anything can go
	// wrong with the attach that follows — except under --json, where the
	// document IS the output and a bare id ahead of it makes the stream
	// unparseable (contract §6.2).
	if !*asJSON {
		fmt.Println(safeField(redactSecrets(cfg, created.ID)))
	}
	if err := rememberCurrentSession(created.ID, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "note: could not record this session as `current`: %v\n", err)
	}

	if *detach {
		if *asJSON {
			return writeJSON(os.Stdout, schemaMutation,
				mutationDocument("new", created.ID, true, created.State, "session created; attach with rainier attach "+created.ID))
		}
		return nil
	}
	// The whole log, not a snapshot: `new`'s attach is "stream everything"
	// (design §4.10/§9), and the interesting output — a setup script's, a
	// clone's — starts before this socket can possibly be up. A session
	// created seconds ago has a log measured in kilobytes, so replaying it
	// from the first entry costs nothing and is the only way the user sees
	// what happened before they got here.
	return attachWithRetry(cfg, created.ID, terminal.SinceAll)
}

// newSessionError turns a refusal to create into the one sentence a person
// can act on.
//
// workspace_not_ready is the case this exists for: the account authenticated
// fine, the CLI is fine, and the workspace has no compute because a plan, a
// payment or a provisioning run is still outstanding. That is a web action,
// so what the CLI owes is the server's own destination and nothing else — no
// session was created, and the CLI must not offer to create the compute.
//
// It branches on the machine-readable code, never on the message text
// (contract §6.3): prose is the server's to reword.
func newSessionError(err error, destination string) error {
	var apiErr *cli.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "workspace_not_ready" {
		return err
	}
	message := "no session was created: workspace compute is not ready; run rainier status"
	if destination != "" {
		message += "\nContinue: " + destination
	}
	return commandError{message: message, cause: err}
}

// environmentByRef resolves an environment name or id to its row. --agent
// needs the id to ask which agents that environment can start, and the server
// accepts either spelling on the environment route.
func environmentByRef(ctx context.Context, c *cli.Client, ref string) (environment, error) {
	envs, err := fetchEnvironments(ctx, c)
	if err != nil {
		return environment{}, err
	}
	for _, env := range envs {
		if env.ID == ref || env.Name == ref {
			return env, nil
		}
	}
	return environment{}, fmt.Errorf("no environment named %q in this workspace", safeField(ref))
}

// workspaceNotReadyDestination fetches the console address to print beside a
// workspace_not_ready refusal, and answers "" when the server publishes none.
// It runs only on that refusal: an ordinary create must not pay for a lookup
// nothing will use.
func workspaceNotReadyDestination(ctx context.Context, cfg cli.Config, c *cli.Client) string {
	active, ok := cfg.Active()
	if !ok {
		return ""
	}
	state, err := fetchCompute(ctx, c, active.Workspace)
	if err != nil {
		return ""
	}
	onboarding, err := fetchOnboarding(ctx, c)
	if err != nil {
		return ""
	}
	return onboarding.destinationFor(state.Status)
}

// createSession posts one create and returns the session it made. It is the
// one place this CLI creates a session — `new` and `agent login` differ in
// what they put in the body, never in how they send it — and it is where the
// idempotency key is settled: a caller with a recovery key of its own passes
// it, and everybody else gets a fresh one, since only the caller knows which
// invocations are retries of the same intent.
func createSession(c *cli.Client, body createSessionRequest, idempotencyKey string) (session, error) {
	if idempotencyKey == "" {
		idempotencyKey = cli.RandHex(8)
	}
	var resp sessionEnvelope
	if err := c.Do(http.MethodPost, "/v0/sessions", body, &resp, cli.IdempotencyKey(idempotencyKey)); err != nil {
		var apiErr *cli.APIError
		if errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500 && apiErr.Status != http.StatusRequestTimeout {
			return session{}, err
		}
		return session{}, commandError{message: fmt.Sprintf("create was not confirmed; inspect rainier ls, or retry the same request with --idempotency-key %s", idempotencyKey), cause: err}
	}
	return resp.Session, nil
}

// attachWithRetry is `new`'s "attach immediately and stream everything"
// (design §4.10): a session that was just created is legitimately a few
// seconds from `running`, so a first attach that fails because it isn't
// ready yet is retried, with a waiting line, for up to 60s rather than
// treated as fatal. attachio.Run's dial wraps that specific failure —
// controld's 503 session_not_ready before the websocket upgrade — as a
// *attachio.DialError matching errors.Is(err, attachio.ErrSessionNotReady);
// a hosted401 gets one credential recovery and retry. Other initial errors
// return immediately instead of spending the readiness budget. Once connected,
// transient failures and explicit hosted lease renewals resume at the cursor.
func attachWithRetry(cfg cli.Config, id string, since uint64) error {
	return attachWithRetrySleep(cfg, id, since, nil)
}

func attachWithRetrySleep(cfg cli.Config, id string, since uint64, sleep func(time.Duration)) error {
	return attachWithRetryBudget(cfg, id, since, sleep, 60*time.Second)
}

func attachWithRetryBudget(cfg cli.Config, id string, since uint64, sleep func(time.Duration), initialWait time.Duration) error {
	return attachWithRetryOwned(cfg, id, since, defaultOwnership(), sleep, initialWait)
}

// defaultOwnership is what a plain `rainier attach` asks for: claim control
// when it is free, and view when somebody else has it. It is zero-click on
// one laptop — the case that has always worked — and honest on two devices.
func defaultOwnership() attachio.Options {
	return attachio.Options{Control: true, Mode: terminal.ModeControl}
}

// withServerPolicy folds what the session view said about the server's
// attachment policy into what this attach asks for. It changes only the COPY a
// person sees — the notices, and whether a take-control key is offered — never
// what the client sends, so an attach whose view read stale, or whose server
// reported nothing, behaves identically and merely says less.
//
// The count excludes this attach, which has not happened yet: "2 other
// terminals attached" is exactly what the view was reporting a moment before
// this one arrived.
func withServerPolicy(own attachio.Options, s session) attachio.Options {
	if !s.Input.sharedInput() {
		return own
	}
	own.Shared = true
	own.OtherTypers = s.Input.Attached
	return own
}

// reconnectOwnership is what the NEXT attempt asks for, and it is the whole
// of "reconnect is conditional". A device that had control presents the
// generation it held, so it resumes only while nobody took it — and comes
// back a viewer, saying so, when somebody did. A device that was viewing
// stays a viewer rather than quietly acquiring control because a network blip
// happened to free it.
//
// Nothing here ever claims on its own. --take is spent by the attempt that
// used it and is not renewed: a client that re-claimed on every reconnect
// would be two devices fighting over a keyboard, which is the failure this
// whole task exists to remove.
func reconnectOwnership(prev attachio.Options, out attachio.Outcome) attachio.Options {
	next := prev
	next.Take = false
	switch out.Mode {
	case terminal.ModeControl:
		next.Mode = terminal.ModeControl
		next.Expected = out.Generation
	case terminal.ModeView:
		next.Mode = terminal.ModeView
		next.Expected = 0
	}
	// An empty mode is a server that never answered: nothing was negotiated,
	// so the request stays exactly what it was.
	return next
}

func attachWithRetryOwned(cfg cli.Config, id string, since uint64, own attachio.Options,
	sleep func(time.Duration), initialWait time.Duration) error {
	// Emulator modes are not termios. Keep them across cursor-only reconnects
	// (which need not replay their enable sequences), but never leave the local
	// shell interpreting mouse movement as typed input on final return.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer attachio.RestoreTerminal(os.Stdin, os.Stdout)
	wait := func(d time.Duration) {
		if sleep != nil {
			sleep(d) // deterministic test clock; production waits are cancelable
			return
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	c := cli.NewClient(cfg)
	wsURL := wsURLFor(cfg.ServerURL, id)
	header := http.Header{}
	// The terminal stream is scoped like every other request on a hosted
	// context: the edge routes it by the same header.
	if ctx, ok := cfg.Active(); ok && ctx.Workspace != "" {
		header.Set("Rainier-Workspace", ctx.Workspace)
	}
	deadline := time.Now().Add(initialWait)
	established := false
	waiting := false
	backoff := 100 * time.Millisecond
	authRetried := false

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header.Set("Authorization", "Bearer "+c.Token)
		attemptStarted := time.Now()
		outcome, err := attachio.Run(ctx, wsURL, header, since, own)
		if err == nil {
			rememberControl(cfg, id, outcome)
			if outcome.Reason != attachio.Disconnected {
				return nil
			}
			// The server sequence is the acknowledgement that a frame reached
			// local stdout. Resume after it: never repaint the whole terminal and
			// never skip output the user had not actually seen.
			established = true
			authRetried = false
			since = outcome.LastSeq
			own = reconnectOwnership(own, outcome)
			if time.Since(attemptStarted) >= 10*time.Second {
				backoff = 100 * time.Millisecond
			}
			// The remote app still owns the screen and cursor. Local status
			// text here would corrupt its next cursor-relative output frame.
			wait(backoff)
			backoff = nextAttachBackoff(backoff)
			continue
		}

		var dialErr *attachio.DialError
		if errors.As(err, &dialErr) && dialErr.Status == http.StatusUnauthorized && c.RefreshToken != "" {
			if authRetried {
				return cli.ErrLoginAgain
			}
			authRetried = true
			refreshCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			refreshErr := c.RefreshAfterUnauthorized(refreshCtx)
			cancel()
			if refreshErr != nil {
				return refreshErr
			}
			continue // same session, workspace, and last rendered cursor
		}

		if established {
			if !retryableAttachError(err) {
				return err
			}
			wait(backoff)
			backoff = nextAttachBackoff(backoff)
			continue
		}

		if !errors.Is(err, attachio.ErrSessionNotReady) {
			return err
		}
		if !time.Now().Before(deadline) {
			return initialAttachGuidance(ctx, cfg, id, since)
		}
		if !waiting {
			fmt.Println("waiting for session… (Ctrl-C stops waiting and keeps the session)")
			waiting = true
		}
		wait(500 * time.Millisecond)
	}
}

func nextAttachBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > 2*time.Second {
		return 2 * time.Second
	}
	return next
}

// retryableAttachError is deliberately narrow. Once a viewer has connected,
// transport failures and transient gateway statuses can recover; authentication
// has its own bounded recovery above. Authorization, not-found, protocol, and
// local-terminal failures must not become an infinite loop. A plain transport failure is a
// *url.Error, while an HTTP response is attachio.DialError.
func retryableAttachError(err error) bool {
	var dialErr *attachio.DialError
	if errors.As(err, &dialErr) {
		switch dialErr.Status {
		case http.StatusTooManyRequests, http.StatusBadGateway,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	var transportErr *url.Error
	return errors.As(err, &transportErr)
}

// ---------------------------------------------------------------------------
// ls
// ---------------------------------------------------------------------------

// runLs is the session table. Three columns by default — NAME, STATE, AGE —
// because that is what a person reads twenty times a day, and because every
// column beyond those three is a diagnostic that belongs behind --verbose
// (docs/cli-v0-contract.md §3.4). Placement, runner identity and
// reachability are the control plane's vocabulary and are not put in front of
// somebody who only wants to know which of their sessions is up.
func runLs(args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	all := fs.Bool("all", false, "include canceled and deleted history")
	verbose := fs.Bool("verbose", false, "add id, environment, runner, reachability and diagnostic state")
	asJSON := fs.Bool("json", false, "print one machine-readable sessions document")
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() != 0 {
		return usagef("usage: rainier ls [--all] [--verbose] [--json]")
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)

	rows, err := listSessions(c, true)
	if err != nil {
		return err
	}
	if !*all {
		rows = slices.DeleteFunc(rows, func(s session) bool { return !activeSession(s) })
	}
	if *asJSON {
		return writeSessionsJSON(os.Stdout, cfg, rows)
	}
	printSessions(os.Stdout, cfg, rows, *verbose)

	return nil
}

// listSessions reads every page in the server's own order. Ordering is
// stable because it is not this CLI's: pages are concatenated as they arrive
// and nothing here re-sorts them.
func listSessions(c *cli.Client, all bool) ([]session, error) {
	var out []session
	cursor := ""
	// A repeated cursor is a server bug, and this loop must not answer it by
	// allocating forever. The old streaming version at least printed rows as
	// it went, so a runaway was visible; this one accumulates, so an
	// unguarded loop would hang `rainier ls` while it consumed memory.
	// listWorkspacesContext guards the same way.
	seen := map[string]bool{}
	for {
		q := url.Values{}
		if all {
			q.Set("all", "true")
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		path := "/v0/sessions"
		if enc := q.Encode(); enc != "" {
			path += "?" + enc
		}
		var page sessionsEnvelope
		if err := c.Do(http.MethodGet, path, nil, &page); err != nil {
			return nil, err
		}
		out = append(out, page.Sessions...)
		if page.NextCursor == "" {
			return out, nil
		}
		if seen[page.NextCursor] {
			return nil, errors.New("session listing repeated a pagination cursor")
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

// printSessions renders the table across the three dimensions a session
// actually has (docs/cli-v0-contract.md §3.1). PROCESS is a column rather
// than a parenthetical on STATE, which is what stops the old
// "running (exited -1)" cell from coming back; CONNECTION is a column rather
// than a lifecycle word, so a runner that dropped its link reads as a
// connection problem and not as a broken session.
//
// Every cell that came off the wire goes through safeField. The session list
// is team-visible and the create route puts no character restriction on a
// name, so a name is untrusted input from another person; the queue reason
// under --verbose is server prose for the same reason.
func printSessions(w io.Writer, cfg cli.Config, rows []session, verbose bool) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if verbose {
		fmt.Fprintln(tw, "NAME\tSTATE\tPROCESS\tCONNECTION\tAGE\tID\tENV\tAPI STATE\tRUNNER\tDETAIL")
	} else {
		fmt.Fprintln(tw, "NAME\tSTATE\tPROCESS\tCONNECTION\tAGE")
	}
	for _, s := range rows {
		if verbose {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				safeField(dashIfEmpty(s.Name)), displayLifecycle(s), displayProcess(s), displayConnection(s),
				formatAge(s.CreatedAt), safeField(s.ID), safeField(dashIfEmpty(s.Environment)),
				safeField(s.State), safeField(dashIfEmpty(s.Runner)),
				diagnosticText(cfg, dashIfEmpty(diagnosticDetail(s))))
			continue
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			safeField(dashIfEmpty(s.Name)), displayLifecycle(s), displayProcess(s), displayConnection(s),
			formatAge(s.CreatedAt))
	}
	tw.Flush()
}

// writeSessionsJSON emits the listing. Each entry is the same document
// `info --json` produces, from the same builder, so a script cannot find one
// shape in a list and a different one in a detail read.
func writeSessionsJSON(w io.Writer, cfg cli.Config, rows []session) error {
	out := make([]map[string]any, 0, len(rows))
	for _, s := range rows {
		out = append(out, sessionDocument(cfg, s))
	}
	return writeJSON(w, schemaSessions, map[string]any{"sessions": out})
}

// dashIfEmpty renders an empty column value as "-", so a scratch session's
// blank ENV reads as "no environment" rather than as a column that failed to
// print.
func dashIfEmpty(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

// formatAge renders a RFC3339 created_at as a short elapsed duration; a
// timestamp that fails to parse (shouldn't happen against a real controld)
// prints as "?" rather than propagating a parse error up through `ls`.
func formatAge(rfc3339 string) string {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		return "?"
	}
	d := time.Since(t).Round(time.Second)
	if d < 0 {
		d = 0
	}
	return d.String()
}

// ---------------------------------------------------------------------------
// attach
// ---------------------------------------------------------------------------

// runAttach attaches to a session's terminal. What the viewer opens with is
// the --since flag's to decide, and "not passed" is one of its three answers
// (attachio.Cursor owns the mapping): no flag paints the current screen,
// `--since 0` replays the whole event log — the runbook's way to read a
// failed setup's full output, and what the disconnect line's advice means
// when it fires before the first frame — and `--since N` resumes after N.
func runAttach(args []string) error {
	ref, cursor, replay, own, err := attachFlags(args)
	if err != nil {
		return err
	}

	cfg, c, id, err := resolveClientAndIDIncludingTerminal(ref)
	if err != nil {
		return err
	}
	row, err := prepareAttach(c, id, replay)
	if err != nil {
		return err
	}
	// What the server said about who may type, folded into the copy this attach
	// will print. It is the view prepareAttach already read; asking again would
	// be a second round trip for a courtesy line.
	own = withServerPolicy(own, row)
	if err := attachWithRetryOwned(cfg, id, cursor, own, nil, 60*time.Second); err != nil {
		return err
	}
	if err := rememberCurrentSession(id, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "note: could not record this session as current")
	}
	return nil
}

// prepareAttach decides what `rainier attach` does about a session that is
// not simply running (docs/cli-v0-contract.md §3.6).
//
// A stopped session is resumed here rather than by a separate command: a
// person who types `attach` means "put me in it", and "first run resume" is a
// step the CLI can take for them. Everything else is a refusal that says
// something true — a finished session has nothing more to show, an
// unavailable one cannot be reached — instead of dropping somebody into a
// frozen screen and letting them work out why.
//
// replay is the one override. `--since` is the diagnostic path: the whole
// reason to attach to a session that failed its setup is to read the log that
// says why, and refusing that would take away the only tool for the case.
//
// It returns the row it read, because the caller needs it too: the session view
// is where the server's attachment policy and its current typer count come
// from, and reading it twice would be two round trips for one question.
func prepareAttach(c *cli.Client, id string, replay bool) (session, error) {
	row, err := getSession(c, id)
	if err != nil {
		return session{}, err
	}
	// Dispatch on the SERVER's state, not on the display word. The endpoint's
	// rules are stated in raw states (controlapp/attachments.go attachable,
	// ResumeSession), and a display word groups states the endpoint treats
	// differently — `queued` and `creating` share the word Starting, and
	// `failed` and `dead` share Failed while only the first is ever
	// attachable.
	switch row.State {
	case "running", "queued", "creating":
		// A running session is attachable outright; queued and creating are
		// not yet, and attachWithRetry waits for them exactly as `new` does.
		// A running session whose child has exited is still attachable — the
		// screen and the sandbox are both still there.
		return row, nil
	case "suspended_warm", "suspended_cold":
		return row, resumeForAttach(c, id)
	case "failed":
		// AttachTerminal admits a failed session only while its runner is
		// still connected, which is what preserves setup-failure diagnosis.
		if row.Reachable || replay {
			return row, nil
		}
		return session{}, unreachableFailedSession(row)
	case "dead", "canceled", "destroyed":
		if replay {
			// --since is the documented diagnostic override. The endpoint
			// will refuse if there is nothing behind it, and that refusal is
			// more informative than one this CLI invents.
			return row, nil
		}
		return session{}, goneSessionResult(row)
	default:
		// A state this build has never heard of. Conservative means making no
		// claim about it, not inventing one: the attach is attempted and the
		// server decides.
		return row, nil
	}
}

// sessionLabel is what to call a session back to the person who named it: the
// name they typed when there is one, and the id otherwise.
func sessionLabel(row session) string {
	if row.Name != "" {
		return row.Name
	}
	return row.ID
}

// goneSessionResult is what a person gets for attaching to a session whose
// sandbox no longer exists. It names the lifecycle accurately — a session
// somebody cancelled and one somebody deleted are different events — and
// points at the two things still worth doing.
func goneSessionResult(row session) error {
	return fmt.Errorf("session %s is %s; its sandbox no longer exists.\n"+
		"Inspect it: rainier info %s\nReplay its output, if any remains: rainier attach %s --since 0",
		safeField(sessionLabel(row)), displayLifecycle(row),
		safeField(sessionLabel(row)), safeField(sessionLabel(row)))
}

// unreachableFailedSession is the CONNECTION failure, said as one. The
// session failed and its runner has since gone; those are two facts and the
// message keeps them apart, because the second one can come back.
func unreachableFailedSession(row session) error {
	return fmt.Errorf("session %s failed, and the runner holding it is no longer connected, "+
		"so there is no terminal to open.\nInspect it: rainier info %s\nDelete it: rainier delete %s",
		safeField(sessionLabel(row)), safeField(sessionLabel(row)), safeField(sessionLabel(row)))
}

// resumeForAttach brings a stopped session back. No convenience endpoint is
// needed server-side — the CLI is the only consumer that wants resume and
// attach composed.
func resumeForAttach(c *cli.Client, id string) error {
	var resumed sessionEnvelope
	resumeErr := c.Do(http.MethodPost, "/v0/sessions/"+id+"/resume", nil, &resumed)
	if resumeErr == nil {
		return nil
	}

	// Another client can win the resume between our GET and POST. A conflict
	// can arrive before that winner's state is committed, so converge for a
	// short bounded window instead of relying on one immediate re-read. The
	// structured error code keeps capacity/auth failures immediate and
	// preserves their more useful original message.
	var apiErr *cli.APIError
	if !errors.As(resumeErr, &apiErr) || apiErr.Code != "conflict" {
		return resumeErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		current, readErr := getSessionContext(ctx, c, id)
		if readErr != nil {
			return resumeErr
		}
		switch current.State {
		case "running", "creating", "queued":
			return nil
		case "suspended_warm", "suspended_cold":
			// Still stopped; the winner's transition has not committed yet.
		default:
			return resumeErr
		}
		select {
		case <-ctx.Done():
			return resumeErr
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func getSession(c *cli.Client, id string) (session, error) {
	return getSessionContext(context.Background(), c, id)
}

func getSessionContext(ctx context.Context, c *cli.Client, id string) (session, error) {
	var resp sessionEnvelope
	if err := c.DoContext(ctx, http.MethodGet, "/v0/sessions/"+id, nil, &resp); err != nil {
		return session{}, err
	}
	return resp.Session, nil
}

// attachFlags parses `attach`'s arguments into the session ref, the attach
// cursor, and whether --since was passed at all. Split out of runAttach so
// the part with no network in it — which of three requests `--since` spells,
// in either argument order — is testable on its own; the flag-after-the-
// positional form is the one the acceptance run reached for when the first
// attempt showed nothing, so it gets pinned rather than assumed (reorderArgs
// is what makes it work).
//
// The third return value is what turns `--since` into the documented
// diagnostic override: passing it means "show me the log", which is a request
// prepareAttach honors even for a session it would otherwise refuse.
func attachFlags(args []string) (ref string, cursor uint64, replay bool, own attachio.Options, err error) {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	since := fs.Uint64("since", 0, "resume from sequence number; 0 replays the whole event log (omit for the current screen)")
	view := fs.Bool("view", false, "watch without ever claiming control")
	take := fs.Bool("take", false,
		"take control on attach, even if another device has it (no effect where every attached terminal may already type)")
	fs.Parse(reorderArgs(fs, args))
	passed := passedFlags(fs)["since"]
	selector, err := requireSelector(fs, "attach")
	if err != nil {
		return "", 0, false, attachio.Options{}, err
	}
	if *view && *take {
		return "", 0, false, attachio.Options{}, commandError{
			message: "--view and --take ask for opposite things; pass one or neither"}
	}
	own = defaultOwnership()
	switch {
	case *view:
		own.Mode = terminal.ModeView
		// And it stays a non-claimer across reconnects, which reading the
		// flag back off Mode could not do: a plain attach that came back a
		// viewer asks for exactly this mode and keeps its take-control key.
		own.NeverClaim = true
	case *take:
		own.Take = true
	}
	return selector, attachio.Cursor(passed, *since), passed, own, nil
}

// ---------------------------------------------------------------------------
// resume / snapshot  (Advanced — contract §2.2)
//
// `stop` and `attach` are the pair an interactive person uses: attach resumes
// a stopped session on its own. resume stays for automation that wants the
// two halves separately, and snapshot for the low-level checkpoint. Neither
// appears in the default help.
// ---------------------------------------------------------------------------

func runResume(args []string) error {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print one machine-readable result document")
	fs.Parse(reorderArgs(fs, args))
	ref, err := requireSelector(fs, "resume")
	if err != nil {
		return err
	}

	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	var resp sessionEnvelope
	if err := c.Do(http.MethodPost, "/v0/sessions/"+id+"/resume", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return writeJSON(os.Stdout, schemaMutation, mutationDocument("resume", resp.Session.ID, true, resp.Session.State, "resume accepted"))
	}
	fmt.Printf("%s -> %s\n", safeField(resp.Session.ID), safeField(resp.Session.State))
	return nil
}

func runSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	fs.Parse(args)
	ref, err := requireSelector(fs, "snapshot")
	if err != nil {
		return err
	}

	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	var resp snapshotResponse
	if err := c.Do(http.MethodPost, "/v0/sessions/"+id+"/snapshot", nil, &resp); err != nil {
		return err
	}
	fmt.Println(resp.Ref)
	return nil
}

// ---------------------------------------------------------------------------
// push / pull  (Advanced)
//
// The two transfer commands. All the work is in internal/cli (Push/Pull) and
// protocol/workspace (the archive rules); what is left here is argument
// parsing and rendering, which is the same split every other subcommand takes.
// ---------------------------------------------------------------------------

func runPush(args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: rainier push <local-dir> <id|name>:<path>")
		os.Exit(2)
	}
	localDir, spec := fs.Arg(0), fs.Arg(1)
	ref, remotePath, err := splitRemote(spec)
	if err != nil {
		return err
	}
	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	if err := cli.Push(c, id, localDir, remotePath, progressPrinter("pushing")); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr)
	fmt.Printf("pushed %s to %s:%s\n", localDir, ref, remotePath)
	return nil
}

func runPull(args []string) error {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: rainier pull <id|name>:<path> <local-dir>")
		os.Exit(2)
	}
	spec, localDir := fs.Arg(0), fs.Arg(1)
	ref, remotePath, err := splitRemote(spec)
	if err != nil {
		return err
	}
	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	if err := cli.Pull(c, id, remotePath, localDir, progressPrinter("pulling")); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr)
	fmt.Printf("pulled %s:%s into %s\n", ref, remotePath, localDir)
	return nil
}

// splitRemote splits a "<session>:<path>" argument.
//
// At the FIRST colon: a session ref never contains one (an id is sess_<hex>,
// a name is a name), and a remote path may. Both halves are required — a bare
// directory is a different command's argument, and half of this one is always
// a mistake worth naming.
func splitRemote(spec string) (ref, path string, err error) {
	ref, path, ok := strings.Cut(spec, ":")
	if !ok || ref == "" || path == "" {
		return "", "", fmt.Errorf("%q must be <id|name>:<path>, e.g. dev-box:widget/vendor", spec)
	}
	return ref, path, nil
}

// progressPrinter returns a progress callback that rewrites one line on
// stderr — stderr so that redirecting stdout to a file (or a pipe) keeps the
// command's real output clean, and a carriage return so a long transfer is one
// line rather than a thousand.
func progressPrinter(verb string) func(done, total int64) {
	return func(done, total int64) {
		fmt.Fprintf(os.Stderr, "\r%s", cli.ProgressLine(verb, done, total))
	}
}

// ---------------------------------------------------------------------------
// agent login / ls / logout
//
// One person logs a coding agent in once, inside an ordinary session, and
// every later session of theirs — in any workspace they belong to, on any
// runner — starts with that agent already authenticated. These three verbs
// are the whole client surface of that: the login flow is the AGENT's own,
// unmodified, and no credential passes through this CLI in either direction.
// ---------------------------------------------------------------------------

const agentUsage = `usage: rainier agent <login|status|logout> [args]

  agent login <claude|codex>            sign the agent in, once, for every session
  agent status [--json]                 what is signed in
  agent logout <claude|codex> [--yes]   sign it out, everywhere

The login runs inside a throwaway session: the agent's own login flow, on your
screen, with nothing pasted anywhere and no credential passing through this
CLI. It uses your workspace's default environment, so there is nothing to
choose. When you exit the agent, the session is removed and the login stays.

status reports one of three things per agent: not configured, ready, or needs
attention. "ready" means a credential is stored — Rainier does not check it
with Anthropic or OpenAI, so it is not a promise that the agent will
authenticate.

logout destroys that login in every workspace you are in; a session already
running keeps what it holds until it exits. It requires --yes in a script.
--env is an advanced override for agent login (rainier help all).`

// agentLoginAttach is how `agent login` attaches to the session it created:
// attachWithRetry, exactly as `new` does — the same stream-everything attach,
// the same retry while the session is still starting. It is a variable only
// so this CLI's own tests can drive the arc around it (create → attach →
// remove → report) without a terminal on the other end.
var agentLoginAttach = attachWithRetry

// agentLoginSettle is how long `agent login` waits, once the login session's
// process has exited, for custody to record the credential before it removes
// the session. The session's exit is the same instant sessiond puts the
// agent's last write; removing the session in that same instant would race
// it. The wait ends the moment the version moves. agentLoginPoll is how often
// it asks; tests shorten both.
var (
	agentLoginSettle = 10 * time.Second
	agentLoginPoll   = 500 * time.Millisecond
)

func runAgent(args []string) error {
	// The synthetic "test" provider is off unless the host turned it on, and
	// BOTH ends have to agree about the table: the end-to-end suite's controld
	// enables it, and `agent login` refuses a provider it does not know before
	// sending anything, so a client that could not see the row would refuse
	// the very login the suite exists to prove. Reading the same variable the
	// suite sets is what keeps the two ends in step; an operator's CLI, which
	// sets nothing, never sees the row.
	if os.Getenv("RAINIER_E2E_TEST_AGENT") == "1" {
		controlapp.EnableTestAgentProvider = true
	}
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, agentUsage)
		os.Exit(2)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "login":
		return runAgentLogin(rest)
	case "status":
		return runAgentStatus(rest)
	case "ls":
		// Compatibility alias (contract §2.1).
		deprecated("agent ls", "agent status")
		return runAgentStatus(rest)
	case "logout":
		return runAgentLogout(rest)
	case "-h", "--help", "help":
		fmt.Fprintln(os.Stdout, agentUsage)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "rainier agent: unknown subcommand %q\n%s\n", sub, agentUsage)
		os.Exit(2)
		return nil
	}
}

// runAgentLogin runs the provider's own login flow in a throwaway session and
// reports what custody holds afterwards.
//
// The version is read BEFORE the session is created, and that ordering is the
// whole test for "did this work": a person who opened the agent, thought
// better of it, and quit has left custody exactly where it was, and the only
// honest thing to say is that nothing was written. Comparing against a
// version read after the fact would call every such exit a success.
//
// The session is removed whether the attach ended cleanly or not. It exists
// for one login and holds no work; leaving it running would leave a session
// whose whole purpose is over.
func runAgentLogin(args []string) error {
	fs := flag.NewFlagSet("agent login", flag.ExitOnError)
	env := fs.String("env", "", "environment whose image carries this provider's CLI (advanced; the workspace's default is used otherwise)")
	fs.Parse(reorderArgs(fs, args))
	p, err := agentProviderNamed(requireAgentProvider(fs, "rainier agent login <claude|codex>"))
	if err != nil {
		return err
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)
	ctx := context.Background()

	// Rainier does not install an agent, and a session started from no
	// environment runs the stock image, which has no agent CLI in it. An
	// ordinary hosted user should never have to know that: the workspace's
	// default environment is the answer, and the server owns which one that
	// is (contract §4.4, §5.2). When there is none, that is a readiness
	// problem and is reported as one — not as a flag somebody forgot.
	environmentName := *env
	if environmentName == "" {
		resolved, resolveErr := resolveDefaultEnvironment(ctx, c)
		if resolveErr != nil {
			if !errors.Is(resolveErr, errNoDefaultEnvironment) {
				return resolveErr
			}
			return fmt.Errorf("this workspace publishes no default environment, so there is no image known to carry %s's CLI. "+
				"Run `rainier status` to see what your workspace is missing, or name one with --env", p.Name)
		}
		environmentName = resolved.ID
	}

	before, err := agentRow(c, p.Name)
	if err != nil {
		return err
	}

	created, err := createSession(c, createSessionRequest{
		// Four hex characters: enough that two logins started in the same
		// minute do not collide on a name, short enough to read.
		Name:        fmt.Sprintf("agent-login-%s-%s", p.Name, cli.RandHex(2)),
		Environment: environmentName,
		Cmd:         p.LoginCmd,
		// Explicitly empty, never absent: an environment that declares
		// repositories would otherwise clone them into a session that exists
		// only to hold a login flow.
		Repos: &[]repoRequest{},
	}, "")
	if err != nil {
		return err
	}
	fmt.Println(created.ID)

	attachErr := agentLoginAttach(cfg, created.ID, terminal.SinceAll)
	// Give custody the moment it needs. sessiond puts the agent's last write
	// as the process exits, which is the same event that ended the attach;
	// the session is removed only once custody has moved, or once the settle
	// bound says the agent wrote nothing.
	after, rowErr := agentRow(c, p.Name)
	for deadline := time.Now().Add(agentLoginSettle); rowErr == nil && after.Version == before.Version && time.Now().Before(deadline); {
		time.Sleep(agentLoginPoll)
		after, rowErr = agentRow(c, p.Name)
	}
	if err := c.Do(http.MethodDelete, "/v0/sessions/"+created.ID, nil, nil); err != nil {
		// The removal failing is worth saying and is not worth losing the
		// login over: the credential is already in custody either way, and
		// the session is one `rainier rm` away.
		fmt.Fprintf(os.Stderr, "could not remove the login session %s: %s\n",
			safeField(created.ID), redactSecrets(cfg, err.Error()))
	}
	if attachErr != nil {
		return attachErr
	}
	if rowErr != nil {
		return rowErr
	}
	if after.Version == before.Version {
		// One more look after the removal: the shutdown put is sessiond's
		// last resort, and it lands as the container stops.
		if again, err := agentRow(c, p.Name); err == nil {
			after = again
		}
	}
	if after.Version == before.Version || agentReadiness(after) != agentReady {
		// Custody did not confirm a new usable credential. Do not report success.
		return errors.New("login did not complete: the agent wrote no credential")
	}
	fmt.Printf("logged in as of %s (v%d)\n", dashIfEmpty(after.Since), after.Version)
	return nil
}

// The three words `agent status` uses, and the whole of what this CLI is
// willing to claim about a coding agent's credential.
//
// "ready" says a credential is in custody and nothing more. Rainier does not
// call Anthropic or OpenAI to see whether it still works, so describing it as
// verified provider access would be a claim nobody here has checked — and the
// person who acts on it finds out inside a session, twenty minutes later. The
// human output says so in a footnote for the same reason.
const (
	agentNotConfigured  = "not configured"
	agentReady          = "ready"
	agentNeedsAttention = "needs attention"
)

// agentReadiness maps one custody row onto those three. A status this build
// does not recognize is "needs attention": it is neither the absence of a
// credential nor a credential known to be in place, and those are the only
// two things the other words may mean.
func agentReadiness(a agent) string {
	switch a.Status {
	case "none", "":
		return agentNotConfigured
	case "logged_in":
		return agentReady
	default:
		return agentNeedsAttention
	}
}

func runAgentStatus(args []string) error {
	fs := flag.NewFlagSet("agent status", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print one machine-readable agent status document")
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() != 0 {
		return usagef("usage: rainier agent status [--json]")
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	rows, err := fetchAgents(cli.NewClient(cfg))
	if err != nil {
		return err
	}
	// The caveat rides on stderr in both modes. It is the difference between
	// "a credential is stored" and "the provider will accept it", and a
	// person reading either form of this output needs to know which one they
	// are looking at.
	defer fmt.Fprintln(os.Stderr, `note: "ready" means a stored credential; Rainier does not verify it with the provider`)

	if *asJSON {
		return writeAgentStatusJSON(os.Stdout, rows)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "AGENT\tSTATUS\tSINCE")
	for _, a := range rows {
		since := "-"
		if a.Since != "" {
			since = formatAge(a.Since)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", safeField(a.Provider), agentReadiness(a), since)
	}
	return w.Flush()
}

func writeAgentStatusJSON(w io.Writer, rows []agent) error {
	out := make([]map[string]any, 0, len(rows))
	for _, a := range rows {
		entry := map[string]any{
			"provider": a.Provider,
			"status":   agentReadiness(a),
			"version":  a.Version,
			// Explicit, so a consumer cannot read "ready" as a verified
			// credential without having been told otherwise.
			"credential_verified": false,
		}
		if a.Since != "" {
			entry["since"] = a.Since
		}
		if len(a.Workspaces) > 0 {
			entry["workspaces"] = a.Workspaces
		}
		out = append(out, entry)
	}
	return writeJSON(w, schemaAgentStatus, map[string]any{"agents": out})
}

// runAgentLogout destroys one login and tells the person what that costs
// before it happens. Both halves of the caveat are true and neither is
// obvious: the credential is keyed by person and agent, not by workspace, so
// a logout reaches every workspace at once; and a session already running
// holds its own copy on disk until the revoke reaches it or it exits.
func runAgentLogout(args []string) error {
	fs := flag.NewFlagSet("agent logout", flag.ExitOnError)
	yes := fs.Bool("yes", false, "skip the confirmation prompt (for scripts)")
	fs.Parse(reorderArgs(fs, args))
	p, err := agentProviderNamed(requireAgentProvider(fs, "rainier agent logout <provider> [--yes]"))
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "this logs %s out of every workspace you are in; "+
		"a running agent keeps what it holds until it exits\n", p.Name)
	if !*yes {
		// No terminal means no answer is coming. Refusing is the only safe
		// outcome: prompting would hang, and proceeding would destroy a login
		// nobody consented to losing (contract §4.4).
		if !interactiveTerminal() {
			return usagef("logging out of %s destroys that login in every workspace; pass --yes to confirm", p.Name)
		}
		ok, err := confirm("continue? [y/N] ")
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stderr, "canceled")
			return nil
		}
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	if err := cli.NewClient(cfg).Do(http.MethodDelete, "/v0/agents/"+url.PathEscape(p.Name), nil, nil); err != nil {
		return err
	}
	fmt.Printf("logged out of %s\n", p.Name)
	return nil
}

// fetchAgents reads GET /v0/agents: one row per provider the server knows,
// in its table's order.
func fetchAgents(c *cli.Client) ([]agent, error) {
	return fetchAgentsContext(context.Background(), c)
}

func fetchAgentsContext(ctx context.Context, c *cli.Client) ([]agent, error) {
	var resp agentsEnvelope
	if err := c.DoContext(ctx, http.MethodGet, "/v0/agents", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Agents, nil
}

// unknownAgentProvider refuses a provider name against the SERVER's list
// rather than this build's table. Which agents exist is the server's answer —
// a CLI that judged it locally would refuse a provider the server had just
// added, and accept one it had removed.
func unknownAgentProvider(name string, rows []agent) error {
	names := make([]string, 0, len(rows))
	for _, a := range rows {
		names = append(names, a.Provider)
	}
	if len(names) == 0 {
		return fmt.Errorf("unknown agent %q; this server reports no coding agents", name)
	}
	return fmt.Errorf("unknown agent %q; this server supports: %s", name, strings.Join(names, ", "))
}

// agentRow reads one provider's row. A server that does not name the provider
// at all answers the zero row — version 0, no since — which is the same thing
// "you have not logged in" means, and lets the caller compare versions
// without a second failure mode.
func agentRow(c *cli.Client, provider string) (agent, error) {
	rows, err := fetchAgents(c)
	if err != nil {
		return agent{}, err
	}
	for _, a := range rows {
		if a.Provider == provider {
			return a, nil
		}
	}
	return agent{Provider: provider, Status: "none"}, nil
}

// agentProviderNamed resolves a provider name against the table, refusing an
// unknown one before any request is made. A typo costs nothing and reaches
// nothing — and the refusal names what this build does support, which is the
// only place a person can find that out.
func agentProviderNamed(name string) (controlapp.AgentProvider, error) {
	for _, p := range controlapp.AgentProviders() {
		if p.Name == name {
			return p, nil
		}
	}
	return controlapp.AgentProvider{}, usagef("unknown agent provider %q; this build supports: %s",
		name, strings.Join(agentProviderNames(), ", "))
}

// agentProviderNames is the table's names in its own order, for a usage line
// and a refusal.
func agentProviderNames() []string {
	rows := controlapp.AgentProviders()
	names := make([]string, 0, len(rows))
	for _, p := range rows {
		names = append(names, p.Name)
	}
	return names
}

// requireAgentProvider pulls the <provider> positional, exiting with usage
// (exit 2) rather than panicking when it is absent — requireRef's shape, with
// its own usage line since a provider is not a session ref.
func requireAgentProvider(fs *flag.FlagSet, usage string) string {
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s\n", usage)
		os.Exit(2)
	}
	return args[0]
}

// confirm asks one yes/no question and reads the answer from stdin. Anything
// but an explicit yes is a no — including end-of-file, which is what a script
// that forgot --yes looks like, and which must not be read as consent to
// destroy a login.
//
// The question goes to stderr, with the warning that precedes it. A prompt is
// an interaction and not a result, and keeping it off stdout is what lets
// `delete --json` promise that the document is the only thing there.
func confirm(question string) (bool, error) {
	fmt.Fprint(os.Stderr, question)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("reading the answer: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

// ---------------------------------------------------------------------------
// secret set / ls / rm
// ---------------------------------------------------------------------------

// secretUsage is printed for `rainier secret` with no (or an unknown)
// subcommand, and for a subcommand missing its NAME.
const secretUsage = `usage: rainier secret <set|ls|rm> [args]

  secret set <NAME> [--value V]   store (or replace) a secret; admin only
  secret ls                       list secret names and timestamps
  secret rm <NAME>                delete a secret; admin only

With no --value, "secret set" reads the value from stdin — a pipe or a
redirect keeps it out of your shell history and out of the process table:

  cat token.txt | rainier secret set GH_TOKEN
  rainier secret set GH_TOKEN < token.txt

One trailing newline is stripped, so an "echo value |" pipeline stores what
you'd expect. Values are write-only: nothing in this CLI or the API can read
one back — replace it if you lose it.`

func runSecret(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, secretUsage)
		os.Exit(2)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "set":
		return runSecretSet(rest)
	case "ls":
		return runSecretLs(rest)
	case "rm":
		return runSecretRm(rest)
	case "-h", "--help", "help":
		fmt.Fprintln(os.Stderr, secretUsage)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "rainier secret: unknown subcommand %q\n%s\n", sub, secretUsage)
		os.Exit(2)
		return nil
	}
}

func runSecretSet(args []string) error {
	fs := flag.NewFlagSet("secret set", flag.ExitOnError)
	value := fs.String("value", "", "the secret value; omit it to read the value from stdin, which keeps it out of your shell history")
	fs.Parse(reorderArgs(fs, args))
	name := requireSecretName(fs, "rainier secret set <NAME> [--value V]")

	cfg, err := requireLogin()
	if err != nil {
		return err
	}

	v := *value
	if v == "" {
		v, err = readSecretFromStdin()
		if err != nil {
			return err
		}
	}
	if v == "" {
		return fmt.Errorf("secret value is empty: pass --value, or pipe the value on stdin")
	}

	c := cli.NewClient(cfg)
	if err := c.Do(http.MethodPut, "/v0/secrets/"+url.PathEscape(name), putSecretRequest{Value: v}, nil); err != nil {
		return err
	}
	// The name, never the value — this line can land in a terminal recording
	// or a CI log.
	fmt.Printf("set %s\n", name)
	return nil
}

// readSecretFromStdin reads the whole of stdin as one secret value,
// stripping a single trailing newline (so `echo hunter2 | rainier secret
// set` stores "hunter2", not "hunter2\n") and nothing else — a value that
// genuinely ends in blank lines keeps all but that last one.
//
// When stdin is a terminal there is nothing piped in, and a silent wait for
// EOF looks exactly like a hang, so say what's happening first.
func readSecretFromStdin() (string, error) {
	if fi, err := os.Stdin.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		fmt.Fprintln(os.Stderr, "reading the secret value from stdin; end with Ctrl-D (or pass --value)")
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("reading the secret value from stdin: %w", err)
	}
	v := string(raw)
	v = strings.TrimSuffix(v, "\n")
	v = strings.TrimSuffix(v, "\r")
	return v, nil
}

func runSecretLs(args []string) error {
	fs := flag.NewFlagSet("secret ls", flag.ExitOnError)
	fs.Parse(args)

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)

	var resp secretsEnvelope
	if err := c.Do(http.MethodGet, "/v0/secrets", nil, &resp); err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tCREATED\tUPDATED")
	for _, s := range resp.Secrets {
		fmt.Fprintf(w, "%s\t%s\t%s\n", s.Name, formatAge(s.CreatedAt), formatAge(s.UpdatedAt))
	}
	return w.Flush()
}

func runSecretRm(args []string) error {
	fs := flag.NewFlagSet("secret rm", flag.ExitOnError)
	fs.Parse(args)
	name := requireSecretName(fs, "rainier secret rm <NAME>")

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)
	if err := c.Do(http.MethodDelete, "/v0/secrets/"+url.PathEscape(name), nil, nil); err != nil {
		return err
	}
	fmt.Println("removed", name)
	return nil
}

// requireSecretName pulls the <NAME> positional a secret subcommand needs,
// exiting with usage (exit 2) rather than panicking when it's absent — the
// same shape as requireRef, with its own usage line since a secret name is
// not an <id|name> session ref.
func requireSecretName(fs *flag.FlagSet, usage string) string {
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s\n", usage)
		os.Exit(2)
	}
	return args[0]
}

// ---------------------------------------------------------------------------
// env create / ls / show / update / rm
// ---------------------------------------------------------------------------

const envUsage = `usage: rainier env <create|ls|show|update|rm> [args]

  env create <name> [flags]     define an environment; admin only
  env ls                        list environments (NAME ID IMAGE CACHED)
  env show <id|name>            print one environment as JSON
  env update <id|name> [flags]  change only the fields you pass; admin only
  env rm <id|name>              delete an environment; admin only

flags for create and update:
  --image IMG                base container image (required at create)
  --setup-file ./setup.sh    shell script run once when a session is first built
  --egress a.com,b.com       default egress allowlist ("" clears it)
  --secret-ref NAME          team secret to inject; repeatable ("" clears them)
  --placement RUNNER         pin this environment's sessions to one runner
  --capability NAME          require a runner capability (e.g. gpu); repeatable
  --setup-timeout-sec N      how long setup may run (0 = server default)
  --connector-json '<json>'  a connector object, or an array of them; repeatable
  --from-devcontainer [dir]  take --image from a devcontainer.json (create only)
  --name NEW                 rename (update only)

Connectors are passed as raw JSON in v0 — the vocabulary is github, files,
tunnel and browser, validated by the server and stored verbatim; the plans
that give them behavior bring friendlier flags with them. Example:

  rainier env create dev --image golang:1.22 --setup-file ./setup.sh \
    --connector-json '{"type":"github","repo":"acme/widgets"}'

CACHED in "env ls" is yes while a built snapshot still matches the
environment's current image+setup; editing either makes it no until the next
session rebuilds the cache.`

func runEnv(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, envUsage)
		os.Exit(2)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		return runEnvCreate(rest)
	case "ls":
		return runEnvLs(rest)
	case "show":
		return runEnvShow(rest)
	case "update":
		return runEnvUpdate(rest)
	case "rm":
		return runEnvRm(rest)
	case "-h", "--help", "help":
		fmt.Fprintln(os.Stderr, envUsage)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "rainier env: unknown subcommand %q\n%s\n", sub, envUsage)
		os.Exit(2)
		return nil
	}
}

// envFlags is the flag set `env create` and `env update` share, registered
// on fs. Both commands read the same fields; only what they do with an
// unpassed one differs (create sends its zero value, update omits it).
type envFlags struct {
	image        *string
	setupFile    *string
	initFile     *string
	egress       *string
	placement    *string
	timeout      *int
	initTimeout  *int
	name         *string
	secretRefs   stringsFlag
	capabilities stringsFlag
	connectors   stringsFlag
	devcontainer optionalPathFlag
}

func registerEnvFlags(fs *flag.FlagSet, forUpdate bool) *envFlags {
	f := &envFlags{
		image:       fs.String("image", "", "base container image"),
		setupFile:   fs.String("setup-file", "", "path to a shell script run once when a session is first built"),
		initFile:    fs.String("init-file", "", "path to a shell script run on every session boot, after the code is in place"),
		egress:      fs.String("egress", "", "comma-separated egress allowlist"),
		placement:   fs.String("placement", "", "pin this environment's sessions to one runner"),
		timeout:     fs.Int("setup-timeout-sec", 0, "how long the setup script may run (0 = server default)"),
		initTimeout: fs.Int("init-timeout-sec", 0, "how long the init script may run (0 = server default)"),
	}
	fs.Var(&f.secretRefs, "secret-ref", "name of a team secret to inject; repeatable")
	fs.Var(&f.capabilities, "capability",
		"a capability a runner must advertise before this environment's sessions land on it; repeatable")
	fs.Var(&f.connectors, "connector-json", "a connector object, or an array of them, as raw JSON; repeatable")
	if forUpdate {
		f.name = fs.String("name", "", "new name for this environment")
	} else {
		fs.Var(&f.devcontainer, "from-devcontainer", "read image from a devcontainer.json (optionally in this dir)")
	}
	return f
}

func runEnvCreate(args []string) error {
	fs := flag.NewFlagSet("env create", flag.ExitOnError)
	f := registerEnvFlags(fs, false)
	fs.Parse(reorderArgs(fs, args))
	name := requireEnvRef(fs, "rainier env create <name> [flags]")

	image := *f.image
	dcDir, extra := devcontainerDir(f.devcontainer, fs.Args()[1:])
	if len(extra) > 0 {
		return fmt.Errorf("unexpected argument(s) after the environment name: %s", strings.Join(extra, " "))
	}
	if f.devcontainer.set {
		dc, err := readDevcontainer(dcDir)
		if err != nil {
			return err
		}
		// Straight to stderr: what was ignored is a message for the operator,
		// and stdout stays the new environment's id for a script to capture.
		for _, line := range dc.report() {
			fmt.Fprintln(os.Stderr, line)
		}
		if image == "" {
			image = dc.Image
		}
	}
	if image == "" {
		return fmt.Errorf("an image is required: pass --image (a devcontainer that builds from a Dockerfile has no image for rainier to take)")
	}

	setup, err := readScriptFile(*f.setupFile, "setup")
	if err != nil {
		return err
	}
	init, err := readScriptFile(*f.initFile, "init")
	if err != nil {
		return err
	}
	connectors, err := assembleConnectors(f.connectors)
	if err != nil {
		return err
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)

	body := createEnvironmentRequest{
		Name:            name,
		Image:           image,
		Setup:           setup,
		Init:            init,
		InitTimeoutSec:  *f.initTimeout,
		EgressAllow:     splitList(*f.egress),
		SecretRefs:      nonEmpty(f.secretRefs),
		Connectors:      connectors,
		Placement:       *f.placement,
		Capabilities:    nonEmpty(f.capabilities),
		SetupTimeoutSec: *f.timeout,
	}
	var resp environmentEnvelope
	if err := c.Do(http.MethodPost, "/v0/environments", body, &resp); err != nil {
		return err
	}
	fmt.Println(resp.Environment.ID)
	return nil
}

func runEnvLs(args []string) error {
	fs := flag.NewFlagSet("env ls", flag.ExitOnError)
	fs.Parse(args)

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)

	var resp environmentsEnvelope
	if err := c.Do(http.MethodGet, "/v0/environments", nil, &resp); err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tID\tIMAGE\tCACHED")
	for _, e := range resp.Environments {
		cached := "no"
		if envCached(e) {
			cached = "yes"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", e.Name, e.ID, e.Image, cached)
	}
	return w.Flush()
}

// envCached reports whether an environment's cached snapshot was built from
// the image+setup it still has. A snapshot from superseded setup is not a
// cache at all — the next session rebuilds — so it must not read as one.
func envCached(e environment) bool {
	return e.SnapshotHash != "" && e.SnapshotHash == e.SetupHash
}

func runEnvShow(args []string) error {
	fs := flag.NewFlagSet("env show", flag.ExitOnError)
	fs.Parse(args)
	ref := requireEnvRef(fs, "rainier env show <id|name>")

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)

	// Decoded as raw JSON and re-indented, so what's printed is exactly what
	// the server said — including any field this CLI's own struct doesn't
	// know about yet.
	var resp struct {
		Environment json.RawMessage `json:"environment"`
	}
	if err := c.Do(http.MethodGet, "/v0/environments/"+url.PathEscape(ref), nil, &resp); err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, resp.Environment, "", "  "); err != nil {
		return err
	}
	fmt.Println(pretty.String())
	return nil
}

func runEnvUpdate(args []string) error {
	fs := flag.NewFlagSet("env update", flag.ExitOnError)
	f := registerEnvFlags(fs, true)
	fs.Parse(reorderArgs(fs, args))
	ref := requireEnvRef(fs, "rainier env update <id|name> [flags]")

	// A patch carries only the fields the caller actually passed: an absent
	// flag means "leave it alone", which is not the same request as a flag
	// set to its zero value ("clear it").
	passed := passedFlags(fs)
	patch := map[string]any{}
	if passed["name"] {
		patch["name"] = *f.name
	}
	if passed["image"] {
		patch["image"] = *f.image
	}
	if passed["setup-file"] {
		setup, err := readScriptFile(*f.setupFile, "setup")
		if err != nil {
			return err
		}
		patch["setup"] = setup
	}
	if passed["init-file"] {
		init, err := readScriptFile(*f.initFile, "init")
		if err != nil {
			return err
		}
		patch["init"] = init
	}
	if passed["egress"] {
		patch["egress_allow"] = splitList(*f.egress)
	}
	if passed["secret-ref"] {
		patch["secret_refs"] = nonEmpty(f.secretRefs)
	}
	if passed["connector-json"] {
		connectors, err := assembleConnectors(f.connectors)
		if err != nil {
			return err
		}
		patch["connectors"] = connectors
	}
	if passed["placement"] {
		patch["placement"] = *f.placement
	}
	if passed["capability"] {
		patch["capabilities"] = nonEmpty(f.capabilities)
	}
	if passed["setup-timeout-sec"] {
		patch["setup_timeout_sec"] = *f.timeout
	}
	if passed["init-timeout-sec"] {
		patch["init_timeout_sec"] = *f.initTimeout
	}
	if len(patch) == 0 {
		return fmt.Errorf("nothing to update: pass at least one of --name, --image, --setup-file, --init-file, --egress, --secret-ref, --connector-json, --placement, --capability, --setup-timeout-sec, --init-timeout-sec")
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)

	var resp environmentEnvelope
	if err := c.Do(http.MethodPatch, "/v0/environments/"+url.PathEscape(ref), patch, &resp); err != nil {
		return err
	}
	fmt.Println(resp.Environment.ID)
	return nil
}

func runEnvRm(args []string) error {
	fs := flag.NewFlagSet("env rm", flag.ExitOnError)
	fs.Parse(args)
	ref := requireEnvRef(fs, "rainier env rm <id|name>")

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := cli.NewClient(cfg)
	if err := c.Do(http.MethodDelete, "/v0/environments/"+url.PathEscape(ref), nil, nil); err != nil {
		return err
	}
	fmt.Println("removed", ref)
	return nil
}

// requireEnvRef pulls the <name> or <id|name> positional an env subcommand
// needs, exiting with usage (exit 2) rather than panicking when it's absent.
func requireEnvRef(fs *flag.FlagSet, usage string) string {
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s\n", usage)
		os.Exit(2)
	}
	return args[0]
}

// passedFlags returns the name of every flag the caller actually passed —
// flag.FlagSet.Visit's whole purpose, and what makes `env update` a patch
// rather than a full replacement.
func passedFlags(fs *flag.FlagSet) map[string]bool {
	out := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { out[f.Name] = true })
	return out
}

// stringsFlag collects a repeatable string flag (--secret-ref,
// --connector-json).
type stringsFlag []string

func (s *stringsFlag) String() string { return strings.Join(*s, ",") }

func (s *stringsFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// optionalPathFlag is a flag whose value is optional: "--from-devcontainer"
// alone means the current directory, "--from-devcontainer=./repo" names one.
// IsBoolFlag is what makes the bare form legal — and, just as importantly,
// what stops flag.Parse and reorderArgs from swallowing the environment name
// that follows it as this flag's value.
type optionalPathFlag struct {
	set  bool
	path string
}

func (f *optionalPathFlag) String() string { return f.path }

func (f *optionalPathFlag) Set(v string) error {
	f.set = true
	if v == "" || v == "true" {
		f.path = "."
	} else {
		f.path = v
	}
	return nil
}

func (f *optionalPathFlag) IsBoolFlag() bool { return true }

// devcontainerDir resolves the directory --from-devcontainer names, given the
// flag and whatever positional arguments were left over after the
// environment name.
//
// An optional-value flag never consumes the next token (that is what
// IsBoolFlag means), so "--from-devcontainer ./repo" — the spelling with a
// space, which is what anyone types first — leaves ./repo sitting in the
// positionals instead. Taking it from there is what makes both that and
// "--from-devcontainer=./repo" mean the same thing. Anything else left over
// is returned as rest, for the caller to refuse rather than silently ignore.
func devcontainerDir(f optionalPathFlag, extra []string) (dir string, rest []string) {
	if f.set && f.path == "." && len(extra) == 1 {
		return extra[0], nil
	}
	return f.path, extra
}

// splitList splits a comma-separated flag value into its entries, dropping
// blanks. An empty value is an empty list, which is how `env update
// --egress ""` clears one.
func splitList(v string) []string {
	out := []string{}
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// nonEmpty drops blank entries from a repeatable flag's values, so
// `--secret-ref ""` clears the list rather than asking the server to inject a
// secret with no name.
func nonEmpty(values []string) []string {
	out := []string{}
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// readScriptFile reads a --setup-file or --init-file path, or returns "" when
// none was given. what names the script in the error, so an operator who
// passes two of these and fatfingers one knows which.
func readScriptFile(path, what string) (string, error) {
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading the %s script: %w", what, err)
	}
	return string(data), nil
}

// assembleConnectors turns the --connector-json values into the JSON array
// the API takes. Each value may be one connector object or an array of them,
// and every value's bytes are passed through UNCHANGED: the server rejects
// unknown fields, and a CLI that re-marshaled these would turn a typo the
// server would have caught into a key it silently dropped.
func assembleConnectors(values []string) (json.RawMessage, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := []json.RawMessage{}
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		var arr []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &arr); err == nil {
			out = append(out, arr...)
			continue
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
			return nil, fmt.Errorf("--connector-json %s: must be a JSON connector object, or an array of them", v)
		}
		out = append(out, json.RawMessage(trimmed))
	}
	return json.Marshal(out)
}

// devcontainerHints is everything `--from-devcontainer` takes from a
// devcontainer.json: the image, and the name of every other key that was
// present.
type devcontainerHints struct {
	Path    string   // the file that was read
	Image   string   // the "image" field, "" when it has none
	Ignored []string // every other key present, sorted
}

// readDevcontainer reads dir's devcontainer.json — dir itself if it names a
// file, else ".devcontainer/devcontainer.json", else "devcontainer.json"
// beside it — and returns its image plus every other key it saw.
//
// rainier reads exactly ONE field. A devcontainer is a far larger contract
// (features, mounts, lifecycle commands, host requirements) and honoring half
// of it silently would be worse than ignoring it loudly, so the caller prints
// what was ignored rather than pretending the file was applied.
func readDevcontainer(dir string) (devcontainerHints, error) {
	if dir == "" {
		dir = "."
	}
	var candidates []string
	if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
		candidates = []string{dir}
	} else {
		candidates = []string{
			filepath.Join(dir, ".devcontainer", "devcontainer.json"),
			filepath.Join(dir, "devcontainer.json"),
		}
	}

	var path string
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			path = c
			break
		}
	}
	if path == "" {
		return devcontainerHints{}, fmt.Errorf("no devcontainer.json found (looked at %s)", strings.Join(candidates, ", "))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return devcontainerHints{}, fmt.Errorf("reading %s: %w", path, err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		// devcontainer.json is officially JSONC; rainier parses plain JSON, so
		// say so instead of leaving the operator with a bare offset.
		return devcontainerHints{}, fmt.Errorf("%s: %v — rainier reads plain JSON here, so comments and trailing commas are not supported; pass --image instead", path, err)
	}

	hints := devcontainerHints{Path: path}
	for key, value := range raw {
		if key == "image" {
			if err := json.Unmarshal(value, &hints.Image); err != nil {
				return devcontainerHints{}, fmt.Errorf("%s: image must be a string", path)
			}
			continue
		}
		hints.Ignored = append(hints.Ignored, key)
	}
	slices.Sort(hints.Ignored)
	return hints, nil
}

// report is what --from-devcontainer prints: the file it read, the image it
// took, and the name of every key it ignored — the whole point of the flag
// being that you can see it did not honor the rest.
func (d devcontainerHints) report() []string {
	image := d.Image
	if image == "" {
		image = "(none — the file names no image)"
	}
	lines := []string{fmt.Sprintf("read %s: image = %s", d.Path, image)}
	if len(d.Ignored) == 0 {
		lines = append(lines, "ignored no other devcontainer keys")
	} else {
		lines = append(lines, fmt.Sprintf("ignored %d other devcontainer key(s): %s",
			len(d.Ignored), strings.Join(d.Ignored, ", ")))
	}
	return lines
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

// requireLogin loads the config and fails with guidance when there's
// nothing to talk to yet.
func requireLogin() (cli.Config, error) {
	cfg, err := cli.Load()
	if err != nil {
		return cli.Config{}, err
	}
	if cfg.ServerURL == "" || cfg.Token == "" {
		return cli.Config{}, fmt.Errorf("not logged in: run `rainier login` first")
	}
	return cfg, nil
}

// resolveClientAndID is the requireLogin+resolveSessionID pair commands that
// operate on non-terminal sessions need. It returns cfg too — runAttach needs
// ServerURL/Token beyond just the resolved id; the other callers discard it.
// rm uses resolveClientAndIDIncludingTerminal because a failed session can
// still own a live container.
func resolveClientAndID(ref string) (cli.Config, *cli.Client, string, error) {
	return resolveClientAndIDWithScope(ref, resolveActive)
}

// resolveClientAndIDForAttach includes failed rows because a failed setup can
// still have a live diagnostic terminal. Other historical terminal rows stay
// hidden, so a stale dead/destroyed name is never selected as an attach target.
func resolveClientAndIDForAttach(ref string) (cli.Config, *cli.Client, string, error) {
	return resolveClientAndIDWithScope(ref, resolveAttachable)
}

// resolveClientAndIDIncludingTerminal is the rm variant. A failed create is
// terminal in controld's store but may still own a live container and runner
// slot, so deletion must be able to find it by the same name the user passed
// to new.
func resolveClientAndIDIncludingTerminal(ref string) (cli.Config, *cli.Client, string, error) {
	return resolveClientAndIDWithScope(ref, resolveAll)
}

func resolveClientAndIDWithScope(ref string, scope sessionResolveScope) (cli.Config, *cli.Client, string, error) {
	cfg, err := requireLogin()
	if err != nil {
		return cli.Config{}, nil, "", err
	}
	c := cli.NewClient(cfg)
	// `current` is settled from local state, before any name lookup: it is a
	// keyword, and the id it names is this context's own record of what the
	// person last worked in (contract §3.2).
	//
	// It is then CONFIRMED against the server before any command acts on it,
	// which is the difference between a clear failure and a confusing one.
	// The id came from this machine's memory, not from anything the person
	// typed, so a bare "no session sess_abc" reads as a bug in the CLI; and
	// `delete current` would otherwise treat the 404 as "already gone" and
	// exit 0, reporting a successful deletion of a session it never targeted.
	if ref == currentSelector {
		id, err := currentSessionID(cfg)
		if err != nil {
			return cli.Config{}, nil, "", err
		}
		if _, err := getSession(c, id); err != nil {
			return cli.Config{}, nil, "", staleCurrentSession(id, err)
		}
		return cfg, c, id, nil
	}
	id, err := resolveSessionIDWithScope(c, cfg.OwnerID, ref, scope)
	if err != nil {
		return cli.Config{}, nil, "", err
	}
	return cfg, c, id, nil
}

// requireSelector pulls the <session> positional every session command needs.
// It returns a usage error rather than exiting, so the caller's own flag
// validation and this one produce the same exit code by the same path
// (contract §6.1).
func requireSelector(fs *flag.FlagSet, cmd string) (string, error) {
	args := fs.Args()
	if len(args) < 1 {
		return "", usagef("usage: rainier %s <session>   (a sess_ id, a session name, or `current`)", cmd)
	}
	if len(args) > 1 {
		return "", usagef("rainier %s takes one session; got %d", cmd, len(args))
	}
	return args[0], nil
}

// resolveSessionID resolves ref to a session id: a "sess_" prefix is
// already an id, verbatim; anything else is looked up through the exact-name
// filter on paginated GET /v0/sessions. The CLI still makes the ambiguity
// decision because that collection is team-visible. The default resolver sees
// only non-terminal sessions. rm's variant opts into terminal rows because a
// failed create can still own a live container. The caller's own rows take
// precedence over teammates' team-visible rows; within that set, an active
// row that reused a terminal row's name wins and the historical one requires
// its id.
//
// Session names are unique only per owner (design), while GET /v0/sessions
// is team-visible — two teammates can each have a session named e.g.
// "dev-box". Every exact-name match is collected across every page
// before deciding anything (a match on page 1 does not short-circuit the
// search): acting on "whichever paginated first" risks silently suspending
// or deleting a teammate's session by mistake.
//
//   - Exactly one match: use it.
//   - No match: "no session named %q found".
//   - More than one match: if myOwnerID is non-empty and exactly one match
//     belongs to it (owner-preference — see myOwnerID below), use that one;
//     otherwise refuse and list every match's id and owner so the caller
//     can pass the id explicitly.
//
// myOwnerID is the caller's own user id, cached in cli.Config.OwnerID by
// `rainier login` from the identity controld returns (the same one GET
// /v0/me answers with). It is the same string a session row carries as
// owner_id, which is the whole reason this comparison is possible. Empty
// only for a config written before logins carried it — owner-preference is
// then unavailable and an ambiguous name errors, exactly as it did.
func resolveSessionID(c *cli.Client, myOwnerID, ref string) (string, error) {
	return resolveSessionIDWithScope(c, myOwnerID, ref, resolveActive)
}

func resolveSessionIDWithTerminal(c *cli.Client, myOwnerID, ref string, includeTerminal bool) (string, error) {
	if includeTerminal {
		return resolveSessionIDWithScope(c, myOwnerID, ref, resolveAll)
	}
	return resolveSessionIDWithScope(c, myOwnerID, ref, resolveActive)
}

type sessionResolveScope int

const (
	resolveActive sessionResolveScope = iota
	resolveAttachable
	resolveAll
)

func resolveSessionIDWithScope(c *cli.Client, myOwnerID, ref string, scope sessionResolveScope) (string, error) {
	if strings.HasPrefix(ref, "sess_") {
		if strings.ContainsAny(ref, "/?#%\\") || strings.TrimSpace(ref) != ref {
			return "", usagef("invalid session id")
		}
		return ref, nil
	}

	type match struct {
		id, owner string
		terminal  bool
	}
	var matches []match

	cursor := ""
	seen := map[string]bool{}
	for {
		q := url.Values{}
		if scope != resolveActive {
			q.Set("all", "true")
		}
		q.Set("name", ref)
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		path := "/v0/sessions"
		if enc := q.Encode(); enc != "" {
			path += "?" + enc
		}
		var page sessionsEnvelope
		if err := c.Do(http.MethodGet, path, nil, &page); err != nil {
			return "", err
		}
		for _, s := range page.Sessions {
			if s.Name == ref {
				if scope == resolveAttachable && terminalSessionState(s.State) && s.State != "failed" {
					continue
				}
				matches = append(matches, match{
					id: s.ID, owner: s.OwnerID, terminal: terminalSessionState(s.State),
				})
			}
		}
		if page.NextCursor == "" {
			break
		}
		if seen[page.NextCursor] {
			return "", errors.New("session listing repeated a pagination cursor")
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
	// GET /v0/sessions is team-visible, but lifecycle commands should operate
	// on the caller's own same-name row whenever one exists. Do this before
	// active-history preference: my failed row is still my cleanup target even
	// if a teammate currently has an active session with the same name.
	if myOwnerID != "" {
		var mine []match
		for _, m := range matches {
			if m.owner == myOwnerID {
				mine = append(mine, m)
			}
		}
		if len(mine) > 0 {
			matches = mine
		}
	}

	// Name uniqueness applies only to non-terminal sessions. Preserve the
	// established `rm name` behavior when an active session has reused an old
	// terminal session's name; the historical row remains addressable by id.
	if myOwnerID != "" {
		var activeMatches []match
		for _, m := range matches {
			if !m.terminal {
				activeMatches = append(activeMatches, m)
			}
		}
		if len(activeMatches) > 0 {
			matches = activeMatches
		}
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no session named %q found", ref)
	case 1:
		return matches[0].id, nil
	}

	if myOwnerID != "" {
		mine, count := -1, 0
		for i, m := range matches {
			if m.owner == myOwnerID {
				mine, count = i, count+1
			}
		}
		if count == 1 {
			return matches[mine].id, nil
		}
	}

	listed := make([]string, len(matches))
	for i, m := range matches {
		listed[i] = fmt.Sprintf("%s (owner %s)", m.id, m.owner)
	}
	return "", fmt.Errorf("ambiguous name %q matches %d sessions: %s — use the session id",
		ref, len(matches), strings.Join(listed, ", "))
}

func terminalSessionState(state string) bool {
	switch state {
	case "canceled", "failed", "dead", "destroyed":
		return true
	default:
		return false
	}
}

// wsURLFor renders id's attach URL against serverURL's http(s) base,
// switched to ws(s) — the same scheme-swap controld's own attachBackURL
// does for the runner side of this same plane.
func wsURLFor(serverURL, id string) string {
	ws := serverURL
	switch {
	case strings.HasPrefix(ws, "https://"):
		ws = "wss://" + strings.TrimPrefix(ws, "https://")
	case strings.HasPrefix(ws, "http://"):
		ws = "ws://" + strings.TrimPrefix(ws, "http://")
	}
	return strings.TrimRight(ws, "/") + "/v0/sessions/" + id + "/attach"
}

// requireRef is requireSelector's older sibling, kept for the two transfer
// commands whose positional is not a bare session ref. It exits rather than
// returning, which is why new code uses requireSelector instead.
func requireRef(fs *flag.FlagSet, cmd string) string {
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: rainier %s <id|name>\n", cmd)
		os.Exit(2)
	}
	return args[0]
}

// reorderArgs rearranges args so every flag (and its value, for a
// non-boolean flag) precedes any positional argument, working around the
// stdlib flag package's stop-parsing-at-first-positional behavior. It
// exists because this CLI's own documented surface puts flags AFTER the
// positional in several commands (e.g. "suspend <id|name> [--cold]",
// "attach <id|name> [--since N]") — flag.Parse alone would silently treat
// a trailing --cold as a positional argument instead of recognizing it.
// "--" always ends flag scanning and everything from it onward (including
// itself) is passed through as positional, matching flag.Parse's own rule
// (this is what lets `new`'s trailing "-- CMD ARGS..." reach fs.Args()
// unchanged).
func reorderArgs(fs *flag.FlagSet, args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i:]...)
			break
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			positional = append(positional, a)
			continue
		}
		name, _, hasEq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		flags = append(flags, a)
		f := fs.Lookup(name)
		if f == nil || hasEq {
			continue // unknown flag (let Parse report it) or value already embedded via "="
		}
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			continue // boolean flag takes no separate value token
		}
		if i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return append(flags, positional...)
}

// rememberControl records what this device held on this session, so `rainier
// info` can answer "is it me?" without the API ever naming another client.
// The generation is enough: nobody else can hold generation N without
// advancing past it, so a live lease still at the number this device was
// granted is this device's.
//
// A failure to write is not worth failing an attach over — the terminal
// worked, and the worst case is one line of `info` reading "another device"
// about a session this device controls.
func rememberControl(cfg cli.Config, id string, out attachio.Outcome) {
	if out.Mode == "" {
		return // nothing was negotiated; there is nothing to remember
	}
	held := ""
	if out.Mode == terminal.ModeControl {
		held = strconv.FormatUint(out.Generation, 10)
	}
	_ = cli.UpdateConfig(func(latest *cli.Config) error {
		name := cfg.ActiveName()
		ctx, ok := latest.Contexts[name]
		if !ok {
			return nil
		}
		// held is empty when this device ended as a viewer, which forgets
		// the generation rather than leaving a stale claim to it behind.
		ctx.LastControlSession, ctx.LastControlGeneration = id, held
		latest.UpdateContext(name, ctx)
		return nil
	})
}

// inputRow renders `info`'s row about who may type: its label and its value.
//
// Under a shared attachment policy the question "who is the controller" has no
// answer, because there is no controller — so the row says what the rule is and
// how many terminals are attached under it. Under an exclusive policy it is
// today's row, word for word, and that is also what an older server (which
// reports no policy at all) gets.
func inputRow(cfg cli.Config, s session) (label, value string) {
	if s.Input.sharedInput() {
		return "Input", fmt.Sprintf("shared (%d attached)", s.Input.Attached)
	}
	return "Controller", controllerLine(cfg, s)
}

// controllerLine renders `info`'s Controller row from what the server says
// and what this device remembers. Three answers, and no fourth: the API
// discloses whether somebody holds control, never who, so "another device" is
// the honest name for everybody that is not this one.
func controllerLine(cfg cli.Config, s session) string {
	if !s.Controller.Held {
		return "none"
	}
	ctx, ok := cfg.Active()
	if ok && ctx.LastControlSession == s.ID && ctx.LastControlGeneration != "" &&
		ctx.LastControlGeneration == s.Controller.Generation {
		return "this device"
	}
	return "another device"
}
