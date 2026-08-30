// Command rainier is the client CLI for controld: log in, create sessions,
// list them, attach to them, and drive their lifecycle (suspend/resume/
// snapshot/rm). Subcommand dispatch follows runnerctl's style — stdlib
// `flag` per subcommand, no cobra.
package main

import (
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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"rainier/internal/attachio"
	"rainier/internal/cli"
	"rainier/internal/wire"
	"rainier/internal/xfer"
)

// devicePollTimeout bounds a single poll request to GitHub's device-flow
// token endpoint — without it, a stalled or blackholed request could park
// the whole `login --client-id` flow indefinitely instead of just failing
// that one poll and trying again on the next interval tick.
const devicePollTimeout = 15 * time.Second

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	cmd, rest := os.Args[1], os.Args[2:]

	var err error
	switch cmd {
	case "login":
		err = runLogin(rest)
	case "new":
		err = runNew(rest)
	case "ls":
		err = runLs(rest)
	case "attach":
		err = runAttach(rest)
	case "suspend":
		err = runSuspend(rest)
	case "resume":
		err = runResume(rest)
	case "snapshot":
		err = runSnapshot(rest)
	case "rm":
		err = runRm(rest)
	case "diff":
		err = runDiff(rest)
	case "push":
		err = runPush(rest)
	case "pull":
		err = runPull(rest)
	case "creds":
		err = runCreds(rest)
	case "secret":
		err = runSecret(rest)
	case "env":
		err = runEnv(rest)
	case "-h", "--help", "help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "rainier: unknown command %q\n", cmd)
		printUsage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `usage: rainier <command> [flags]

commands:
  login    [--from-gh] [--token GH_TOKEN] [--client-id ID] [--server URL]
           [--refresh PROVIDER]
  new      [--name N] [--env ENV] [--image IMG] [--egress host,host] [--detach]
           [-- CMD ARGS...]
  ls       [--all]
  attach   <id|name> [--since N]
  suspend  <id|name> [--cold]
  resume   <id|name>
  snapshot <id|name>
  rm       <id|name>
  diff     <id|name>
  push     <local-dir> <id|name>:<path>
  pull     <id|name>:<path> <local-dir>
  creds
  secret   set <NAME> [--value V] | ls | rm <NAME>
  env      create <name> [flags] | ls | show <ref> | update <ref> [flags] | rm <ref>

new --env starts the session from an environment (by name or id): its image,
setup script, egress and secrets. --image and --egress override the
environment's own for that one session; everything else comes from it.

attach opens on the session's current screen. --since 0 replays the whole
event log instead — a failed setup's full output, or a day of scrollback —
and --since N resumes after sequence number N, the number the disconnect
line prints when an attach drops.

login stores your GitHub token in the server's credential vault (sealed), so
sessions can clone, pull and push as you. "creds" shows what's stored:
provider, status, scopes and when it was last verified and used. A status of
needs_refresh means git saw that token rejected — run

  rainier login --refresh github

to log in again with a fresh token and clear it. Use the same command when
"creds" shows scopes without "repo": that token can prove who you are but
cannot do git.

diff shows, per repository the session cloned, what its branch changed against
the base branch it started from — git's own "--stat", read from inside the
session. push and pull move a directory between your machine and a session's
workspace: they are one-shot and bounded (256MiB per transfer), the remote
path is always inside /workspace, and neither follows a symlink out of the
tree it is moving.

secret set reads the value from stdin when --value is omitted, so it never
lands in your shell history:  cat token.txt | rainier secret set GH_TOKEN
Secret values are write-only: this API never gives one back, and "secret ls"
shows names and timestamps only. Names are [A-Z0-9_], up to 64 characters —
they become environment variables inside your sessions.

<id|name>: a "sess_" prefix is used as a session id directly. Anything else
is resolved by name against your team's non-terminal sessions — names are
unique only per owner, so two teammates can share one. If the name matches
more than one session, your own is preferred when it's the only one of the
matches that's yours (login records who you are); otherwise the name is
rejected as ambiguous and every matching session's id and owner are listed
so you can pass the id explicitly.`)
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
}

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
// both POST /v1/auth/github and GET /v1/me. ID is the caller's own user id —
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

// credential mirrors one element of GET /v1/credentials. As with `secret`,
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
}

type suspendRequest struct {
	Warm *bool `json:"warm,omitempty"`
}

// secret mirrors one element of GET /v1/secrets. There is no value field
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
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Image           string            `json:"image"`
	Setup           string            `json:"setup"`
	SetupHash       string            `json:"setup_hash"`
	Init            string            `json:"init"`
	InitTimeoutSec  int               `json:"init_timeout_sec"`
	EgressAllow     []string          `json:"egress_allow"`
	SecretRefs      []string          `json:"secret_refs"`
	Connectors      []json.RawMessage `json:"connectors"`
	Placement       string            `json:"placement"`
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
	fs.Parse(reorderArgs(fs, args))

	if *refresh != "" && !slices.Contains(refreshableProviders, *refresh) {
		return fmt.Errorf("--refresh %s: unknown provider; rainier stores credentials for: %s",
			*refresh, strings.Join(refreshableProviders, ", "))
	}

	cfg, _ := cli.Load() // a missing/unreadable config is not fatal here: --server can still supply everything
	serverURL := *server
	if serverURL == "" {
		serverURL = cfg.ServerURL
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
	if err := c.Do(http.MethodPost, "/v1/auth/github", map[string]string{"access_token": ghToken}, &resp); err != nil {
		return err
	}

	// Login is where this CLI learns who it is. The identity in the exchange
	// response is the same one GET /v1/me answers with — id, login, role —
	// and its id is what resolveSessionID compares against a session's
	// owner_id, so caching it here is what makes owner-preference work from
	// the first command after a login rather than only after a `new`.
	//
	// An older controld that does not send an id leaves whatever is already
	// cached alone: a refresh of a GitHub credential is no reason to forget
	// who the caller is.
	cfg.ServerURL, cfg.Token = serverURL, resp.Token
	if resp.User.ID != "" {
		cfg.OwnerID = resp.User.ID
	}
	if err := cli.Save(cfg); err != nil {
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

// ---------------------------------------------------------------------------
// creds
// ---------------------------------------------------------------------------

// runCreds renders GET /v1/credentials: what the server's vault holds for
// the caller, and nothing about anyone else. There is no value in the
// response and none in this table by construction — a credential is
// write-only at that API exactly like a secret.
func runCreds(args []string) error {
	fs := flag.NewFlagSet("creds", flag.ExitOnError)
	fs.Parse(args)

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	var resp credentialsEnvelope
	if err := c.Do(http.MethodGet, "/v1/credentials", nil, &resp); err != nil {
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
// new
// ---------------------------------------------------------------------------

func runNew(args []string) error {
	fs := flag.NewFlagSet("new", flag.ExitOnError)
	name := fs.String("name", "", "session name")
	env := fs.String("env", "", "environment to start from (name or id)")
	image := fs.String("image", "", "container image (overrides the environment's)")
	egress := fs.String("egress", "", "comma-separated egress allowlist (overrides the environment's)")
	detach := fs.Bool("detach", false, "create without attaching")
	idempotencyKey := fs.String("idempotency-key", "", "stable create retry key (developer tooling)")
	fs.Parse(reorderArgs(fs, args))
	cmdArgs := fs.Args() // whatever followed "--"

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	// --env and the two override flags compose: the environment supplies
	// everything the flags don't, and controld resolves the pair (design §4.3).
	body := createSessionRequest{Name: *name, Image: *image, Environment: *env}
	if len(cmdArgs) > 0 {
		body.Cmd = cmdArgs
	}
	if *egress != "" {
		body.EgressAllow = strings.Split(*egress, ",")
	}

	var resp sessionEnvelope
	key := *idempotencyKey
	if key == "" {
		key = cli.RandHex(8)
	}
	if err := c.Do(http.MethodPost, "/v1/sessions", body, &resp, cli.IdempotencyKey(key)); err != nil {
		return err
	}
	fmt.Println(resp.Session.ID)

	if *detach {
		return nil
	}
	// The whole log, not a snapshot: `new`'s attach is "stream everything"
	// (design §4.10/§9), and the interesting output — a setup script's, a
	// clone's — starts before this socket can possibly be up. A session
	// created seconds ago has a log measured in kilobytes, so replaying it
	// from the first entry costs nothing and is the only way the user sees
	// what happened before they got here.
	return attachWithRetry(cfg, resp.Session.ID, wire.SinceAll)
}

// attachWithRetry is `new`'s "attach immediately and stream everything"
// (design §4.10): a session that was just created is legitimately a few
// seconds from `running`, so a first attach that fails because it isn't
// ready yet is retried, with a waiting line, for up to 60s rather than
// treated as fatal. attachio.Run's dial wraps that specific failure —
// controld's 503 session_not_ready before the websocket upgrade — as a
// *attachio.DialError matching errors.Is(err, attachio.ErrSessionNotReady);
// any other error (including a *DialError for some other status) is
// treated as fatal immediately rather than burning the retry budget on a
// failure that will never resolve itself.
func attachWithRetry(cfg cli.Config, id string, since uint64) error {
	return attachWithRetrySleep(cfg, id, since, time.Sleep)
}

func attachWithRetrySleep(cfg cli.Config, id string, since uint64, sleep func(time.Duration)) error {
	wsURL := wsURLFor(cfg.ServerURL, id)
	header := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	deadline := time.Now().Add(60 * time.Second)
	established := false
	backoff := 100 * time.Millisecond

	for {
		attemptStarted := time.Now()
		outcome, err := attachio.Run(context.Background(), wsURL, header, since)
		if err == nil {
			if outcome.Reason != attachio.Disconnected {
				return nil
			}
			// The server sequence is the acknowledgement that a frame reached
			// local stdout. Resume after it: never repaint the whole terminal and
			// never skip output the user had not actually seen.
			established = true
			since = outcome.LastSeq
			if time.Since(attemptStarted) >= 10*time.Second {
				backoff = 100 * time.Millisecond
			}
			fmt.Printf("[reconnecting in %s…]\n", backoff)
			sleep(backoff)
			backoff = nextAttachBackoff(backoff)
			continue
		}

		if established {
			if !retryableAttachError(err) {
				return err
			}
			fmt.Printf("[reconnecting in %s…]\n", backoff)
			sleep(backoff)
			backoff = nextAttachBackoff(backoff)
			continue
		}

		if !errors.Is(err, attachio.ErrSessionNotReady) || !time.Now().Before(deadline) {
			return err
		}
		fmt.Println("waiting for session…")
		sleep(500 * time.Millisecond)
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
// transport failures and transient gateway statuses can recover; auth,
// authorization, not-found, protocol, and local-terminal failures cannot and
// must not become an infinite loop. A plain websocket transport failure is a
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

func runLs(args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	all := fs.Bool("all", false, "include terminal (canceled/failed/dead/destroyed) sessions")
	fs.Parse(reorderArgs(fs, args))

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tENV\tSTATE\tRUNNER\tREACHABLE\tAGE")

	cursor := ""
	for {
		q := url.Values{}
		if *all {
			q.Set("all", "true")
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		path := "/v1/sessions"
		if enc := q.Encode(); enc != "" {
			path += "?" + enc
		}
		var page sessionsEnvelope
		if err := c.Do(http.MethodGet, path, nil, &page); err != nil {
			return err
		}
		for _, s := range page.Sessions {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%t\t%s\n",
				s.ID, s.Name, dashIfEmpty(s.Environment), sessionStateCell(s), s.Runner, s.Reachable, formatAge(s.CreatedAt))
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if err := w.Flush(); err != nil {
		return err
	}

	if !*all {
		var failed sessionsEnvelope
		if err := c.Do(http.MethodGet, "/v1/sessions?all=true&limit=1&state=failed", nil, &failed); err == nil && len(failed.Sessions) > 0 {
			fmt.Println("hint: failed sessions are hidden; run `rainier ls --all` to inspect or remove them")
		}
	}
	return nil
}

// sessionStateCell renders the STATE column: the state alone, plus whatever
// the state alone leaves unanswered.
//
// "queued" by itself invites the wrong question ("is it broken?"); "queued
// (waiting for runner rainier-gpu)" answers it in the same glance. "running"
// has the same problem once the agent inside has finished: the session IS
// still up — attachable, holding its slot — and nothing is ever going to
// print in it again, so "running (exited 0)" is what the row actually means.
//
// The two annotations are mutually exclusive in practice (a queued session
// has no agent to have exited) and the queue reason wins if they ever meet:
// it explains why nothing is happening, which is the more urgent of the two.
func sessionStateCell(s session) string {
	switch {
	case s.QueueReason != "":
		return s.State + " (" + s.QueueReason + ")"
	case s.ChildExitCode != nil:
		return s.State + " (exited " + strconv.Itoa(*s.ChildExitCode) + ")"
	default:
		return s.State
	}
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
	ref, cursor := attachFlags(args)

	cfg, c, id, err := resolveClientAndIDForAttach(ref)
	if err != nil {
		return err
	}
	if err := prepareAttach(c, id); err != nil {
		return err
	}
	return attachWithRetry(cfg, id, cursor)
}

// prepareAttach makes `rainier attach` the one entry command for an existing
// session. Running/starting sessions can go straight to the attach plane;
// suspended sessions first use the ordinary resume endpoint. No convenience
// endpoint is needed server-side — the CLI is the only consumer that wants
// these two operations composed.
//
// A failed session is deliberately admitted. Plan 5 made a failed setup
// attachable while its runner is still connected so `--since 0` can show the
// complete diagnostic log; the attach endpoint remains the authority on
// whether that particular failed row still has a live sessiond behind it.
func prepareAttach(c *cli.Client, id string) error {
	row, err := getSession(c, id)
	if err != nil {
		return err
	}
	switch row.State {
	case "running", "creating", "queued", "failed":
		return nil
	case "suspended_warm", "suspended_cold":
		var resumed sessionEnvelope
		resumeErr := c.Do(http.MethodPost, "/v1/sessions/"+id+"/resume", nil, &resumed)
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
			case "running", "creating":
				return nil
			case "suspended_warm", "suspended_cold":
			case "queued":
				return nil
			default:
				return resumeErr
			}
			select {
			case <-ctx.Done():
				return resumeErr
			case <-time.After(50 * time.Millisecond):
			}
		}
	default:
		return fmt.Errorf("session %s is %s and cannot be attached", id, row.State)
	}
}

func getSession(c *cli.Client, id string) (session, error) {
	return getSessionContext(context.Background(), c, id)
}

func getSessionContext(ctx context.Context, c *cli.Client, id string) (session, error) {
	var resp sessionEnvelope
	if err := c.DoContext(ctx, http.MethodGet, "/v1/sessions/"+id, nil, &resp); err != nil {
		return session{}, err
	}
	return resp.Session, nil
}

// attachFlags parses `attach`'s arguments into the session ref and the
// attach cursor. Split out of runAttach so the part with no network in it —
// which of three requests `--since` spells, in either argument order — is
// testable on its own; the flag-after-the-positional form is the one the
// acceptance run reached for when the first attempt showed nothing, so it
// gets pinned rather than assumed (reorderArgs is what makes it work).
func attachFlags(args []string) (ref string, cursor uint64) {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	since := fs.Uint64("since", 0, "resume from sequence number; 0 replays the whole event log (omit for the current screen)")
	fs.Parse(reorderArgs(fs, args))
	return requireRef(fs, "attach"), attachio.Cursor(passedFlags(fs)["since"], *since)
}

// ---------------------------------------------------------------------------
// suspend / resume / snapshot / rm
// ---------------------------------------------------------------------------

func runSuspend(args []string) error {
	fs := flag.NewFlagSet("suspend", flag.ExitOnError)
	cold := fs.Bool("cold", false, "cold suspend (stop the container) instead of warm (pause it)")
	fs.Parse(reorderArgs(fs, args))
	ref := requireRef(fs, "suspend")

	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}

	req := suspendRequest{}
	if *cold {
		warm := false
		req.Warm = &warm
	}
	var resp sessionEnvelope
	if err := c.Do(http.MethodPost, "/v1/sessions/"+id+"/suspend", req, &resp); err != nil {
		return err
	}
	fmt.Printf("%s -> %s\n", resp.Session.ID, resp.Session.State)
	return nil
}

func runResume(args []string) error {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	fs.Parse(args)
	ref := requireRef(fs, "resume")

	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	var resp sessionEnvelope
	if err := c.Do(http.MethodPost, "/v1/sessions/"+id+"/resume", nil, &resp); err != nil {
		return err
	}
	fmt.Printf("%s -> %s\n", resp.Session.ID, resp.Session.State)
	return nil
}

func runSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	fs.Parse(args)
	ref := requireRef(fs, "snapshot")

	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	var resp snapshotResponse
	if err := c.Do(http.MethodPost, "/v1/sessions/"+id+"/snapshot", nil, &resp); err != nil {
		return err
	}
	fmt.Println(resp.Ref)
	return nil
}

func runRm(args []string) error {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	fs.Parse(args)
	ref := requireRef(fs, "rm")

	_, c, id, err := resolveClientAndIDIncludingTerminal(ref)
	if err != nil {
		return err
	}
	if err := c.Do(http.MethodDelete, "/v1/sessions/"+id, nil, nil); err != nil {
		return err
	}
	fmt.Println("removed", id)
	return nil
}

// ---------------------------------------------------------------------------
// diff / push / pull
//
// The three workspace-inspection commands. All the work is in internal/cli
// (Push/Pull) and internal/xfer (the archive rules); what is left here is
// argument parsing and rendering, which is the same split every other
// subcommand takes.
// ---------------------------------------------------------------------------

func runDiff(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	fs.Parse(args)
	ref := requireRef(fs, "diff")

	_, c, id, err := resolveClientAndID(ref)
	if err != nil {
		return err
	}
	var ans xfer.DiffAnswer
	if err := c.Do(http.MethodGet, "/v1/sessions/"+id+"/diff", nil, &ans); err != nil {
		return err
	}
	renderDiff(os.Stdout, ans)
	return nil
}

// renderDiff prints one heading per repository — both branches named, because
// "what changed" is meaningless without saying against what — and git's stat
// underneath it.
//
// A repository with an empty stat says so explicitly. Printing nothing there
// reads as a bug in this command rather than as an answer about the session.
func renderDiff(w io.Writer, ans xfer.DiffAnswer) {
	if len(ans.Repos) == 0 {
		fmt.Fprintln(w, "this session has no repositories")
		return
	}
	for i, r := range ans.Repos {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintf(w, "%s  %s vs origin/%s\n", r.Repo, r.SessionBranch, r.BaseBranch)
		stat := strings.TrimRight(r.Stat, "\n")
		if stat == "" {
			fmt.Fprintln(w, "  (no changes)")
			continue
		}
		fmt.Fprintln(w, stat)
	}
}

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

	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	if err := c.Do(http.MethodPut, "/v1/secrets/"+url.PathEscape(name), putSecretRequest{Value: v}, nil); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	var resp secretsEnvelope
	if err := c.Do(http.MethodGet, "/v1/secrets", nil, &resp); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	if err := c.Do(http.MethodDelete, "/v1/secrets/"+url.PathEscape(name), nil, nil); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

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
		SetupTimeoutSec: *f.timeout,
	}
	var resp environmentEnvelope
	if err := c.Do(http.MethodPost, "/v1/environments", body, &resp); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	var resp environmentsEnvelope
	if err := c.Do(http.MethodGet, "/v1/environments", nil, &resp); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	// Decoded as raw JSON and re-indented, so what's printed is exactly what
	// the server said — including any field this CLI's own struct doesn't
	// know about yet.
	var resp struct {
		Environment json.RawMessage `json:"environment"`
	}
	if err := c.Do(http.MethodGet, "/v1/environments/"+url.PathEscape(ref), nil, &resp); err != nil {
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
	if passed["setup-timeout-sec"] {
		patch["setup_timeout_sec"] = *f.timeout
	}
	if passed["init-timeout-sec"] {
		patch["init_timeout_sec"] = *f.initTimeout
	}
	if len(patch) == 0 {
		return fmt.Errorf("nothing to update: pass at least one of --name, --image, --setup-file, --init-file, --egress, --secret-ref, --connector-json, --placement, --setup-timeout-sec, --init-timeout-sec")
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	var resp environmentEnvelope
	if err := c.Do(http.MethodPatch, "/v1/environments/"+url.PathEscape(ref), patch, &resp); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	if err := c.Do(http.MethodDelete, "/v1/environments/"+url.PathEscape(ref), nil, nil); err != nil {
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
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	id, err := resolveSessionIDWithScope(c, cfg.OwnerID, ref, scope)
	if err != nil {
		return cli.Config{}, nil, "", err
	}
	return cfg, c, id, nil
}

// resolveSessionID resolves ref to a session id: a "sess_" prefix is
// already an id, verbatim; anything else is looked up through the exact-name
// filter on paginated GET /v1/sessions. The CLI still makes the ambiguity
// decision because that collection is team-visible. The default resolver sees
// only non-terminal sessions. rm's variant opts into terminal rows because a
// failed create can still own a live container. The caller's own rows take
// precedence over teammates' team-visible rows; within that set, an active
// row that reused a terminal row's name wins and the historical one requires
// its id.
//
// Session names are unique only per owner (design), while GET /v1/sessions
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
// /v1/me answers with). It is the same string a session row carries as
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
		return ref, nil
	}

	type match struct {
		id, owner string
		terminal  bool
	}
	var matches []match

	cursor := ""
	for {
		q := url.Values{}
		if scope != resolveActive {
			q.Set("all", "true")
		}
		q.Set("name", ref)
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		path := "/v1/sessions"
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
		cursor = page.NextCursor
	}
	// GET /v1/sessions is team-visible, but lifecycle commands should operate
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
	return strings.TrimRight(ws, "/") + "/v1/sessions/" + id + "/attach"
}

// requireRef pulls the positional <id|name> argument every lifecycle
// command needs, exiting with a usage message (exit 2) instead of
// panicking with an index-out-of-range when it's missing — same pattern as
// runnerctl's requireID.
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
