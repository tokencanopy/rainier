// Command rainier is the client CLI for controld: log in, create sessions,
// list them, attach to them, and drive their lifecycle (suspend/resume/
// snapshot/rm). Subcommand dispatch follows runnerctl's style — stdlib
// `flag` per subcommand, no cobra.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"text/tabwriter"
	"time"

	"rainier/internal/attachio"
	"rainier/internal/cli"
)

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
  new      [--name N] [--image IMG] [--egress host,host] [--detach] [-- CMD ARGS...]
  ls       [--all]
  attach   <id|name> [--since N]
  suspend  <id|name> [--cold]
  resume   <id|name>
  snapshot <id|name>
  rm       <id|name>`)
}

// ---------------------------------------------------------------------------
// wire shapes (mirrors internal/controld/api.go's client-facing JSON —
// cmd/rainier only decodes the fields it displays or acts on)
// ---------------------------------------------------------------------------

type session struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Image       string   `json:"image"`
	Cmd         []string `json:"cmd"`
	EgressAllow []string `json:"egress_allow"`
	State       string   `json:"state"`
	Runner      string   `json:"runner"`
	Reachable   bool     `json:"reachable"`
	Error       string   `json:"error"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
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

type userView struct {
	Login string `json:"login"`
	Role  string `json:"role"`
}

type authResponse struct {
	Token string   `json:"token"`
	User  userView `json:"user"`
}

type createSessionRequest struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
}

type suspendRequest struct {
	Warm *bool `json:"warm,omitempty"`
}

// ---------------------------------------------------------------------------
// login
// ---------------------------------------------------------------------------

func runLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	fromGH := fs.Bool("from-gh", false, "obtain a GitHub token via `gh auth token`")
	token := fs.String("token", "", "a GitHub access token to use directly")
	clientID := fs.String("client-id", "", "GitHub OAuth App client id — runs the device flow")
	server := fs.String("server", "", "controld server URL")
	fs.Parse(reorderArgs(fs, args))

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

	c := &cli.Client{Base: serverURL}
	var resp authResponse
	if err := c.Do(http.MethodPost, "/v1/auth/github", map[string]string{"access_token": ghToken}, &resp); err != nil {
		return err
	}
	if err := cli.Save(cli.Config{ServerURL: serverURL, Token: resp.Token}); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}
	fmt.Printf("logged in as %s (%s)\n", resp.User.Login, resp.User.Role)
	return nil
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
	form := url.Values{"client_id": {clientID}, "scope": {"read:user"}}
	req, err := http.NewRequest(http.MethodPost, "https://github.com/login/device/code", strings.NewReader(form.Encode()))
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

func pollAccessToken(clientID, deviceCode string) (accessTokenResponse, error) {
	form := url.Values{
		"client_id":   {clientID},
		"device_code": {deviceCode},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
	}
	req, err := http.NewRequest(http.MethodPost, "https://github.com/login/oauth/access_token", strings.NewReader(form.Encode()))
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
	image := fs.String("image", "", "container image")
	egress := fs.String("egress", "", "comma-separated egress allowlist")
	detach := fs.Bool("detach", false, "create without attaching")
	fs.Parse(reorderArgs(fs, args))
	cmdArgs := fs.Args() // whatever followed "--"

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}

	body := createSessionRequest{Name: *name, Image: *image}
	if len(cmdArgs) > 0 {
		body.Cmd = cmdArgs
	}
	if *egress != "" {
		body.EgressAllow = strings.Split(*egress, ",")
	}

	var resp sessionEnvelope
	if err := c.Do(http.MethodPost, "/v1/sessions", body, &resp, cli.IdempotencyKey(cli.RandHex(8))); err != nil {
		return err
	}
	fmt.Println(resp.Session.ID)

	if *detach {
		return nil
	}
	return attachWithRetry(c, cfg, resp.Session.ID, 0)
}

// attachWithRetry is `new`'s "attach immediately and stream everything"
// (design §4.10): a session that was just created is legitimately a few
// seconds from `running`, so a first attach that fails because it isn't
// ready yet (controld answers 503 session_not_ready before the websocket
// upgrade — attachio.Run's dial then fails with that status in its error
// text) is retried, with a waiting line, for up to 60s rather than treated
// as fatal.
func attachWithRetry(c *cli.Client, cfg cli.Config, id string, since uint64) error {
	wsURL := wsURLFor(cfg.ServerURL, id)
	header := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	deadline := time.Now().Add(60 * time.Second)

	for {
		err := attachio.Run(context.Background(), wsURL, header, since)
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "503") || !time.Now().Before(deadline) {
			return err
		}
		fmt.Println("waiting for session…")
		time.Sleep(500 * time.Millisecond)
	}
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
	fmt.Fprintln(w, "ID\tNAME\tSTATE\tRUNNER\tREACHABLE\tAGE")

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
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%t\t%s\n", s.ID, s.Name, s.State, s.Runner, s.Reachable, formatAge(s.CreatedAt))
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return w.Flush()
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

func runAttach(args []string) error {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	since := fs.Uint64("since", 0, "resume from sequence number")
	fs.Parse(reorderArgs(fs, args))
	ref := requireRef(fs, "attach")

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	id, err := resolveSessionID(c, ref)
	if err != nil {
		return err
	}

	wsURL := wsURLFor(cfg.ServerURL, id)
	header := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	return attachio.Run(context.Background(), wsURL, header, *since)
}

// ---------------------------------------------------------------------------
// suspend / resume / snapshot / rm
// ---------------------------------------------------------------------------

func runSuspend(args []string) error {
	fs := flag.NewFlagSet("suspend", flag.ExitOnError)
	cold := fs.Bool("cold", false, "cold suspend (stop the container) instead of warm (pause it)")
	fs.Parse(reorderArgs(fs, args))
	ref := requireRef(fs, "suspend")

	c, id, err := resolveClientAndID(ref)
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

	c, id, err := resolveClientAndID(ref)
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

	c, id, err := resolveClientAndID(ref)
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

	c, id, err := resolveClientAndID(ref)
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

// resolveClientAndID is the requireLogin+resolveSessionID pair every
// lifecycle command (suspend/resume/snapshot/rm) needs.
func resolveClientAndID(ref string) (*cli.Client, string, error) {
	cfg, err := requireLogin()
	if err != nil {
		return nil, "", err
	}
	c := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	id, err := resolveSessionID(c, ref)
	if err != nil {
		return nil, "", err
	}
	return c, id, nil
}

// resolveSessionID resolves ref to a session id: a "sess_" prefix is
// already an id, verbatim; anything else is looked up by paging GET
// /v1/sessions and matching the name field client-side — controld has no
// by-name endpoint in v0 (see the design's route table), so this is the
// CLI's own job. Only non-terminal sessions are visible to this search
// (the default GET /v1/sessions view), which matches every command that
// uses it: you attach, suspend, resume, or snapshot a live session, not a
// dead one.
func resolveSessionID(c *cli.Client, ref string) (string, error) {
	if strings.HasPrefix(ref, "sess_") {
		return ref, nil
	}
	cursor := ""
	for {
		path := "/v1/sessions"
		if cursor != "" {
			path += "?cursor=" + url.QueryEscape(cursor)
		}
		var page sessionsEnvelope
		if err := c.Do(http.MethodGet, path, nil, &page); err != nil {
			return "", err
		}
		for _, s := range page.Sessions {
			if s.Name == ref {
				return s.ID, nil
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return "", fmt.Errorf("no session named %q found", ref)
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
