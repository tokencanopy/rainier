package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
)

// A personal provider connection — today only GitHub — is the hosted account's
// own link to an outside account, and this file is the whole of what the CLI
// does with one: read it, replace its browser authorization, and choose which
// of your workspaces it reaches.
//
// It is deliberately NOT `rainier creds`. That command reads the self-hosted
// vault, whose row is a stored token with scopes and a verification status; a
// hosted connection has no scopes, no status, and no token anywhere the CLI can
// see — what it has instead is an access mode and a workspace selection. The
// two answer different questions and share no columns, so they stay two
// commands, and `creds` says so when it is pointed at a hosted context.
//
// Share and unshare cannot widen access on their own: they never send the
// access mode. Reconnect is the one exception, because its whole contract is
// to restore the exact mode the owner had already chosen before it revoked the
// old authorization.
//
// What the workspace edit does and does not guarantee, precisely, because the
// distinction matters and is easy to overstate:
//
// PATCH /v0/connections/{provider} REPLACES the selection; there is no add or
// remove verb. So `share` and `unshare` are GET, edit one entry, PATCH the
// whole set. Server-side the workspace replacement itself is atomic — the
// adapter takes a FOR UPDATE lock on the connection row and does its
// delete-and-insert inside that transaction, so no reader sees a partial
// selection and two selection writes serialize. A PATCH carrying both the
// selection and access mode is currently two store operations; reconnect
// therefore reads back after an uncertain write and never claims restoration
// unless the complete policy is visible.
//
// The CLI's read-modify-write is NOT atomic, and cannot be made so. It
// preserves every grant present in the snapshot it read, which is what stops a
// share from wiping the workspaces the person previously chose. It does not
// preserve a grant some other client adds between that read and this write:
// the write replaces the set, so a concurrent edit is lost. The connect
// surface offers no way to close that window — GET returns no ETag, the
// connection carries no version or revision, the wire body has no field to
// echo back, and SetWorkspaces takes no expected-version argument — so this is
// a property of the API, not a shortcut taken here. The stub tests in
// connection_test.go record the client behavior; they do not detect changes
// to the hosted API contract.
//
// Single-developer dogfood is the setting this ships into, where two
// simultaneous edits of one person's own connection are not a realistic
// concern. The commands check the returned mode and selection before reporting
// success. That response is still a snapshot, not a concurrency guarantee.

const connectionUsage = `usage: rainier connection <ls|share|unshare|reconnect> [args]

  connection ls                              what you have connected, and where it reaches
  connection share <provider> [--workspace ID]    let this workspace use the connection
  connection unshare <provider> [--workspace ID]  stop this workspace using it
  connection reconnect <provider>            authorize again and restore its access policy

Connecting GitHub itself happens in the browser: run "rainier login --cloud
URL" and choose Connect GitHub on the last step of the page. A connection
starts shared with nothing, so sessions cannot clone or push until you share
it with the workspace they run in.

--workspace defaults to your current workspace ("rainier workspace use" picks
it). Sharing requires current membership; unsharing also lets you remove a
saved grant after leaving a workspace. Both keep the other workspaces the
connection reached when the command read it. Neither
command changes the access mode, and neither prints a credential — the CLI
never sees one.

The change is read-then-replace: this API has no add or remove verb and no
conditional write, so a change made by another client in between is
overwritten, including restoring a grant another client removed. Avoid editing
one connection from two places at once; the printed list is a server snapshot.

Reconnect revokes the old connection before the browser flow because the hosted
API permits one live connection per account. If that flow is abandoned, GitHub
remains disconnected; the command prints the saved non-secret policy and the
steps for recovering it.`

// connectionProviders is the set of providers a connection command will act
// on. It is a list rather than a constant because the API is a list for the
// same reason: a second provider should not be a breaking change to a client
// that has already shipped.
var connectionProviders = []string{"github"}

// connectionView is one row of GET /v0/connections.
//
// It mirrors the edge's connectionBody exactly, and what is absent from both is
// the point: there is no token field, no ciphertext field, and no key field, so
// there is nowhere for a credential to arrive even if the server changed.
type connectionView struct {
	Provider   string   `json:"provider"`
	Login      string   `json:"login"`
	AccessMode string   `json:"access_mode"`
	Workspaces []string `json:"workspaces"`
	CreatedAt  string   `json:"created_at"`
	RevokedAt  *string  `json:"revoked_at"`
}

type connectionsEnvelope struct {
	Connections []connectionView `json:"connections"`
}

// accessAll is the access mode that means "every workspace I am a member of,
// now and later". accessSelected is the default, in which the selection this
// file edits is what governs.
const (
	accessAll      = "all"
	accessSelected = "selected"
)

func runConnection(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, connectionUsage)
		os.Exit(2)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "ls":
		return runConnectionLs(rest)
	case "share":
		return runConnectionShare(rest, true)
	case "unshare":
		return runConnectionShare(rest, false)
	case "reconnect":
		return runConnectionReconnect(rest)
	case "-h", "--help", "help":
		fmt.Fprintln(os.Stderr, connectionUsage)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "rainier connection: unknown subcommand %q\n%s\n", sub, connectionUsage)
		os.Exit(2)
		return nil
	}
}

const (
	connectionReconnectPollInterval = 2 * time.Second
	connectionReconnectWindow       = 10 * time.Minute
	// The browser's Connect GitHub ticket expires after ten minutes. Polling
	// beyond it could only turn a missed authorization into an unbounded CLI.
	connectionReconnectPollLimit = 300
)

type cloudLoginRunner func(edgeURL, deviceName, contextName string) error

// runConnectionReconnect replaces one hosted provider authorization and, once
// the browser creates the new connection, restores the exact access mode and
// workspace selection the old connection held.
//
// The hosted API has no pending replacement: a second live connection is a
// conflict and revocation is terminal. DELETE must therefore precede the
// browser flow. Every check the CLI can perform happens before that DELETE;
// after it, failures are explicit that the account is disconnected and render
// the non-secret policy needed for recovery.
func runConnectionReconnect(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	login := func(edgeURL, deviceName, contextName string) error {
		loginCtx, cancel := context.WithTimeout(ctx, loginAttemptFallbackTTL)
		defer cancel()
		return runCloudLoginContext(loginCtx, edgeURL, deviceName, contextName, nil)
	}
	return runConnectionReconnectContext(ctx, args, login, nil, connectionReconnectPollLimit)
}

func runConnectionReconnectWith(args []string, login cloudLoginRunner, sleep func(time.Duration), pollLimit int) error {
	return runConnectionReconnectContext(context.Background(), args, login, sleep, pollLimit)
}

func runConnectionReconnectContext(ctx context.Context, args []string, login cloudLoginRunner, sleep func(time.Duration), pollLimit int) error {
	fs := flag.NewFlagSet("connection reconnect", flag.ExitOnError)
	fs.Parse(args)
	provider, err := connectionProviderNamed(fs.Arg(0), "reconnect")
	if err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: rainier connection reconnect <provider>")
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	current, _ := cfg.Active()
	if !current.Hosted() {
		return fmt.Errorf("connection reconnect: context %s is not a hosted one; provider connections come from `rainier login --cloud EDGE_URL`", cfg.ActiveName())
	}

	contextName := cfg.ActiveName()
	server := strings.TrimRight(current.Server, "/")
	if !validReconnectOpaqueID(contextName) || !validReconnectServer(server) {
		return errors.New("connection reconnect: the active hosted context is invalid; the existing connection was not changed")
	}
	c := cli.NewClient(cfg)
	originalAccount, err := fetchConnectionAccountBounded(ctx, c)
	if err != nil {
		return errors.New("connection reconnect: could not verify the current Rainier account; the existing connection was not changed")
	}
	freezeReconnectClient(c)
	rows, err := fetchConnectionsBounded(ctx, c)
	if err != nil {
		return errors.New("connection reconnect: could not read the current GitHub connection; the existing connection was not changed")
	}
	i := slices.IndexFunc(rows, func(v connectionView) bool { return v.Provider == provider })
	if i < 0 {
		return errors.New(notConnectedHint(cfg))
	}
	previous := rows[i]
	if previous.AccessMode != accessSelected && previous.AccessMode != accessAll {
		return fmt.Errorf("connection reconnect: unsupported access mode %q; the existing connection was not changed", previous.AccessMode)
	}
	if previous.Workspaces == nil {
		return errors.New("connection reconnect: server omitted the workspace selection; the existing connection was not changed")
	}
	if err := validateReconnectSelection(previous.Workspaces); err != nil {
		return err
	}
	previous.Workspaces = append([]string{}, previous.Workspaces...)

	fmt.Printf("Reconnecting replaces the stored %s authorization. If the browser flow is not completed, GitHub remains disconnected.\n", provider)
	if err := doConnectionBounded(ctx, c, http.MethodDelete, "/v0/connections/"+provider, nil, nil); err != nil {
		return reconnectDeleteUncertainError(provider, server, contextName, previous)
	}

	fmt.Println("Finish signing in in the browser, then choose Connect GitHub on the last step.")
	if err := login(server, "", contextName); err != nil {
		return reconnectDisconnectedError(provider, server, contextName, previous, "the browser login did not finish")
	}

	// Login may rotate the Rainier token pair. Reload that exact named context,
	// never whichever context is active now: another terminal is allowed to
	// switch Current while this browser flow is open.
	latest, err := cli.Load()
	if err != nil {
		return reconnectDisconnectedError(provider, server, contextName, previous, "the saved hosted context is no longer available")
	}
	reconnectClient, err := reconnectClientForPinnedContext(latest, contextName, server)
	if err != nil {
		return reconnectDisconnectedError(provider, server, contextName, previous, "the saved hosted context changed during login")
	}
	account, err := fetchConnectionAccountBounded(ctx, reconnectClient)
	if err != nil {
		return reconnectDisconnectedError(provider, server, contextName, previous, "the new Rainier login could not be verified")
	}
	if account != originalAccount {
		return reconnectDisconnectedError(provider, server, contextName, previous, "the browser login authenticated a different Rainier account")
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, connectionReconnectWindow)
	defer cancelWait()
	connected, err := waitForConnection(waitCtx, reconnectClient, provider, sleep, pollLimit)
	if err != nil {
		return reconnectDisconnectedError(provider, server, contextName, previous, "the GitHub browser connection did not finish")
	}
	if !validReconnectDisplay(connected.Login) {
		return reconnectConnectedPolicyError(provider, contextName, previous, "the replacement connection returned an invalid login")
	}

	body := struct {
		AccessMode string   `json:"access_mode"`
		Workspaces []string `json:"workspaces"`
	}{AccessMode: previous.AccessMode, Workspaces: previous.Workspaces}
	var restored connectionView
	patchErr := doConnectionBounded(ctx, reconnectClient, http.MethodPatch, "/v0/connections/"+provider, body, &restored)
	if patchErr != nil || !reconnectPolicyConfirmed(restored, connected, previous) {
		// A gateway can lose a successful PATCH response. Read back once before
		// declaring repair necessary; if the edge's older two-step handler only
		// applied part of the policy, this same read exposes that honestly.
		rows, readErr := fetchConnectionsBounded(ctx, reconnectClient)
		if readErr != nil {
			return reconnectReplacementUncertainError(provider, server, contextName, previous)
		}
		i := slices.IndexFunc(rows, func(v connectionView) bool { return v.Provider == provider })
		if i < 0 {
			return reconnectDisconnectedError(provider, server, contextName, previous, "the replacement connection disappeared before its access policy was restored")
		}
		if !reconnectPolicyConfirmed(rows[i], connected, previous) {
			return reconnectConnectedPolicyError(provider, contextName, previous, "the saved access policy was not confirmed")
		}
		restored = rows[i]
	}

	if restored.AccessMode == accessAll {
		fmt.Printf("reconnected %s as %s and restored access to all current and future workspaces\n",
			provider, safeReconnectDisplay(restored.Login))
		return nil
	}
	fmt.Printf("reconnected %s as %s and restored %s access to %s\n",
		provider, safeReconnectDisplay(restored.Login), restored.AccessMode, dashIfEmpty(strings.Join(restored.Workspaces, ", ")))
	return nil
}

func waitForConnection(ctx context.Context, c *cli.Client, provider string, sleep func(time.Duration), pollLimit int) (connectionView, error) {
	if pollLimit < 1 {
		return connectionView{}, errors.New("the GitHub browser connection did not finish")
	}
	for attempt := 0; attempt < pollLimit; attempt++ {
		rows, err := fetchConnectionsContext(ctx, c)
		if err != nil {
			return connectionView{}, err
		}
		if i := slices.IndexFunc(rows, func(v connectionView) bool { return v.Provider == provider }); i >= 0 {
			return rows[i], nil
		}
		if attempt+1 < pollLimit {
			if sleep != nil {
				sleep(connectionReconnectPollInterval)
				if err := ctx.Err(); err != nil {
					return connectionView{}, err
				}
			} else {
				timer := time.NewTimer(connectionReconnectPollInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return connectionView{}, ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
	return connectionView{}, errors.New("the GitHub browser connection did not finish before its ten-minute window closed")
}

func doConnectionBounded(parent context.Context, c *cli.Client, method, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(parent, edgeRequestTimeout)
	defer cancel()
	return c.DoContext(ctx, method, path, in, out)
}

func fetchConnectionsBounded(parent context.Context, c *cli.Client) ([]connectionView, error) {
	ctx, cancel := context.WithTimeout(parent, edgeRequestTimeout)
	defer cancel()
	return fetchConnectionsContext(ctx, c)
}

func fetchConnectionAccountBounded(parent context.Context, c *cli.Client) (string, error) {
	var me struct {
		User userView `json:"user"`
	}
	if err := doConnectionBounded(parent, c, http.MethodGet, "/v0/me", nil, &me); err != nil {
		return "", err
	}
	if !validReconnectOpaqueID(me.User.ID) {
		return "", errors.New("invalid account identity")
	}
	return me.User.ID, nil
}

func reconnectClientForPinnedContext(cfg cli.Config, name, server string) (*cli.Client, error) {
	ctx, ok := cfg.Contexts[name]
	if !ok || !ctx.Hosted() || strings.TrimRight(ctx.Server, "/") != server {
		return nil, errors.New("pinned context changed")
	}
	if !cfg.Use(name) {
		return nil, errors.New("pinned context disappeared")
	}
	c := cli.NewClient(cfg)
	freezeReconnectClient(c)
	return c, nil
}

func freezeReconnectClient(c *cli.Client) {
	// This login just minted a fresh access token, which outlives the bounded
	// reconnect window. Do not adopt a newer token pair from disk after the
	// live account check: a sibling browser login can replace this named
	// context with another account while preserving its stale cached OwnerID.
	// Refusing refresh means a 401 becomes safe recovery instead of a chance to
	// apply the old account's policy to that sibling token family.
	c.RefreshToken = ""
	c.LoadTokens = nil
	c.SaveTokens = nil
}

func reconnectPolicyConfirmed(got, connected, previous connectionView) bool {
	return got.Provider == connected.Provider && got.Provider == previous.Provider &&
		got.Login == connected.Login && validReconnectDisplay(got.Login) &&
		got.AccessMode == previous.AccessMode && got.Workspaces != nil &&
		sameWorkspaceSelection(got.Workspaces, previous.Workspaces)
}

func sameWorkspaceSelection(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func reconnectDeleteUncertainError(provider, server, contextName string, previous connectionView) error {
	var recovery strings.Builder
	fmt.Fprintf(&recovery, "connection reconnect: could not confirm whether the previous %s connection was disconnected.\n", provider)
	appendReconnectContext(&recovery, contextName)
	fmt.Fprintln(&recovery, "Check first: rainier connection ls")
	fmt.Fprintln(&recovery, "If it is still connected, rerun this reconnect command. If it is absent, connect it again:")
	fmt.Fprintf(&recovery, "  rainier login --cloud %s --context %s\n", shellQuote(server), shellQuote(contextName))
	fmt.Fprintln(&recovery, "In the browser, sign in to the same Rainier account this context used before.")
	appendReconnectPolicyRecovery(&recovery, provider, previous)
	return errors.New(strings.TrimSpace(recovery.String()))
}

func reconnectDisconnectedError(provider, server, contextName string, previous connectionView, reason string) error {
	var recovery strings.Builder
	fmt.Fprintf(&recovery, "connection reconnect: %s; the previous %s connection is disconnected and no saved policy was applied.\n", reason, provider)
	appendReconnectContext(&recovery, contextName)
	fmt.Fprintf(&recovery, "Connect it again: rainier login --cloud %s --context %s, then choose Connect GitHub.\n", shellQuote(server), shellQuote(contextName))
	fmt.Fprintln(&recovery, "In the browser, sign in to the same Rainier account this context used before.")
	appendReconnectPolicyRecovery(&recovery, provider, previous)
	return errors.New(strings.TrimSpace(recovery.String()))
}

func reconnectConnectedPolicyError(provider, contextName string, previous connectionView, reason string) error {
	var recovery strings.Builder
	fmt.Fprintf(&recovery, "connection reconnect: %s. GitHub is connected, but its saved access policy is not confirmed.\n", reason)
	appendReconnectContext(&recovery, contextName)
	fmt.Fprintln(&recovery, "Do not reconnect again. Check the current policy: rainier connection ls")
	appendReconnectPolicyRecovery(&recovery, provider, previous)
	return errors.New(strings.TrimSpace(recovery.String()))
}

func reconnectReplacementUncertainError(provider, server, contextName string, previous connectionView) error {
	var recovery strings.Builder
	fmt.Fprintf(&recovery, "connection reconnect: could not confirm whether the replacement %s connection is still live or whether its saved access policy was restored.\n", provider)
	appendReconnectContext(&recovery, contextName)
	fmt.Fprintln(&recovery, "Check first: rainier connection ls")
	fmt.Fprintln(&recovery, "If it is absent, connect it again:")
	fmt.Fprintf(&recovery, "  rainier login --cloud %s --context %s\n", shellQuote(server), shellQuote(contextName))
	fmt.Fprintln(&recovery, "In the browser, sign in to the same Rainier account this context used before.")
	fmt.Fprintln(&recovery, "If it is present, do not reconnect again; repair only the saved policy:")
	appendReconnectPolicyRecovery(&recovery, provider, previous)
	return errors.New(strings.TrimSpace(recovery.String()))
}

func appendReconnectContext(recovery *strings.Builder, contextName string) {
	fmt.Fprintf(recovery, "Use the original context first: rainier context use %s\n", shellQuote(contextName))
}

func appendReconnectPolicyRecovery(recovery *strings.Builder, provider string, previous connectionView) {
	if previous.AccessMode == accessAll {
		fmt.Fprintln(recovery, "The previous policy covered all current and future workspaces; ask the hosted operator to restore all-workspace access.")
		if len(previous.Workspaces) > 0 {
			fmt.Fprintf(recovery, "Its dormant saved selection was: %s\n", safeReconnectList(previous.Workspaces))
		}
	} else if len(previous.Workspaces) > 0 {
		fmt.Fprintln(recovery, "Restore the previous workspace selection:")
		for _, workspace := range previous.Workspaces {
			fmt.Fprintf(recovery, "  rainier connection share %s --workspace %s\n", provider, shellQuote(workspace))
		}
	}
}

func validateReconnectSelection(workspaces []string) error {
	if len(workspaces) > 256 {
		return errors.New("connection reconnect: invalid workspace selection from server; the existing connection was not changed")
	}
	seen := make(map[string]struct{}, len(workspaces))
	for _, workspace := range workspaces {
		if !validReconnectOpaqueID(workspace) {
			return errors.New("connection reconnect: invalid workspace selection from server; the existing connection was not changed")
		}
		if _, duplicate := seen[workspace]; duplicate {
			return errors.New("connection reconnect: invalid workspace selection from server; the existing connection was not changed")
		}
		seen[workspace] = struct{}{}
	}
	return nil
}

func validReconnectOpaqueID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func validReconnectServer(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Host != "" && (u.Scheme == "https" || u.Scheme == "http") &&
		u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func validReconnectDisplay(value string) bool {
	return validReconnectOpaqueID(value)
}

func safeReconnectDisplay(value string) string {
	if validReconnectDisplay(value) {
		return value
	}
	return strconv.QuoteToASCII(value)
}

func safeReconnectList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, strconv.QuoteToASCII(value))
	}
	return strings.Join(quoted, ", ")
}

// shellQuote renders one opaque server value as one POSIX-shell argument.
// It is used only in recovery commands; values are still validated before a
// destructive operation, and quoting prevents printable punctuation from
// becoming another command when copied.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

// runConnectionLs renders GET /v0/connections: one row per connected provider,
// the outside login it names, and the workspaces it reaches.
//
// An account that has connected nothing gets the sentence rather than a bare
// header. "Nothing here" and "you have not done the browser step yet" look
// identical in a two-line empty table, and the second is what is almost always
// true — so say the next action instead of leaving the person to infer it.
func runConnectionLs(args []string) error {
	fs := flag.NewFlagSet("connection ls", flag.ExitOnError)
	fs.Parse(args)

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	rows, err := fetchConnections(cli.NewClient(cfg))
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Println(notConnectedHint(cfg))
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "PROVIDER\tLOGIN\tACCESS\tWORKSPACES")
	for _, c := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			c.Provider, dashIfEmpty(c.Login), dashIfEmpty(c.AccessMode), renderReach(c))
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// A connection shared with nothing is the state a fresh one is in, and it
	// is indistinguishable at a glance from a working one — the row looks
	// complete. The clone that fails an hour later is the same fact discovered
	// expensively, so name it here.
	current, _ := cfg.Active()
	for _, c := range rows {
		if c.AccessMode == accessSelected && len(c.Workspaces) == 0 {
			fmt.Printf("\n%s is connected but shared with no workspace, so sessions cannot use it.\n"+
				"Share it with the workspace you work in:  rainier connection share %s\n", c.Provider, c.Provider)
		} else if c.AccessMode == accessSelected && current.Workspace != "" && !slices.Contains(c.Workspaces, current.Workspace) {
			fmt.Printf("\n%s is not shared with your current workspace (%s), so sessions there cannot use it.\n"+
				"Share it:  rainier connection share %s\n", c.Provider, current.Workspace, c.Provider)
		}
	}
	return nil
}

// renderReach describes what a connection currently reaches, which is the
// selection only when the mode says so. Printing the stored selection beside an
// "all" mode would read as a limit that is not being applied.
func renderReach(c connectionView) string {
	if c.AccessMode == accessAll {
		return "(all workspaces)"
	}
	return dashIfEmpty(strings.Join(c.Workspaces, ","))
}

// runConnectionShare adds or removes one workspace from a connection's
// selection. The two directions are one function because they are one
// operation with one difference: whether the named workspace is in the set
// that gets written back.
//
// The read-modify-write preserves the grants in the snapshot it read, and is
// the only shape the API offers — PATCH replaces the whole selection, and
// there is no precondition to make the write conditional on that snapshot
// still being current. A concurrent edit landing in between is therefore lost.
// See the guarantee note at the top of this file; it is the API's window, not
// one this command opened.
func runConnectionShare(args []string, add bool) error {
	verb := "unshare"
	if add {
		verb = "share"
	}
	fs := flag.NewFlagSet("connection "+verb, flag.ExitOnError)
	workspace := fs.String("workspace", "", "workspace `id` to change (default: your current workspace)")
	fs.Parse(reorderArgs(fs, args))

	provider, err := connectionProviderNamed(fs.Arg(0), verb)
	if err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("usage: rainier connection %s <provider> [--workspace ID]", verb)
	}

	cfg, err := requireLogin()
	if err != nil {
		return err
	}
	current, _ := cfg.Active()
	if !current.Hosted() {
		return fmt.Errorf("connection %s: context %s is not a hosted one; provider connections come from `rainier login --cloud EDGE_URL`",
			verb, cfg.ActiveName())
	}
	c := cli.NewClient(cfg)

	target, err := resolveConnectionWorkspace(c, current.Workspace, *workspace, verb)
	if err != nil {
		return err
	}

	rows, err := fetchConnections(c)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(rows, func(v connectionView) bool { return v.Provider == provider })
	if i < 0 {
		return errors.New(notConnectedHint(cfg))
	}
	connection := rows[i]

	// The mode, not the selection, is what governs when the mode is "all".
	// Editing the selection underneath it would report a change in reach that
	// is not happening, in whichever direction the person asked for.
	if connection.AccessMode == accessAll {
		if add {
			fmt.Printf("%s (%s) is already shared with all your workspaces, including %s; nothing to do\n",
				provider, connection.Login, target.ID)
			return nil
		}
		return fmt.Errorf("%s is set to reach all current and future workspaces, so unsharing one would not stop it.\n"+
			"Narrowing means returning the connection to selected-workspace access, which this CLI does not change:\n"+
			"  PATCH /v0/connections/%s  {\"access_mode\":\"selected\"}\n"+
			"After that, `rainier connection share %s` chooses which workspaces it reaches",
			provider, provider, provider)
	}

	if connection.AccessMode != accessSelected {
		return fmt.Errorf("connection %s: unsupported access mode %q; inspect it with `rainier connection ls`", verb, connection.AccessMode)
	}

	if connection.Workspaces == nil {
		return fmt.Errorf("connection %s: server omitted the workspace selection; inspect it with `rainier connection ls` before trying again", verb)
	}

	next, changed := applyWorkspaceSelection(connection.Workspaces, target.ID, add)
	if !changed {
		if add {
			fmt.Printf("%s (%s) is already shared with %s\n", provider, connection.Login, describeWorkspace(target))
		} else {
			fmt.Printf("%s (%s) is not shared with %s\n", provider, connection.Login, describeWorkspace(target))
		}
		fmt.Printf("workspaces: %s\n", dashIfEmpty(strings.Join(next, ", ")))
		return nil
	}

	// Only the selection is sent. Omitting access_mode is load-bearing: the
	// edge applies each field only when present, so a share can never reset a
	// mode it was not asked to touch.
	body := struct {
		Workspaces []string `json:"workspaces"`
	}{Workspaces: next}
	var updated connectionView
	if err := c.Do(http.MethodPatch, "/v0/connections/"+provider, body, &updated); err != nil {
		return connectionError(err, provider, verb, cfg)
	}

	// The server can observe another edit before it constructs this response.
	// Never report an effective access change contradicted by that snapshot.
	if updated.Provider != provider || updated.Workspaces == nil ||
		updated.AccessMode != accessSelected || slices.Contains(updated.Workspaces, target.ID) != add {
		return fmt.Errorf("connection %s: the server response did not confirm the requested access for %s (access mode: %s). The selection may have changed concurrently; inspect it with `rainier connection ls` before trying again", verb, target.ID, updated.AccessMode)
	}

	if add {
		fmt.Printf("shared your %s connection (%s) with %s\n", provider, updated.Login, describeWorkspace(target))
	} else {
		fmt.Printf("removed %s from your %s connection (%s) workspace selection\n", describeWorkspace(target), provider, updated.Login)
	}
	fmt.Printf("workspaces now: %s\n", dashIfEmpty(strings.Join(updated.Workspaces, ", ")))
	return nil
}

// applyWorkspaceSelection returns the selection with id added or removed, and
// whether that was a change. The result is sorted so a selection the CLI writes
// does not depend on the order the server happened to return — Selection is
// documented as unordered.
func applyWorkspaceSelection(current []string, id string, add bool) ([]string, bool) {
	next := make([]string, 0, len(current)+1)
	found := false
	for _, w := range current {
		if w == id {
			found = true
			if !add {
				continue
			}
		}
		next = append(next, w)
	}
	if add && !found {
		next = append(next, id)
	}
	slices.Sort(next)
	return next, add != found
}

// resolveConnectionWorkspace decides which workspace the command acts on and
// requires membership for additions. Removals may target a workspace the
// caller has left: the personal connection is still theirs to narrow.
//
// Selecting a workspace is intent, not authorization — the broker re-checks
// membership at every delivery — so the server would accept an id the person
// cannot reach and store a selection that silently never delivers. Checking it
// here against the caller's own memberships turns that into an error at the
// moment it can still be fixed, and is the same check `rainier workspace use`
// already makes.
func resolveConnectionWorkspace(c *cli.Client, currentWorkspace, requested, verb string) (workspaceView, error) {
	id := requested
	if id == "" {
		id = currentWorkspace
	}
	if id == "" {
		return workspaceView{}, fmt.Errorf("connection %s: no workspace selected; pick one with `rainier workspace use <id>` or name it with --workspace", verb)
	}

	spaces, err := listWorkspaces(c)
	if err != nil {
		return workspaceView{}, fmt.Errorf("listing your workspaces: %w", err)
	}
	if i := slices.IndexFunc(spaces, func(w workspaceView) bool { return w.ID == id }); i >= 0 {
		return spaces[i], nil
	}

	if verb == "unshare" {
		return workspaceView{ID: id}, nil
	}

	ids := make([]string, 0, len(spaces))
	for _, w := range spaces {
		ids = append(ids, w.ID)
	}
	if len(ids) == 0 {
		return workspaceView{}, fmt.Errorf("connection %s: you are not a member of any workspace, so there is nothing to share with", verb)
	}
	return workspaceView{}, fmt.Errorf("connection %s: you are not a member of workspace %s; yours are: %s",
		verb, id, strings.Join(ids, ", "))
}

func describeWorkspace(w workspaceView) string {
	if w.Name == "" {
		return w.ID
	}
	return fmt.Sprintf("%s (%s)", w.ID, w.Name)
}

// fetchConnections reads GET /v0/connections.
//
// A 404 here is not "you have no connection" — the list answers an empty list
// for that. It is a server with no connection surface at all: a self-hosted
// controld, or a hosted edge deployed without the GitHub app configured. Those
// are an operator's problem and not something the person at the keyboard can
// fix by connecting harder, so say which it is.
func fetchConnections(c *cli.Client) ([]connectionView, error) {
	return fetchConnectionsContext(context.Background(), c)
}

func fetchConnectionsContext(ctx context.Context, c *cli.Client) ([]connectionView, error) {
	var resp connectionsEnvelope
	if err := c.DoContext(ctx, http.MethodGet, "/v0/connections", nil, &resp); err != nil {
		var apiErr *cli.APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			return nil, errors.New("this server has no provider connections: it is either a self-hosted rainier, " +
				"where `rainier creds` and `rainier login --refresh github` are the GitHub path, " +
				"or a hosted cell deployed without the GitHub connection turned on — ask your operator")
		}
		return nil, err
	}
	return resp.Connections, nil
}

// connectionError translates the edge's deliberately contentless refusals.
//
// The connect surface answers a fixed vocabulary with fixed prose ("resource
// not found", "conflict") so that no server message can leak a fact about
// another account. That is the right server behavior and it leaves the CLI to
// supply the meaning, which it can, because it knows what it just asked for.
func connectionError(err error, provider, verb string, cfg cli.Config) error {
	var apiErr *cli.APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	switch apiErr.Status {
	case http.StatusNotFound:
		// The connection was there when we read it a moment ago, so this is a
		// disconnect that landed in between rather than a state to explain.
		return errors.New(notConnectedHint(cfg))
	case http.StatusConflict:
		return fmt.Errorf("your %s connection was disconnected, so its workspace access can no longer be changed.\n"+
			"Connect it again from the login page: rainier login --cloud %s, then Connect GitHub", provider, cfg.ServerURL)
	case http.StatusForbidden:
		return fmt.Errorf("connection %s: refused for %s — a connection is personal, and only the account that "+
			"connected it may change where it reaches", verb, provider)
	case http.StatusBadRequest:
		return fmt.Errorf("connection %s: the server refused the workspace selection for %s as invalid", verb, provider)
	}
	return err
}

// notConnectedHint is the one sentence for "the account has no live connection
// for this provider". A revoked connection reads exactly the same way — the
// list only ever shows live ones — and the action is the same either way, so
// the two deliberately share a sentence.
func notConnectedHint(cfg cli.Config) string {
	target := cfg.ServerURL
	if target == "" {
		target = "EDGE_URL"
	}
	return fmt.Sprintf("GitHub is not connected to your account.\n"+
		"Connect it in the browser:  rainier login --cloud %s  and choose Connect GitHub on the last step", target)
}

// connectionProviderNamed validates the positional provider. A mutation names
// what it changes: `agent login` and `agent logout` both take the provider
// explicitly for the same reason, and a defaulted provider in a command that
// grants access is a worse trade than one more word.
func connectionProviderNamed(name, verb string) (string, error) {
	switch name {
	case "":
		suffix := " [--workspace ID]"
		if verb == "reconnect" {
			suffix = ""
		}
		return "", fmt.Errorf("usage: rainier connection %s <provider>%s\nproviders: %s",
			verb, suffix, strings.Join(connectionProviders, ", "))
	default:
		if slices.Contains(connectionProviders, name) {
			return name, nil
		}
		return "", fmt.Errorf("connection %s: unknown provider %q; this CLI knows: %s",
			verb, name, strings.Join(connectionProviders, ", "))
	}
}
