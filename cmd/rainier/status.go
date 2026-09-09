package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/tokencanopy/rainier/internal/cli"
)

// `rainier status` is the second command in the product's happy path and the
// one a person runs whenever something is not working. It answers one
// question — can I start coding? — in seven lines, and it answers it from the
// server rather than from this machine (docs/cli-v0-contract.md §4.3).
//
// Two rules shape everything below. The first: readiness is the server's to
// state. A token on disk proves a login happened, not that a workspace has
// compute; a URL somebody was redirected through proves nothing at all. So
// every row here is a projection of an API answer, and a row whose probe
// could not run is reported as unknown rather than assumed good. The second:
// where a person has to go next is also the server's to state. When compute
// needs a plan, payment or provisioning, the destination printed is the one
// the server named, and when the server named none the CLI prints no
// destination at all.

// statusRow is one line of the report: its label, the word after the colon,
// and — when the server named one — the web address that continues it.
type statusRow struct {
	Label string
	// Key is the row's name in --json: stable, lowercase, and not derived
	// from Label, so rewording a label never breaks a script.
	Key      string
	Value    string
	Continue string
	// Required rows decide the exit code. GitHub and the agents are reported
	// honestly and do not fail the command: neither blocks `rainier new`.
	Required bool
	// Ready is the row's own verdict, kept apart from Value because several
	// different words ("ready", "connected") mean ready and several
	// ("setup required", "unknown") do not.
	Ready bool
	Facts map[string]any
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	verbose := fs.Bool("verbose", false, "include the full readiness diagnostics")
	asJSON := fs.Bool("json", false, "print one machine-readable status document")
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() != 0 {
		return usagef("usage: rainier status [--verbose] [--json]")
	}
	return status(context.Background(), *verbose, *asJSON, os.Stdout, os.Stderr)
}

func status(ctx context.Context, verbose, asJSON bool, out, diag io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 2*doctorTimeout)
	defer cancel()
	cfg, err := cli.Load()
	if err != nil {
		// An unreadable config is not a readiness answer; it is a broken
		// installation, and every row below would be a guess.
		return fmt.Errorf("cannot read the rainier config: %w", err)
	}
	rows, ready := collectStatus(ctx, cfg)

	if asJSON {
		if err := writeStatusJSON(out, cfg, rows, ready); err != nil {
			return err
		}
	} else {
		printStatus(out, rows)
	}
	// --verbose is doctor's report, and doctor writes diagnostics. They go to
	// stderr so `rainier status --verbose --json | jq` still works.
	if verbose {
		target := out
		if asJSON {
			target = diag
		}
		fmt.Fprintln(target)
		fmt.Fprintln(target, "diagnostics:")
		_ = doctorReport(ctx, cfg, target) // its verdict is already in `ready`
	}
	if !ready {
		return errNotReady
	}
	return nil
}

var errNotReady = errors.New("workspace is not ready; follow the actions above")

// collectStatus builds the report. Each probe is independent: one failing
// leaves its own row unknown and does not stop the others, because a person
// whose GitHub connection is down still needs to see that their compute is
// fine.
func collectStatus(ctx context.Context, cfg cli.Config) ([]statusRow, bool) {
	ctx, cancel := context.WithTimeout(ctx, doctorTimeout)
	defer cancel()
	active, ok := cfg.Active()
	if !ok || !active.SignedIn() {
		return []statusRow{{
			Label: "Signed in", Key: "signed_in", Value: "no", Required: true,
			Continue: "", Ready: false,
		}}, false
	}

	c := cli.NewClient(cfg)
	c.HTTP = readinessHTTP()

	rows := []statusRow{}
	add := func(r statusRow) { rows = append(rows, r) }

	// Signed in, from /v0/me and not from the file that holds the token.
	var me struct {
		User userView `json:"user"`
	}
	meErr := readinessGET(ctx, c, "/v0/me", &me)
	if meErr == nil && me.User.ID == "" {
		meErr = errMalformedReadiness
	}
	switch {
	case meErr != nil:
		add(statusRow{Label: "Signed in", Key: "signed_in", Value: "no (" + readinessError(meErr) + ")", Required: true})
		// Nothing below can be answered by a server that will not say who we
		// are; reporting seven unknown rows would bury the one that matters.
		return rows, false
	default:
		who := "yes"
		if me.User.Login != "" {
			who = "yes (" + me.User.Login + ")"
		}
		add(statusRow{Label: "Signed in", Key: "signed_in", Value: who, Required: true, Ready: true})
	}

	// The compute enrollment is the authoritative answer to "can this
	// workspace run anything", and the onboarding map is the server's answer
	// to "where do I send somebody whose compute is in this state". They are
	// separate reads because they have separate owners: the enrollment is
	// regional tenant state, the console address is edge deployment config.
	compute, computeErr := fetchCompute(ctx, c, active.Workspace)
	onboarding, _ := fetchOnboarding(ctx, c)

	add(workspaceRow(ctx, c, active))
	cr := computeRow(compute, computeErr, onboarding)
	if !active.Hosted() && !compute.Published && computeErr == nil {
		runners, err := fetchReadinessRunners(ctx, c)
		if err != nil {
			cr.Value = "unknown (" + readinessError(err) + ")"
		} else {
			cr.Ready, cr.Value = runnerReadiness(runners)
		}
	}
	add(cr)
	add(environmentRow(ctx, c))
	add(githubRow(ctx, c, active, onboarding))
	rows = append(rows, agentRows(ctx, c)...)

	allReady := true
	for _, r := range rows {
		if r.Required && !r.Ready {
			allReady = false
		}
	}
	return rows, allReady
}

func workspaceRow(ctx context.Context, c *cli.Client, active cli.Context) statusRow {
	row := statusRow{Label: "Workspace", Key: "workspace", Required: true}
	if !active.Hosted() {
		// A self-hosted controld scopes by the server itself; there is no
		// workspace to choose and none to be missing.
		row.Value, row.Ready = "self-hosted (scoped by the server)", true
		return row
	}
	if active.Workspace == "" {
		row.Value = "none selected"
		return row
	}
	spaces, err := listWorkspacesContext(ctx, c)
	if err != nil {
		// The id is what the requests are actually scoped by, so it is a
		// true answer even when the name lookup failed.
		row.Value = "unknown (" + readinessError(err) + ")"
		return row
	}
	for _, w := range spaces {
		if w.ID == active.Workspace {
			row.Value, row.Ready = w.Name, true
			return row
		}
	}
	row.Value = active.Workspace + " (no longer available to this account)"
	return row
}

// computeRow is the row the product boundary is about, and it is read from
// the server's compute enrollment rather than inferred from anything.
//
// Two fields decide it, not one. `status` is what the workspace is entitled
// to; `health` is whether that entitlement can be reached right now. Ready
// capacity with no connected runner is `ready` + `unavailable`, and reporting
// that as ready would send somebody to `rainier new` to be refused — so both
// have to agree before this row says ready.
//
// When the server publishes no compute route at all — an older cell, a
// self-hosted controld, a cell composed to sell no compute — the CLI falls
// back to what it can observe: a connected runner with free capacity is
// compute, and none is not. It prints no destination in that mode, because it
// has none to print.
func computeRow(compute computeState, computeErr error, onboarding onboardingDestinations) statusRow {
	row := statusRow{Label: "Compute", Key: "compute", Required: true}
	if computeErr != nil {
		row.Value = "unknown (" + readinessError(computeErr) + ")"
		return row
	}
	if !compute.Published {
		row.Value = "unknown (this server does not publish compute readiness)"
		return row
	}
	row.Facts = map[string]any{"status": compute.Status, "health": compute.Health}
	row.Continue = onboarding.destinationFor(compute.Status)
	if compute.ready() {
		row.Value, row.Ready = "ready", true
		return row
	}
	switch compute.Status {
	case computeNeedsPlan:
		row.Value = "setup required (no plan selected)"
	case computeAwaitingPayment:
		row.Value = "setup required (awaiting payment)"
	case computeProvisioning:
		row.Value = "provisioning"
	case computeReady:
		// Entitled but not reachable. Say which of the two it is, because the
		// recovery is completely different from selecting a plan.
		row.Value = "entitled, but not reachable right now (health " + safeField(compute.Health) + ")"
	case computeFailed:
		row.Value = "failed"
	case computeCancelling:
		row.Value = "cancelling"
	case computeCancelled:
		row.Value = "cancelled"
	default:
		row.Value = "unknown (server reported state " + safeField(compute.Status) + ")"
	}
	return row
}

// environmentRow reports whether the workspace has an environment a session
// can start from. The server marks its default; a catalog with exactly one is
// the same answer read a longer way round.
func environmentRow(ctx context.Context, c *cli.Client) statusRow {
	row := statusRow{Label: "Default environment", Key: "default_environment", Required: true}
	env, err := resolveDefaultEnvironment(ctx, c)
	switch {
	case err == nil:
		row.Value, row.Ready = "ready ("+safeField(env.Name)+")", true
	case errors.Is(err, errNoDefaultEnvironment):
		// Several environments and no server-published default is not a
		// broken workspace: `rainier new --env NAME` still works, and so does
		// a scratch session. Say which it is.
		row.Value = "no unambiguous default environment; configure one on the web or use new --env NAME"
	default:
		row.Value = "unknown (" + readinessError(err) + ")"
	}
	return row
}

// githubRow reports whether sessions can reach the developer's repositories.
// Hosted and self-hosted answer this from different places — a browser-
// authorized connection versus a vaulted credential — and neither is a check
// this CLI can perform itself, so both are read rather than inferred.
func githubRow(ctx context.Context, c *cli.Client, active cli.Context, onboarding onboardingDestinations) statusRow {
	row := statusRow{Label: "GitHub", Key: "github"}
	_ = onboarding // no server route publishes a GitHub-specific destination
	if active.Hosted() {
		var resp connectionsEnvelope
		if err := readinessGET(ctx, c, "/v0/connections", &resp); err != nil {
			row.Value = "unknown (" + readinessError(err) + ")"
			return row
		}
		for _, conn := range resp.Connections {
			if conn.Provider != "github" || conn.RevokedAt != nil {
				continue
			}
			// A connection that reaches no workspace cannot serve the
			// session this person is about to start, and calling that
			// "connected" is the mistake worth avoiding here. An "all"
			// access mode reaches every workspace by definition, so its
			// selection list is not the question.
			if conn.AccessMode != accessAll && active.Workspace != "" && !containsString(conn.Workspaces, active.Workspace) {
				row.Value = "connected, but not shared with this workspace"
				return row
			}
			row.Value, row.Ready = "connected", true
			return row
		}
		row.Value = "not connected"
		return row
	}

	var creds credentialsEnvelope
	if err := readinessGET(ctx, c, "/v0/credentials", &creds); err != nil {
		row.Value = "unknown (" + readinessError(err) + ")"
		return row
	}
	for _, cr := range creds.Credentials {
		if cr.Provider != "github" {
			continue
		}
		if cr.Status == "valid" {
			row.Value, row.Ready = "connected", true
			return row
		}
		row.Value = "needs attention (" + safeTerminal(cr.Status) + ")"
		return row
	}
	row.Value = "not connected"
	return row
}

// agentRows renders one row per coding agent the server knows, in the
// server's own order. The provider list is the server's: a build that
// hardcoded "Claude" and "Codex" would stop telling the truth the day a third
// agent shipped.
func agentRows(ctx context.Context, c *cli.Client) []statusRow {
	rows, err := fetchAgentsContext(ctx, c)
	if err != nil {
		return []statusRow{{Label: "Coding agents", Key: "agents", Value: "unknown (" + readinessError(err) + ")"}}
	}
	out := make([]statusRow, 0, len(rows))
	for _, a := range rows {
		state := agentReadiness(a)
		out = append(out, statusRow{
			Label: agentLabel(a.Provider),
			Key:   "agent_" + a.Provider,
			Value: state,
			Ready: state == agentReady,
		})
	}
	return out
}

// agentLabel capitalizes a provider name for a human-facing label without
// inventing a display-name table: the server owns the names, and this is
// presentation, not data.
func agentLabel(provider string) string {
	if provider == "" {
		return "Agent"
	}
	return capitalizeFirst(provider)
}

// capitalizeFirst upper-cases the first RUNE of a display word.
//
// A rune and not a byte: slicing s[:1] off a multi-byte first character hands
// strings.ToUpper a lone continuation byte, which comes back as U+FFFD and
// renders the word as a replacement glyph.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	first, width := utf8.DecodeRuneInString(s)
	return strings.ToUpper(string(first)) + s[width:]
}

// printStatus writes the report. Labels, values and the destination all
// contain strings the server chose — a workspace name, an environment name, a
// provider name, an onboarding URL — so each goes through safeField before it
// reaches a terminal.
func printStatus(w io.Writer, rows []statusRow) {
	for _, r := range rows {
		fmt.Fprintf(w, "%s: %s\n", safeField(r.Label), safeField(r.Value))
		// The continue line belongs to the row that is not ready, and only to
		// it: repeating one destination under four healthy rows would make
		// the report unreadable and the action ambiguous.
		if !r.Ready && r.Continue != "" {
			fmt.Fprintf(w, "Continue: %s\n", safeField(r.Continue))
		}
	}
}

func writeStatusJSON(w io.Writer, cfg cli.Config, rows []statusRow, ready bool) error {
	checks := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		check := map[string]any{
			"name":     r.Key,
			"label":    redactSecrets(cfg, r.Label),
			"value":    redactSecrets(cfg, r.Value),
			"ready":    r.Ready,
			"required": r.Required,
		}
		if r.Facts != nil {
			check["facts"] = r.Facts
		}
		// The destination belongs to the row that needs it, in JSON exactly as
		// on screen: a ready row carrying a "continue" reads as an action
		// still outstanding, and a consumer branching on its presence would
		// act on one.
		if !r.Ready && r.Continue != "" {
			check["continue"] = r.Continue
		}
		checks = append(checks, check)
	}
	return writeJSON(w, schemaStatus, map[string]any{
		"ready":  ready,
		"checks": checks,
	})
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// runDoctor is the compatibility alias (contract §2.1): `doctor` is exactly
// `status --verbose`, so it cannot drift from the command that replaced it.
func runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	fs.Parse(args)
	if fs.NArg() != 0 {
		return usagef("usage: rainier doctor")
	}
	deprecated("doctor", "status --verbose")
	return status(context.Background(), true, false, os.Stdout, os.Stderr)
}

// deprecated prints one line on stderr when a compatibility alias is used. On
// stderr, because it is a diagnostic about the invocation and must not land
// in output somebody is parsing.
func deprecated(alias, canonical string) {
	fmt.Fprintf(os.Stderr, "note: `rainier %s` is a compatibility alias for `rainier %s`\n", alias, canonical)
}
