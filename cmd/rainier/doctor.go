package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/tokencanopy/rainier/internal/cli"
)

const doctorTimeout = 15 * time.Second
const readinessRequestTimeout = 4 * time.Second

var errDoctorFailed = errors.New("basic session readiness failed; follow the actions above, then run rainier status")
var errMalformedReadiness = errors.New("malformed readiness response")

// safeTerminal is intentionally bounded and removes terminal controls and bidi
// formatting. Callers must still avoid passing credentials or raw response bodies.
func safeTerminal(s string) string {
	var b strings.Builder
	n := 0
	for _, r := range stripDiagnosticControls(s) {
		if n >= 180 {
			b.WriteString("...")
			break
		}
		b.WriteRune(r)
		n++
	}
	return b.String()
}

func stripDiagnosticControls(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, s)
}

func safeServer(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "invalid server URL"
	}
	// Paths, queries, fragments, and userinfo can all contain credentials.
	// Final normalization, redaction, and truncation belong to diagnosticText.
	return u.Scheme + "://" + u.Host
}

var diagnosticURL = regexp.MustCompile(`(?i)(?:https?|wss?)://[^\s]+`)

func diagnosticText(cfg cli.Config, text string, currentTokens ...string) string {
	// Normalize before matching: otherwise removing a control after redaction
	// could reconstruct a token or credential-bearing URL in the final output.
	text = stripDiagnosticControls(text)
	secrets := append([]string{cfg.Token}, currentTokens...)
	for _, active := range cfg.Contexts {
		secrets = append(secrets, active.Token, active.RefreshToken)
	}
	// Remove URL credentials/paths first, even when a short credential happens
	// to match a URL's scheme. redactAll then does the credentials in one pass:
	// a loop of ReplaceAll can match a later secret inside the marker an
	// earlier one just wrote, which nests markers instead of redacting.
	text = diagnosticURL.ReplaceAllStringFunc(text, safeServer)
	return safeTerminal(redactAll(text, secrets))
}

// readinessHTTP bounds response size without changing shared client decoding or
// refresh semantics. Redirects are refused: diagnostic probes must stay on the
// configured server and must not forward workspace scope to another origin.
func readinessHTTP() *http.Client {
	return &http.Client{Transport: readinessTransport{http.DefaultTransport}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

type readinessTransport struct{ base http.RoundTripper }

func (t readinessTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	resp.Body = http.MaxBytesReader(nil, resp.Body, 1<<20)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Readiness requires exactly one bounded JSON document. Keep the shared
		// client's decoding contract unchanged for other commands and transfers.
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if !json.Valid(data) {
			return nil, errMalformedReadiness
		}
		resp.Body = io.NopCloser(bytes.NewReader(data))
	}
	return resp, nil
}

func readinessGET(ctx context.Context, c *cli.Client, path string, out any) error {
	reqCtx, cancel := context.WithTimeout(ctx, readinessRequestTimeout)
	defer cancel()
	return c.DoContext(reqCtx, http.MethodGet, path, nil, out)
}

func doctorReport(ctx context.Context, cfg cli.Config, w io.Writer) error {
	// The caller's deadline may be shorter for cancellation; this cap also protects
	// other in-package callers. Every request and refresh descends from this context.
	ctx, cancel := context.WithTimeout(ctx, doctorTimeout)
	defer cancel()
	active, ok := cfg.Active()
	var currentClient *cli.Client
	report := func(level, check, message string) {
		var currentTokens []string
		if currentClient != nil {
			currentTokens = []string{currentClient.Token, currentClient.RefreshToken}
		}
		fmt.Fprintf(w, "%s %s: %s\n", level, check, diagnosticText(cfg, message, currentTokens...))
	}
	if !ok || active.Token == "" || active.Server == "" {
		report("FAIL", "config", "no active login; run rainier login with your server URL (rainier help login)")
		return errDoctorFailed
	}
	u, err := url.Parse(active.Server)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		report("FAIL", "config", "invalid server URL; use a server URL without credentials, query, or fragment (rainier help login)")
		return errDoctorFailed
	}
	report("PASS", "config", "context "+cfg.ActiveName()+"; server "+safeServer(active.Server))
	if active.Hosted() && active.Workspace == "" {
		report("FAIL", "workspace", "no workspace selected; use rainier workspace use <id> from your login's workspace list")
		return errDoctorFailed
	}
	if active.Workspace != "" {
		report("PASS", "workspace", active.Workspace)
	} else {
		report("PASS", "workspace", "self-hosted context; server determines scope")
	}
	c := cli.NewClient(cfg)
	currentClient = c
	c.HTTP = readinessHTTP()
	var me struct {
		User userView `json:"user"`
	}
	err = readinessGET(ctx, c, "/v0/me", &me)
	if err == nil && me.User.ID == "" {
		err = errMalformedReadiness
	}
	if err != nil {
		report("FAIL", "authentication", readinessError(err))
		return errDoctorFailed
	}
	report("PASS", "authentication", "server accepted the current credentials")
	report("WARN", "backend version", "unknown; no version advertised by the probed API")
	failed, incomplete := false, false
	runners, err := fetchReadinessRunners(ctx, c)
	if err != nil {
		report("FAIL", "runners", readinessError(err))
		failed = true
	} else {
		ready, message := runnerReadiness(runners)
		if ready {
			report("PASS", "runners", message)
		} else {
			report("FAIL", "runners", message+"; ask your administrator to check runner availability/capacity")
			failed = true
		}
	}
	// A scope/auth refusal on any protected route invalidates dependent probes.
	if readinessAuthFailure(err) {
		return errDoctorFailed
	}
	var envs environmentsEnvelope
	err = readinessGET(ctx, c, "/v0/environments", &envs)
	if err == nil && envs.Environments == nil {
		err = errMalformedReadiness
	}
	if err == nil {
		for _, env := range envs.Environments {
			if env.ID == "" || env.Name == "" || env.Image == "" {
				err = errMalformedReadiness
				break
			}
		}
	}
	if err != nil {
		report("WARN", "environments", readinessError(err))
		incomplete = true
	} else if len(envs.Environments) == 0 {
		report("WARN", "environments", "none available; ask an administrator to create one (rainier help env)")
		incomplete = true
	} else {
		report("PASS", "environments", fmt.Sprintf("%d available; inspect with rainier env ls and rainier env show <name>", len(envs.Environments)))
	}
	if errors.Is(err, cli.ErrLoginAgain) || isHTTPStatus(err, 401) {
		report("FAIL", "authentication", "environment access rejected; check your login/workspace with the administrator")
		return errDoctorFailed
	}
	var agents agentsEnvelope
	err = readinessGET(ctx, c, "/v0/agents", &agents)
	if err == nil && agents.Agents == nil {
		err = errMalformedReadiness
	}
	loggedIn := 0
	for _, a := range agents.Agents {
		if a.Provider == "" || (a.Status != "logged_in" && a.Status != "none") {
			err = errMalformedReadiness
			break
		}
		if a.Status == "logged_in" {
			loggedIn++
		}
	}
	if err != nil {
		report("WARN", "agents", readinessError(err))
		incomplete = true
	} else if loggedIn == 0 {
		report("WARN", "agents", "no agent login stored; run rainier agent status, then rainier agent login <provider>")
		incomplete = true
	} else {
		report("PASS", "agents", fmt.Sprintf("%d stored login(s); provider validity and installed CLI are not checked (rainier agent status)", loggedIn))
	}
	// Optional route permission/absence does not invalidate basic shell sessions.
	if errors.Is(err, cli.ErrLoginAgain) || isHTTPStatus(err, 401) {
		report("FAIL", "authentication", "credentials rejected; log in again (rainier help login)")
		return errDoctorFailed
	}
	if incomplete {
		report("WARN", "coding-agent readiness", "coding-agent readiness incomplete; finish environment/agent setup before starting a coding agent")
	}
	if failed {
		return errDoctorFailed
	}
	report("PASS", "basic session readiness", "basic session readiness passed; capacity is a current observation, not a placement guarantee")
	return nil
}

// Pointers distinguish omitted observations from zero capacity/disconnection.
// These fields mirror GET /v0/runners (v0wire.RunnerView); no new API is assumed.
type readinessRunner struct {
	Connected     *bool `json:"connected"`
	CapacityTotal *int  `json:"capacity_total"`
	CapacityUsed  *int  `json:"capacity_used"`
}

func fetchReadinessRunners(ctx context.Context, c *cli.Client) ([]readinessRunner, error) {
	var resp struct {
		Runners []readinessRunner `json:"runners"`
	}
	if err := readinessGET(ctx, c, "/v0/runners", &resp); err != nil {
		return nil, err
	}
	if resp.Runners == nil {
		return nil, errMalformedReadiness
	}
	for _, r := range resp.Runners {
		if r.Connected == nil || r.CapacityTotal == nil || r.CapacityUsed == nil || *r.CapacityTotal < 0 || *r.CapacityUsed < 0 {
			return nil, errMalformedReadiness
		}
	}
	return resp.Runners, nil
}
func runnerReadiness(runners []readinessRunner) (bool, string) {
	if len(runners) == 0 {
		return false, "no runners reported"
	}
	connected := 0
	for _, r := range runners {
		if *r.Connected {
			connected++
			if *r.CapacityTotal > *r.CapacityUsed {
				return true, "connected runner reports free capacity"
			}
		}
	}
	if connected == 0 {
		return false, "no connected runners reported"
	}
	return false, "connected runners report no free capacity"
}

func isHTTPStatus(err error, status int) bool {
	var api *cli.APIError
	return errors.As(err, &api) && api.Status == status
}
func readinessAuthFailure(err error) bool {
	return errors.Is(err, cli.ErrLoginAgain) || isHTTPStatus(err, 401) || isHTTPStatus(err, 403)
}

// Never include err.Error(): transport errors can contain full URLs, while API
// errors can contain response bodies or credentials echoed by an upstream.
func readinessError(err error) string {
	if errors.Is(err, cli.ErrLoginAgain) {
		return "authentication rejected; log in again (rainier help login)"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout; check server connectivity and rerun rainier status"
	}
	if errors.Is(err, context.Canceled) {
		return "request cancelled; rerun rainier status when ready"
	}
	var api *cli.APIError
	if errors.As(err, &api) {
		retry := ""
		if n, e := strconv.ParseUint(api.RetryAfter, 10, 32); e == nil {
			retry = fmt.Sprintf("; Retry-After: %d seconds", n)
		} else if when, e := http.ParseTime(api.RetryAfter); e == nil {
			retry = "; Retry-After: " + when.UTC().Format(http.TimeFormat)
		}
		switch api.Status {
		case 401:
			return "authentication rejected (401); log in again (rainier help login)"
		case 403:
			return "access forbidden (403); check workspace access with your administrator"
		case 404:
			return "endpoint unavailable; compatibility not established (404); ask your administrator"
		case 429:
			return "rate limited (429); wait before retrying" + retry
		default:
			if api.Status >= 500 {
				return fmt.Sprintf("server error (%d); ask your administrator or retry later", api.Status) + retry
			}
			return fmt.Sprintf("unexpected HTTP status (%d); verify the configured server with your administrator", api.Status)
		}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "DNS lookup failed; check the configured server hostname"
	}
	var cert *tls.CertificateVerificationError
	if errors.As(err, &cert) {
		return "TLS certificate verification failed; check server certificates with your administrator"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout; check server connectivity and rerun rainier status"
	}
	var syntax *json.SyntaxError
	var field *json.UnmarshalTypeError
	var tooLarge *http.MaxBytesError
	if errors.Is(err, errMalformedReadiness) || errors.As(err, &syntax) || errors.As(err, &field) || errors.As(err, &tooLarge) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "malformed response; compatibility not established; ask your administrator"
	}
	return "connection or response failure; verify the server URL and connectivity with your administrator"
}
