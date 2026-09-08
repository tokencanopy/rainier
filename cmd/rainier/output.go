package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/tokencanopy/rainier/internal/cli"
)

// ---------------------------------------------------------------------------
// streams and exit codes (docs/cli-v0-contract.md §6.1)
// ---------------------------------------------------------------------------

// usageError is an invalid invocation: the user typed something this CLI
// cannot act on, as opposed to something the server refused. main renders it
// on stderr and exits 2, which is the distinction a script needs — a 2 is
// never worth retrying and a 1 sometimes is.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return usageError{msg: fmt.Sprintf(format, args...)}
}

// exitCodeFor is main's whole exit-code policy.
func exitCodeFor(err error) int {
	if err == nil {
		return 0
	}
	var u usageError
	if errors.As(err, &u) {
		return 2
	}
	return 1
}

// ---------------------------------------------------------------------------
// redaction (contract §6.3)
// ---------------------------------------------------------------------------

// redactSecrets is the last thing every human-facing error passes through. It
// removes control and bidi characters — server prose reaches this CLI
// untrusted, and a terminal escape in it can rewrite lines the user already
// read — and replaces any credential this machine holds with [redacted].
//
// It deliberately does NOT strip URLs, which doctor's stricter diagnosticText
// does: the onboarding destination and the login URL are the entire point of
// several of these messages. The credential substitution is what matters
// here, and it is exact rather than pattern-based, so it cannot mangle a
// perfectly ordinary word that happens to look token-shaped.
func redactSecrets(cfg cli.Config, text string) string {
	var secrets []string
	for _, ctx := range cfg.Contexts {
		secrets = append(secrets, ctx.Token, ctx.RefreshToken)
	}
	secrets = append(secrets, cfg.Token)
	return redactAll(stripControlsKeepingLines(text), secrets)
}

// minRedactableSecret is the shortest string this redactor will substitute.
//
// It exists because substitution is not free. Every credential these servers
// issue is far longer than this, so nothing real is missed; a shorter value in
// a config — a hand-edited file, a fixture, a truncated write — is
// indistinguishable from ordinary text, and replacing it turns every letter of
// a message into "[redacted]" while protecting nothing. An unreadable error is
// its own kind of failure.
const minRedactableSecret = 8

// redactAll replaces every occurrence of every secret with [redacted], in ONE
// pass over the text.
//
// The single pass is the point, and a loop of strings.ReplaceAll is not
// equivalent: the marker it writes contains letters, so a later secret can
// match inside a replacement the redactor itself just made, and the output
// degenerates into nested markers. Scanning once and skipping past what has
// been emitted cannot do that. Longest match wins at each position, so a short
// credential that is a prefix of a long one never redacts half of it and
// leaves the rest legible.
func redactAll(text string, secrets []string) string {
	cleaned := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		if s := stripControlsKeepingLines(secret); len(s) >= minRedactableSecret {
			cleaned = append(cleaned, s)
		}
	}
	if len(cleaned) == 0 {
		return text
	}
	sort.Slice(cleaned, func(i, j int) bool { return len(cleaned[i]) > len(cleaned[j]) })

	var b strings.Builder
	for i := 0; i < len(text); {
		matched := 0
		for _, secret := range cleaned {
			if strings.HasPrefix(text[i:], secret) {
				matched = len(secret)
				break // cleaned is longest-first, so this is the longest match here
			}
		}
		if matched == 0 {
			b.WriteByte(text[i])
			i++
			continue
		}
		b.WriteString("[redacted]")
		i += matched
	}
	return b.String()
}

// stripControlsKeepingLines removes every control and bidi character except
// the newline and the tab.
//
// doctor's stripDiagnosticControls removes those two as well, and it is right
// to: its report is one line per check, and a newline arriving inside server
// prose would forge a check line. An ordinary command error is the opposite
// case — several of them are deliberately two or three lines, one of which is
// the "Continue:" address or the command to run next — and stripping the
// newlines runs those lines together into an unreadable sentence.
func stripControlsKeepingLines(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, s)
}

// reportError renders one command failure on stderr: redacted, with the
// server's stable error code and request id when there is one. The code is
// printed because it is what a caller branches on and what a bug report
// should quote; the request id because it is the only handle support has on
// one exchange out of a day's worth.
func reportError(cfg cli.Config, w io.Writer, err error) {
	fmt.Fprintln(w, redactSecrets(cfg, err.Error()))
	var apiErr *cli.APIError
	if errors.As(err, &apiErr) && (apiErr.Code != "" || apiErr.RequestID != "") {
		parts := []string{}
		// Both are server-controlled — the code comes out of the error
		// envelope and the request id out of a response header — so neither
		// reaches the terminal without passing the same sanitizer the message
		// above it does.
		if apiErr.Code != "" {
			parts = append(parts, "code "+safeField(redactSecrets(cfg, apiErr.Code)))
		}
		if apiErr.RequestID != "" {
			parts = append(parts, "request "+safeField(redactSecrets(cfg, apiErr.RequestID)))
		}
		fmt.Fprintln(w, "  ("+strings.Join(parts, ", ")+")")
	}
}

// safeField renders one server-supplied value into human-facing output.
//
// Every string in a session row, a status line or an error envelope came off
// the wire, and several of them came off the wire from somebody else:
// GET /v0/sessions is team-visible and the create route puts no character
// restriction on a session name, so a teammate can name a session with a
// terminal escape sequence in it. Printing that raw lets them redraw the
// screen of anyone who runs `rainier ls` — clearing lines the reader has
// already seen, or forging output that looks like this CLI's own.
//
// stripDiagnosticControls is the strict variant on purpose here: a table cell
// and a status line are single-line by construction, so an embedded newline is
// itself a way to forge a row. The bound is safeTerminal's, so one absurd name
// cannot push a table off the screen.
func safeField(v string) string { return safeTerminal(v) }

// ---------------------------------------------------------------------------
// --json documents (contract §6.2)
// ---------------------------------------------------------------------------

// jsonSchemaVersion is bumped only when a document changes incompatibly.
// Adding a field is not that: every consumer of these documents is expected
// to ignore what it does not know.
const jsonSchemaVersion = 1

// The document names. One per command whose answer a script consumes.
const (
	schemaStatus      = "rainier.v0.status"
	schemaSessions    = "rainier.v0.sessions"
	schemaSession     = "rainier.v0.session"
	schemaAgentStatus = "rainier.v0.agent_status"
	schemaMutation    = "rainier.v0.mutation"
)

// writeJSON emits one document on stdout and nothing else. The schema and
// version lead so a reader can dispatch on them before decoding the rest, and
// the encoder is indented because a person reads this output too — a pipe is
// not the only consumer, and `--json` is explicit rather than inferred from
// one (contract §6.1).
func writeJSON(w io.Writer, schema string, doc map[string]any) error {
	out := map[string]any{"schema": schema, "version": jsonSchemaVersion}
	for k, v := range doc {
		out[k] = v
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// mutationDocument is the shape every asynchronous mutation answers with:
// what was acted on, what the server said about it, and whether the work is
// finished or merely accepted.
func mutationDocument(action, id string, accepted bool, state string, detail string) map[string]any {
	doc := map[string]any{
		"action":   action,
		"session":  id,
		"accepted": accepted,
	}
	if state != "" {
		doc["state"] = state
	}
	if detail != "" {
		doc["detail"] = detail
	}
	return doc
}

// interactiveTerminal reports whether there is a human at this invocation to
// answer a question. Both ends have to be a terminal: stdin because the
// answer has to come from somewhere, stdout because a prompt written into a
// pipe is a prompt nobody sees. A command that needs consent and finds this
// false must refuse, never block — a script that forgot --yes has to fail,
// not hang forever on a read that can never return.
func interactiveTerminal() bool { return isInteractive() }

// isInteractive is a variable so the confirmation paths can be driven by a
// test with a pipe for stdin, which is the only way to exercise the prompt at
// all. Production never replaces it.
var isInteractive = func() bool {
	return charDevice(os.Stdin) && charDevice(os.Stdout)
}

func charDevice(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
