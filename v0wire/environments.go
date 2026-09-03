// v0wire/environments.go
package v0wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// envNamePattern is the whole vocabulary of an environment name: it is a CLI
// handle (`rainier new --env dev`) and half of a snapshot ref, so it stays in
// the lowercase-kebab alphabet that is safe in both a shell word and an OCI
// tag. Names can never contain "_", which is what tells a name from an id on
// the {id}-shaped routes.
var envNamePattern = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)

// EnvironmentView is the client-facing rendering of an Environment. Like
// SessionView, no field is omitempty: the key set is identical on every
// environment, including the three snapshot fields, which are present and
// empty until a session build caches one (a client compares snapshot_hash
// against setup_hash to see staleness for itself).
type EnvironmentView struct {
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
	Capabilities    []string          `json:"capabilities"`
	SetupTimeoutSec int               `json:"setup_timeout_sec"`
	SnapshotRef     string            `json:"snapshot_ref"`
	SnapshotRunner  string            `json:"snapshot_runner"`
	SnapshotHash    string            `json:"snapshot_hash"`
	CreatedAt       string            `json:"created_at"`
	UpdatedAt       string            `json:"updated_at"`
}

type EnvironmentEnvelope struct {
	Environment EnvironmentView `json:"environment"`
}

type EnvironmentsEnvelope struct {
	Environments []EnvironmentView `json:"environments"`
}

// RenderEnvironment renders e as its client-facing view.
//
// Two of the wire's fields have no field of their own on control.Environment,
// which names no runner: `placement` is carried as the portable capability
// "placement:<runner>" (EnvironmentRequirements) and read back out of it here,
// and `snapshot_runner` — which the control model does not carry at all — is
// passed in by the host, which read it off its own store row for the view.
// `capabilities` shares Requirements with the pin and is the rest of it: what
// this environment needs a runner to be able to DO, with the host's own
// spellings of WHERE (placement:, snapshot:) filtered back out.
func RenderEnvironment(e control.Environment, snapshotRunner control.RunnerID) EnvironmentView {
	return EnvironmentView{
		ID:              string(e.ID),
		Name:            e.Name,
		Image:           e.Image,
		Setup:           e.Setup,
		SetupHash:       e.SetupHash,
		Init:            e.Init,
		InitTimeoutSec:  e.InitTimeoutSec,
		EgressAllow:     emptyIfNil(e.EgressAllow),
		SecretRefs:      emptyIfNil(e.SecretRefs),
		Connectors:      connectorsJSON(e.Connectors),
		Placement:       PlacementOf(e.Requirements),
		Capabilities:    emptyIfNil(portableCapabilities(e.Requirements.Capabilities)),
		SetupTimeoutSec: e.SetupTimeoutSec,
		SnapshotRef:     e.Snapshot.Ref,
		SnapshotRunner:  string(snapshotRunner),
		SnapshotHash:    e.SnapshotHash,
		CreatedAt:       e.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:       e.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

// connectorsJSON renders cs as the JSON array a client sent: the stored bytes
// of each connector, handed back without re-rendering. (What survives the
// round trip is the JSON VALUE — a store may keep the client's exact bytes,
// while Postgres's jsonb preserves the value but may re-render whitespace and
// member order.) Never nil, so the array renders as "[]" rather than null.
func connectorsJSON(cs []control.Connector) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(cs))
	for _, c := range cs {
		raw := c.Raw
		if len(raw) == 0 {
			// Unreachable through this wire — ValidateConnectors always keeps
			// the caller's original bytes — but a Connector with no Raw would
			// encode as invalid JSON and truncate the whole response, so a
			// row from anywhere else degrades to the one field we still know.
			raw = json.RawMessage(`{"type":` + strconv.Quote(c.Type) + `}`)
		}
		out = append(out, raw)
	}
	return out
}

// ---------------------------------------------------------------------------
// requirements: one field on the row, two on the wire
// ---------------------------------------------------------------------------

// placementCapabilityPrefix is the capability spelling of an explicit runner
// pin (environment.placement), which control.Environment cannot name
// directly. It is a host prefix, and the only one this wire synthesizes.
const placementCapabilityPrefix = "placement:"

// EnvironmentRequirements composes an environment's one requirements list out
// of the two halves the wire keeps apart: the operator's runner pin, which
// control.Environment cannot name directly and so round-trips through the
// capability "placement:<runner>", and the portable capabilities the operator
// asked for. The pin goes first, so the list reads where-then-what, and
// RenderEnvironment takes the two halves back out by the same rule. No pin and
// no capabilities is no requirements at all rather than an empty list.
func EnvironmentRequirements(placement string, capabilities []string) control.Requirements {
	var caps []string
	if placement != "" {
		caps = append(caps, placementCapabilityPrefix+placement)
	}
	caps = append(caps, capabilities...)
	return control.Requirements{Capabilities: caps}
}

// PlacementOf is the read half: the runner an environment is pinned to, or ""
// when it is pinned to none.
func PlacementOf(reqs control.Requirements) string {
	for _, c := range reqs.Capabilities {
		if after, ok := strings.CutPrefix(c, placementCapabilityPrefix); ok {
			return after
		}
	}
	return ""
}

// portableCapabilities returns the capabilities of caps that are claims about
// what a runner can DO, dropping a host's own spellings of where something is
// (placement:, snapshot:). The colon is the whole test, and it is exact:
// ValidateCapabilities refuses a colon in anything a runner or an operator
// supplies, so every capability carrying one is a host's.
func portableCapabilities(caps []string) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if strings.Contains(c, ":") {
			continue
		}
		out = append(out, c)
	}
	return out
}

// ---------------------------------------------------------------------------
// request bodies
// ---------------------------------------------------------------------------

type CreateEnvironmentRequest struct {
	Name            string          `json:"name,omitempty"`
	Image           string          `json:"image,omitempty"`
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

// PatchEnvironmentRequest is PATCH's body: every field is a pointer (or, for
// connectors, a nil-able raw message) so "absent" is distinguishable from
// "set to the zero value" — clearing a list and leaving it alone are
// different requests.
type PatchEnvironmentRequest struct {
	Name            *string         `json:"name,omitempty"`
	Image           *string         `json:"image,omitempty"`
	Setup           *string         `json:"setup,omitempty"`
	Init            *string         `json:"init,omitempty"`
	InitTimeoutSec  *int            `json:"init_timeout_sec,omitempty"`
	EgressAllow     *[]string       `json:"egress_allow,omitempty"`
	SecretRefs      *[]string       `json:"secret_refs,omitempty"`
	Connectors      json.RawMessage `json:"connectors,omitempty"`
	Placement       *string         `json:"placement,omitempty"`
	Capabilities    *[]string       `json:"capabilities,omitempty"`
	SetupTimeoutSec *int            `json:"setup_timeout_sec,omitempty"`
}

// ValidateEnvironmentBasics checks the four scalar rules create and patch
// share, returning a client-facing message (or "" when the row is fine).
// Placement is deliberately unchecked: an environment may be pinned to a
// runner that hasn't joined the fleet yet, which is exactly how the hardware
// case is set up. Neither script is checked either — a shell script is only
// wrong once it runs.
func ValidateEnvironmentBasics(name, image string, setupTimeoutSec, initTimeoutSec int) string {
	switch {
	case !envNamePattern.MatchString(name):
		return "name must match [a-z0-9-]{1,64}"
	case image == "":
		return "image is required"
	case setupTimeoutSec < 0:
		return "setup_timeout_sec must not be negative"
	case initTimeoutSec < 0:
		return "init_timeout_sec must not be negative"
	}
	return ""
}

// ---------------------------------------------------------------------------
// the capability token rule
// ---------------------------------------------------------------------------

// MaxCapabilities bounds how many capabilities one claim may carry.
const MaxCapabilities = 32

// capabilityToken is the token rule: a lowercase token, and never a host
// prefix. It is deliberately narrow — a capability is matched by exact string
// equality across a fleet of runners nobody re-deploys at once, so a spelling
// that varies by case or whitespace is a placement that silently never
// happens.
var capabilityToken = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// validCapability reports whether s is a portable capability token.
func validCapability(s string) bool { return capabilityToken.MatchString(s) }

// ValidateCapabilities applies that rule to caps, returning a client-facing
// sentence naming the first thing wrong with it. what names the list in that
// sentence, so the same rule can answer for an environment's field and for a
// runner's announce without either message claiming to be the other.
//
// Host prefixes are refused rather than ignored: a capability naming a
// runner's own name is the HOST's claim about it, and a runner or an operator
// that could write one could pin any environment to any runner.
func ValidateCapabilities(what string, caps []string) error {
	if len(caps) > MaxCapabilities {
		return fmt.Errorf("%s: at most %d are allowed, got %d", what, MaxCapabilities, len(caps))
	}
	seen := make(map[string]bool, len(caps))
	for _, c := range caps {
		switch {
		case strings.Contains(c, ":"):
			return fmt.Errorf("%s: %q carries a host prefix, which only controld may claim", what, clip(c))
		case !validCapability(c):
			return fmt.Errorf("%s: %q must match [a-z0-9][a-z0-9._-]{0,63}", what, clip(c))
		case seen[c]:
			return fmt.Errorf("%s: %q is listed twice", what, clip(c))
		}
		seen[c] = true
	}
	return nil
}

// clip bounds caller-supplied text before it reaches an error message (which
// may become a WebSocket close reason, capped at 123 bytes), keeping the
// result valid UTF-8 even when the cut lands mid-rune.
func clip(s string) string {
	const max = 48
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "") + "..."
}

// ---------------------------------------------------------------------------
// connector vocabulary
//
// A connector is a declared attachment an environment's sessions get. The
// vocabulary is VALIDATED AND STORED here and nothing else: validating the
// shape is what lets the behaviors land without a migration, and rejecting
// unknown types is what keeps an old server from silently ignoring a
// connector a client relied on.
// ---------------------------------------------------------------------------

// DefaultBaseBranch is the branch a github connector clones when it names
// none.
const DefaultBaseBranch = "main"

// repoPattern is the "owner/name" spelling of a GitHub repository — the same
// two-segment shape `gh repo clone` accepts, and nothing else. It is the SHAPE
// check; validRepoRef below is the whole rule.
var repoPattern = regexp.MustCompile(`^[\w.-]+/[\w.-]+$`)

// validRepoRef reports whether s names a repository this wire will accept.
//
// The shape above is not sufficient on its own, because the name does not stay
// a name: a scheduler splits it and puts the second segment straight into a
// session's repo Dir, which a sandbox joins to /workspace un-cleaned and later
// hands to `git -C`. Validating that here is the point of a boundary — the
// alternative is relying on git's own accidents downstream, and an accident is
// not a rule.
func validRepoRef(s string) bool {
	if !repoPattern.MatchString(s) {
		return false
	}
	owner, name, _ := strings.Cut(s, "/")
	return validRepoSegment(owner) && validRepoSegment(name)
}

// validRepoSegment refuses the two segments that are not names at all.
//
//   - "." and "..": path elements. `/workspace/..` is `/`, and the only thing
//     otherwise standing between that and a clone outside the workspace is git
//     refusing a non-empty destination — its accident, not this boundary's
//     rule. GitHub does not allow either as a repository name anyway.
//   - a leading "-": an option wherever this string later sits in an argv, and
//     neither a GitHub login nor a repository name starts with one.
//
// A leading "." is deliberately still ALLOWED: `.github` is a real and common
// repository name, and refusing it would reject a legitimate connector to close
// nothing (a dotted directory under /workspace is still under /workspace).
func validRepoSegment(s string) bool {
	return s != "." && s != ".." && !strings.HasPrefix(s, "-")
}

// GitHubConnector is the github connector's v0 shape. BaseBranch is a pointer
// so an absent base_branch (which means DefaultBaseBranch) is distinguishable
// from an explicit empty one — an empty branch name is a typo, never a
// request for the default, and it must not reach a clone as one.
type GitHubConnector struct {
	Type       string  `json:"type"`
	Repo       string  `json:"repo"`
	BaseBranch *string `json:"base_branch"`
}

type filesConnector struct {
	Type  string   `json:"type"`
	Paths []string `json:"paths"`
}

type tunnelConnector struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`
}

type browserConnector struct {
	Type string `json:"type"`
	Tier string `json:"tier"`
}

// ValidateConnectors decodes and validates raw — the "connectors" member of
// an environment body — into the rows a store persists. An absent or empty
// array is no connectors at all.
//
// Every returned Connector carries the element's ORIGINAL bytes in Raw: stores
// render an empty Raw differently, so keeping Raw always-populated here is
// what keeps that difference out of reachable space, and it is what lets a
// client read back exactly the object it wrote.
//
// Errors are written for the caller: each names the offending element by
// index and says what was wrong with it, and none carries internal detail.
func ValidateConnectors(raw json.RawMessage) ([]control.Connector, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, errors.New("connectors must be an array of objects")
	}
	if len(elems) == 0 {
		return nil, nil
	}

	out := make([]control.Connector, 0, len(elems))
	for i, elem := range elems {
		// Loose decode first, for the discriminator alone: the strict decode
		// below can't run until we know which shape to check against.
		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(elem, &head); err != nil {
			return nil, fmt.Errorf("connectors[%d] must be an object", i)
		}
		if head.Type == "" {
			return nil, fmt.Errorf("connectors[%d] is missing type", i)
		}
		if err := validateConnector(head.Type, elem); err != nil {
			return nil, fmt.Errorf("connectors[%d]: %w", i, err)
		}
		out = append(out, control.Connector{Type: head.Type, Raw: elem})
	}
	return out, nil
}

// validateConnector strictly decodes one connector element against the shape
// its already-decoded connType names, and checks its fields. Unknown types
// are rejected by name (fail closed).
func validateConnector(connType string, elem json.RawMessage) error {
	switch connType {
	case "github":
		_, err := DecodeGitHubConnector(elem)
		return err

	case "files":
		var c filesConnector
		if err := strictDecode(elem, &c); err != nil {
			return err
		}
		if len(c.Paths) == 0 {
			return errors.New("files connector needs at least one entry in paths")
		}
		for _, p := range c.Paths {
			if p == "" {
				return errors.New("files connector has an empty string in paths")
			}
		}
		return nil

	case "tunnel":
		var c tunnelConnector
		if err := strictDecode(elem, &c); err != nil {
			return err
		}
		if c.Name == "" {
			return errors.New("tunnel connector needs a name")
		}
		if c.TargetHost == "" {
			return errors.New("tunnel connector needs a target_host")
		}
		if c.TargetPort < 1 || c.TargetPort > 65535 {
			return fmt.Errorf("tunnel connector target_port %d is outside 1..65535", c.TargetPort)
		}
		return nil

	case "browser":
		var c browserConnector
		if err := strictDecode(elem, &c); err != nil {
			return err
		}
		if c.Tier != "dedicated" && c.Tier != "extension" {
			return fmt.Errorf("browser connector tier must be dedicated or extension, got %q", c.Tier)
		}
		return nil

	default:
		return fmt.Errorf("unknown connector type %q", connType)
	}
}

// DecodeGitHubConnector strictly decodes elem as a github connector. The
// returned BaseBranch is never nil: an absent base_branch is filled in with
// DefaultBaseBranch here, in the decode a clone path repeats against the
// stored bytes — the default lives here, not in the stored row, so an
// environment keeps exactly the object its author wrote.
func DecodeGitHubConnector(elem json.RawMessage) (GitHubConnector, error) {
	var c GitHubConnector
	if err := strictDecode(elem, &c); err != nil {
		return GitHubConnector{}, err
	}
	if !validRepoRef(c.Repo) {
		return GitHubConnector{}, fmt.Errorf("github connector repo must be \"owner/name\", got %q", c.Repo)
	}
	if c.BaseBranch == nil {
		def := DefaultBaseBranch
		c.BaseBranch = &def
	} else if *c.BaseBranch == "" {
		return GitHubConnector{}, errors.New("github connector base_branch is empty; omit it for the default (" + DefaultBaseBranch + ")")
	}
	return c, nil
}

// strictDecode decodes elem into v rejecting unknown fields — the per-type
// half of connector validation, and the reason a typo'd key is a 400 instead
// of a silently dropped setting.
func strictDecode(elem json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(elem))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
