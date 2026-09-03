// v0wire/sessions.go
package v0wire

import (
	"fmt"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// SessionView is the client-facing rendering of a Session: every field the
// route table promises, RFC 3339 UTC timestamps, and nil Cmd/EgressAllow
// normalized to "[]" so the API never exposes one store's nil-vs-empty-slice
// difference from another's. No field is omitempty — the key set is meant to
// be identical on every session, which is what the response-shape regression
// tests pin.
//
// Image is the image the session ACTUALLY runs: for a session started from an
// environment that is the resolved one (the environment's image, or its
// cached snapshot), not the empty override the client sent. Environment and
// QueueReason are derived, never stored — see SessionDerived.
type SessionView struct {
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
	Environment string   `json:"environment"`
	QueueReason string   `json:"queue_reason"`
	// ChildExitCode is the exit status of the session's agent process, once
	// it has one, and null until then. A POINTER, and rendered as null rather
	// than omitted, because exit 0 is an ANSWER: a session whose agent
	// finished cleanly has to be distinguishable from one still working, and
	// a plain int would make those two the same 0. Present on every session
	// like every other field here — a key that appears only sometimes cannot
	// be told apart from an older controld that never had it.
	ChildExitCode *int   `json:"child_exit_code"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
	LastEventAt   string `json:"last_event_at"`
}

// SessionDerived carries the three view fields that cannot be read off the
// session row: each depends on live connection state or on another table, and
// none is stored. Reachable follows the rule s.Runner != "" &&
// runnerConnected(s.Runner) && !s.State.Terminal(); Environment is the
// environment's NAME ("" for a scratch session, or one whose environment has
// since been deleted); QueueReason explains a queued session that is waiting
// on a specific runner. The host computes all three — they are the questions
// only it can answer.
type SessionDerived struct {
	Reachable   bool
	Environment string
	QueueReason string
}

// RenderSession renders s as its client-facing view, with d supplying the
// fields the row itself cannot answer for.
func RenderSession(s control.Session, d SessionDerived) SessionView {
	return SessionView{
		ID:          string(s.ID),
		OwnerID:     string(s.CreatorID),
		Name:        s.Name,
		Image:       s.Spec.Image,
		Cmd:         emptyIfNil(s.Spec.Cmd),
		EgressAllow: emptyIfNil(s.Spec.EgressAllow),
		State:       string(s.State),
		Runner:      string(s.RunnerID),
		Reachable:   d.Reachable,
		Error:       s.Error,
		Environment: d.Environment,
		QueueReason: d.QueueReason,
		// Copied, never aliased: the row's pointer may be into a store's own
		// map (memstore hands out clones for exactly this reason, but a view
		// that relied on that would be one refactor away from letting a
		// response mutate the store).
		ChildExitCode: copyIntPtr(s.ChildExitCode),
		CreatedAt:     s.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:     s.UpdatedAt.UTC().Format(time.RFC3339),
		LastEventAt:   s.LastEventAt.UTC().Format(time.RFC3339),
	}
}

type SessionEnvelope struct {
	Session SessionView `json:"session"`
}

type SessionsEnvelope struct {
	Sessions   []SessionView `json:"sessions"`
	NextCursor string        `json:"next_cursor"`
}

// ---------------------------------------------------------------------------
// request bodies
// ---------------------------------------------------------------------------

// CreateSessionRequest is POST /v0/sessions's body. Environment names the
// environment this session starts from, by name or by id; omitting it is a
// scratch session, exactly as before environments existed. Image and
// EgressAllow are overrides when it is present — the host resolves them.
type CreateSessionRequest struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
	Environment string   `json:"environment,omitempty"`
	// Repos overrides the repositories the environment's github connectors
	// declare. Absent inherits them; an explicit empty array clones nothing,
	// exactly the nil-vs-empty distinction egress_allow already draws — and
	// for the same reason: "I didn't say" and "I said none" are different
	// answers, and a session that means to be scratch under a repo-carrying
	// environment has no other way to say so.
	Repos []RepoRequest `json:"repos,omitempty"`
}

// RepoRequest is one entry of that array. BaseBranch is a pointer for the
// same reason the github connector's is: an explicitly empty base_branch is a
// typo, never a request for the default, and it must not reach the clone as
// one.
type RepoRequest struct {
	Repo       string  `json:"repo"`
	BaseBranch *string `json:"base_branch"`
}

// SuspendRequest's Warm is a pointer so an absent field is distinguishable
// from an explicit false — the default is true either way.
type SuspendRequest struct {
	Warm *bool `json:"warm,omitempty"`
}

// DecodeCreateSession turns a decoded create body into the command the
// session service takes, returning the client-facing 400 message (empty when
// the body is fine) instead of an error: every way this can fail is the
// caller's own request, named by field and by index.
//
// Two fields are deliberately NOT set here, because neither is in the body:
// IdempotencyKey travels as a header, and EnvironmentID is the resolution of
// the body's `environment` reference, which only a host with the name index
// can perform. The caller sets both on the returned command.
//
// The session's own spec always travels: for a scratch session it is the
// whole description, for an environment session it is layered over the
// environment field by field (control.PortableSpec) by the service.
func DecodeCreateSession(req CreateSessionRequest) (control.CreateSession, string) {
	repos, msg := repoOverrides(req.Repos)
	if msg != "" {
		return control.CreateSession{}, msg
	}
	return control.CreateSession{
		Name:  req.Name,
		Repos: repos,
		Spec: control.PortableSpec{
			Image:       req.Image,
			Cmd:         req.Cmd,
			EgressAllow: req.EgressAllow,
		},
	}, ""
}

// repoOverrides validates a create body's `repos` and returns the refs to
// record on the session row. It preserves nil-vs-empty: nil in, nil out
// (inherit the environment's connectors); empty in, empty out (clone
// nothing).
//
// The messages are the caller's to read — each names the offending entry by
// index and what was wrong with it — and none carries internal detail.
func repoOverrides(reqs []RepoRequest) ([]control.RepoRef, string) {
	if reqs == nil {
		return nil, ""
	}
	out := make([]control.RepoRef, 0, len(reqs))
	for i, req := range reqs {
		if !validRepoRef(req.Repo) {
			return nil, fmt.Sprintf("repos[%d].repo must be \"owner/name\", got %q", i, req.Repo)
		}
		ref := control.RepoRef{Repo: req.Repo}
		if req.BaseBranch != nil {
			if *req.BaseBranch == "" {
				return nil, fmt.Sprintf("repos[%d].base_branch is empty; omit it for the default (%s)", i, DefaultBaseBranch)
			}
			ref.BaseBranch = *req.BaseBranch
		}
		out = append(out, ref)
	}
	return out, ""
}

// emptyIfNil returns ss, or a non-nil empty slice in its place, so
// json.Marshal always produces "[]" rather than the JSON scalar null.
func emptyIfNil(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	return ss
}

// copyIntPtr clones a nullable int so a rendered view never shares storage
// with the row it came from. nil stays nil — which is the wire's null, and
// the honest answer for a session whose agent has not exited.
func copyIntPtr(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
