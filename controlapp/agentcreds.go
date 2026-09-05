package controlapp

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// Custody of a person's agent credential sets: the port a host stores them
// behind, and the one service that decides who may read, write, or destroy
// one. agents.go is the provider TABLE — data, and nothing but data. This
// file is the behavior over it, and it is where the authorization for both
// hosts lives, once.
//
// The shape of the problem. A credential set belongs to a PERSON and an
// AGENT, not to a workspace and not to a session: someone logs Claude Code in
// once and every session they later start, in any workspace they are a member
// of, on any runner, finds that login already there. So the store is keyed by
// (user, provider) and knows nothing about workspaces, while every delivery
// re-asks whether the person is still a member of the workspace the asking
// session runs in. Custody is the thing that must never trust a runner, and
// the thing whose refusals must never say anything a runner could learn from.
//
// Sealing is the HOST's, not this package's: self-hosted seals with the fleet
// key in internal/controld/agentvault.go, a hosted cell seals with its KMS.
// The port below therefore takes and returns PLAINTEXT file maps, exactly as
// LaunchMaterial (scheduler.go) carries resolved secrets in memory only — and
// under the same rule: nothing here is stored, logged, put in an error, put
// in an event, or rendered into test output.

// AgentCredentialSet is one provider's credential files for one person, plus
// the version custody has assigned them. Version 0 with no files is the
// truthful answer for a person who has not logged that agent in — it is an
// answer, not a refusal.
type AgentCredentialSet struct {
	Version uint64
	Files   map[string][]byte
}

// AgentCredentialStatus is everything about a stored set EXCEPT the set: the
// provider it belongs to, the version, and when it last moved. Listing
// answers with these, so no listing path can return a credential byte —
// AgentCredentialStore.ListAgentCredentials has no way to express one.
type AgentCredentialStatus struct {
	Provider  string
	Version   uint64
	UpdatedAt time.Time
}

// AgentCredentialStore is custody's persistence port. It is keyed by (user,
// provider) and knows nothing about workspaces or sessions: the projection of
// one person's login into every workspace they belong to is the service's
// doing, above this line.
//
// The port takes and returns PLAINTEXT file maps and the implementation
// seals, because sealing is the host's: the self-hosted store seals under the
// fleet key, a hosted one under its KMS, and neither wants the other's
// ciphertext format in this contract. Implementations must therefore treat
// every map that crosses this interface as secret material.
//
// controlapp/repotest.RunAgentCredentialStore is the executable contract;
// these comments say what the behavior is, and that suite is where it is
// pinned.
type AgentCredentialStore interface {
	// FetchAgentCredentials returns the stored set. A set that was never put,
	// or that a revoke destroyed, is version 0 with no files and a nil error
	// — "you have not logged in" is not a failure.
	FetchAgentCredentials(ctx context.Context, user control.ActorID, provider string) (AgentCredentialSet, error)
	// PutAgentCredentials replaces the set with files and returns the new
	// version, which is one more than the version it replaced (1 for a first
	// put). It is last-writer-wins: two sessions of the same person racing
	// each other both succeed, and the later write is the one that stands.
	PutAgentCredentials(ctx context.Context, user control.ActorID, provider string, files map[string][]byte) (uint64, error)
	// RevokeAgentCredentials destroys the set. It is idempotent: revoking
	// what is not there is a nil error, because "there is no credential" is
	// the state the caller asked for either way.
	RevokeAgentCredentials(ctx context.Context, user control.ActorID, provider string) error
	// ListAgentCredentials returns one status per provider the user has a set
	// for, and no bytes.
	ListAgentCredentials(ctx context.Context, user control.ActorID) ([]AgentCredentialStatus, error)
}

// AgentCredentialSetMaxBytes bounds one put: the sum of every file's bytes
// and every file's name. It is the same 64 KiB sessiond refuses to send above
// (plan §Task 3), enforced a second time here because the sandbox is an
// untrusted peer and a bound only the sender applies is not a bound.
const AgentCredentialSetMaxBytes = 64 << 10

// agentListPageLimit is how many sessions one downward-revoke sweep reads per
// page, and agentListMaxPages how many pages it will read at all. The second
// is a guard, not a policy: ListSessions is a port a host implements, and a
// cursor that never empties must cost a bounded sweep rather than a spinning
// goroutine.
const (
	agentListPageLimit = 200
	agentListMaxPages  = 64
)

// agentRefusal is a fixed, user-facing sentence over one control sentinel.
//
// Its Error() is the SENTENCE and nothing else, which is the whole point: a
// refusal from this service travels down the session RPC, out of sessiond,
// and into a boot note somebody reads. A sentinel's own text ("control:
// denied") in that sentence would say nothing, and a store's error text there
// would say far too much — so the two are kept apart, with errors.Is finding
// the sentinel and Error() carrying the words.
type agentRefusal struct {
	sentence string
	sentinel error
}

func (e *agentRefusal) Error() string { return e.sentence }
func (e *agentRefusal) Unwrap() error { return e.sentinel }

// The complete refusal vocabulary of AnswerFetch and AnswerPut. Every failure
// those two can produce is one of these; there is no path on which a store's
// error, a provider's response, or a sandbox's own words reach the wire.
var (
	// ErrAgentSessionHasNoCreator refuses a session row that names nobody.
	// The creator IS the authority a fetch acts with, and a lookup for the
	// empty user is one stray row away from handing a sandbox a credential
	// nobody granted it — the same reasoning the git mint's ownerless
	// refusal is written on.
	ErrAgentSessionHasNoCreator error = &agentRefusal{
		"this session has no creator to fetch an agent credential for", control.ErrInvalid}
	// ErrUnknownAgentProvider refuses a provider that is not a row of
	// AgentProviders(). Custody stores what the table names and nothing else,
	// so an unknown name is refused rather than stored under.
	ErrUnknownAgentProvider error = &agentRefusal{
		"unknown agent provider", control.ErrInvalid}
	// ErrAgentMembershipGone is what a denied authorization becomes. It says
	// membership and not "denied" because that is what a denial of
	// ActionAttach on your own session in your own workspace actually means:
	// you are no longer a member of it.
	ErrAgentMembershipGone error = &agentRefusal{
		"your workspace membership no longer allows this", control.ErrDenied}
	// ErrAgentCredentialTooLarge refuses a put over AgentCredentialSetMaxBytes.
	ErrAgentCredentialTooLarge error = &agentRefusal{
		"the agent credential set is too large", control.ErrInvalid}
	// ErrAgentFileNotAllowed refuses a file name that is not a bare name on
	// the provider's own allowlist. The allowlist is what makes the whole
	// scheme mechanical — it is what the sync reads, what a revoke deletes,
	// and what a checkpoint excludes — so a name outside it is refused here
	// rather than written into somebody's agent home.
	ErrAgentFileNotAllowed error = &agentRefusal{
		"that file is not part of this agent's credential set", control.ErrInvalid}
	// ErrAgentCredentialUnreadable is every failure of the store on the way
	// out, flattened. A store's own text may name a row, a DSN, or a column,
	// and none of that is something a sandbox may learn.
	ErrAgentCredentialUnreadable error = &agentRefusal{
		"the agent credential could not be read", control.ErrUnavailable}
	// ErrAgentCredentialUnwritable is the same flattening on the way in.
	ErrAgentCredentialUnwritable error = &agentRefusal{
		"the agent credential could not be stored", control.ErrUnavailable}
	// ErrAgentCredentialNotYours refuses a logout by anyone but the account.
	// A credential set is not the workspace's property, so no role over the
	// workspace reaches it — an owner and an admin get this too.
	ErrAgentCredentialNotYours error = &agentRefusal{
		"an agent credential belongs to the person who logged in, not to a workspace role", control.ErrDenied}
)

// AgentRefusalSentence returns the fixed user-facing sentence err carries,
// and false for anything else. A host relaying a custody refusal into a
// sandbox uses it so that ONLY these sentences can ever be relayed: an error
// this service did not author renders as the host's own flat sentence instead
// of leaking whatever text it happens to hold.
func AgentRefusalSentence(err error) (string, bool) {
	var r *agentRefusal
	if errors.As(err, &r) {
		return r.sentence, true
	}
	return "", false
}

// agentRPCSender is the narrowest thing a downward revoke needs: the ability
// to ask one running sandbox to perform one method. It is deliberately an
// interface with an UNEXPORTED method, which makes it unimplementable outside
// this package — the only production value is *AttachmentService, whose
// sessionRPC already owns the correlation, the validation of a hostile
// answer, and the refusal-vs-unreachable distinction. Taking the interface
// rather than the concrete service is what lets this file's tests record the
// downward traffic without a runner; taking a concrete *AttachmentService in
// the exported constructor is what keeps a host from supplying a second,
// less careful implementation of that seam.
type agentRPCSender interface {
	sessionRPC(ctx context.Context, row control.Session, method string, payload any, out any) error
}

// AgentWorkspaceLister answers which workspaces a person is a member of. It
// is optional, and it exists because control.SessionRepository has no
// cross-workspace scan: every one of its methods is keyed by workspace, by
// design. Without it Logout can only sweep the scope's own workspace — which
// is the whole truth for a self-hosted installation, since that installation
// IS one workspace — and a host with many workspaces supplies one so that
// "logged out everywhere" means everywhere.
type AgentWorkspaceLister interface {
	AgentWorkspaces(ctx context.Context, user control.ActorID) ([]control.WorkspaceID, error)
}

// AgentCredentialService is custody's behavior: the two answers a sandbox's
// upward RPC gets, the logout a person performs, the withdrawal a lost
// membership triggers, and the listing the API renders. It owns the
// authorization for both hosts — a hosted gateway and self-hosted controld
// answer the same RPC through this same service — so a rule added here is
// added for both at once.
type AgentCredentialService struct {
	store     AgentCredentialStore
	auth      control.Authorizer
	sessions  control.SessionRepository
	rpc       agentRPCSender
	places    control.PlacementScope
	workspace AgentWorkspaceLister
}

// AgentCredentialOption configures the two things the four constructor
// arguments cannot carry: the host's placement context, and its membership
// index. Both default to the narrowest honest behavior, so a host that
// supplies neither still gets a correct service.
type AgentCredentialOption func(*AgentCredentialService)

// WithAgentPlacement supplies the placement context the service stamps on the
// scope it authorizes a sandbox's request in. A sandbox-initiated request
// arrives with no scope of its own — the row and the runner are all there is
// — so the service builds the creator's scope, and the placement half of it
// is knowledge only the host has. Without this option the constructed scope
// carries no placement, which the self-hosted authorizer ignores and a hosted
// one would refuse; every host that has a placement should pass it.
func WithAgentPlacement(p control.PlacementScope) AgentCredentialOption {
	return func(s *AgentCredentialService) { s.places = p }
}

// WithAgentWorkspaces supplies the membership index Logout sweeps. See
// AgentWorkspaceLister for why it is not a constructor argument.
func WithAgentWorkspaces(l AgentWorkspaceLister) AgentCredentialOption {
	return func(s *AgentCredentialService) { s.workspace = l }
}

// NewAgentCredentialService builds the service over its four dependencies:
// the custody store, the host's current authorization authority, the session
// repository the downward sweep reads, and the attachment service whose
// sessionRPC carries a revoke into a running sandbox.
//
// It returns no error, unlike its three siblings, because it holds no
// defaults to validate and no goroutine to start. A nil store or authorizer
// is a composition bug rather than a runtime condition, and every method
// refuses with control.ErrInvalid rather than panicking on one.
func NewAgentCredentialService(store AgentCredentialStore, authz control.Authorizer,
	sessions control.SessionRepository, rpc *AttachmentService, opts ...AgentCredentialOption) *AgentCredentialService {
	var sender agentRPCSender
	if rpc != nil {
		// Assigned through the guard so a nil *AttachmentService cannot
		// become a non-nil interface holding a nil pointer, which would turn
		// a composition mistake into a panic three layers down.
		sender = rpc
	}
	return newAgentCredentialService(store, authz, sessions, sender, opts...)
}

func newAgentCredentialService(store AgentCredentialStore, authz control.Authorizer,
	sessions control.SessionRepository, rpc agentRPCSender, opts ...AgentCredentialOption) *AgentCredentialService {
	s := &AgentCredentialService{store: store, auth: authz, sessions: sessions, rpc: rpc}
	for _, o := range opts {
		o(s)
	}
	return s
}

// AnswerFetch answers one sandbox's boot-time request for its creator's
// credential set. Every check that makes the answer safe happens here, in
// this order: the row names somebody, the provider is one this build knows,
// and that somebody may still attach to this session in this workspace.
//
// The membership re-check is the point of the whole method. The host's
// SessionRequest guard has already established that the ASKING RUNNER is the
// row's runner (internal/controld/srpc.go does; a hosted gateway does), so
// what is left to establish is that the PERSON is still entitled — and
// "the creator may still attach to their own session in this workspace" is
// exactly "the creator is a current member of this workspace", asked with an
// action the frozen contract already has.
func (s *AgentCredentialService) AnswerFetch(ctx context.Context, row control.Session, provider string) (AgentCredentialSet, error) {
	p, err := s.answerable(ctx, row, provider)
	if err != nil {
		return AgentCredentialSet{}, err
	}
	set, err := s.store.FetchAgentCredentials(ctx, row.CreatorID, provider)
	if err != nil {
		return AgentCredentialSet{}, ErrAgentCredentialUnreadable
	}
	// Filtered against the CURRENT allowlist on the way out, not merely
	// against the one in force when the set was put. sessiond writes what it
	// is handed, by name, inside the provider's home; a stored row that names
	// anything else — an older table, a hosted store, an operator's hand —
	// must not become a write outside the allowlist. A put cannot create such
	// a name (see AnswerPut); this is the second half of the same rule, held
	// at the delivery point where it actually matters.
	set.Files = allowedAgentFiles(p, set.Files)
	return set, nil
}

// AnswerPut records the set a sandbox observed on disk and returns the
// version custody assigned it. The authorization is the fetch's, and then two
// bounds the sandbox cannot be trusted to have applied itself: the 64 KiB cap
// and the provider's file allowlist.
//
// The wire's put also carries the version the sandbox last saw
// (runner.MethodPutAgentCredentials). It is deliberately not a parameter
// here: v0 custody is last-writer-wins, so the answer to a stale version is
// the same as the answer to a current one — store it, and hand back the
// version it became. Nothing is answered before the store has accepted the
// write, so a version a sandbox holds is always a version custody has.
func (s *AgentCredentialService) AnswerPut(ctx context.Context, row control.Session, provider string, files map[string][]byte) (uint64, error) {
	p, err := s.answerable(ctx, row, provider)
	if err != nil {
		return 0, err
	}
	if err := checkAgentFiles(p, files); err != nil {
		return 0, err
	}
	version, err := s.store.PutAgentCredentials(ctx, row.CreatorID, provider, files)
	if err != nil {
		return 0, ErrAgentCredentialUnwritable
	}
	return version, nil
}

// Logout destroys the caller's own set for provider and then tells every
// running sandbox of theirs to forget the copy it holds.
//
// There is no "other user" parameter, and that is the access control: a
// workspace owner and an admin have no path to somebody else's set, because
// the method cannot express one. What is left to check is that the caller is
// a PERSON — a service principal acts for background work, and background
// work does not own a subscription login.
//
// The store revoke happens first and is the operation's result. The downward
// sends are best effort and cannot fail it: a session that is gone, wedged,
// or on a disconnected runner has not kept a credential the control plane
// still honors, and refusing the logout over it would leave the person unable
// to log out at all.
func (s *AgentCredentialService) Logout(ctx context.Context, sc control.Scope, provider string) error {
	if s.store == nil {
		return control.ErrInvalid
	}
	if err := sc.Validate(); err != nil {
		return err
	}
	if sc.Actor.Kind != control.ActorUser {
		return ErrAgentCredentialNotYours
	}
	if _, ok := agentProviderByName(provider); !ok {
		return ErrUnknownAgentProvider
	}
	if err := s.store.RevokeAgentCredentials(ctx, sc.Actor.ID, provider); err != nil {
		return portError(err)
	}
	workspaces, err := s.logoutWorkspaces(ctx, sc)
	if err != nil {
		return err
	}
	for _, ws := range workspaces {
		s.revokeDownward(ctx, ws, sc.Actor.ID, provider)
	}
	return nil
}

// Withdraw tells the user's running sandboxes IN ONE WORKSPACE to forget
// every provider's credential set, and leaves custody untouched. It is what a
// lost membership means: the person still owns their login — they may still
// have it in another workspace, and they will have it again if they are
// re-added — but nothing running in this workspace may keep holding it.
//
// The asymmetry with Logout is the whole design: Logout destroys and sweeps,
// Withdraw only sweeps.
func (s *AgentCredentialService) Withdraw(ctx context.Context, ws control.WorkspaceID, user control.ActorID) error {
	if ws == "" || user == "" {
		return control.ErrInvalid
	}
	for _, p := range AgentProviders() {
		s.revokeDownward(ctx, ws, user, p.Name)
	}
	return nil
}

// List returns one status per set the caller holds, in the store's order and
// carrying no bytes. Like Logout it answers about the CALLER and nobody else.
func (s *AgentCredentialService) List(ctx context.Context, sc control.Scope) ([]AgentCredentialStatus, error) {
	if s.store == nil {
		return nil, control.ErrInvalid
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	if sc.Actor.Kind != control.ActorUser {
		return nil, ErrAgentCredentialNotYours
	}
	out, err := s.store.ListAgentCredentials(ctx, sc.Actor.ID)
	if err != nil {
		return nil, portError(err)
	}
	return out, nil
}

// answerable is the shared front half of AnswerFetch and AnswerPut: the three
// checks that must pass before either touches the store, in the order that
// discloses least. It returns the provider row, which both callers then need.
func (s *AgentCredentialService) answerable(ctx context.Context, row control.Session, provider string) (AgentProvider, error) {
	if s.store == nil || s.auth == nil {
		return AgentProvider{}, control.ErrInvalid
	}
	if row.CreatorID == "" {
		return AgentProvider{}, ErrAgentSessionHasNoCreator
	}
	p, ok := agentProviderByName(provider)
	if !ok {
		return AgentProvider{}, ErrUnknownAgentProvider
	}
	scope := control.Scope{
		WorkspaceID: row.WorkspaceID,
		Actor:       control.Actor{ID: row.CreatorID, Kind: control.ActorUser},
		Placement:   s.places,
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	if err := s.auth.Authorize(ctx, scope, control.ActionAttach, resource); err != nil {
		return AgentProvider{}, ErrAgentMembershipGone
	}
	return p, nil
}

// logoutWorkspaces names the workspaces one logout sweeps. With a membership
// index it is every workspace the person belongs to; without one it is the
// scope's own, which is the honest answer a per-workspace SessionRepository
// can give — and the complete one for a self-hosted installation, which has
// exactly one workspace.
func (s *AgentCredentialService) logoutWorkspaces(ctx context.Context, sc control.Scope) ([]control.WorkspaceID, error) {
	if s.workspace == nil {
		return []control.WorkspaceID{sc.WorkspaceID}, nil
	}
	wss, err := s.workspace.AgentWorkspaces(ctx, sc.Actor.ID)
	if err != nil {
		return nil, portError(err)
	}
	return wss, nil
}

// revokeDownward asks every RUNNING session of user in ws to drop provider's
// files. It is best effort in both halves: a listing that fails ends the
// sweep for that workspace, and a session that refuses or cannot be reached
// is skipped.
//
// It logs nothing, which is this package's rule and not an oversight: no
// service here logs, and the hygiene requirement on this path — a log line
// may name a session id and never a credential byte — is met most simply by
// having no log line at all. The host's transport already records what it
// could not reach.
func (s *AgentCredentialService) revokeDownward(ctx context.Context, ws control.WorkspaceID, user control.ActorID, provider string) {
	if s.rpc == nil || s.sessions == nil || ws == "" || user == "" {
		return
	}
	payload := struct {
		Provider string `json:"provider"`
	}{provider}
	for _, row := range s.liveSessionsOf(ctx, ws, user) {
		// The answer body is deliberately ignored (the wire shape is `{}`):
		// what the sandbox says about a revoke changes nothing custody does.
		_ = s.rpc.sessionRPC(ctx, row, runner.MethodRevokeAgentCredentials, payload, nil)
	}
}

// liveSessionsOf pages ws's non-terminal sessions and returns the running
// ones user created. Only running sessions are swept: a queued session has no
// sandbox to hold a credential yet, and a suspended one will fetch again when
// it resumes, by which time custody no longer has the set to hand it.
func (s *AgentCredentialService) liveSessionsOf(ctx context.Context, ws control.WorkspaceID, user control.ActorID) []control.Session {
	var out []control.Session
	cursor := ""
	for page := 0; page < agentListMaxPages; page++ {
		rows, next, err := s.sessions.ListSessions(ctx, ws, control.SessionQuery{
			Limit: agentListPageLimit, Cursor: cursor,
		})
		if err != nil {
			return out
		}
		for _, row := range rows {
			if row.CreatorID == user && row.State == control.StateRunning {
				out = append(out, row)
			}
		}
		if next == "" {
			return out
		}
		cursor = next
	}
	return out
}

// agentProviderByName looks one row up in the table. It is the only place
// custody turns a wire string into a provider, so an unknown name has exactly
// one fate.
func agentProviderByName(name string) (AgentProvider, bool) {
	for _, p := range AgentProviders() {
		if p.Name == name {
			return p, true
		}
	}
	return AgentProvider{}, false
}

// checkAgentFiles applies the two bounds a put must satisfy: every name is a
// bare name on the provider's allowlist, and the whole set fits in
// AgentCredentialSetMaxBytes.
//
// An empty map passes. It is how a person's set becomes "logged in, holding
// nothing" — the agent removed its own credential file — and it must be
// storable, or the last state a sandbox could report would be a stale one.
func checkAgentFiles(p AgentProvider, files map[string][]byte) error {
	total := 0
	for name, body := range files {
		if !bareAgentFileName(name) || !slices.Contains(p.Files, name) {
			return ErrAgentFileNotAllowed
		}
		total += len(name) + len(body)
		if total > AgentCredentialSetMaxBytes {
			return ErrAgentCredentialTooLarge
		}
	}
	return nil
}

// allowedAgentFiles copies files down to the provider's current allowlist.
// The copy is deliberate: the returned map is the caller's, and it must not
// alias a store's own memory.
func allowedAgentFiles(p AgentProvider, files map[string][]byte) map[string][]byte {
	if len(files) == 0 {
		return files
	}
	out := make(map[string][]byte, len(files))
	maps.Copy(out, files)
	for name := range out {
		if !bareAgentFileName(name) || !slices.Contains(p.Files, name) {
			delete(out, name)
		}
	}
	return out
}

// bareAgentFileName reports whether name is a file inside the provider's own
// directory and nothing else: no separator of either flavor, no traversal, no
// empty name, no NUL. It is the invariant TestAgentProvidersNeverSpellASecretPath
// holds over the table, applied here to a name that came off the wire.
func bareAgentFileName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsAny(name, `/\`+"\x00")
}
