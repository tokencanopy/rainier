package controlapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The custody service's tests, over a fake store, a fake authorizer, a fake
// session repository, and a fake downward transport. Nothing here is a
// credential: the fixture bytes are the literal words credential_example and
// auth_example, and every case that could print one prints its length instead.

const (
	agentTestCredential = "credential_example"
	agentTestUser       = control.ActorID("user_example")
	agentTestOther      = control.ActorID("user_other")
)

// agentTestProvider returns the first table row and its first allowlisted
// file. Reading them off AgentProviders() rather than spelling them keeps
// this file inside the plan's rule: agents.go names providers, and nothing
// else does — including the tests, which would otherwise have to be edited
// every time a row is added.
func agentTestProvider(t *testing.T) (AgentProvider, string) {
	t.Helper()
	rows := AgentProviders()
	if len(rows) == 0 || len(rows[0].Files) == 0 {
		t.Fatal("the provider table has no row with a file to store")
	}
	return rows[0], rows[0].Files[0]
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

// agentFakeStore is custody's persistence, counted. It holds plaintext maps
// (the port's contract: sealing is the host's), so a test can assert that a
// refused request never reached it at all.
type agentFakeStore struct {
	mu       sync.Mutex
	sets     map[string]AgentCredentialSet
	statuses []AgentCredentialStatus

	fetchErr, putErr, revokeErr, listErr error

	fetches, puts, revokes, lists int
	revoked                       []string
}

func newAgentFakeStore() *agentFakeStore {
	return &agentFakeStore{sets: map[string]AgentCredentialSet{}}
}

func agentKey(user control.ActorID, provider string) string { return string(user) + "\x00" + provider }

func (f *agentFakeStore) FetchAgentCredentials(_ context.Context, user control.ActorID, provider string) (AgentCredentialSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	if f.fetchErr != nil {
		return AgentCredentialSet{}, f.fetchErr
	}
	return f.sets[agentKey(user, provider)], nil
}

func (f *agentFakeStore) PutAgentCredentials(_ context.Context, user control.ActorID, provider string, files map[string][]byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.putErr != nil {
		return 0, f.putErr
	}
	cur := f.sets[agentKey(user, provider)]
	cur.Version++
	cur.Files = files
	f.sets[agentKey(user, provider)] = cur
	return cur.Version, nil
}

func (f *agentFakeStore) RevokeAgentCredentials(_ context.Context, user control.ActorID, provider string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokes++
	if f.revokeErr != nil {
		return f.revokeErr
	}
	f.revoked = append(f.revoked, agentKey(user, provider))
	delete(f.sets, agentKey(user, provider))
	return nil
}

func (f *agentFakeStore) ListAgentCredentials(_ context.Context, _ control.ActorID) ([]AgentCredentialStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.statuses, nil
}

func (f *agentFakeStore) counts() (fetches, puts, revokes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetches, f.puts, f.revokes
}

func (f *agentFakeStore) revokedKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.revoked...)
}

// agentFakeSessions is a session repository holding rows per workspace, so a
// downward sweep can be watched choosing among them. Only ListSessions is
// real; the rest of the port is present because the interface requires it.
type agentFakeSessions struct {
	mu       sync.Mutex
	rows     map[control.WorkspaceID][]control.Session
	err      error
	listedIn []control.WorkspaceID
}

func (f *agentFakeSessions) ListSessions(_ context.Context, ws control.WorkspaceID, _ control.SessionQuery) ([]control.Session, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listedIn = append(f.listedIn, ws)
	if f.err != nil {
		return nil, "", f.err
	}
	return f.rows[ws], "", nil
}

func (f *agentFakeSessions) workspacesListed() []control.WorkspaceID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]control.WorkspaceID(nil), f.listedIn...)
}

func (f *agentFakeSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, nil
}
func (f *agentFakeSessions) GetSession(context.Context, control.WorkspaceID, control.SessionID) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (f *agentFakeSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (f *agentFakeSessions) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (f *agentFakeSessions) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (f *agentFakeSessions) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}
func (f *agentFakeSessions) NextControllerGeneration(context.Context, control.WorkspaceID, control.SessionID) (uint64, error) {
	return 0, nil
}

// agentSentRPC is one downward request the service made.
type agentSentRPC struct {
	Session control.SessionID
	Method  string
	Payload string
}

// agentFakeSender records the downward session RPCs instead of dispatching
// them. It implements the package-private agentRPCSender seam, which is what
// makes the seam unexported in the first place: only this package can supply
// a second implementation of it.
type agentFakeSender struct {
	mu   sync.Mutex
	sent []agentSentRPC
	// failOn makes one session's request fail, so a test can prove that a
	// sandbox nobody can reach does not fail the operation.
	failOn control.SessionID
}

func (f *agentFakeSender) sessionRPC(_ context.Context, row control.Session, method string, payload any, _ any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	f.sent = append(f.sent, agentSentRPC{Session: row.ID, Method: method, Payload: string(raw)})
	if row.ID == f.failOn {
		return control.ErrUnavailable
	}
	return nil
}

func (f *agentFakeSender) requests() []agentSentRPC {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]agentSentRPC(nil), f.sent...)
}

// agentFakeWorkspaces is the optional membership index.
type agentFakeWorkspaces struct {
	list []control.WorkspaceID
	err  error
}

func (f agentFakeWorkspaces) AgentWorkspaces(context.Context, control.ActorID) ([]control.WorkspaceID, error) {
	return f.list, f.err
}

type agentFixture struct {
	svc      *AgentCredentialService
	store    *agentFakeStore
	auth     *attachmentFakeAuthorizer
	sessions *agentFakeSessions
	sender   *agentFakeSender
}

func newAgentFixture(t *testing.T, opts ...AgentCredentialOption) *agentFixture {
	t.Helper()
	fx := &agentFixture{
		store:    newAgentFakeStore(),
		auth:     &attachmentFakeAuthorizer{},
		sessions: &agentFakeSessions{rows: map[control.WorkspaceID][]control.Session{}},
		sender:   &agentFakeSender{},
	}
	fx.svc = newAgentCredentialService(fx.store, fx.auth, fx.sessions, fx.sender, opts...)
	return fx
}

// agentSessionRow is the running session a sandbox's request arrives for.
func agentSessionRow() control.Session {
	return control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", CreatorID: agentTestUser,
		State: control.StateRunning, PoolID: "pool_example", RunnerID: "runner_example",
	}
}

func agentScopeFor(user control.ActorID, kind control.ActorKind) control.Scope {
	return control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: user, Kind: kind},
		Placement: control.PlacementScope{
			ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated,
		},
	}
}

// ---------------------------------------------------------------------------
// AnswerFetch
// ---------------------------------------------------------------------------

// TestAnswerFetchIsForTheCreatorOnly: the two checks that run before the store
// is touched at all. A row that names nobody has no authority to act with —
// the empty user is one stray store row away from handing a sandbox a
// credential nobody granted it — and a provider outside the table is refused
// rather than looked up, so custody can only ever answer about agents this
// build knows.
func TestAnswerFetchIsForTheCreatorOnly(t *testing.T) {
	provider, _ := agentTestProvider(t)

	t.Run("a session with no creator", func(t *testing.T) {
		fx := newAgentFixture(t)
		row := agentSessionRow()
		row.CreatorID = ""
		_, err := fx.svc.AnswerFetch(context.Background(), row, provider.Name)
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("got %v, want a refusal wrapping ErrInvalid", err)
		}
		if err.Error() != "this session has no creator to fetch an agent credential for" {
			t.Fatalf("sentence = %q", err.Error())
		}
		if fetches, _, _ := fx.store.counts(); fetches != 0 {
			t.Fatalf("a creatorless session reached the store %d times", fetches)
		}
		if fx.auth.calls != 0 {
			t.Fatal("a creatorless session was authorized at all")
		}
	})

	t.Run("a provider outside the table", func(t *testing.T) {
		fx := newAgentFixture(t)
		_, err := fx.svc.AnswerFetch(context.Background(), agentSessionRow(), "provider_example")
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("got %v, want a refusal wrapping ErrInvalid", err)
		}
		if err.Error() != "unknown agent provider" {
			t.Fatalf("sentence = %q", err.Error())
		}
		if fetches, _, _ := fx.store.counts(); fetches != 0 {
			t.Fatalf("an unknown provider reached the store %d times", fetches)
		}
	})
}

// TestAnswerFetchReChecksMembership is the reason this service exists. The
// host's placement guard has already established WHICH RUNNER asked; what is
// left is whether the PERSON is still entitled, and that is asked of the
// host's current authorizer on every single delivery rather than cached from
// when the session was created.
//
// It also pins the shape of the question, because a wrong resource here would
// authorize the wrong thing while still looking like a check: ActionAttach,
// on the session, in the creator's own scope.
func TestAnswerFetchReChecksMembership(t *testing.T) {
	provider, file := agentTestProvider(t)
	row := agentSessionRow()

	fx := newAgentFixture(t, WithAgentPlacement(control.PlacementScope{
		ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated,
	}))
	fx.store.sets[agentKey(agentTestUser, provider.Name)] = AgentCredentialSet{
		Version: 3, Files: map[string][]byte{file: []byte(agentTestCredential)},
	}

	// Permitted: the set comes back.
	set, err := fx.svc.AnswerFetch(context.Background(), row, provider.Name)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if set.Version != 3 || !bytes.Equal(set.Files[file], []byte(agentTestCredential)) {
		t.Fatalf("fetch = version %d with %d files, want version 3 carrying the fixture", set.Version, len(set.Files))
	}
	if fx.auth.lastAction != control.ActionAttach {
		t.Fatalf("authorized action = %q, want %q", fx.auth.lastAction, control.ActionAttach)
	}
	if fx.auth.lastScope.Actor.ID != agentTestUser || fx.auth.lastScope.Actor.Kind != control.ActorUser {
		t.Fatalf("authorized as %+v, want the row's creator as a user", fx.auth.lastScope.Actor)
	}
	if fx.auth.lastScope.WorkspaceID != row.WorkspaceID || fx.auth.lastScope.Placement.HomeCell != "cell-1" {
		t.Fatalf("authorized in scope %+v, want the row's workspace and the host's placement", fx.auth.lastScope)
	}
	want := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	if fx.auth.lastResource != want {
		t.Fatalf("authorized resource = %+v, want %+v", fx.auth.lastResource, want)
	}

	// Denied: refused, and NOTHING is read. A membership that went away must
	// not cost a store round-trip whose result is then thrown away.
	fx.auth.err = control.ErrDenied
	before, _, _ := fx.store.counts()
	if _, err := fx.svc.AnswerFetch(context.Background(), row, provider.Name); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want a refusal wrapping ErrDenied", err)
	} else if err.Error() != "your workspace membership no longer allows this" {
		t.Fatalf("sentence = %q", err.Error())
	}
	if after, _, _ := fx.store.counts(); after != before {
		t.Fatalf("a denied fetch read the store (%d → %d)", before, after)
	}
}

// TestAnswerFetchNeverDeliversAnUnlistedFile: the allowlist is applied on the
// way OUT as well as on the way in. sessiond writes what it is handed, by
// name, inside the agent home; a stored row naming anything else — an older
// table, a hosted store, an operator's hand — must not become a write outside
// the allowlist, and the delivery point is where that has to be true.
func TestAnswerFetchNeverDeliversAnUnlistedFile(t *testing.T) {
	provider, file := agentTestProvider(t)
	fx := newAgentFixture(t)
	fx.store.sets[agentKey(agentTestUser, provider.Name)] = AgentCredentialSet{
		Version: 1,
		Files: map[string][]byte{
			file:                       []byte(agentTestCredential),
			"../../workspace/notes":    []byte(agentTestCredential),
			"file_example_not_on_list": []byte(agentTestCredential),
		},
	}
	set, err := fx.svc.AnswerFetch(context.Background(), agentSessionRow(), provider.Name)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(set.Files) != 1 {
		names := make([]string, 0, len(set.Files))
		for n := range set.Files {
			names = append(names, n)
		}
		t.Fatalf("delivered files %v, want only the allowlisted one", names)
	}
	if _, ok := set.Files[file]; !ok {
		t.Fatalf("the allowlisted file was dropped")
	}
}

// ---------------------------------------------------------------------------
// AnswerPut
// ---------------------------------------------------------------------------

// TestAnswerPutSealsBeforeAnswering: nothing is answered before the store has
// accepted the write. A version handed back for a put that did not land would
// be worse than an error — the sandbox would record it as its baseline and
// stop trying, and the person's login would be silently lost.
func TestAnswerPutSealsBeforeAnswering(t *testing.T) {
	provider, file := agentTestProvider(t)
	fx := newAgentFixture(t)
	fx.store.putErr = errors.New("pgstore: put agent credential for provider \"x\": connection refused to db.internal")

	version, err := fx.svc.AnswerPut(context.Background(), agentSessionRow(), provider.Name,
		map[string][]byte{file: []byte(agentTestCredential)})
	if err == nil {
		t.Fatal("a failed store write answered with a version")
	}
	if version != 0 {
		t.Fatalf("version = %d after a failed write, want 0", version)
	}
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want a refusal wrapping ErrUnavailable", err)
	}
	// The store's own words never reach the wire: this refusal travels into a
	// sandbox, and "connection refused to db.internal" is a fact about the
	// control plane's inside that no runner may learn.
	if err.Error() != "the agent credential could not be stored" {
		t.Fatalf("sentence = %q, want the fixed one", err.Error())
	}
	if strings.Contains(err.Error(), "db.internal") || strings.Contains(err.Error(), "pgstore") {
		t.Fatalf("the store's error text reached the refusal: %q", err.Error())
	}

	// And the happy path answers with the version the store assigned.
	fx.store.putErr = nil
	if v, err := fx.svc.AnswerPut(context.Background(), agentSessionRow(), provider.Name,
		map[string][]byte{file: []byte(agentTestCredential)}); err != nil || v != 1 {
		t.Fatalf("put = %d, %v; want version 1 and no error", v, err)
	}
}

// TestAnswerPutBoundsWhatTheSandboxSends: the sandbox is an untrusted peer,
// so the two bounds it applies to itself are applied again here. A name off
// the allowlist is refused rather than stored — a stored one would come back
// out of a fetch and be written into an agent home — and a set over 64 KiB is
// refused before it is stored rather than after.
func TestAnswerPutBoundsWhatTheSandboxSends(t *testing.T) {
	provider, file := agentTestProvider(t)
	for _, tc := range []struct {
		name  string
		files map[string][]byte
		want  string
		is    error
	}{
		{"a traversal", map[string][]byte{"../../etc/passwd": []byte(agentTestCredential)},
			"that file is not part of this agent's credential set", control.ErrInvalid},
		{"a nested path", map[string][]byte{"sub/" + file: []byte(agentTestCredential)},
			"that file is not part of this agent's credential set", control.ErrInvalid},
		{"a name off the allowlist", map[string][]byte{"file_example": []byte(agentTestCredential)},
			"that file is not part of this agent's credential set", control.ErrInvalid},
		{"an empty name", map[string][]byte{"": []byte(agentTestCredential)},
			"that file is not part of this agent's credential set", control.ErrInvalid},
		{"a set over the cap", map[string][]byte{file: bytes.Repeat([]byte("x"), AgentCredentialSetMaxBytes+1)},
			"the agent credential set is too large", control.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newAgentFixture(t)
			_, err := fx.svc.AnswerPut(context.Background(), agentSessionRow(), provider.Name, tc.files)
			if !errors.Is(err, tc.is) {
				t.Fatalf("got %v, want a refusal wrapping %v", err, tc.is)
			}
			if err.Error() != tc.want {
				t.Fatalf("sentence = %q, want %q", err.Error(), tc.want)
			}
			if _, puts, _ := fx.store.counts(); puts != 0 {
				t.Fatalf("a refused put reached the store %d times", puts)
			}
		})
	}

	// A set exactly at the cap is stored: the bound is a bound, not a margin.
	t.Run("a set exactly at the cap", func(t *testing.T) {
		fx := newAgentFixture(t)
		body := bytes.Repeat([]byte("x"), AgentCredentialSetMaxBytes-len(file))
		if _, err := fx.svc.AnswerPut(context.Background(), agentSessionRow(), provider.Name,
			map[string][]byte{file: body}); err != nil {
			t.Fatalf("a set at exactly the cap was refused: %v", err)
		}
	})

	// An empty set is a real state — the agent removed its own credential
	// file — and must be storable, or the last thing custody heard would stay
	// true forever.
	t.Run("an empty set", func(t *testing.T) {
		fx := newAgentFixture(t)
		if v, err := fx.svc.AnswerPut(context.Background(), agentSessionRow(), provider.Name,
			map[string][]byte{}); err != nil || v != 1 {
			t.Fatalf("put of an empty set = %d, %v; want version 1 and no error", v, err)
		}
	})
}

// ---------------------------------------------------------------------------
// Logout and Withdraw
// ---------------------------------------------------------------------------

// agentSweepFixture builds two workspaces, each holding: a running session of
// the person logging out, a running session of somebody else, and a suspended
// session of the person. Only the first of each pair may be swept.
func agentSweepFixture(t *testing.T, opts ...AgentCredentialOption) *agentFixture {
	t.Helper()
	fx := newAgentFixture(t, opts...)
	mk := func(id control.SessionID, ws control.WorkspaceID, who control.ActorID, state control.SessionState) control.Session {
		return control.Session{ID: id, WorkspaceID: ws, CreatorID: who, State: state,
			PoolID: "pool_example", RunnerID: "runner_example"}
	}
	fx.sessions.rows["ws_example"] = []control.Session{
		mk("sess_mine_a", "ws_example", agentTestUser, control.StateRunning),
		mk("sess_theirs_a", "ws_example", agentTestOther, control.StateRunning),
		mk("sess_suspended_a", "ws_example", agentTestUser, control.StateSuspendedWarm),
	}
	fx.sessions.rows["ws_other"] = []control.Session{
		mk("sess_mine_b", "ws_other", agentTestUser, control.StateRunning),
		mk("sess_theirs_b", "ws_other", agentTestOther, control.StateRunning),
	}
	return fx
}

func agentSentTo(reqs []agentSentRPC) []control.SessionID {
	out := make([]control.SessionID, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Session)
	}
	return out
}

// TestLogoutRevokesEverywhere: a logout destroys custody's copy and then
// tells every running sandbox of that person, in every workspace they belong
// to, to drop the copy it holds. The store revoke is the operation; the
// downward sends are what make "logged out" true before the next boot.
func TestLogoutRevokesEverywhere(t *testing.T) {
	provider, _ := agentTestProvider(t)
	fx := agentSweepFixture(t, WithAgentWorkspaces(agentFakeWorkspaces{
		list: []control.WorkspaceID{"ws_example", "ws_other"},
	}))

	if err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestUser, control.ActorUser), provider.Name); err != nil {
		t.Fatalf("logout: %v", err)
	}

	if got := fx.store.revokedKeys(); len(got) != 1 || got[0] != agentKey(agentTestUser, provider.Name) {
		t.Fatalf("store revoked %v, want exactly the caller's own set", got)
	}
	reqs := fx.sender.requests()
	if got, want := agentSentTo(reqs), []control.SessionID{"sess_mine_a", "sess_mine_b"}; !equalSessionIDs(got, want) {
		t.Fatalf("revoked downward to %v, want %v — one per RUNNING session of the caller, in every workspace", got, want)
	}
	for _, r := range reqs {
		if r.Method != runner.MethodRevokeAgentCredentials {
			t.Fatalf("downward method = %q, want %q", r.Method, runner.MethodRevokeAgentCredentials)
		}
		if want := `{"provider":"` + provider.Name + `"}`; r.Payload != want {
			t.Fatalf("downward payload = %s, want %s", r.Payload, want)
		}
	}
}

// TestLogoutWithoutAMembershipIndexSweepsItsOwnWorkspace pins the narrower
// honest behavior a host without a membership index gets. control's session
// repository is keyed by workspace at every method — there is no
// cross-workspace scan in the contract — so a service given no index sweeps
// the scope's own workspace and says so, which is the WHOLE truth for a
// self-hosted installation because that installation is one workspace.
func TestLogoutWithoutAMembershipIndexSweepsItsOwnWorkspace(t *testing.T) {
	provider, _ := agentTestProvider(t)
	fx := agentSweepFixture(t)

	if err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestUser, control.ActorUser), provider.Name); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if got := fx.sessions.workspacesListed(); len(got) != 1 || got[0] != "ws_example" {
		t.Fatalf("listed sessions in %v, want only the scope's own workspace", got)
	}
	if got, want := agentSentTo(fx.sender.requests()), []control.SessionID{"sess_mine_a"}; !equalSessionIDs(got, want) {
		t.Fatalf("revoked downward to %v, want %v", got, want)
	}
	// Custody is still emptied everywhere: the STORE is keyed by (user,
	// provider) and holds no workspace, so one revoke is every workspace's.
	if got := fx.store.revokedKeys(); len(got) != 1 {
		t.Fatalf("store revoked %v, want one set", got)
	}
}

// TestWithdrawTouchesOnlyThatWorkspace: a lost membership is not a logout.
// The person still owns their login — they may have it in another workspace,
// and they get it back if they are re-added — so Withdraw sweeps ONE
// workspace's sandboxes and leaves custody entirely alone.
func TestWithdrawTouchesOnlyThatWorkspace(t *testing.T) {
	fx := agentSweepFixture(t, WithAgentWorkspaces(agentFakeWorkspaces{
		list: []control.WorkspaceID{"ws_example", "ws_other"},
	}))

	if err := fx.svc.Withdraw(context.Background(), "ws_example", agentTestUser); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	if _, _, revokes := fx.store.counts(); revokes != 0 {
		t.Fatalf("withdraw revoked custody %d times, want 0 — the login is the person's, not the workspace's", revokes)
	}
	for _, ws := range fx.sessions.workspacesListed() {
		if ws != "ws_example" {
			t.Fatalf("withdraw listed sessions in %s, want only ws_example", ws)
		}
	}
	// One request per provider, all to the one running session of that person
	// in that workspace: a membership that went away takes every agent with
	// it, not just one.
	reqs := fx.sender.requests()
	if len(reqs) != len(AgentProviders()) {
		t.Fatalf("sent %d downward requests, want one per provider (%d)", len(reqs), len(AgentProviders()))
	}
	seen := map[string]bool{}
	for _, r := range reqs {
		if r.Session != "sess_mine_a" {
			t.Fatalf("withdraw reached session %s, want only the person's running session in ws_example", r.Session)
		}
		seen[r.Payload] = true
	}
	if len(seen) != len(AgentProviders()) {
		t.Fatalf("withdraw named %d distinct providers, want %d", len(seen), len(AgentProviders()))
	}
}

// TestLogoutRefusesARoleWithoutTheAccount: a credential set is the person's,
// not the workspace's, so no role over the workspace reaches it. There is no
// "other user" parameter at all — an owner or an admin has no way to spell
// somebody else's set — and what is left to refuse is a principal that is not
// a person: background work does not own a subscription login.
func TestLogoutRefusesARoleWithoutTheAccount(t *testing.T) {
	provider, _ := agentTestProvider(t)

	t.Run("a service principal", func(t *testing.T) {
		fx := agentSweepFixture(t)
		err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestUser, control.ActorService), provider.Name)
		if !errors.Is(err, control.ErrDenied) {
			t.Fatalf("got %v, want a refusal wrapping ErrDenied", err)
		}
		if err.Error() != "an agent credential belongs to the person who logged in, not to a workspace role" {
			t.Fatalf("sentence = %q", err.Error())
		}
		if _, _, revokes := fx.store.counts(); revokes != 0 {
			t.Fatalf("a refused logout revoked %d sets", revokes)
		}
		if got := fx.sender.requests(); len(got) != 0 {
			t.Fatalf("a refused logout sent %d downward requests", len(got))
		}
	})

	// An owner of the workspace logging THEMSELVES out reaches their own set
	// and nobody else's — which is the same statement, from the other side:
	// the actor is the only account the method can name.
	t.Run("an owner reaches only their own set", func(t *testing.T) {
		fx := agentSweepFixture(t)
		if err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestOther, control.ActorUser), provider.Name); err != nil {
			t.Fatalf("logout: %v", err)
		}
		if got := fx.store.revokedKeys(); len(got) != 1 || got[0] != agentKey(agentTestOther, provider.Name) {
			t.Fatalf("store revoked %v, want only %s's own set", got, agentTestOther)
		}
		if got, want := agentSentTo(fx.sender.requests()), []control.SessionID{"sess_theirs_a"}; !equalSessionIDs(got, want) {
			t.Fatalf("revoked downward to %v, want %v — the other person's session is untouched", got, want)
		}
	})

	t.Run("an unknown provider", func(t *testing.T) {
		fx := agentSweepFixture(t)
		err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestUser, control.ActorUser), "provider_example")
		if !errors.Is(err, control.ErrInvalid) || err.Error() != "unknown agent provider" {
			t.Fatalf("got %v", err)
		}
		if _, _, revokes := fx.store.counts(); revokes != 0 {
			t.Fatalf("an unknown provider revoked %d sets", revokes)
		}
	})
}

// TestLogoutIsNotFailedByAnUnreachableSession: the downward sends are best
// effort and cannot fail the logout. A session that is gone, wedged, or on a
// disconnected runner is not holding a credential the control plane still
// honors, and refusing the logout over it would leave the person unable to
// log out at all — the one outcome a logout must never produce.
func TestLogoutIsNotFailedByAnUnreachableSession(t *testing.T) {
	provider, _ := agentTestProvider(t)
	fx := agentSweepFixture(t, WithAgentWorkspaces(agentFakeWorkspaces{
		list: []control.WorkspaceID{"ws_example", "ws_other"},
	}))
	fx.sender.failOn = "sess_mine_a"

	if err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestUser, control.ActorUser), provider.Name); err != nil {
		t.Fatalf("logout with an unreachable session: %v", err)
	}
	// The sweep continued past the failure into the second workspace.
	if got, want := agentSentTo(fx.sender.requests()), []control.SessionID{"sess_mine_a", "sess_mine_b"}; !equalSessionIDs(got, want) {
		t.Fatalf("revoked downward to %v, want %v", got, want)
	}
}

// TestLogoutStopsWhenCustodyItselfFails: the store revoke IS the operation,
// so its failure is the operation's failure — and nothing is swept, because
// telling sandboxes to drop a set the control plane still holds would leave
// the fleet disagreeing with custody.
func TestLogoutStopsWhenCustodyItselfFails(t *testing.T) {
	provider, _ := agentTestProvider(t)
	fx := agentSweepFixture(t)
	fx.store.revokeErr = control.ErrUnavailable

	if err := fx.svc.Logout(context.Background(), agentScopeFor(agentTestUser, control.ActorUser), provider.Name); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if got := fx.sender.requests(); len(got) != 0 {
		t.Fatalf("a failed revoke still swept %d sandboxes", len(got))
	}
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

// TestListAnswersAboutTheCallerAndCarriesNoBytes: the status type has no
// field a credential could travel in, which is the guarantee — the rest is
// that a listing is about the caller and refuses a principal that is not a
// person, exactly as a logout does.
func TestListAnswersAboutTheCallerAndCarriesNoBytes(t *testing.T) {
	provider, _ := agentTestProvider(t)
	fx := newAgentFixture(t)
	fx.store.statuses = []AgentCredentialStatus{
		{Provider: provider.Name, Version: 4, UpdatedAt: time.Unix(1700000000, 0).UTC()},
	}

	got, err := fx.svc.List(context.Background(), agentScopeFor(agentTestUser, control.ActorUser))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 || got[0].Provider != provider.Name || got[0].Version != 4 {
		t.Fatalf("list = %+v", got)
	}
	// The rendered status cannot hold a credential: encoding it and searching
	// for the fixture is how that is asserted rather than assumed.
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), agentTestCredential) {
		t.Fatalf("a listing carried a credential: %s", raw)
	}

	if _, err := fx.svc.List(context.Background(), agentScopeFor(agentTestUser, control.ActorService)); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("a service principal listed: %v", err)
	}
	if _, err := fx.svc.List(context.Background(), control.Scope{}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("an invalid scope listed: %v", err)
	}
}

// TestAgentRefusalSentenceRecognizesOnlyThisPackagesRefusals: the predicate a
// host relays through. Recognizing an arbitrary error would defeat the whole
// arrangement — the host would relay whatever text that error happened to
// hold, which on this path can be a row, a column, or a value.
func TestAgentRefusalSentenceRecognizesOnlyThisPackagesRefusals(t *testing.T) {
	if got, ok := AgentRefusalSentence(ErrAgentMembershipGone); !ok || got != "your workspace membership no longer allows this" {
		t.Fatalf("AgentRefusalSentence(ErrAgentMembershipGone) = %q, %v", got, ok)
	}
	for _, err := range []error{
		errors.New("pgstore: get agent credential: connection refused to db.internal"),
		control.ErrDenied,
		context.Canceled,
		nil,
	} {
		if got, ok := AgentRefusalSentence(err); ok {
			t.Fatalf("AgentRefusalSentence(%v) relayed %q", err, got)
		}
	}
}

func equalSessionIDs(a, b []control.SessionID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
