// internal/controld/srpc_test.go
package controld

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// helpers — the scripted sandbox behind a fake runner
//
// fakeRunner (runners_test.go) is runnerd; in production runnerd forwards a
// session RPC verbatim between its controld socket and the session's control
// channel, so a test that scripts the fake's session_req messages IS scripting
// the sandbox on the far side of that forwarder.
// ---------------------------------------------------------------------------

// sandboxSession seeds a running session placed on runner — the row
// sessionRPC resolves to find which connection to dispatch on.
func sandboxSession(t *testing.T, st MemStore, id, runner string) string {
	t.Helper()
	seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: control.RunnerID(runner)})
	return id
}

// nextSessionRPC returns the next session_rpc command controld sent, failing
// the test when it carries no envelope: everything downstream is asserted
// against the envelope, so a missing one must fail here rather than nil-panic
// three lines later.
func nextSessionRPC(t *testing.T, f *fakeRunner) runner.ToRunner {
	t.Helper()
	cmd := nextOfType(t, f, "session_rpc")
	if cmd.RPC == nil {
		t.Fatalf("session_rpc for %s carried no envelope: %+v", cmd.Session, cmd)
	}
	return cmd
}

// answerRPC scripts the sandbox's answer to a controld-initiated request. It
// travels up as a session_req whose envelope Method is "resp", echoing the
// request's own id — the pass-through correlation runnerd performs verbatim.
func (f *fakeRunner) answerRPC(t *testing.T, cmd runner.ToRunner, ok bool, payload string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "session_req", Session: cmd.Session,
		RPC: &runner.RPCEnvelope{ID: cmd.RPC.ID, Method: "resp", OK: ok, Payload: rawOrNil(payload)}})
}

// sandboxSessionFor is sandboxSession for a session with an OWNER: the mint
// reads whose credential to unseal off the row, so the tests that drive one
// have to say who that is.
func sandboxSessionFor(t *testing.T, st MemStore, id, runner, userID string) string {
	t.Helper()
	seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: control.RunnerID(runner), CreatorID: control.ActorID(userID)})
	return id
}

// sandboxRequest scripts a sandbox-initiated request arriving upward — the
// direction the credential mint travels.
func (f *fakeRunner) sandboxRequest(t *testing.T, session string, id uint64, method, payload string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "session_req", Session: session,
		RPC: &runner.RPCEnvelope{ID: id, Method: method, Payload: rawOrNil(payload)}})
}

func rawOrNil(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	return json.RawMessage(s)
}

// ---------------------------------------------------------------------------
// controld-initiated: where the downward half's tests went
//
// The six tests that drove Server.sessionRPC directly are gone with it. The
// behavior they pinned did not go anywhere — it moved to the two seams that
// replaced that method, and each one is named here so the coverage can be
// followed rather than assumed:
//
//   - TestSessionRPCRoundTrip           → controlapp TestSessionRPCSuccess (the
//     request's shape and the decoded answer) and runnerplane/plane_test.go
//     TestTransportDispatchSessionRPCCorrelatesByEnvelopeID (the envelope-ID
//     correlation over one connection, ReqID left at 0, table cleaned up).
//   - TestSessionRPCTimesOut            → runnerplane/plane_test.go
//     TestTransportDispatchFailuresAreUnavailableWithoutRunnerText, case "no
//     answer before OpTimeout".
//   - TestSessionRPCFailsFastOnConnDeath → the same test's "connection closed"
//     case, plus TestTransportDispatchHonorsCallerCancellation for not waiting
//     out a budget once the answer can no longer come.
//   - TestSessionRPCSurfacesSandboxErrors → controlapp
//     TestSessionRPCHostileResponses, case "false ok" (now ErrRunnerRefused,
//     which wraps ErrUnavailable); the 409 a refusal renders as on the wire is
//     pinned by files_test.go's three "refusal" cases. The sandbox's own
//     sentence is deliberately no longer relayed (D1).
//   - TestSessionRPCsCorrelateOutOfOrder → controlapp
//     TestSessionRPCConcurrentOutOfOrder, over the same per-call id.
//   - TestSessionRPCUnreachable         → "runner is not connected here" is
//     runnerplane/plane_test.go's "unknown runner" case; "session is placed
//     nowhere" and "no such session" are the attachment service's reads now
//     (controlapp TestWorkspaceDiffRequiresRunning and
//     TestAttachTerminalRejections), and files_test.go's readiness subtests
//     pin the 404/503/502 those become on the wire.
//
// TestOrphanSessionRPCResponseIsDropped moved to runnerplane/plane_test.go
// with the routing it is about: an unmatched "resp" is dropped by the plane's
// per-connection table, not by anything this file still owns.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// sandbox-initiated: session_req
// ---------------------------------------------------------------------------

// TestSandboxRequestIsAnswered pins the upward direction's routing: a request
// controld has no method for still gets an answer, because the sandbox is
// waiting on one. v0 has no methods at all (Task 8 lands the mint), so
// "unknown method" IS the whole handler and the transport is what's proven.
func TestSandboxRequestIsAnswered(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_req_up", "vm1")

	f.sandboxRequest(t, id, 41, "no_such_method", `{"a":1}`)

	cmd := nextSessionRPC(t, f)
	if cmd.Session != id {
		t.Fatalf("answer routed to session %q, want %q", cmd.Session, id)
	}
	if cmd.RPC.ID != 41 {
		t.Fatalf("answer id = %d, want the request's 41", cmd.RPC.ID)
	}
	if cmd.RPC.Method != "resp" {
		t.Fatalf("answer method = %q, want \"resp\"", cmd.RPC.Method)
	}
	if cmd.RPC.OK {
		t.Fatal("answer OK = true, want false for an unknown method")
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(cmd.RPC.Payload, &body); err != nil {
		t.Fatalf("decoding the error payload %s: %v", cmd.RPC.Payload, err)
	}
	if !strings.Contains(body.Error, "unknown method") || !strings.Contains(body.Error, "no_such_method") {
		t.Fatalf("error = %q, want it to name the unknown method", body.Error)
	}
}

// TestSandboxRequestFromTheWrongRunnerIsRefused: the runner token is
// fleet-wide, so a session_req proves only that SOME runner sent it. Every
// method behind this routing acts with the session owner's authority (Task 8
// mints their GitHub token), so a request for a session the store does not
// place on the asking runner is refused before any method runs — the same
// guard applyEvent's terminal arms apply to events.
func TestSandboxRequestFromTheWrongRunnerIsRefused(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	sandboxSession(t, st, "sess_elsewhere_req", "vm2")

	f.sandboxRequest(t, "sess_elsewhere_req", 5, "mint_git_credential", "")

	cmd := nextSessionRPC(t, f)
	if cmd.RPC.ID != 5 || cmd.RPC.Method != "resp" || cmd.RPC.OK {
		t.Fatalf("answer = %+v, want an ok:false resp for id 5", cmd.RPC)
	}

	// An unknown session is refused the same way — there is no row to
	// authorize the request against.
	f.sandboxRequest(t, "sess_ghost", 6, "mint_git_credential", "")
	cmd = nextSessionRPC(t, f)
	if cmd.RPC.ID != 6 || cmd.RPC.OK {
		t.Fatalf("answer = %+v, want an ok:false resp for id 6", cmd.RPC)
	}
}

// ---------------------------------------------------------------------------
// sandbox-initiated: mint_git_credential
// ---------------------------------------------------------------------------

// rpcAnswer reads one answer envelope's two mutually exclusive bodies: the
// {"token": …} a success carries, and the {"error": …} a refusal does.
func rpcAnswer(t *testing.T, env *runner.RPCEnvelope) (token, errText string) {
	t.Helper()
	var body struct {
		Token string `json:"token"`
		Error string `json:"error"`
	}
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &body); err != nil {
			t.Fatalf("decoding the answer payload %s: %v", env.Payload, err)
		}
	}
	return body.Token, body.Error
}

// TestMintGitCredentialAnswersTheSandbox is the credential loop's controld
// half end to end: a sandbox asks, the row says who owns it, the vault
// unseals that user's token, and it goes back down in the exact shape the
// in-sandbox helper reads (cmd/sessiond/helper.go: {"token": …} and nothing
// else).
func TestMintGitCredentialAnswersTheSandbox(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	u := seedVaultUser(t, st, 5150, "alice")
	stale := seedGitHubCredential(t, s, st, u.ID)
	id := sandboxSessionFor(t, st, "sess_mint_ok", "vm1", u.ID)

	f.sandboxRequest(t, id, 11, "mint_git_credential", `{"host":"github.com"}`)

	cmd := nextSessionRPC(t, f)
	if cmd.Session != id {
		t.Fatalf("answer routed to session %q, want %q", cmd.Session, id)
	}
	if cmd.RPC.ID != 11 || cmd.RPC.Method != "resp" {
		t.Fatalf("answer = id %d method %q, want id 11 method \"resp\"", cmd.RPC.ID, cmd.RPC.Method)
	}
	if !cmd.RPC.OK {
		_, errText := rpcAnswer(t, cmd.RPC)
		t.Fatalf("mint refused with %q, want the token", errText)
	}
	// Pinned as bytes, not as a decode: the helper on the far side reads this
	// one key, so the wire shape is the contract, not merely a struct that
	// happens to round-trip.
	if got, want := string(cmd.RPC.Payload), `{"token":"`+vaultToken+`"}`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}

	// A mint is a USE: last_used_at moves so `rainier creds` can show it, and
	// the row stays valid — nothing about handing a token out is a rejection.
	c := getCredential(t, st, u.ID)
	if !c.LastUsedAt.After(stale) {
		t.Fatalf("last_used_at = %s, want it moved past the seeded %s", c.LastUsedAt, stale)
	}
	if c.Status != CredentialValid {
		t.Fatalf("status after a mint = %q, want %q", c.Status, CredentialValid)
	}
}

// TestMintGitCredentialRefusesWithTheNamedAction: both vault refusals reach
// the sandbox as ok:false carrying the sentinel's own words. That text is not
// decoration — sessiond hands it to git, git prints it, and the user reads the
// exact command that fixes their session. Asserted verbatim for that reason.
func TestMintGitCredentialRefusesWithTheNamedAction(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(t *testing.T, s *Server, st MemStore, userID string)
		want string
	}{
		{
			name: "a credential something has already rejected",
			seed: func(t *testing.T, s *Server, st MemStore, userID string) {
				seedGitHubCredential(t, s, st, userID)
				s.rejectCredential(context.Background(), userID, githubProvider)
			},
			want: ErrCredentialNeedsRefresh.Error(),
		},
		{
			name: "no credential at all",
			seed: func(t *testing.T, s *Server, st MemStore, userID string) {},
			want: ErrCredentialMissing.Error(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st, ts := newTestControld(t)
			f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
			u := seedVaultUser(t, st, 5151, "alice")
			tc.seed(t, s, st, u.ID)
			id := sandboxSessionFor(t, st, "sess_mint_refused", "vm1", u.ID)

			f.sandboxRequest(t, id, 12, "mint_git_credential", "")

			cmd := nextSessionRPC(t, f)
			if cmd.RPC.ID != 12 || cmd.RPC.OK {
				t.Fatalf("answer = %+v, want an ok:false resp for id 12", cmd.RPC)
			}
			token, errText := rpcAnswer(t, cmd.RPC)
			if token != "" {
				t.Fatal("a refusal carried a token")
			}
			if errText != tc.want {
				t.Fatalf("error = %q, want the sentinel's own words %q", errText, tc.want)
			}
		})
	}
}

// TestCredentialRejectedMakesTheNextMintRefuse is the whole lazy-revocation
// loop in one test: a token is minted, GitHub refuses it, the sandbox says so,
// and the NEXT mint refuses with the named action instead of handing out a
// value that is known not to work. Without the flip, every git operation in
// every session that user owns keeps failing with GitHub's own opaque wording.
func TestCredentialRejectedMakesTheNextMintRefuse(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	u := seedVaultUser(t, st, 5152, "alice")
	seedGitHubCredential(t, s, st, u.ID)
	id := sandboxSessionFor(t, st, "sess_mint_then_reject", "vm1", u.ID)

	f.sandboxRequest(t, id, 21, "mint_git_credential", "")
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the first mint was refused: %s", cmd.RPC.Payload)
	}

	f.event(t, id, "credential_rejected")
	wantCredentialStatus(t, st, u.ID, CredentialNeedsRefresh)

	f.sandboxRequest(t, id, 22, "mint_git_credential", "")
	cmd := nextSessionRPC(t, f)
	if cmd.RPC.ID != 22 || cmd.RPC.OK {
		t.Fatalf("the second mint = %+v, want an ok:false resp for id 22", cmd.RPC)
	}
	token, errText := rpcAnswer(t, cmd.RPC)
	if token != "" {
		t.Fatal("a rejected credential was minted anyway")
	}
	if errText != ErrCredentialNeedsRefresh.Error() {
		t.Fatalf("error = %q, want %q", errText, ErrCredentialNeedsRefresh.Error())
	}
}

// TestMintForASessionWithNoOwnerIsRefused: the owner is the whole authority
// the mint acts with, so a row that names none must produce a refusal rather
// than a lookup for the empty user — which would otherwise be one accidental
// store row away from handing a sandbox somebody else's credential.
func TestMintForASessionWithNoOwnerIsRefused(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	// Written straight through the store: seedSession supplies an owner, and
	// the row this test needs is the one nothing in the API can produce.
	if _, err := st.Sessions().CreateSession(context.Background(), installWorkspace,
		control.Session{ID: "sess_ownerless", State: control.StateRunning, PoolID: installPool, RunnerID: "vm1"}); err != nil {
		t.Fatalf("seeding an ownerless session: %v", err)
	}

	f.sandboxRequest(t, "sess_ownerless", 31, "mint_git_credential", "")

	cmd := nextSessionRPC(t, f)
	if cmd.RPC.ID != 31 || cmd.RPC.OK {
		t.Fatalf("answer = %+v, want an ok:false resp for id 31", cmd.RPC)
	}
}

// syncLog is a log sink a test can read while the connection goroutines are
// still writing to it.
type syncLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureLogs redirects the standard logger for the duration of one test.
func captureLogs(t *testing.T) *syncLog {
	t.Helper()
	sink := &syncLog{}
	prev := log.Writer()
	log.SetOutput(sink)
	t.Cleanup(func() { log.SetOutput(prev) })
	return sink
}

// TestMintGitCredentialNeverLogsTheToken holds the plan's hygiene rule where
// it is easiest to break: a minted token may appear in the RPC response
// payload and NOWHERE else. controld's logs are the fleet's most-copied
// artifact — pasted into issues, shipped to an aggregator — and a token that
// reaches one has escaped the vault for good.
//
// It drives every path that has a token in scope: the success, the failure
// that follows a rejection, and the rejection itself.
func TestMintGitCredentialNeverLogsTheToken(t *testing.T) {
	sink := captureLogs(t)
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	u := seedVaultUser(t, st, 5153, "alice")
	seedGitHubCredential(t, s, st, u.ID)
	id := sandboxSessionFor(t, st, "sess_mint_quiet", "vm1", u.ID)

	f.sandboxRequest(t, id, 41, "mint_git_credential", "")
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the mint was refused: %s", cmd.RPC.Payload)
	}
	f.event(t, id, "credential_rejected")
	wantCredentialStatus(t, st, u.ID, CredentialNeedsRefresh)
	f.sandboxRequest(t, id, 42, "mint_git_credential", "")
	if cmd := nextSessionRPC(t, f); cmd.RPC.OK {
		t.Fatal("the mint after a rejection succeeded")
	}

	if got := sink.String(); strings.Contains(got, vaultToken) {
		t.Fatalf("the minted token reached the log:\n%s", got)
	}
}

// blockingGetStore wedges GetSession for one session id, so a test can hold a
// sandbox request inside its handler and prove the connection's reader is not
// waiting behind it.
type blockingGetStore struct {
	MemStore
	id      string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingGetStore) Sessions() control.SessionRepository {
	return blockingGetSessions{SessionRepository: b.MemStore.Sessions(), owner: b}
}

type blockingGetSessions struct {
	control.SessionRepository
	owner *blockingGetStore
}

func (b blockingGetSessions) GetSession(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	if o := b.owner; string(id) == o.id {
		o.once.Do(func() { close(o.entered) })
		<-o.release
	}
	return b.SessionRepository.GetSession(ctx, ws, id)
}

// TestSandboxRequestsDoNotBlockTheRunnerReader: answering a sandbox request
// reads the store (this task's placement guard; Task 8's mint reads more), and
// the connection's reader is the ONE goroutine delivering every result and
// event that runner sends. Answering inline would stall the whole connection
// for the duration — proven here by wedging the store read and requiring an
// ordinary dispatch, whose result that same reader must deliver, to complete
// meanwhile.
func TestSandboxRequestsDoNotBlockTheRunnerReader(t *testing.T) {
	base := NewMemStore()
	bs := &blockingGetStore{MemStore: base, id: "sess_req_slow",
		entered: make(chan struct{}), release: make(chan struct{})}
	s, ts := newTestControldOver(t, bs)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, base, "sess_req_slow", "vm1")

	var once sync.Once
	closeRelease := func() { once.Do(func() { close(bs.release) }) }
	defer closeRelease()

	f.sandboxRequest(t, id, 7, "mint_git_credential", "")
	select {
	case <-bs.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the sandbox request never reached the store")
	}

	// While that handler is wedged, an ordinary dispatch still completes.
	done := make(chan error, 1)
	go func() {
		_, err := s.transport.Dispatch(context.Background(), installPool, "vm1",
			runner.ToRunner{Type: "suspend", Session: id})
		done <- err
	}()
	cmd := nextOfType(t, f, "suspend")
	f.reply(t, cmd, true, "")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("dispatch while a session request is wedged: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the runner connection's reader is blocked behind a session request")
	}

	closeRelease()
	if answer := nextSessionRPC(t, f); answer.RPC.ID != 7 {
		t.Fatalf("answer id = %d, want 7 once the handler was released", answer.RPC.ID)
	}
}

// ---------------------------------------------------------------------------
// sandbox-initiated: agent credentials
//
// The two upward methods that keep a person's coding-agent login equal on
// both sides of the sandbox boundary. Everything below drives them through
// the real transport — a fake runner's session_req, forwarded exactly as
// runnerd forwards one — so what is proven is the wire, not a method call.
//
// No provider is spelled here. The names come off controlapp's table, which
// is the only place in the repository that has one.
// ---------------------------------------------------------------------------

// agentCredentialFixture is the bytes every case below stores and reads back.
// It is not a credential, it is the WORD for one, so that the hygiene tests
// can assert its absence and mean something by it.
const agentCredentialFixture = "credential_example"

// agentProviderRow returns the first table row and the first file it
// allowlists, plus a name that is definitely NOT on it.
func agentProviderRow(t *testing.T) (name, file string) {
	t.Helper()
	rows := controlapp.AgentProviders()
	if len(rows) == 0 || len(rows[0].Files) == 0 {
		t.Fatal("the provider table has no row with a file to store")
	}
	return rows[0].Name, rows[0].Files[0]
}

// agentRPCBody reads one answer envelope's three mutually exclusive bodies.
func agentRPCBody(t *testing.T, env *runner.RPCEnvelope) (version uint64, files map[string][]byte, errText string) {
	t.Helper()
	var body struct {
		Version uint64            `json:"version"`
		Files   map[string][]byte `json:"files"`
		Error   string            `json:"error"`
	}
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &body); err != nil {
			t.Fatalf("decoding the answer payload: %v", err)
		}
	}
	return body.Version, body.Files, body.Error
}

// TestSessionRequestAnswersFetchAndPut is the custody loop's controld half
// end to end: a sandbox asks at boot and is told the truth (nothing yet), the
// person's agent writes its file and the sandbox puts it, and the next boot's
// fetch hands the same bytes back at the version the put returned.
//
// The shapes are pinned as BYTES where the far side reads them as bytes: the
// agent stage in sessiond reads "version" and "files", and a renamed field
// here would break the agent home in every session rather than fail a build.
func TestSessionRequestAnswersFetchAndPut(t *testing.T) {
	provider, file := agentProviderRow(t)
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	u := seedVaultUser(t, st, 6100, "alice")
	id := sandboxSessionFor(t, st, "sess_agent_ok", "vm1", u.ID)

	// Boot with no login: an ANSWER, not a refusal. The agent starts and asks
	// the person to log in, which is the truthful state.
	f.sandboxRequest(t, id, 51, runner.MethodFetchAgentCredentials, `{"provider":"`+provider+`"}`)
	cmd := nextSessionRPC(t, f)
	if cmd.RPC.ID != 51 || !cmd.RPC.OK {
		_, _, errText := agentRPCBody(t, cmd.RPC)
		t.Fatalf("the first fetch = %+v (%q), want an ok:true answer", cmd.RPC, errText)
	}
	if got, want := string(cmd.RPC.Payload), `{"version":0,"files":{}}`; got != want {
		t.Fatalf("empty fetch payload = %s, want %s", got, want)
	}

	// The person logs in; the sandbox puts what the agent wrote.
	encoded := base64.StdEncoding.EncodeToString([]byte(agentCredentialFixture))
	f.sandboxRequest(t, id, 52, runner.MethodPutAgentCredentials,
		`{"provider":"`+provider+`","files":{"`+file+`":"`+encoded+`"},"version":0}`)
	cmd = nextSessionRPC(t, f)
	if cmd.RPC.ID != 52 || !cmd.RPC.OK {
		_, _, errText := agentRPCBody(t, cmd.RPC)
		t.Fatalf("the put = %+v (%q), want an ok:true answer", cmd.RPC, errText)
	}
	if got, want := string(cmd.RPC.Payload), `{"version":1}`; got != want {
		t.Fatalf("put payload = %s, want %s", got, want)
	}

	// A second boot — the whole point of custody — finds the login already
	// there, at the version the put reported.
	f.sandboxRequest(t, id, 53, runner.MethodFetchAgentCredentials, `{"provider":"`+provider+`"}`)
	cmd = nextSessionRPC(t, f)
	if cmd.RPC.ID != 53 || !cmd.RPC.OK {
		t.Fatalf("the second fetch = %+v, want an ok:true answer", cmd.RPC)
	}
	version, files, _ := agentRPCBody(t, cmd.RPC)
	if version != 1 {
		t.Fatalf("fetched version = %d, want 1", version)
	}
	if got := files[file]; string(got) != agentCredentialFixture {
		t.Fatalf("fetched %d bytes for %q, want the %d the put sent", len(got), file, len(agentCredentialFixture))
	}
	// A second put advances the version: that is how `rainier agent ls` and
	// the sandbox's own baseline tell a fresh login from a stale one.
	f.sandboxRequest(t, id, 54, runner.MethodPutAgentCredentials,
		`{"provider":"`+provider+`","files":{"`+file+`":"`+encoded+`"},"version":1}`)
	cmd = nextSessionRPC(t, f)
	if got, want := string(cmd.RPC.Payload), `{"version":2}`; !cmd.RPC.OK || got != want {
		t.Fatalf("second put payload = %s, want %s", got, want)
	}
}

// TestSessionRequestRefusesAnAgentCredentialItMustNot walks every refusal the
// two methods have, and asserts the same thing about each: the answer is
// ok:false, it carries a fixed sentence, and it carries NOTHING ELSE — no
// version a sandbox could record as a baseline, and no byte of anybody's set.
func TestSessionRequestRefusesAnAgentCredentialItMustNot(t *testing.T) {
	provider, file := agentProviderRow(t)
	encoded := base64.StdEncoding.EncodeToString([]byte(agentCredentialFixture))
	oversize := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), controlapp.AgentCredentialSetMaxBytes+1))

	for _, tc := range []struct {
		name    string
		method  string
		payload string
		// seed returns the session id to ask about, having arranged whatever
		// makes this case refuse.
		seed func(t *testing.T, s *Server, st MemStore) string
		want string
	}{
		{
			name: "a session placed on another runner", method: runner.MethodFetchAgentCredentials,
			payload: `{"provider":"` + provider + `"}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				u := seedVaultUser(t, st, 6101, "alice")
				return sandboxSessionFor(t, st, "sess_agent_elsewhere", "vm2", u.ID)
			},
			want: "this session is not placed on the runner that asked",
		},
		{
			name: "a session nobody has", method: runner.MethodFetchAgentCredentials,
			payload: `{"provider":"` + provider + `"}`,
			seed:    func(t *testing.T, s *Server, st MemStore) string { return "sess_agent_ghost" },
			want:    "no such session",
		},
		{
			name: "a session with no creator", method: runner.MethodFetchAgentCredentials,
			payload: `{"provider":"` + provider + `"}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				// Written straight through the store: seedSession supplies a
				// creator, and this is the row nothing in the API produces.
				if _, err := st.Sessions().CreateSession(context.Background(), installWorkspace,
					control.Session{ID: "sess_agent_ownerless", State: control.StateRunning, PoolID: installPool, RunnerID: "vm1"}); err != nil {
					t.Fatalf("seeding an ownerless session: %v", err)
				}
				return "sess_agent_ownerless"
			},
			want: "this session has no creator to fetch an agent credential for",
		},
		{
			name: "a creator who is no longer an operator here", method: runner.MethodFetchAgentCredentials,
			payload: `{"provider":"` + provider + `"}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				return sandboxSessionFor(t, st, "sess_agent_stranger", "vm1", "usr_no_such_operator")
			},
			want: "your workspace membership no longer allows this",
		},
		{
			name: "a provider outside the table", method: runner.MethodFetchAgentCredentials,
			payload: `{"provider":"provider_example"}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				u := seedVaultUser(t, st, 6102, "alice")
				return sandboxSessionFor(t, st, "sess_agent_unknown_provider", "vm1", u.ID)
			},
			want: "unknown agent provider",
		},
		{
			name: "a set over the cap", method: runner.MethodPutAgentCredentials,
			payload: `{"provider":"` + provider + `","files":{"` + file + `":"` + oversize + `"}}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				u := seedVaultUser(t, st, 6103, "alice")
				return sandboxSessionFor(t, st, "sess_agent_oversize", "vm1", u.ID)
			},
			want: "the agent credential set is too large",
		},
		{
			name: "a file name off the allowlist", method: runner.MethodPutAgentCredentials,
			payload: `{"provider":"` + provider + `","files":{"../../workspace/notes":"` + encoded + `"}}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				u := seedVaultUser(t, st, 6104, "alice")
				return sandboxSessionFor(t, st, "sess_agent_traversal", "vm1", u.ID)
			},
			want: "that file is not part of this agent's credential set",
		},
		{
			// Valid JSON, wrong shape. A body that is not JSON at all cannot
			// be built here — the envelope's payload is a json.RawMessage and
			// the transport refuses to marshal one — and the sandbox's own
			// encoder is under the same constraint, so this IS the malformed
			// request that can actually arrive.
			name: "a body that is not the shape", method: runner.MethodPutAgentCredentials,
			payload: `{"provider":7,"files":"not_a_map"}`,
			seed: func(t *testing.T, s *Server, st MemStore) string {
				u := seedVaultUser(t, st, 6105, "alice")
				return sandboxSessionFor(t, st, "sess_agent_garbage", "vm1", u.ID)
			},
			want: "the agent credential request could not be decoded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st, ts := newTestControld(t)
			f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
			id := tc.seed(t, s, st)

			f.sandboxRequest(t, id, 61, tc.method, tc.payload)
			cmd := nextSessionRPC(t, f)
			if cmd.RPC.ID != 61 || cmd.RPC.OK {
				t.Fatalf("answer = %+v, want an ok:false resp for id 61", cmd.RPC)
			}
			version, files, errText := agentRPCBody(t, cmd.RPC)
			if errText != tc.want {
				t.Fatalf("error = %q, want %q", errText, tc.want)
			}
			if version != 0 || len(files) != 0 {
				t.Fatalf("a refusal carried version %d and %d files", version, len(files))
			}
			if strings.Contains(string(cmd.RPC.Payload), agentCredentialFixture) ||
				strings.Contains(string(cmd.RPC.Payload), encoded) {
				t.Fatalf("a refusal carried a credential: %s", cmd.RPC.Payload)
			}
		})
	}
}

// TestNoCredentialByteReachesALogOrError holds the plan's hygiene rule where
// it is easiest to break. controld's logs are the fleet's most-copied
// artifact — pasted into issues, shipped to an aggregator — and an agent
// credential that reaches one has escaped custody for good.
//
// It drives every path that has a set in scope: the empty fetch, the put, the
// fetch that hands it back, and two refusals.
//
// What it scans for needs saying. The full fixture is checked as-is. Its
// THREE-BYTE runs are checked in the form the bytes actually travel in —
// base64, inside the RPC payload — because that is the shape a partial leak
// would take, and because a three-byte scan of the plaintext would fire on
// the honest words the log is written in: "agent credential" contains "cre",
// "red", "ent", "ial" and the rest of them. A scan that cannot pass is not an
// assertion, it is a broken test that would be deleted at the first failure.
func TestNoCredentialByteReachesALogOrError(t *testing.T) {
	provider, file := agentProviderRow(t)
	sink := captureLogs(t)
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	u := seedVaultUser(t, st, 6200, "alice")
	id := sandboxSessionFor(t, st, "sess_agent_quiet", "vm1", u.ID)
	encoded := base64.StdEncoding.EncodeToString([]byte(agentCredentialFixture))

	f.sandboxRequest(t, id, 71, runner.MethodFetchAgentCredentials, `{"provider":"`+provider+`"}`)
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the empty fetch was refused: %s", cmd.RPC.Payload)
	}
	f.sandboxRequest(t, id, 72, runner.MethodPutAgentCredentials,
		`{"provider":"`+provider+`","files":{"`+file+`":"`+encoded+`"}}`)
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the put was refused: %s", cmd.RPC.Payload)
	}
	f.sandboxRequest(t, id, 73, runner.MethodFetchAgentCredentials, `{"provider":"`+provider+`"}`)
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the second fetch was refused: %s", cmd.RPC.Payload)
	}
	// Two refusals, which are the arms that build a message out of what went
	// wrong and are therefore the ones most likely to quote the value.
	f.sandboxRequest(t, id, 74, runner.MethodPutAgentCredentials,
		`{"provider":"`+provider+`","files":{"file_example_off_the_list":"`+encoded+`"}}`)
	if cmd := nextSessionRPC(t, f); cmd.RPC.OK {
		t.Fatal("a file off the allowlist was stored")
	}
	// The undecodable body CARRIES the fixture, which is the case worth
	// asserting: a decode error's own text quotes the bytes it choked on, so
	// wrapping one into a log line would publish the credential.
	f.sandboxRequest(t, id, 75, runner.MethodPutAgentCredentials,
		`{"provider":"`+provider+`","files":"`+encoded+`"}`)
	if cmd := nextSessionRPC(t, f); cmd.RPC.OK {
		t.Fatal("an undecodable body was stored")
	}

	got := sink.String()
	if strings.Contains(got, agentCredentialFixture) {
		t.Fatalf("the credential fixture reached the log:\n%s", got)
	}
	if strings.Contains(got, encoded) {
		t.Fatalf("the encoded credential reached the log:\n%s", got)
	}
	for i := 0; i+3 <= len(encoded); i++ {
		if run := encoded[i : i+3]; strings.Contains(got, run) {
			t.Fatalf("a 3-byte run of the encoded credential (%q, at offset %d) reached the log:\n%s", run, i, got)
		}
	}
}
