// internal/controld/srpc_test.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

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
func sandboxSession(t *testing.T, st Store, id, runner string) string {
	t.Helper()
	seedSession(t, st, Session{ID: id, State: StateRunning, Runner: runner})
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
func sandboxSessionFor(t *testing.T, st Store, id, runner, userID string) string {
	t.Helper()
	seedSession(t, st, Session{ID: id, State: StateRunning, Runner: runner, OwnerID: userID})
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

// srpcPending reports how many entries a runner connection's session-RPC
// table still holds — every test that ends a call asserts this is back to
// zero, because a pending entry that outlives its call is a leak no timeout
// would ever clean up.
func srpcPending(t *testing.T, s *Server, runner string) int {
	t.Helper()
	rc := s.conn(runner)
	if rc == nil {
		t.Fatalf("runner %q has no connection", runner)
	}
	return rc.srpc.len()
}

// ---------------------------------------------------------------------------
// controld-initiated: sessionRPC
// ---------------------------------------------------------------------------

// TestSessionRPCRoundTrip is the whole downward path in one test: controld
// asks a sandbox something, the answer comes back, and the caller gets it
// decoded into its own type.
func TestSessionRPCRoundTrip(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_rpc_ok", "vm1")

	type answer struct {
		Pong string `json:"pong"`
	}
	var got answer
	errc := make(chan error, 1)
	go func() {
		errc <- s.sessionRPC(context.Background(), id, "ping", map[string]string{"hello": "world"}, &got)
	}()

	cmd := nextSessionRPC(t, f)
	if cmd.Session != id {
		t.Fatalf("session_rpc.Session = %q, want %q", cmd.Session, id)
	}
	if cmd.RPC.Method != "ping" {
		t.Fatalf("envelope method = %q, want \"ping\"", cmd.RPC.Method)
	}
	if cmd.RPC.ID == 0 {
		t.Fatal("envelope id = 0; a request with no id can never be correlated")
	}
	if cmd.ReqID != 0 {
		t.Fatalf("ReqID = %d, want 0 — session RPC correlates on the envelope's own id, not the runner-dispatch space", cmd.ReqID)
	}
	if string(cmd.RPC.Payload) != `{"hello":"world"}` {
		t.Fatalf("payload = %s, want the caller's own object", cmd.RPC.Payload)
	}

	f.answerRPC(t, cmd, true, `{"pong":"yes"}`)
	if err := <-errc; err != nil {
		t.Fatalf("sessionRPC: %v", err)
	}
	if got.Pong != "yes" {
		t.Fatalf("decoded answer = %+v, want Pong \"yes\"", got)
	}
	if n := srpcPending(t, s, "vm1"); n != 0 {
		t.Fatalf("%d pending entries after the answer landed, want 0", n)
	}
}

// TestSessionRPCTimesOut: a sandbox that never answers must not park the
// caller forever, and the pending entry must go with the call — a timeout is
// the one exit that has no delivery to clean up after it.
func TestSessionRPCTimesOut(t *testing.T) {
	s, st, ts := newTestControld(t, func(c *Config) { c.OpTimeout = 250 * time.Millisecond })
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_rpc_slow", "vm1")

	start := time.Now()
	err := s.sessionRPC(context.Background(), id, "diff", nil, nil)
	if err == nil {
		t.Fatal("sessionRPC with no answer = nil, want a timeout")
	}
	if !errors.Is(err, ErrRunnerUnreachable) || !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("err = %v, want it to wrap both ErrRunnerUnreachable and ErrDispatchTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("waited %s, want ~the 250ms OpTimeout", elapsed)
	}
	if n := srpcPending(t, s, "vm1"); n != 0 {
		t.Fatalf("%d pending entries after a timeout, want 0", n)
	}
	// The command really was sent — the timeout is about the answer, not the
	// dispatch.
	if cmd := nextSessionRPC(t, f); cmd.Session != id {
		t.Fatalf("session_rpc.Session = %q, want %q", cmd.Session, id)
	}
}

// TestSessionRPCFailsFastOnConnDeath: when the runner's connection dies there
// will never be an answer, and waiting out OpTimeout for a fact already known
// is exactly the stall the conn-death signal exists to prevent.
func TestSessionRPCFailsFastOnConnDeath(t *testing.T) {
	s, st, ts := newTestControld(t, func(c *Config) { c.OpTimeout = 30 * time.Second })
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_rpc_dies", "vm1")

	errc := make(chan error, 1)
	go func() { errc <- s.sessionRPC(context.Background(), id, "diff", nil, nil) }()
	nextSessionRPC(t, f) // the request is in flight
	f.close()

	select {
	case err := <-errc:
		if !errors.Is(err, ErrRunnerUnreachable) {
			t.Fatalf("err = %v, want it to wrap ErrRunnerUnreachable", err)
		}
		if errors.Is(err, ErrDispatchTimeout) {
			t.Fatalf("err = %v, want a conn-death error, not a timeout — the connection is known dead", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sessionRPC did not fail when the connection died (it waited for OpTimeout)")
	}
}

// TestSessionRPCSurfacesSandboxErrors: a refusal inside the sandbox is not a
// transport failure, and its message is the whole point — the named action a
// user has to run reaches them through this path verbatim.
func TestSessionRPCSurfacesSandboxErrors(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_rpc_refused", "vm1")

	const msg = "github credentials need a refresh: run `rainier login --refresh github`"
	errc := make(chan error, 1)
	go func() { errc <- s.sessionRPC(context.Background(), id, "mint_git_credential", nil, nil) }()
	cmd := nextSessionRPC(t, f)
	body, err := json.Marshal(map[string]string{"error": msg})
	if err != nil {
		t.Fatal(err)
	}
	f.answerRPC(t, cmd, false, string(body))

	err = <-errc
	var se *sandboxError
	if !errors.As(err, &se) {
		t.Fatalf("err = %v (%T), want a *sandboxError", err, err)
	}
	if se.Error() != msg {
		t.Fatalf("error text = %q, want the sandbox's own message %q", se.Error(), msg)
	}
	if errors.Is(err, ErrRunnerUnreachable) {
		t.Fatalf("err = %v, want a sandbox refusal, not an unreachable-runner error", err)
	}
	if n := srpcPending(t, s, "vm1"); n != 0 {
		t.Fatalf("%d pending entries after a refusal, want 0", n)
	}
}

// TestSessionRPCsCorrelateOutOfOrder: two calls to the same sandbox are in
// flight at once and the answers come back in the opposite order. Each caller
// must get its own answer — this is what the per-call id buys, and reversing
// the replies is the only way to prove the table is doing the matching rather
// than arrival order.
func TestSessionRPCsCorrelateOutOfOrder(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_rpc_concurrent", "vm1")

	type reply struct {
		Which string `json:"which"`
	}
	type outcome struct {
		got reply
		err error
	}
	results := make(chan outcome, 2)
	call := func(method string) {
		var got reply
		err := s.sessionRPC(context.Background(), id, method, nil, &got)
		results <- outcome{got, err}
	}
	go call("first")
	go call("second")

	a := nextSessionRPC(t, f)
	b := nextSessionRPC(t, f)
	if a.RPC.ID == b.RPC.ID {
		t.Fatalf("both requests got id %d; concurrent calls must not collide", a.RPC.ID)
	}
	// Answered second-then-first, deliberately.
	f.answerRPC(t, b, true, `{"which":"`+b.RPC.Method+`"}`)
	f.answerRPC(t, a, true, `{"which":"`+a.RPC.Method+`"}`)

	seen := map[string]bool{}
	for range 2 {
		o := <-results
		if o.err != nil {
			t.Fatalf("sessionRPC: %v", o.err)
		}
		seen[o.got.Which] = true
	}
	if !seen["first"] || !seen["second"] {
		t.Fatalf("answers landed as %v, want each call to receive its own", seen)
	}
	if n := srpcPending(t, s, "vm1"); n != 0 {
		t.Fatalf("%d pending entries after both answers, want 0", n)
	}
}

// TestSessionRPCUnreachable covers the three ways a request never reaches a
// sandbox at all. All three are ErrRunnerUnreachable, exactly as a runner
// dispatch is: from the caller's side "nobody answered" is one fact.
func TestSessionRPCUnreachable(t *testing.T) {
	s, st, ts := newTestControld(t)
	joinRunner(t, s, ts, runnerScript{Name: "vm1"})

	t.Run("session is placed nowhere", func(t *testing.T) {
		seedSession(t, st, Session{ID: "sess_unplaced", State: StateQueued})
		err := s.sessionRPC(context.Background(), "sess_unplaced", "diff", nil, nil)
		if !errors.Is(err, ErrRunnerUnreachable) {
			t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
		}
	})
	t.Run("runner is not connected here", func(t *testing.T) {
		sandboxSession(t, st, "sess_elsewhere", "vm-elsewhere")
		err := s.sessionRPC(context.Background(), "sess_elsewhere", "diff", nil, nil)
		if !errors.Is(err, ErrRunnerUnreachable) {
			t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
		}
	})
	t.Run("no such session", func(t *testing.T) {
		err := s.sessionRPC(context.Background(), "sess_nope", "diff", nil, nil)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

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

// TestOrphanSessionRPCResponseIsDropped: a response whose id nobody is waiting
// on (its caller timed out, or the sandbox is confused) is logged and dropped.
// What must NOT happen is the connection's reader dying with it — proven by
// the ordinary RPC that still round-trips right after.
func TestOrphanSessionRPCResponseIsDropped(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := sandboxSession(t, st, "sess_orphan", "vm1")

	f.write(t, runner.FromRunner{Type: "session_req", Session: id,
		RPC: &runner.RPCEnvelope{ID: 999, Method: "resp", OK: true, Payload: json.RawMessage(`{"stale":true}`)}})
	// A malformed session_req (no envelope at all) is dropped the same way.
	f.write(t, runner.FromRunner{Type: "session_req", Session: id})

	errc := make(chan error, 1)
	go func() { errc <- s.sessionRPC(context.Background(), id, "ping", nil, nil) }()
	cmd := nextSessionRPC(t, f)
	f.answerRPC(t, cmd, true, "")
	if err := <-errc; err != nil {
		t.Fatalf("sessionRPC after an orphan response: %v", err)
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
		seed func(t *testing.T, s *Server, st Store, userID string)
		want string
	}{
		{
			name: "a credential something has already rejected",
			seed: func(t *testing.T, s *Server, st Store, userID string) {
				seedGitHubCredential(t, s, st, userID)
				s.rejectCredential(context.Background(), userID, githubProvider)
			},
			want: ErrCredentialNeedsRefresh.Error(),
		},
		{
			name: "no credential at all",
			seed: func(t *testing.T, s *Server, st Store, userID string) {},
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
	if _, err := st.CreateSession(context.Background(),
		Session{ID: "sess_ownerless", State: StateRunning, Runner: "vm1"}); err != nil {
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
	Store
	id      string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingGetStore) GetSession(ctx context.Context, id string) (Session, error) {
	if id == b.id {
		b.once.Do(func() { close(b.entered) })
		<-b.release
	}
	return b.Store.GetSession(ctx, id)
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
	bs := &blockingGetStore{Store: base, id: "sess_req_slow",
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
