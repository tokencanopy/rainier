// internal/controld/exec_test.go
package controld

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// dialExec performs the client-side exec dial. It is a ROUTE, not a
// parameter on attach, which is the whole compatibility design: a plane that
// predates exec answers 404 here, where it would have silently opened a
// terminal attachment there.
func dialExec(t *testing.T, ts *httptest.Server, id, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	return websocket.Dial(ctx, wsBase(ts)+"/v0/sessions/"+id+"/exec",
		&websocket.DialOptions{HTTPHeader: hdr})
}

func execStart(argv ...string) terminal.ClientMessage {
	return terminal.ClientMessage{Type: terminal.TypeExecStart,
		Exec: &runner.ExecSpec{Argv: argv}}
}

// scriptedExec answers one exec the way a new sessiond does: exec_started
// first, then whatever the test asked for.
func scriptedExec(msgs ...terminal.ServerMessage) func(relay.Frame) []terminal.ServerMessage {
	return func(relay.Frame) []terminal.ServerMessage {
		return append([]terminal.ServerMessage{{Type: terminal.TypeExecStarted}}, msgs...)
	}
}

// ---------------------------------------------------------------------------
// the status table (design §"API and CLI contract")
// ---------------------------------------------------------------------------

// TestExecNotFound: a session that does not exist, or that the caller cannot
// see, is 404 — the non-disclosing answer, the same one attach gives.
func TestExecNotFound(t *testing.T) {
	_, st, ts := newAttachControld(t)
	_, tok := loginUser(t, st, "alice", "member")
	c, resp, err := dialExec(t, ts, "sess_nope", tok)
	if err == nil {
		c.CloseNow()
		t.Fatal("exec on an unknown session was upgraded")
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	assertErrCode(t, resp, "not_found")
}

// TestExecForbidden: a principal the policy refuses the CONTROLLER question
// is refused outright. There is no reduced exec — the attach path's "admit as
// a viewer" rule has no meaning here — so this is a 403 and never a downgrade.
func TestExecForbidden(t *testing.T) {
	_, st, ts := newAttachControld(t)
	owner, _ := loginUser(t, st, "alice", "member")
	_, otherTok := loginUser(t, st, "bob", "member")
	seedSession(t, st, control.Session{ID: "sess_x", CreatorID: control.ActorID(owner.ID),
		State: control.StateRunning, RunnerID: "vm1"})

	c, resp, err := dialExec(t, ts, "sess_x", otherTok)
	if err == nil {
		c.CloseNow()
		t.Fatal("exec by another member was upgraded")
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	assertErrCode(t, resp, "forbidden")
}

// TestExecOnASessionThatIsNotRunning is 409 WITH THE STATE IN THE BODY, and
// no wait. Attach waits up to AttachWait because a person who just typed
// `rainier new` is legitimately early; exec's caller is a script that wants
// an answer now, and a suspended session is deliberately not resumed —
// resuming costs minutes and changes what the caller is billed for.
func TestExecOnASessionThatIsNotRunning(t *testing.T) {
	for _, state := range []control.SessionState{
		control.StateSuspendedWarm, control.StateSuspendedCold,
		control.StateQueued, control.StateFailed, control.StateDead,
	} {
		t.Run(string(state), func(t *testing.T) {
			// A generous AttachWait, so a route that WAITED would be caught
			// by this test taking it rather than by the assertion.
			_, st, ts := newAttachControld(t, func(c *Config) { c.AttachWait = 30 * time.Second })
			owner, tok := loginUser(t, st, "alice", "member")
			seedSession(t, st, control.Session{ID: "sess_x", CreatorID: control.ActorID(owner.ID),
				State: state, RunnerID: "vm1"})

			started := time.Now()
			c, resp, err := dialExec(t, ts, "sess_x", tok)
			if err == nil {
				c.CloseNow()
				t.Fatalf("exec on a %s session was upgraded", state)
			}
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d, want 409", resp.StatusCode)
			}
			if took := time.Since(started); took > 5*time.Second {
				t.Fatalf("exec waited %s for a session to start; it must not wait at all", took)
			}
			defer resp.Body.Close()
			var body struct {
				Error struct{ Code, Message string } `json:"error"`
				State string                         `json:"state"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != "session_not_running" {
				t.Fatalf("code = %q", body.Error.Code)
			}
			if body.State != string(state) {
				t.Fatalf("the 409 body carried state %q, want %q — a caller cannot decide "+
					"what to do without it", body.State, state)
			}
		})
	}
}

// TestExecWithAnUnreachableRunner is 503 rather than attach's 502: the runner
// is a dependency that is expected back, and 503 is the code a client retries
// on. A gateway that turns a 502 into a retry is guessing.
func TestExecWithAnUnreachableRunner(t *testing.T) {
	_, st, ts := newAttachControld(t)
	owner, tok := loginUser(t, st, "alice", "member")
	seedSession(t, st, control.Session{ID: "sess_x", CreatorID: control.ActorID(owner.ID),
		State: control.StateRunning, RunnerID: "vm1"})

	c, resp, err := dialExec(t, ts, "sess_x", tok)
	if err == nil {
		c.CloseNow()
		t.Fatal("exec with no runner connected was upgraded")
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	assertErrCode(t, resp, "runner_unreachable")
}

// TestExecReadinessStatusTable is the readiness half of the design's status
// table, every row, as a pure decision.
//
// The last row is here rather than against a live fleet on purpose: a runnerd
// built from this tree ALWAYS announces exec.v1 — it is a fact about the
// build, not a flag an operator sets — so "a runner that cannot forward an
// exec" is a state the in-process fixture cannot be put into without racing
// the announce that undoes it. The decision is what matters, and it is here;
// that the route asks it is covered by TestExecOnARunnerWithoutTheCapability
// being impossible to write and by the handler having exactly one call site.
func TestExecReadinessStatusTable(t *testing.T) {
	running := control.Session{ID: "sess_example", State: control.StateRunning, RunnerID: "vm1"}
	yes := func() bool { return true }
	no := func() bool { return false }

	for name, tc := range map[string]struct {
		row        control.Session
		hostExec   bool
		connected  bool
		supports   func() bool
		wantStatus int
		wantCode   string
		wantState  control.SessionState
	}{
		"running, connected, capable": {running, true, true, yes, 0, "", ""},
		"suspended": {control.Session{State: control.StateSuspendedWarm, RunnerID: "vm1"},
			true, true, yes, http.StatusConflict, "session_not_running", control.StateSuspendedWarm},
		"queued": {control.Session{State: control.StateQueued},
			true, true, yes, http.StatusConflict, "session_not_running", control.StateQueued},
		"failed": {control.Session{State: control.StateFailed, RunnerID: "vm1"},
			true, true, yes, http.StatusConflict, "session_not_running", control.StateFailed},
		"destroyed": {control.Session{State: control.StateDestroyed},
			true, true, yes, http.StatusConflict, "session_not_running", control.StateDestroyed},
		"runner gone": {running, true, false, yes,
			http.StatusServiceUnavailable, "runner_unreachable", ""},
		"runner cannot exec": {running, true, true, no,
			http.StatusNotImplemented, "exec_unsupported", ""},
		// A host that composed no exec plane at all. It answers 501 whatever
		// the session's state, because the refusal is about this server.
		"this host has no exec plane": {running, false, true, yes,
			http.StatusNotImplemented, "exec_unsupported", ""},
		"no exec plane, and the session is stopped too": {
			control.Session{State: control.StateSuspendedWarm, RunnerID: "vm1"},
			false, true, yes, http.StatusNotImplemented, "exec_unsupported", ""},
	} {
		t.Run(name, func(t *testing.T) {
			got, refused := execReadiness(tc.row, tc.hostExec, tc.connected, tc.supports)
			if tc.wantStatus == 0 {
				if refused {
					t.Fatalf("a ready session was refused %+v", got)
				}
				return
			}
			if !refused || got.status != tc.wantStatus || got.code != tc.wantCode {
				t.Fatalf("got (%+v, %v), want status %d code %q",
					got, refused, tc.wantStatus, tc.wantCode)
			}
			if got.state != tc.wantState {
				t.Fatalf("the refusal named state %q, want %q", got.state, tc.wantState)
			}
			// And a 409 renders the state into the BODY, which is the one
			// fact that makes "not running" actionable.
			rec := httptest.NewRecorder()
			got.write(rec)
			if rec.Code != tc.wantStatus {
				t.Fatalf("rendered %d, want %d", rec.Code, tc.wantStatus)
			}
			var body struct {
				Error struct{ Code, Message string } `json:"error"`
				State string                         `json:"state"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != tc.wantCode {
				t.Fatalf("body code = %q", body.Error.Code)
			}
			if tc.wantStatus == http.StatusConflict && body.State != string(tc.wantState) {
				t.Fatalf("the 409 body carried state %q, want %q", body.State, tc.wantState)
			}
		})
	}
}

// TestExecReadinessAsksTheStoreOnlyWhenItHasTo: the capability check is a
// store read, and a caller refused earlier must not pay for one.
func TestExecReadinessAsksTheStoreOnlyWhenItHasTo(t *testing.T) {
	asked := 0
	count := func() bool { asked++; return true }
	execReadiness(control.Session{State: control.StateSuspendedWarm}, true, true, count)
	execReadiness(control.Session{State: control.StateRunning, RunnerID: "vm1"}, true, false, count)
	execReadiness(control.Session{State: control.StateRunning, RunnerID: "vm1"}, false, true, count)
	if asked != 0 {
		t.Fatalf("the capability read ran %d times for callers refused before it", asked)
	}
	execReadiness(control.Session{State: control.StateRunning, RunnerID: "vm1"}, true, true, count)
	if asked != 1 {
		t.Fatalf("the capability read ran %d times, want once", asked)
	}
}

// ---------------------------------------------------------------------------
// end to end
// ---------------------------------------------------------------------------

// execFixture is attachFixture with an exec-aware sandbox: the whole plane in
// one process — controld over a memstore, a real runnerd holding a control
// conn, and a scripted sessiond answering exec opens.
type execFixture = attachFixture

func newExecFixture(t *testing.T) *execFixture {
	t.Helper()
	return newAttachFixture(t)
}

// TestExecEndToEnd is the whole exec plane in one process: a client's
// websocket at controld, paired with a real runnerd's outbound dial-back,
// spliced onto a scripted sandbox — asserting that the spec reaches the
// sandbox on the OPENING FRAME (never a URL), that the streams come back
// separated, and that the exit status is the answer.
func TestExecEndToEnd(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = scriptedExec(
		terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("to stdout")},
		terminal.ServerMessage{Type: terminal.TypeExecStderr, Data: []byte("to stderr")},
		terminal.ServerMessage{Type: terminal.TypeExecExit, ExitCode: 7},
	)

	cli, resp, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	defer cli.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("exec status = %d, want 101", resp.StatusCode)
	}
	cli.SetReadLimit(16 << 20)
	writeClient(t, cli, execStart("git", "status", "--short"))

	open := <-fx.sd.execs
	if open.Kind != runner.KindExec || open.Exec == nil || len(open.Exec.Argv) != 3 ||
		open.Exec.Argv[0] != "git" {
		t.Fatalf("the exec open reached the sandbox as %+v", open)
	}

	if m := readServer(t, cli); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first server message = %+v, want exec_started", m)
	}
	out := readServer(t, cli)
	errs := readServer(t, cli)
	exit := readServer(t, cli)
	if out.Type != terminal.TypeExecStdout || string(out.Data) != "to stdout" {
		t.Fatalf("stdout = %+v", out)
	}
	if errs.Type != terminal.TypeExecStderr || string(errs.Data) != "to stderr" {
		t.Fatalf("stderr = %+v", errs)
	}
	if exit.Type != terminal.TypeExecExit || exit.ExitCode != 7 {
		t.Fatalf("exit = %+v", exit)
	}

	// And the one audit record it leaves: the command NAME, and nothing else
	// of what was typed.
	events := fx.st.Events()
	var execEvents []control.Event
	for _, e := range events {
		if e.Action == control.ActionExec {
			execEvents = append(execEvents, e)
		}
	}
	if len(execEvents) != 1 {
		t.Fatalf("recorded %d exec events, want exactly one", len(execEvents))
	}
	if execEvents[0].Command != "git" {
		t.Fatalf("the audit event names %q, want the command name alone", execEvents[0].Command)
	}
	if execEvents[0].Resource.ID != fx.id {
		t.Fatalf("the audit event names session %q", execEvents[0].Resource.ID)
	}
}

// TestExecAgainstAnOldSandboxIsRefusedCleanly is the permanent row of the
// compatibility matrix. A session keeps the sessiond it booted with for as
// long as it lives, so a NEW plane will be holding sessions whose sandbox has
// never heard of exec — forever, not for a mixed-version week. Such a sandbox
// reads a FrameOpen whose Kind it does not know and opens a TERMINAL
// attachment, answering with a snapshot; the plane refuses on the missing
// exec_started, tells the caller in its own vocabulary, and forwards nothing.
func TestExecAgainstAnOldSandboxIsRefusedCleanly(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = nil // an old sessiond: every open is a terminal open

	cli, resp, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	defer cli.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("exec status = %d, want 101", resp.StatusCode)
	}
	writeClient(t, cli, execStart("git", "status"))
	// The caller is piping input the whole time, which is what a script does.
	writeClient(t, cli, terminal.ClientMessage{Type: "stdin", Data: []byte("rm -rf /\n")})

	m := readServer(t, cli)
	if m.Type != terminal.TypeExecError || m.Reason != terminal.ReasonUnsupported {
		t.Fatalf("the caller was told %+v, want exec_error unsupported", m)
	}
	if m.Data != nil {
		t.Fatalf("the agent's screen leaked to an exec caller: %q", m.Data)
	}
	// The socket closes, and the sandbox was never sent a byte of stdin.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Read(ctx); err == nil {
		t.Fatal("the exec socket stayed open after exec_unsupported")
	}
	// allClient, not execClient, and the difference is the whole assertion.
	// This test is the OLD sandbox, so it scripts no exec reply — which means
	// execIDs is never populated and a leaked frame routes into the fake's
	// TERMINAL branch, where nothing on execClient could ever see it.
	select {
	case got := <-fx.sd.allClient:
		t.Fatalf("a client frame reached a sandbox that never said exec_started: %+v", got)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestExecDoesNotTouchTheTerminalLease is the composition rule with
// conditional controller ownership, through the real plane: an exec while
// another device holds the lease works, and leaves the controller generation
// and the lease holder exactly where it found them.
func TestExecDoesNotTouchTheTerminalLease(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = scriptedExec(terminal.ServerMessage{Type: terminal.TypeExecExit})

	// A controller is attached and holds the lease.
	term, _, err := dialAttach(t, fx.ts, fx.id, "?control=v1&mode=control", fx.tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer term.CloseNow()
	writeClient(t, term, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	if m := readServer(t, term); m.Type != terminal.TypeAttached || m.Mode != terminal.ModeControl {
		t.Fatalf("the terminal did not take control: %+v", m)
	}
	fx.sd.nextOpen(t)
	if m := readServer(t, term); m.Type != "snapshot" {
		t.Fatalf("the terminal opened with %+v, want its snapshot", m)
	}

	before, err := fx.st.Sessions().GetSession(context.Background(),
		installWorkspace, control.SessionID(fx.id))
	if err != nil {
		t.Fatal(err)
	}

	// An exec runs beside it, start to finish.
	cli, _, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, execStart("true"))
	if m := readServer(t, cli); m.Type != terminal.TypeExecStarted {
		t.Fatalf("exec opened with %+v", m)
	}
	if m := readServer(t, cli); m.Type != terminal.TypeExecExit {
		t.Fatalf("exec ended with %+v", m)
	}

	after, err := fx.st.Sessions().GetSession(context.Background(),
		installWorkspace, control.SessionID(fx.id))
	if err != nil {
		t.Fatal(err)
	}
	if after.ControllerGeneration != before.ControllerGeneration {
		t.Fatalf("an exec moved the controller generation from %d to %d",
			before.ControllerGeneration, after.ControllerGeneration)
	}
	if control.ControllerLeaseOf(after).Holder != control.ControllerLeaseOf(before).Holder {
		t.Fatal("an exec changed who holds the controller lease")
	}

	// And the terminal still types, which is the thing the generation
	// protects.
	writeClient(t, term, terminal.ClientMessage{Type: "stdin", Data: []byte("hi"),
		Generation: terminal.GenOf(after.ControllerGeneration)})
	if m := readServer(t, term); m.Type != "output" || string(m.Data) != "hi" {
		t.Fatalf("the controller lost its terminal to an exec: %+v", m)
	}
}

// TestExecForwardsTheCallersInput end to end, and only the caller's input:
// the ownership vocabulary is dropped at the plane, so a `claim` on an exec
// reaches nothing.
func TestExecForwardsTheCallersInput(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = scriptedExec()

	cli, _, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, execStart("cat"))
	if m := readServer(t, cli); m.Type != terminal.TypeExecStarted {
		t.Fatalf("exec opened with %+v", m)
	}
	writeClient(t, cli, terminal.ClientMessage{Type: terminal.TypeClaim,
		Expected: terminal.GenOf(1)})
	writeClient(t, cli, terminal.ClientMessage{Type: "stdin", Data: []byte("marker")})
	writeClient(t, cli, terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})

	select {
	case m := <-fx.sd.execClient:
		if m.Type != "stdin" || string(m.Data) != "marker" {
			t.Fatalf("the sandbox received %+v first; a claim was forwarded", m)
		}
		if m.Generation != "" {
			t.Fatalf("the plane stamped an exec frame with %q", m.Generation)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing reached the sandbox")
	}
	select {
	case m := <-fx.sd.execClient:
		if m.Type != terminal.TypeExecStdinEOF {
			t.Fatalf("the sandbox received %+v, want the stdin EOF", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stdin EOF never reached the sandbox")
	}
}

// TestExecRequiresTheOpeningExecStart: a client that opens with anything else
// is closed with the same policy violation an attach that skips its resize
// gets, and nothing is ever asked of the runner.
func TestExecRequiresTheOpeningExecStart(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = scriptedExec()

	cli, _, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, terminal.ClientMessage{Type: "stdin", Data: []byte("no spec")})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Read(ctx); err == nil {
		t.Fatal("an exec that opened with stdin was not closed")
	}
	select {
	case f := <-fx.sd.execs:
		t.Fatalf("an exec with no spec reached the sandbox: %+v", f)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestExecRefusesAMalformedSpecBeforeTheRunner: the plane shape-checks what
// it can without a filesystem, so a cwd outside the workspace never costs a
// dial-back.
func TestExecRefusesAMalformedSpecBeforeTheRunner(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = scriptedExec()

	cli, _, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, terminal.ClientMessage{Type: terminal.TypeExecStart,
		Exec: &runner.ExecSpec{Argv: []string{"git"}, Cwd: "/etc"}})

	// NAMED, not just refused: the socket is already upgraded, so a status
	// code has nowhere to go, and a caller closed without a word would report
	// "the connection ended" for a cwd it could have fixed.
	if m := readServer(t, cli); m.Type != terminal.TypeExecError ||
		m.Reason != terminal.ReasonCwdRefused {
		t.Fatalf("the caller was told %+v, want exec_error cwd_refused", m)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Read(ctx); err == nil {
		t.Fatal("an exec with a cwd outside the workspace was not closed")
	}
	select {
	case f := <-fx.sd.execs:
		t.Fatalf("a refused spec reached the sandbox: %+v", f)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestExecRouteIsNotAnAttachParameter is the negative half of "its own
// route": `attach?kind=exec` must NOT run a command. An old plane would have
// ignored the parameter and opened a controller attachment — a take-over,
// displacing whoever was at the keyboard, with the command never running.
func TestExecRouteIsNotAnAttachParameter(t *testing.T) {
	fx := newExecFixture(t)
	fx.sd.execReply = scriptedExec()

	cli, _, err := dialAttach(t, fx.ts, fx.id, "?kind=exec", fx.tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	open := fx.sd.nextOpen(t)
	if open.Kind != runner.KindTerminal || open.Exec != nil {
		t.Fatalf("attach?kind=exec opened %+v; the parameter must mean nothing", open)
	}
}

// TestRunnerSupportsExecIsAPreCheckThatFailsOpen pins the capability rule as a
// unit, including the two ways it is deliberately WRONG in the permissive
// direction. A store that cannot answer, or a runner with no row yet, yields
// "supported" — because the sandbox's own exec_started is the authoritative
// fence one round trip later, and refusing on a failed store read would 501 a
// perfectly good exec because the database blinked.
func TestRunnerSupportsExecIsAPreCheckThatFailsOpen(t *testing.T) {
	s, st, _ := newAttachControld(t)
	ctx := context.Background()

	seed := func(id string, caps []string) {
		t.Helper()
		if err := st.Fleet().UpsertRunner(ctx, installPool, control.Runner{
			ID: control.RunnerID(id), PoolID: installPool, Connected: true,
			CapacityTotal: 4, Capabilities: caps}); err != nil {
			t.Fatal(err)
		}
	}
	seed("vm-new", []string{"gpu", runner.CapabilityExecV1})
	seed("vm-old", []string{"gpu"})
	seed("vm-bare", nil)

	for id, want := range map[string]bool{
		"vm-new":  true,
		"vm-old":  false,
		"vm-bare": false,
		// No row at all: the permissive answer, on purpose.
		"vm-unknown": true,
	} {
		if got := s.runnerSupportsExec(ctx, control.RunnerID(id)); got != want {
			t.Fatalf("runnerSupportsExec(%s) = %v, want %v", id, got, want)
		}
	}
}

// TestExecOnARunnerWithoutTheCapabilityIs501 pins the last row of the status
// table ON THE ROUTE.
//
// It was pinned only as a pure decision, and the comment above that table said
// why: a runnerd built from this tree always announces exec.v1, so the
// in-process fixture cannot be put into the state. A fake runner can — it is
// what an operator's older runnerd is — and with it, mutating
// runnerSupportsExec to `return true` stops leaving the suite green.
func TestExecOnARunnerWithoutTheCapabilityIs501(t *testing.T) {
	_, st, ts := newAttachControld(t)
	owner, tok := loginUser(t, st, "alice", "member")
	// A connected runner that announces everything EXCEPT exec.v1 — an
	// operator's runnerd from before the roll.
	startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4, Capabilities: []string{"gpu"}})
	waitRunnerRow(t, st, "vm1", func(r control.Runner) bool { return r.Connected })
	seedSession(t, st, control.Session{ID: "sess_x", CreatorID: control.ActorID(owner.ID),
		State: control.StateRunning, RunnerID: "vm1"})

	c, resp, err := dialExec(t, ts, "sess_x", tok)
	if err == nil {
		c.CloseNow()
		t.Fatal("exec on a runner that cannot forward one was upgraded")
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
	assertErrCode(t, resp, "exec_unsupported")
}

// waitRunnerRow polls the fleet until the named runner's row satisfies cond.
func waitRunnerRow(t *testing.T, st MemStore, name string, cond func(control.Runner) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := st.Fleet().ListRunners(context.Background(), installPool)
		if err == nil {
			for _, r := range rows {
				if string(r.ID) == name && cond(r) {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("runner %s never reached the expected row state", name)
}
