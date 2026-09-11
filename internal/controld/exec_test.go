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

// TestExecOnARunnerWithoutTheCapability is 501: the runner announced no
// `exec.v1`, so the plane refuses early rather than spending a dial-back to
// find out. It is a pre-check and not the fence — the sandbox's own
// exec_started is that — but it turns a close reason into a status code.
func TestExecOnARunnerWithoutTheCapability(t *testing.T) {
	fx := newExecFixture(t)
	// Re-announce the runner without the capability, as a runnerd older than
	// exec would have.
	rows, err := fx.st.Fleet().ListRunners(context.Background(), installPool)
	if err != nil {
		t.Fatal(err)
	}
	var row control.Runner
	for _, r := range rows {
		if r.ID == "vm1" {
			row = r
		}
	}
	row.Capabilities = nil
	row.Generation++
	if err := fx.st.Fleet().UpsertRunner(context.Background(), installPool, row); err != nil {
		t.Fatal(err)
	}

	c, resp, err := dialExec(t, fx.ts, fx.id, fx.tok)
	if err == nil {
		c.CloseNow()
		t.Fatal("exec on a runner without exec.v1 was upgraded")
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
	assertErrCode(t, resp, "exec_unsupported")
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
	select {
	case got := <-fx.sd.execClient:
		t.Fatalf("stdin reached a sandbox that never said exec_started: %+v", got)
	case <-time.After(200 * time.Millisecond):
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
