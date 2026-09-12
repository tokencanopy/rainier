package execio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// the exit-code policy
// ---------------------------------------------------------------------------

// TestThePublishedExitCodesAreWhatIsPublished. §6.1 of the command contract
// publishes these three numbers and a script's `if` is written against them,
// so they are LITERALS here rather than the constants under test: 126 and 127
// are saved by the end-to-end suite, and 125 — the one a caller sees whenever
// a session ends under its command — was pinned nowhere at all.
func TestThePublishedExitCodesAreWhatIsPublished(t *testing.T) {
	if ExitNoStatus != 125 {
		t.Fatalf("ExitNoStatus = %d, want 125: accepted, but no exit status ever "+
			"arrived", ExitNoStatus)
	}
	if ExitNotExecutable != 126 {
		t.Fatalf("ExitNotExecutable = %d, want 126", ExitNotExecutable)
	}
	if ExitNotFound != 127 {
		t.Fatalf("ExitNotFound = %d, want 127", ExitNotFound)
	}
}

// TestExitCodeFor is the whole contract as a pure function: the command's
// status IS the CLI's status, and Rainier's own failures use the codes the
// shell vocabulary already reserves for a wrapper.
func TestExitCodeFor(t *testing.T) {
	code := func(n int) *int { return &n }
	for name, tc := range map[string]struct {
		res     Result
		want    int
		decided bool
	}{
		"exit 0":   {Result{Started: true, ExitCode: code(0)}, 0, true},
		"exit 1":   {Result{Started: true, ExitCode: code(1)}, 1, true},
		"exit 7":   {Result{Started: true, ExitCode: code(7)}, 7, true},
		"exit 126": {Result{Started: true, ExitCode: code(126)}, 126, true},
		"exit 255": {Result{Started: true, ExitCode: code(255)}, 255, true},

		"killed by TERM":       {Result{Started: true, Signal: "TERM"}, 143, true},
		"killed by KILL":       {Result{Started: true, Signal: "KILL"}, 137, true},
		"killed by INT":        {Result{Started: true, Signal: "INT"}, 130, true},
		"killed by SEGV":       {Result{Started: true, Signal: "SEGV"}, 139, true},
		"a numbered signal":    {Result{Started: true, Signal: "64"}, 192, true},
		"an unnameable signal": {Result{Started: true, Signal: "WHAT"}, ExitNoStatus, true},

		"no status ever arrived": {Result{Started: true}, ExitNoStatus, true},

		"not found":      {Result{Reason: terminal.ReasonNotFound}, ExitNotFound, true},
		"not executable": {Result{Reason: terminal.ReasonNotExecutable}, ExitNotExecutable, true},
		"cwd refused":    {Result{Reason: terminal.ReasonCwdRefused}, ExitNotExecutable, true},
		"env refused":    {Result{Reason: terminal.ReasonEnvRefused}, ExitNotExecutable, true},
		"log refused":    {Result{Reason: terminal.ReasonLogRefused}, ExitNotExecutable, true},

		"unsupported":    {Result{Reason: terminal.ReasonUnsupported}, 1, false},
		"too many execs": {Result{Reason: terminal.ReasonTooManyExecs}, 1, false},
		"never started":  {Result{}, 1, false},

		"detached": {Result{Started: true, Detached: true, PID: 4321}, 0, true},
	} {
		t.Run(name, func(t *testing.T) {
			got, decided := ExitCodeFor(tc.res)
			if got != tc.want || decided != tc.decided {
				t.Fatalf("ExitCodeFor(%+v) = (%d, %v), want (%d, %v)",
					tc.res, got, decided, tc.want, tc.decided)
			}
		})
	}
}

// TestExitCodeForSignalMatchesTheShell: 128+N, with N the signal's real
// number, which is what a shell reports and what a script comparing against
// 137 is written for.
func TestExitCodeForSignalMatchesTheShell(t *testing.T) {
	for name, want := range map[string]int{
		"HUP": 129, "INT": 130, "QUIT": 131, "KILL": 137, "PIPE": 141, "TERM": 143,
	} {
		got, _ := ExitCodeFor(Result{Started: true, Signal: name})
		if got != want {
			t.Fatalf("a %s exits %d, want %d", name, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// the loop, over a real socket
// ---------------------------------------------------------------------------

// fakePlane is a websocket server standing in for the exec route: it accepts
// the opening exec_start, hands it to a script, and speaks whatever that
// script returns.
type fakePlane struct {
	*httptest.Server

	mu     sync.Mutex
	start  terminal.ClientMessage
	client []terminal.ClientMessage
}

func newFakePlane(t *testing.T, serve func(p *fakePlane, ctx context.Context, c *websocket.Conn)) *fakePlane {
	t.Helper()
	p := &fakePlane{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(readLimit)
		ctx := r.Context()
		var first terminal.ClientMessage
		if wsjson.Read(ctx, c, &first) != nil {
			return
		}
		p.mu.Lock()
		p.start = first
		p.mu.Unlock()
		serve(p, ctx, c)
	}))
	t.Cleanup(p.Close)
	return p
}

func (p *fakePlane) wsURL() string { return "ws" + strings.TrimPrefix(p.URL, "http") }

func (p *fakePlane) openingSpec(t *testing.T) runner.ExecSpec {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.start.Exec == nil {
		t.Fatal("no exec_start reached the plane")
	}
	return *p.start.Exec
}

func (p *fakePlane) record(m terminal.ClientMessage) {
	p.mu.Lock()
	p.client = append(p.client, m)
	p.mu.Unlock()
}

func (p *fakePlane) received() []terminal.ClientMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]terminal.ClientMessage(nil), p.client...)
}

func send(t *testing.T, ctx context.Context, c *websocket.Conn, m terminal.ServerMessage) {
	t.Helper()
	if err := wsjson.Write(ctx, c, m); err != nil {
		t.Errorf("plane write: %v", err)
	}
}

// TestRunSendsTheSpecInTheFirstMessage — never on the URL, because a URL is
// written to the access log of every proxy between a caller and a cell.
func TestRunSendsTheSpecInTheFirstMessage(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit})
	})
	spec := runner.ExecSpec{Argv: []string{"git", "status"}, Cwd: "app",
		Env: map[string]string{"CI": "1"}, TTY: true, Cols: 100, Rows: 40}
	res, err := Run(context.Background(), p.wsURL(), Options{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Started || res.ExitCode == nil || *res.ExitCode != 0 {
		t.Fatalf("result = %+v", res)
	}
	got := p.openingSpec(t)
	if len(got.Argv) != 2 || got.Cwd != "app" || got.Env["CI"] != "1" ||
		!got.TTY || got.Cols != 100 || got.Rows != 40 {
		t.Fatalf("the spec arrived as %+v", got)
	}
}

// TestRunSeparatesTheStreams is the rule the shell redirection depends on:
// the command's stdout is the caller's stdout, byte for byte, with nothing
// added and no trailing newline invented.
func TestRunSeparatesTheStreams(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("out")})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStderr, Data: []byte("err")})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("put")})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit, ExitCode: 7})
	})
	var out, errs bytes.Buffer
	res, err := Run(context.Background(), p.wsURL(), Options{
		Spec: runner.ExecSpec{Argv: []string{"x"}}, Stdout: &out, Stderr: &errs})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "output" {
		t.Fatalf("stdout = %q, want %q", out.String(), "output")
	}
	if errs.String() != "err" {
		t.Fatalf("stderr = %q, want %q", errs.String(), "err")
	}
	if res.ExitCode == nil || *res.ExitCode != 7 {
		t.Fatalf("exit = %+v", res)
	}
}

// TestRunWithNoStdinSaysSoAtOnce: a command that reads until EOF has to
// terminate, and a caller with no input was never going to type.
func TestRunWithNoStdinSaysSoAtOnce(t *testing.T) {
	done := make(chan struct{})
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		var m terminal.ClientMessage
		if wsjson.Read(ctx, c, &m) == nil {
			p.record(m)
		}
		close(done)
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit})
	})
	if _, err := Run(context.Background(), p.wsURL(), Options{
		Spec: runner.ExecSpec{Argv: []string{"cat"}}}); err != nil {
		t.Fatal(err)
	}
	<-done
	got := p.received()
	if len(got) != 1 || got[0].Type != terminal.TypeExecStdinEOF {
		t.Fatalf("a caller with no stdin sent %+v, want one exec_stdin_eof", got)
	}
}

// TestRunForwardsStdinAndThenItsEOF, from a real file, which is what a pipe
// is: `rainier exec s -- cat > f < input` has to terminate.
func TestRunForwardsStdinAndThenItsEOF(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/input"
	if err := os.WriteFile(path, []byte("piped input"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	done := make(chan struct{})
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		for {
			var m terminal.ClientMessage
			if wsjson.Read(ctx, c, &m) != nil {
				break
			}
			p.record(m)
			if m.Type == terminal.TypeExecStdinEOF {
				break
			}
		}
		close(done)
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit})
	})
	if _, err := Run(context.Background(), p.wsURL(), Options{
		Spec: runner.ExecSpec{Argv: []string{"cat"}}, Stdin: f}); err != nil {
		t.Fatal(err)
	}
	<-done
	var body bytes.Buffer
	var sawEOF bool
	for _, m := range p.received() {
		switch m.Type {
		case "stdin":
			body.Write(m.Data)
		case terminal.TypeExecStdinEOF:
			sawEOF = true
		}
	}
	if body.String() != "piped input" || !sawEOF {
		t.Fatalf("stdin = %q eof=%v", body.String(), sawEOF)
	}
}

// TestRunReportsASignalRatherThanACode, which is what makes 128+N possible.
func TestRunReportsASignalRatherThanACode(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit, Signal: "KILL"})
	})
	res, err := Run(context.Background(), p.wsURL(), Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Signal != "KILL" || res.ExitCode != nil {
		t.Fatalf("result = %+v", res)
	}
	if code, _ := ExitCodeFor(res); code != 137 {
		t.Fatalf("exit code = %d, want 137", code)
	}
}

// TestRunReportsNoStatusWhenTheConnectionDies is the 125 path: the command
// may well have done everything it was asked, and nothing here can tell.
func TestRunReportsNoStatusWhenTheConnectionDies(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("half")})
		c.CloseNow()
	})
	var out bytes.Buffer
	res, err := Run(context.Background(), p.wsURL(), Options{
		Spec: runner.ExecSpec{Argv: []string{"x"}}, Stdout: &out})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Started || res.ExitCode != nil || res.Signal != "" {
		t.Fatalf("result = %+v", res)
	}
	if code, ok := ExitCodeFor(res); code != ExitNoStatus || !ok {
		t.Fatalf("exit = (%d, %v), want (125, true)", code, ok)
	}
	if out.String() != "half" {
		t.Fatalf("the output before the death was lost: %q", out.String())
	}
}

// TestRunReportsAnExecError, and the reason with it, so the caller maps it to
// the right code and the right sentence.
func TestRunReportsAnExecError(t *testing.T) {
	for _, reason := range []string{
		terminal.ReasonUnsupported, terminal.ReasonNotFound, terminal.ReasonNotExecutable,
		terminal.ReasonCwdRefused, terminal.ReasonEnvRefused, terminal.ReasonLogRefused,
		terminal.ReasonTooManyExecs,
	} {
		t.Run(reason, func(t *testing.T) {
			p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
				send(t, ctx, c, terminal.ServerMessage{
					Type: terminal.TypeExecError, Reason: reason})
			})
			res, err := Run(context.Background(), p.wsURL(),
				Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
			if err != nil {
				t.Fatal(err)
			}
			if res.Reason != reason || res.Started {
				t.Fatalf("result = %+v", res)
			}
		})
	}
}

// TestRunOnADetachedExecReturnsThePidAndStops: a detached exec has nothing to
// stream and nothing to wait for.
func TestRunOnADetachedExecReturnsThePidAndStops(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted, PID: 4321})
		<-ctx.Done()
	})
	spec := runner.ExecSpec{Argv: []string{"claude", "--continue"}, Detach: true, LogPath: "run.log"}
	done := make(chan Result, 1)
	go func() {
		res, err := Run(context.Background(), p.wsURL(), Options{Spec: spec})
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()
	select {
	case res := <-done:
		if !res.Started || res.PID != 4321 || !res.Detached {
			t.Fatalf("result = %+v", res)
		}
		if code, ok := ExitCodeFor(res); code != 0 || !ok {
			t.Fatalf("a detached exec exits (%d, %v), want (0, true)", code, ok)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a detached exec waited for an exit that was never coming")
	}
}

// ---------------------------------------------------------------------------
// refused upgrades
// ---------------------------------------------------------------------------

// TestRunReadsTheErrorEnvelopeOffARefusedUpgrade, including the state on a
// 409 — the one fact that makes "not running" actionable.
func TestRunReadsTheErrorEnvelopeOffARefusedUpgrade(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   any
		code   string
		state  string
	}{
		"404": {http.StatusNotFound, map[string]any{
			"error": map[string]string{"code": "not_found", "message": "session not found"}},
			"not_found", ""},
		"403": {http.StatusForbidden, map[string]any{
			"error": map[string]string{"code": "forbidden", "message": "not authorized"}},
			"forbidden", ""},
		"409": {http.StatusConflict, map[string]any{
			"error": map[string]string{"code": "session_not_running", "message": "session is suspended_warm, not running"},
			"state": "suspended_warm"},
			"session_not_running", "suspended_warm"},
		"501": {http.StatusNotImplemented, map[string]any{
			"error": map[string]string{"code": "exec_unsupported", "message": "no"}},
			"exec_unsupported", ""},
		"503": {http.StatusServiceUnavailable, map[string]any{
			"error": map[string]string{"code": "runner_unreachable", "message": "no"}},
			"runner_unreachable", ""},
		"a 404 with no envelope at all": {http.StatusNotFound, nil, "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				if tc.body != nil {
					_ = json.NewEncoder(w).Encode(tc.body)
				}
			}))
			defer srv.Close()
			_, err := Run(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"),
				Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
			var de *DialError
			if !errors.As(err, &de) {
				t.Fatalf("got %v, want a DialError", err)
			}
			if de.Status != tc.status || de.Code != tc.code || de.State != tc.state {
				t.Fatalf("DialError = %+v, want status %d code %q state %q",
					de, tc.status, tc.code, tc.state)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// a sandbox that lies
// ---------------------------------------------------------------------------

// TestASandboxCannotReportASuccessItDidNotHave is the range check on the one
// number a sandbox supplies that the CLI turns straight into its own exit
// status. os.Exit masks to eight bits, so a sandbox reporting 256 would make
// `rainier exec` exit 0 — telling a script a failing command succeeded, which
// is the single worst thing this command could get wrong.
func TestASandboxCannotReportASuccessItDidNotHave(t *testing.T) {
	for _, reported := range []int{256, 512, 300, -1, -256, 1 << 30} {
		t.Run(fmt.Sprint(reported), func(t *testing.T) {
			p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
				send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
				send(t, ctx, c, terminal.ServerMessage{
					Type: terminal.TypeExecExit, ExitCode: reported})
			})
			res, err := Run(context.Background(), p.wsURL(),
				Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
			if err != nil {
				t.Fatal(err)
			}
			if res.ExitCode != nil {
				t.Fatalf("a reported status of %d was accepted as %d", reported, *res.ExitCode)
			}
			code, ok := ExitCodeFor(res)
			if code != ExitNoStatus || !ok {
				t.Fatalf("exit = (%d, %v), want (125, true) — never 0", code, ok)
			}
		})
	}

	// And the whole legal range still passes through verbatim.
	for _, reported := range []int{0, 1, 125, 126, 127, 128, 254, 255} {
		p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
			send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
			send(t, ctx, c, terminal.ServerMessage{
				Type: terminal.TypeExecExit, ExitCode: reported})
		})
		res, err := Run(context.Background(), p.wsURL(),
			Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
		if err != nil {
			t.Fatal(err)
		}
		if code, _ := ExitCodeFor(res); code != reported {
			t.Fatalf("a reported status of %d exited %d", reported, code)
		}
	}
}

// TestASecondExecStartedIsIgnored: a sandbox that says it started twice does
// not get to move the clock a second time, or to re-arm the input pumps.
func TestASecondExecStartedIsIgnored(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted, PID: 1})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted, PID: 999})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit, ExitCode: 3})
	})
	res, err := Run(context.Background(), p.wsURL(),
		Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.PID != 1 {
		t.Fatalf("a second exec_started rewrote the pid to %d", res.PID)
	}
	if res.ExitCode == nil || *res.ExitCode != 3 {
		t.Fatalf("result = %+v", res)
	}
}

// TestOutputAfterTheExitIsNotWaitedFor: the exit is the end of the exec, so a
// sandbox that keeps talking afterwards cannot hold the caller.
func TestOutputAfterTheExitIsNotWaitedFor(t *testing.T) {
	p := newFakePlane(t, func(p *fakePlane, ctx context.Context, c *websocket.Conn) {
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		send(t, ctx, c, terminal.ServerMessage{Type: terminal.TypeExecExit, ExitCode: 0})
		<-ctx.Done() // and never closes
	})
	done := make(chan Result, 1)
	go func() {
		res, err := Run(context.Background(), p.wsURL(),
			Options{Spec: runner.ExecSpec{Argv: []string{"x"}}})
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()
	select {
	case res := <-done:
		if res.ExitCode == nil || *res.ExitCode != 0 {
			t.Fatalf("result = %+v", res)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the caller waited past the exit status it already had")
	}
}

// TestADoubleInterruptIsADecidedOutcome pins the Ctrl-C policy's second half.
// A caller that pressed Ctrl-C twice and left killed the command; 130 is what
// a shell reports for a command an interrupt ended, and it is the honest
// answer — the status is missing because this caller stopped waiting for it,
// not because anything went wrong with the connection.
func TestADoubleInterruptIsADecidedOutcome(t *testing.T) {
	code, ok := ExitCodeFor(Result{Started: true, Interrupted: true})
	if !ok || code != 130 {
		t.Fatalf("an interrupted exec exits (%d, %v), want (130, true)", code, ok)
	}
	// A command that exited on its own before the second press still reports
	// its own status: the interrupt only decides when nothing else did.
	seven := 7
	if got, _ := ExitCodeFor(Result{Started: true, Interrupted: true, ExitCode: &seven}); got != 7 {
		t.Fatalf("an interrupted exec that DID report exited %d, want 7", got)
	}
	if got, _ := ExitCodeFor(Result{Started: true, Interrupted: true, Signal: "KILL"}); got != 137 {
		t.Fatalf("an interrupted exec that was signalled exited %d, want 137", got)
	}
}

// TestRainiersOwnRefusalsNeverDecideTheExitCode: every reason that is
// Rainier's failure rather than the command's is reported as such, so the
// caller prints a sentence instead of inventing a status.
func TestRainiersOwnRefusalsNeverDecideTheExitCode(t *testing.T) {
	for _, reason := range []string{
		terminal.ReasonUnsupported, terminal.ReasonTooManyExecs,
		terminal.ReasonNoAnswer, terminal.ReasonStdinOverrun,
	} {
		code, ok := ExitCodeFor(Result{Reason: reason})
		if ok || code != 1 {
			t.Fatalf("%s = (%d, %v), want (1, false)", reason, code, ok)
		}
	}
}
