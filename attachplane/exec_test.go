package attachplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// These tests drive the exec half of the plane through the same two public
// surfaces the attach half uses — the broker the application calls and the
// dial-back handler the runner dials — with a scripted sandbox on the far end
// of a real websocket.

func execSpecOf(argv ...string) runner.ExecSpec { return runner.ExecSpec{Argv: argv} }

// runExec parks one exec through the broker with a scripted sandbox behind
// it, and returns the client stream so the test can read what arrived.
func runExec(t *testing.T, spec runner.ExecSpec, serve func(conn relay.Conn)) (
	*scriptedStream, chan *runner.Attach, chan struct{}) {
	t.Helper()
	p, h, ts := newTestPlane(t, Options{})
	attaches := make(chan *runner.Attach, 4)
	served := make(chan struct{})
	h.dialBack = func(at *runner.Attach) {
		attaches <- at
		c, _, err := dialAttachBack(t, ts, at.AttachID, testRunnerToken)
		if err != nil {
			close(served)
			return
		}
		defer c.CloseNow()
		// The same read limit runnerd's own dial-back sets. Without it
		// coder/websocket's 32 KiB default closes the socket on the first
		// frame bigger than that, which is every frame this hop is supposed
		// to carry.
		c.SetReadLimit(attachReadLimit)
		serve(relay.WSConn(c))
		close(served)
	}
	stream := newScriptedStream()
	go func() {
		_ = p.ExecBroker().Exec(context.Background(), brokerTarget("sess_test", "vm1"), spec, stream)
	}()
	return stream, attaches, served
}

// sandboxSend writes one server message onto the sandbox's end of the splice.
func sandboxSend(t *testing.T, conn relay.Conn, m terminal.ServerMessage) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testDeadline)
	defer cancel()
	if err := conn.Write(ctx, raw); err != nil {
		t.Errorf("sandbox write: %v", err)
	}
}

// sandboxRead reads one client message off the sandbox's end.
func sandboxRead(t *testing.T, conn relay.Conn) (terminal.ClientMessage, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testDeadline)
	defer cancel()
	raw, err := conn.Read(ctx)
	if err != nil {
		return terminal.ClientMessage{}, err
	}
	var m terminal.ClientMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("sandbox read: %v", err)
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// the dial_attach
// ---------------------------------------------------------------------------

// TestExecDialAttachCarriesTheKindAndTheSpec, and no binding: an exec
// attachment holds no controller lease, so nothing about ownership rides the
// command that opens it.
func TestExecDialAttachCarriesTheKindAndTheSpec(t *testing.T) {
	spec := runner.ExecSpec{Argv: []string{"git", "status"}, Cwd: "/workspace/app",
		TTY: true, Cols: 100, Rows: 40}
	stream, attaches, _ := runExec(t, spec, func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecExit})
	})
	at := <-attaches
	if at.Kind != runner.KindExec || at.Exec == nil {
		t.Fatalf("the dial_attach was %+v, want the exec kind", at)
	}
	if len(at.Exec.Argv) != 2 || at.Exec.Cwd != "/workspace/app" || !at.Exec.TTY {
		t.Fatalf("the spec arrived as %+v", at.Exec)
	}
	if at.Cols != 100 || at.Rows != 40 {
		t.Fatalf("the pty size did not reach the runner: %dx%d", at.Cols, at.Rows)
	}
	if at.Mode != "" || at.Generation != 0 {
		t.Fatalf("an exec dial_attach carried a controller binding: mode=%q gen=%d",
			at.Mode, at.Generation)
	}
	if at.Since != 0 {
		t.Fatalf("an exec dial_attach carried an attach cursor: %d", at.Since)
	}
	stream.nextServerMsg(t) // exec_started
}

// ---------------------------------------------------------------------------
// the handshake
// ---------------------------------------------------------------------------

// TestExecRequiresExecStarted is the fence for the permanent compatibility
// case: a session keeps the sessiond it booted with for life, so an old
// sandbox reads a FrameOpen whose Kind it does not know and opens a TERMINAL
// attachment, answering with a snapshot. That must be refused, and refused
// before one byte of the caller's stdin is forwarded.
func TestExecRequiresExecStarted(t *testing.T) {
	for name, first := range map[string]terminal.ServerMessage{
		"a snapshot from an old sandbox": {Type: "snapshot", Cols: 80, Rows: 24,
			Data: []byte("the agent's screen")},
		"output":                      {Type: "output", Seq: 1, Data: []byte("hi")},
		"an exit":                     {Type: "exit", ExitCode: 0},
		"an attached":                 {Type: terminal.TypeAttached, Mode: terminal.ModeControl},
		"stdout before the handshake": {Type: terminal.TypeExecStdout, Data: []byte("x")},
	} {
		t.Run(name, func(t *testing.T) {
			sawStdin := make(chan struct{}, 1)
			stream, _, served := runExec(t, execSpecOf("git", "status"), func(conn relay.Conn) {
				sandboxSend(t, conn, first)
				// If the plane forwarded anything, it would arrive here.
				if _, err := sandboxRead(t, conn); err == nil {
					sawStdin <- struct{}{}
				}
			})
			// The caller is typing the whole time, which is the shape a
			// script piping into an exec has.
			stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("rm -rf /\n")}

			m := stream.nextServerMsg(t)
			if m.Type != terminal.TypeExecError || m.Reason != terminal.ReasonUnsupported {
				t.Fatalf("the client was told %+v, want exec_error unsupported", m)
			}
			if err := stream.closeReason(t); !errors.Is(err, errExecUnsupported) {
				t.Fatalf("closed with %v, want errExecUnsupported", err)
			}
			<-served
			select {
			case <-sawStdin:
				t.Fatal("stdin was forwarded to a sandbox that never said exec_started")
			default:
			}
		})
	}
}

// TestExecRefusesASilentSandbox: a sandbox that answers nothing at all is
// refused too, bounded rather than left open — but with a DIFFERENT word.
//
// "Unsupported" means "this session was created before exec shipped", which
// is permanent and sends a caller to make a new session. A sandbox that was
// merely slow, or that died between the dial-back and its first frame, is
// none of those things, and saying so would be a false statement a caller
// would act on.
func TestExecRefusesASilentSandbox(t *testing.T) {
	// ExecHandshakeTimeout, not ControlAckTimeout. The latter only bounds the
	// exec_error SEND, so passing it here looked like configuring the budget
	// and configured nothing — this test burned the full production ten
	// seconds, ten times the next slowest in the package and 66s at -count=5.
	started := time.Now()
	p, h, ts := newTestPlane(t, Options{
		ControlAckTimeout:    150 * time.Millisecond,
		ExecHandshakeTimeout: 150 * time.Millisecond,
	})
	hold := make(chan struct{})
	h.dialBack = func(at *runner.Attach) {
		c, _, err := dialAttachBack(t, ts, at.AttachID, testRunnerToken)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-hold // say nothing
	}
	defer close(hold)

	stream := newScriptedStream()
	go func() {
		_ = p.ExecBroker().Exec(context.Background(),
			brokerTarget("sess_test", "vm1"), execSpecOf("true"), stream)
	}()
	m := stream.nextServerMsg(t)
	if m.Type != terminal.TypeExecError || m.Reason != terminal.ReasonNoAnswer {
		t.Fatalf("a silent sandbox produced %+v, want exec_error no_answer", m)
	}
	if err := stream.closeReason(t); !errors.Is(err, errExecNoAnswer) {
		t.Fatalf("closed with %v, want errExecNoAnswer", err)
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("the silent-sandbox refusal took %s; the budget is meant to be "+
			"configurable and this test is meant to configure it", took)
	}
}

// TestTheProductionExecHandshakeBudget pins the value the budget above
// defaults to, separately from the test that shortens it. Ten seconds is not
// the acknowledgement timeout even though both are "one small frame on an
// already-open socket": that one acknowledges a binding already installed,
// and this one waits for a SPAWN — a path resolution, a symlink-resolving
// containment check on a caller-named directory, possibly a log file created
// on a cold filesystem, and a fork. The cost of being wrong is telling a user
// their session predates the feature.
func TestTheProductionExecHandshakeBudget(t *testing.T) {
	if defaultExecHandshakeTimeout != 10*time.Second {
		t.Fatalf("the exec handshake budget is %s, want 10s", defaultExecHandshakeTimeout)
	}
	if defaultExecHandshakeTimeout <= defaultControlAckTimeout {
		t.Fatal("the handshake budget must be longer than the acknowledgement one: " +
			"it waits for a fork, not for a frame")
	}
	p := New(&fakeHost{}, Options{})
	if p.execHandshake != defaultExecHandshakeTimeout {
		t.Fatalf("a zero ExecHandshakeTimeout became %s, want the default %s",
			p.execHandshake, defaultExecHandshakeTimeout)
	}
	p = New(&fakeHost{}, Options{ExecHandshakeTimeout: 3 * time.Second})
	if p.execHandshake != 3*time.Second {
		t.Fatalf("ExecHandshakeTimeout was not honoured: %s", p.execHandshake)
	}
}

// TestExecStdinIsSplitToFitTheFrameLayer. A caller's stdin bytes are base64
// inside the message, and the runner base64s the whole message again into the
// frame — so a message well under the plane's own 16 MiB limit becomes an
// oversized FRAME at the sandbox, whose read limit closes the SESSION conn:
// every viewer dropped and every in-flight exec killed, from one message.
//
// The bytes and their order are the caller's; only the framing changes.
func TestExecStdinIsSplitToFitTheFrameLayer(t *testing.T) {
	const total = 3*maxExecStdinChunk + 1234
	got := make(chan []byte, 1)
	stream, _, _ := runExec(t, execSpecOf("cat"), func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		var seen []byte
		for len(seen) < total {
			m, err := sandboxRead(t, conn)
			if err != nil {
				break
			}
			if m.Type != "stdin" {
				continue
			}
			if len(m.Data) > maxExecStdinChunk {
				t.Errorf("a %d-byte stdin frame reached the sandbox; the limit is %d",
					len(m.Data), maxExecStdinChunk)
				break
			}
			seen = append(seen, m.Data...)
		}
		got <- seen
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecExit})
	})
	stream.nextServerMsg(t) // exec_started

	body := make([]byte, total)
	for i := range body {
		body[i] = byte(i)
	}
	stream.in <- terminal.ClientMessage{Type: "stdin", Data: body}

	select {
	case seen := <-got:
		if !bytes.Equal(seen, body) {
			t.Fatalf("the sandbox received %d bytes, want the caller's %d, unchanged",
				len(seen), len(body))
		}
	case <-time.After(testDeadline):
		t.Fatal("the split stdin never arrived")
	}
}

// TestExecForwardsASandboxRefusal: an exec_error is a perfectly good first
// message — a cwd outside the workspace, an env name the rule refuses, one
// exec too many — and it is forwarded so the caller learns WHY rather than
// being told "unsupported" for something the sandbox understood completely.
func TestExecForwardsASandboxRefusal(t *testing.T) {
	stream, _, _ := runExec(t, execSpecOf("git"), func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{
			Type: terminal.TypeExecError, Reason: terminal.ReasonCwdRefused})
	})
	m := stream.nextServerMsg(t)
	if m.Type != terminal.TypeExecError || m.Reason != terminal.ReasonCwdRefused {
		t.Fatalf("the client was told %+v, want the sandbox's own cwd_refused", m)
	}
}

// ---------------------------------------------------------------------------
// the splice
// ---------------------------------------------------------------------------

// TestExecSplicesBothWays is the plane doing its job for an exec: the
// sandbox's streams reach the caller and the caller's input reaches the
// sandbox, verbatim and in order.
func TestExecSplicesBothWays(t *testing.T) {
	got := make(chan []terminal.ClientMessage, 1)
	stream, _, _ := runExec(t, execSpecOf("cat"), func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		var seen []terminal.ClientMessage
		for i := 0; i < 3; i++ {
			m, err := sandboxRead(t, conn)
			if err != nil {
				break
			}
			seen = append(seen, m)
		}
		got <- seen
		sandboxSend(t, conn, terminal.ServerMessage{
			Type: terminal.TypeExecStdout, Data: []byte("out")})
		sandboxSend(t, conn, terminal.ServerMessage{
			Type: terminal.TypeExecStderr, Data: []byte("err")})
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecExit, ExitCode: 7})
	})

	if m := stream.nextServerMsg(t); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("hello")}
	stream.in <- terminal.ClientMessage{Type: terminal.TypeExecStdinEOF}
	stream.in <- terminal.ClientMessage{Type: terminal.TypeExecSignal, Signal: terminal.SignalINT}

	seen := <-got
	if len(seen) != 3 || seen[0].Type != "stdin" || string(seen[0].Data) != "hello" ||
		seen[1].Type != terminal.TypeExecStdinEOF ||
		seen[2].Type != terminal.TypeExecSignal || seen[2].Signal != terminal.SignalINT {
		t.Fatalf("the sandbox received %+v", seen)
	}

	out := stream.nextServerMsg(t)
	errs := stream.nextServerMsg(t)
	exit := stream.nextServerMsg(t)
	if out.Type != terminal.TypeExecStdout || string(out.Data) != "out" {
		t.Fatalf("stdout arrived as %+v", out)
	}
	if errs.Type != terminal.TypeExecStderr || string(errs.Data) != "err" {
		t.Fatalf("stderr arrived as %+v", errs)
	}
	if exit.Type != terminal.TypeExecExit || exit.ExitCode != 7 {
		t.Fatalf("exit arrived as %+v", exit)
	}
}

// TestExecNeverStampsAGeneration is the mirror of #84's splice rule. A plane
// stamps every TERMINAL frame it forwards with the generation that attach
// holds; it stamps no exec frame, ever, and a generation a client puts on one
// is stripped rather than carried — because a sandbox that read it would
// walk its own controller fence forward and lock the person at the keyboard
// out of their terminal.
func TestExecNeverStampsAGeneration(t *testing.T) {
	got := make(chan terminal.ClientMessage, 1)
	stream, _, _ := runExec(t, execSpecOf("true"), func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		m, err := sandboxRead(t, conn)
		if err == nil {
			got <- m
		}
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecExit})
	})
	stream.nextServerMsg(t)
	stream.in <- terminal.ClientMessage{
		Type: "stdin", Data: []byte("x"), Generation: terminal.GenOf(999)}

	select {
	case m := <-got:
		if m.Generation != "" {
			t.Fatalf("an exec frame reached the sandbox stamped %q", m.Generation)
		}
	case <-time.After(testDeadline):
		t.Fatal("nothing reached the sandbox")
	}
}

// TestExecDropsTheOwnershipVocabulary, in both directions. A claim on an exec
// has nothing to claim; a `control` would be a client naming its own mode and
// generation at a pty this attachment must never touch; and a client told
// `attached control 99` by a sandbox would believe it holds a lease nobody
// granted.
func TestExecDropsTheOwnershipVocabulary(t *testing.T) {
	got := make(chan terminal.ClientMessage, 4)
	stream, _, _ := runExec(t, execSpecOf("true"), func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecStarted})
		// Everything a broken or compromised sandbox might say.
		for _, m := range []terminal.ServerMessage{
			{Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(99)},
			{Type: terminal.TypeStale, Generation: terminal.GenOf(99)},
			{Type: terminal.TypeControlChanged, Mode: terminal.ModeView},
			{Type: terminal.TypeControlAck, Generation: terminal.GenOf(99)},
			{Type: "snapshot", Data: []byte("the agent's screen")},
			{Type: "output", Seq: 4, Data: []byte("the agent's output")},
			{Type: terminal.TypeExecStarted},
			{Type: terminal.TypeExecExit, ExitCode: 3},
		} {
			sandboxSend(t, conn, m)
		}
		for i := 0; i < 8; i++ {
			m, err := sandboxRead(t, conn)
			if err != nil {
				return
			}
			got <- m
		}
	})

	if m := stream.nextServerMsg(t); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	// Everything a client might send that an exec has no business carrying,
	// followed by one stdin so the test can tell "dropped" from "not yet".
	for _, m := range []terminal.ClientMessage{
		{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)},
		{Type: terminal.TypeRelease},
		{Type: terminal.TypeControl, Mode: terminal.ModeControl, Generation: terminal.GenOf(99)},
		{Type: terminal.TypeControlAck},
		{Type: terminal.TypeExecStart, Exec: &runner.ExecSpec{Argv: []string{"rm", "-rf", "/"}}},
		{Type: "stdin", Data: []byte("marker")},
	} {
		stream.in <- m
	}

	// The next thing the client hears is the exit — every ownership message
	// and every terminal message from the sandbox was dropped.
	if m := stream.nextServerMsg(t); m.Type != terminal.TypeExecExit || m.ExitCode != 3 {
		t.Fatalf("the client was told %+v, want only the exec_exit", m)
	}

	select {
	case m := <-got:
		if m.Type != "stdin" || string(m.Data) != "marker" {
			t.Fatalf("the sandbox received %+v before the marker; an ownership message "+
				"or a second exec_start was forwarded", m)
		}
	case <-time.After(testDeadline):
		t.Fatal("the marker never reached the sandbox")
	}
}

// TestExecStartIsNeverForwardedTwice: the spec was settled before the socket
// was upgraded, so a second one is a client trying to run a command the
// application never authorized or audited. Covered by the marker above; this
// pins the rule as a pure function so it cannot be relaxed silently.
func TestExecForwardableVocabularies(t *testing.T) {
	for typ, want := range map[string]bool{
		"stdin": true, "resize": true,
		terminal.TypeExecStdinEOF: true, terminal.TypeExecSignal: true,
		terminal.TypeExecStart: false, terminal.TypeClaim: false,
		terminal.TypeRelease: false, terminal.TypeControl: false,
		terminal.TypeControlAck: false, "": false, "anything else": false,
	} {
		if got := execClientForwardable(typ); got != want {
			t.Fatalf("execClientForwardable(%q) = %v, want %v", typ, got, want)
		}
	}
	for typ, want := range map[string]bool{
		terminal.TypeExecStdout: true, terminal.TypeExecStderr: true,
		terminal.TypeExecExit: true, terminal.TypeExecError: true,
		terminal.TypeExecStarted: false, terminal.TypeAttached: false,
		terminal.TypeStale: false, terminal.TypeControlChanged: false,
		terminal.TypeControlAck: false, "snapshot": false, "output": false,
		"exit": false, "": false,
	} {
		if got := execServerForwardable(typ); got != want {
			t.Fatalf("execServerForwardable(%q) = %v, want %v", typ, got, want)
		}
	}
}

// TestADetachedExecClosesAfterItsPid is the shape of --detach at this hop:
// the sandbox answers exec_started with the pid and closes the attachment,
// and the plane passes both on without waiting for an exit that is never
// coming.
func TestADetachedExecClosesAfterItsPid(t *testing.T) {
	spec := execSpecOf("claude", "--continue")
	spec.Detach, spec.LogPath = true, "run.log"
	stream, attaches, _ := runExec(t, spec, func(conn relay.Conn) {
		sandboxSend(t, conn, terminal.ServerMessage{Type: terminal.TypeExecStarted, PID: 4321})
		// And closes: a detached exec has nothing more to say.
	})
	at := <-attaches
	if at.Exec == nil || !at.Exec.Detach || at.Exec.LogPath != "run.log" {
		t.Fatalf("the detach did not reach the runner: %+v", at.Exec)
	}
	m := stream.nextServerMsg(t)
	if m.Type != terminal.TypeExecStarted || m.PID != 4321 {
		t.Fatalf("a detached exec answered %+v", m)
	}
	if err := stream.closeReason(t); !errors.Is(err, errExecEnded) {
		t.Fatalf("closed with %v, want errExecEnded", err)
	}
}

// TestExecWithNoDialBackClosesOnTheTTL: nobody may hold a parked socket
// forever, and an exec inherits the pairing TTL unchanged.
func TestExecWithNoDialBackClosesOnTheTTL(t *testing.T) {
	p, h, _ := newTestPlane(t, Options{PairTTL: 100 * time.Millisecond})
	h.dialBack = nil // the runner takes the command and never comes back

	stream := newScriptedStream()
	done := make(chan error, 1)
	go func() {
		done <- p.ExecBroker().Exec(context.Background(),
			brokerTarget("sess_test", "vm1"), execSpecOf("true"), stream)
	}()
	if err := stream.closeReason(t); !errors.Is(err, errAttachNoDialBack) {
		t.Fatalf("closed with %v, want errAttachNoDialBack", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("an exec whose runner never dialled back reported success")
		}
	case <-time.After(testDeadline):
		t.Fatal("the broker never returned")
	}
	if n := pendingAttaches(p); n != 0 {
		t.Fatalf("%d pairings were left parked", n)
	}
}

// TestExecWithAnUnreachableRunnerIsUnavailable, with the pairing taken back
// rather than left for a runner that can never claim it.
func TestExecWithAnUnreachableRunnerIsUnavailable(t *testing.T) {
	p, h, _ := newTestPlane(t, Options{})
	h.sendErr = errors.New("no control connection")

	stream := newScriptedStream()
	err := p.ExecBroker().Exec(context.Background(),
		brokerTarget("sess_test", "vm1"), execSpecOf("true"), stream)
	if err == nil {
		t.Fatal("an exec to an unreachable runner reported success")
	}
	if n := pendingAttaches(p); n != 0 {
		t.Fatalf("%d pairings were left parked", n)
	}
}

// TestExecFirstMessageRequiresExecStart pins what a route must read before it
// calls the service: the spec travels in the opening message and nothing else
// opens an exec.
func TestExecFirstMessageRequiresExecStart(t *testing.T) {
	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: terminal.TypeExecStart,
		Exec: &runner.ExecSpec{Argv: []string{"git"}}}
	m, err := ExecFirstMessage(context.Background(), stream)
	if err != nil || m.Exec == nil || m.Exec.Argv[0] != "git" {
		t.Fatalf("a valid exec_start was read as (%+v, %v)", m, err)
	}

	for name, first := range map[string]terminal.ClientMessage{
		"a resize": {Type: "resize", Cols: 80, Rows: 24},
		"stdin":    {Type: "stdin", Data: []byte("x")},
		"a claim":  {Type: terminal.TypeClaim},
		"no spec":  {Type: terminal.TypeExecStart},
	} {
		t.Run(name, func(t *testing.T) {
			s := newScriptedStream()
			s.in <- first
			if _, err := ExecFirstMessage(context.Background(), s); !errors.Is(err, ErrExecFirstMessage) {
				t.Fatalf("got %v, want ErrExecFirstMessage", err)
			}
		})
	}
}
