package controlapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// execFixture is the attachment fixture with an exec broker composed in.
type execFixture struct {
	*attachmentFixture
	execs *fakeExecBroker
}

func newExecFixture(t *testing.T) *execFixture {
	t.Helper()
	fx := &attachmentFixture{
		now:       time.Unix(1_700_000_000, 0).UTC(),
		auth:      &attachmentFakeAuthorizer{},
		policy:    &attachmentFakePolicy{},
		sessions:  &attachmentFakeSessions{found: true, row: attachmentRunningSession()},
		transport: &attachmentFakeTransport{},
		broker:    &attachmentFakeBroker{},
		events:    &attachmentFakeEvents{},
		ids:       &attachmentFakeIDs{eventID: "evt_example"},
	}
	execs := &fakeExecBroker{}
	svc, err := NewAttachmentService(AttachmentOptions{
		Authorizer: fx.auth,
		Policy:     fx.policy,
		Sessions:   fx.sessions,
		Transport:  fx.transport,
		Broker:     fx.broker,
		ExecBroker: execs,
		Events:     fx.events,
		Clock:      attachmentFakeClock(func() time.Time { return fx.now }),
		IDs:        fx.ids,
		UnitOfWork: directUOW{},
	})
	if err != nil {
		t.Fatalf("NewAttachmentService: %v", err)
	}
	fx.svc = svc
	return &execFixture{attachmentFixture: fx, execs: execs}
}

type fakeExecBroker struct {
	mu     sync.Mutex
	err    error
	calls  int
	target control.AttachTarget
	spec   runner.ExecSpec
}

func (f *fakeExecBroker) Exec(_ context.Context, target control.AttachTarget,
	spec runner.ExecSpec, _ control.TerminalStream) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.target, f.spec = target, spec
	return f.err
}

func (f *fakeExecBroker) seen() (int, control.AttachTarget, runner.ExecSpec) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.target, f.spec
}

func execTestCommand() ExecCommand {
	return ExecCommand{SessionID: "sess_example", Argv: []string{"git", "status", "--short"}}
}

// ---------------------------------------------------------------------------
// authority
// ---------------------------------------------------------------------------

// TestExecAsksTheControllerQuestionExactlyOnce is the design's authority rule
// in one test: an exec writes into the sandbox and runs arbitrary code there,
// which is what a controller does, so that is the question — asked LIVE, at
// the moment the exec is requested, and never from an answer cached at some
// earlier attach.
func TestExecAsksTheControllerQuestionExactlyOnce(t *testing.T) {
	fx := newExecFixture(t)
	if err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		execTestCommand(), &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("exec under a control grant: %v", err)
	}
	modes := fx.policy.modes()
	if len(modes) != 1 || modes[0] != control.AttachmentController {
		t.Fatalf("policy was asked %v, want exactly one controller question", modes)
	}
	if fx.auth.lastAction != control.ActionAttach {
		t.Fatalf("the generic authorizer was asked %q, want %q — a new verb would fail closed "+
			"on every adapter that has never seen it", fx.auth.lastAction, control.ActionAttach)
	}
}

// TestExecRefusesAViewOnlyPrincipal: there is no reduced exec. The attach
// path's "admit as a viewer" rule has no meaning here, so a host that grants
// viewing without granting driving gets ErrDenied — the same answer, for the
// same reason, that its take-control key gets.
func TestExecRefusesAViewOnlyPrincipal(t *testing.T) {
	for name, deny := range map[string]map[control.AttachmentMode]bool{
		"view only": {control.AttachmentController: true},
		"neither":   {control.AttachmentController: true, control.AttachmentViewer: true},
	} {
		t.Run(name, func(t *testing.T) {
			fx := newExecFixture(t)
			fx.policy.deny = deny
			err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
				execTestCommand(), &attachmentRecordingTerminalStream{})
			if !errors.Is(err, control.ErrDenied) {
				t.Fatalf("got %v, want ErrDenied", err)
			}
			if calls, _, _ := fx.execs.seen(); calls != 0 {
				t.Fatal("a refused exec reached the broker")
			}
			if fx.events.calls != 0 {
				t.Fatal("a refused exec was audited as though it had run")
			}
			// It is never silently downgraded: the viewer question is not
			// even asked, because there is nothing a viewer's exec would be.
			for _, m := range fx.policy.modes() {
				if m == control.AttachmentViewer {
					t.Fatal("exec asked whether the caller may WATCH; there is no reduced exec")
				}
			}
		})
	}
}

// TestExecActionDenialPrecedesPolicy: the generic verb is answered first, and
// a caller it refuses never reaches the policy, the broker or the audit log.
func TestExecActionDenialPrecedesPolicy(t *testing.T) {
	fx := newExecFixture(t)
	fx.auth.err = control.ErrDenied
	err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		execTestCommand(), &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if fx.policy.calls != 0 {
		t.Fatalf("policy ran %d times after action denial", fx.policy.calls)
	}
}

// TestExecOnAHostWithNoExecPlaneIsUnsupported. Composing the exec broker is
// optional — a host may have no plane for it — and saying so is honest where
// pretending would leave a caller waiting on a dial-back nobody will make.
func TestExecOnAHostWithNoExecPlaneIsUnsupported(t *testing.T) {
	fx := newAttachmentFixture(t) // composed without an ExecBroker
	err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		execTestCommand(), &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

// ---------------------------------------------------------------------------
// readiness
// ---------------------------------------------------------------------------

// TestExecRefusesASessionThatIsNotRunning, with no wait. Attach waits, because
// a person who just typed `rainier new` is legitimately early. Exec's caller
// is a script that wants an answer now, and a suspended session is
// deliberately not resumed: resuming costs minutes and changes what the
// caller is billed for.
func TestExecRefusesASessionThatIsNotRunning(t *testing.T) {
	for _, state := range []control.SessionState{
		control.StateQueued, control.StateCreating, control.StateSuspendedWarm,
		control.StateSuspendedCold, control.StateFailed, control.StateDead,
		control.StateDestroyed,
	} {
		t.Run(string(state), func(t *testing.T) {
			fx := newExecFixture(t)
			row := attachmentRunningSession()
			row.State = state
			fx.sessions.row = row
			// Even with the runner connected, which is what makes a `failed`
			// session ATTACHABLE: an exec into a session whose boot chain
			// never finished is not a diagnosis, it is a command in an
			// environment that was never built.
			fx.transport.connected = true
			err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
				execTestCommand(), &attachmentRecordingTerminalStream{})
			if !errors.Is(err, control.ErrConflict) {
				t.Fatalf("state %s: got %v, want ErrConflict", state, err)
			}
			if calls, _, _ := fx.execs.seen(); calls != 0 {
				t.Fatalf("state %s reached the broker", state)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the binding
// ---------------------------------------------------------------------------

// TestExecTakesNoLeaseAndAdvancesNoGeneration is the composition rule with
// #84, pinned at the seam where it would break: an exec carries no keeper, is
// not negotiated, may not claim, and never asks the repository to move a
// generation.
func TestExecTakesNoLeaseAndAdvancesNoGeneration(t *testing.T) {
	fx := newExecFixture(t)
	row := attachmentRunningSession()
	row.ControllerGeneration = 12
	fx.sessions.row = row

	if err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		execTestCommand(), &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatal(err)
	}
	_, target, _ := fx.execs.seen()
	if target.Controller != nil {
		t.Fatal("an exec was handed a controller lease keeper")
	}
	if target.Negotiated || target.MayClaim {
		t.Fatalf("an exec was marked negotiated=%v mayClaim=%v", target.Negotiated, target.MayClaim)
	}
	if target.ControllerGeneration != 12 {
		t.Fatalf("the target carries generation %d, want the row's own 12 read and not written",
			target.ControllerGeneration)
	}
	if fx.sessions.nextCalls != 0 || fx.sessions.casCalls != 0 || fx.sessions.renewCalls != 0 {
		t.Fatalf("an exec touched the controller lease: next=%d cas=%d renew=%d",
			fx.sessions.nextCalls, fx.sessions.casCalls, fx.sessions.renewCalls)
	}
}

// TestExecCarriesTheSpecVerbatim to the broker: the command a caller composed
// is the command the sandbox validates, with no re-encoding between them.
func TestExecCarriesTheSpecVerbatim(t *testing.T) {
	fx := newExecFixture(t)
	cmd := ExecCommand{
		SessionID: "sess_example",
		Argv:      []string{"make", "test"},
		Cwd:       "/workspace/app",
		Env:       map[string]string{"CI": "1"},
		TTY:       true, Cols: 100, Rows: 40,
	}
	if err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		cmd, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatal(err)
	}
	_, _, spec := fx.execs.seen()
	if len(spec.Argv) != 2 || spec.Argv[1] != "test" || spec.Cwd != "/workspace/app" ||
		spec.Env["CI"] != "1" || !spec.TTY || spec.Cols != 100 || spec.Rows != 40 {
		t.Fatalf("the spec arrived as %+v", spec)
	}
}

// ---------------------------------------------------------------------------
// the audit event
// ---------------------------------------------------------------------------

// TestExecAuditsTheCommandNameAndNothingElse. "Somebody ran git in this
// session at this time" is the whole of what the record says — which answers
// the question an audit log is for without turning the log into a transcript.
func TestExecAuditsTheCommandNameAndNothingElse(t *testing.T) {
	fx := newExecFixture(t)
	cmd := ExecCommand{
		SessionID: "sess_example",
		Argv:      []string{"/usr/bin/git", "push", "--force", "https://token@example.invalid/x"},
		Cwd:       "/workspace/secret-dir",
		Env:       map[string]string{"SECRET_TOKEN": "s3cr3t-value"},
	}
	if err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		cmd, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatal(err)
	}
	got := fx.events.snapshot()
	if len(got) != 1 {
		t.Fatalf("recorded %d events, want exactly one", len(got))
	}
	e := got[0]
	if e.Action != control.ActionExec {
		t.Fatalf("action = %q, want %q", e.Action, control.ActionExec)
	}
	if e.Command != "git" {
		t.Fatalf("command = %q, want the base name alone", e.Command)
	}
	if e.ActorID != "act_example" || e.WorkspaceID != "ws_example" ||
		e.Resource.ID != "sess_example" || e.PlacementGeneration != 7 {
		t.Fatalf("event = %+v", e)
	}
	// Nothing else from the request is anywhere in it. The event is rendered
	// WHOLE — every exported field, by reflection — rather than by naming the
	// fields the test already knows about, which would pass unchanged if a
	// future field started carrying an argument.
	rendered := fmt.Sprintf("%+v", e)
	for _, forbidden := range []string{"push", "--force", "token", "secret-dir",
		"SECRET_TOKEN", "s3cr3t-value", "/usr/bin"} {
		if strings.Contains(rendered, forbidden) {
			t.Fatalf("the audit event carried %q: %s", forbidden, rendered)
		}
	}
}

// TestExecIsAuditedOnAcceptance, not on completion. The broker holds an exec
// for its whole life, so a build that runs for an hour returns from it an
// hour from now; recording afterwards would leave that hour unaudited and
// would lose the record entirely if the replica restarted mid-exec. An audit
// log that only reports the commands that finished while the plane stayed up
// is not an audit log.
//
// The consequence is deliberate: an exec the RUNNER then refused still leaves
// a record. "Somebody with the authority to run commands here ran one" is
// exactly the question an audit log is being asked, and it is true either way.
func TestExecIsAuditedOnAcceptance(t *testing.T) {
	fx := newExecFixture(t)
	fx.execs.err = errors.New("the runner never dialled back")
	err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		execTestCommand(), &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	got := fx.events.snapshot()
	if len(got) != 1 || got[0].Command != "git" {
		t.Fatalf("recorded %+v, want one exec event naming git", got)
	}
}

// TestExecIsNotAuditedWhenItWasNeverACCEPTED: a refusal above the broker —
// authority, readiness, shape — leaves no record, because nothing was
// dispatched. Covered per-case above; this pins the boundary itself.
func TestExecIsNotAuditedWhenItWasRefused(t *testing.T) {
	for name, stage := range map[string]func(*execFixture){
		"denied": func(fx *execFixture) { fx.auth.err = control.ErrDenied },
		"view only": func(fx *execFixture) {
			fx.policy.deny = map[control.AttachmentMode]bool{control.AttachmentController: true}
		},
		"not running": func(fx *execFixture) {
			row := attachmentRunningSession()
			row.State = control.StateSuspendedWarm
			fx.sessions.row = row
		},
	} {
		t.Run(name, func(t *testing.T) {
			fx := newExecFixture(t)
			stage(fx)
			_ = fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
				execTestCommand(), &attachmentRecordingTerminalStream{})
			if fx.events.calls != 0 {
				t.Fatalf("a refused exec left %d audit records", fx.events.calls)
			}
		})
	}
}

// TestExecCommandName is the audit rule as a pure function, which is where
// its edges live: a path, an empty argv, a name with an escape sequence in
// it, one longer than the cap.
func TestExecCommandName(t *testing.T) {
	for _, tc := range []struct {
		argv []string
		want string
	}{
		{[]string{"git", "push", "--force"}, "git"},
		{[]string{"/usr/bin/make", "test"}, "make"},
		{[]string{"./scripts/deploy.sh"}, "deploy.sh"},
		{[]string{"../../bin/tool"}, "tool"},
		{nil, "?"},
		{[]string{""}, "?"},
		{[]string{"/"}, "?"},
		{[]string{"."}, "?"},
		{[]string{".."}, "?"},
		{[]string{"wéird"}, "?"},
		{[]string{"esc\x1b[2Jape"}, "?"},
		{[]string{"nl\nname"}, "?"},
		{[]string{strings.Repeat("a", 65)}, "?"},
		{[]string{strings.Repeat("a", 64)}, strings.Repeat("a", 64)},
	} {
		if got := ExecCommandName(tc.argv); got != tc.want {
			t.Fatalf("ExecCommandName(%q) = %q, want %q", tc.argv, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// shape
// ---------------------------------------------------------------------------

// TestExecRefusalWordsMeanWhatTheySay: "not found" is for a command that was
// LOOKED FOR, so an argv that is merely too big to carry is "could not be
// executed" — 126, which a script reads as "Rainier refused this", rather
// than 127, which it reads as "you typed a name this sandbox does not have".
func TestExecRefusalWordsMeanWhatTheySay(t *testing.T) {
	huge := execTestCommand()
	huge.Argv = make([]string, maxExecArgv+1)
	for i := range huge.Argv {
		huge.Argv[i] = "x"
	}
	if got := ExecRefusal(huge); got != terminal.ReasonNotExecutable {
		t.Fatalf("an over-long argv = %q, want %q", got, terminal.ReasonNotExecutable)
	}
	empty := execTestCommand()
	empty.Argv = nil
	if got := ExecRefusal(empty); got != terminal.ReasonNotFound {
		t.Fatalf("an empty argv = %q, want %q", got, terminal.ReasonNotFound)
	}
}

// TestValidateExec is the hop-before-the-sandbox check: what can be refused
// without a filesystem is refused here, early, where it is still a status
// code. It is deliberately not the whole check — the sandbox re-derives every
// path against the tree it actually has, because the last hop before a
// syscall trusts nobody.
func TestValidateExec(t *testing.T) {
	ok := func(mutate func(*ExecCommand)) ExecCommand {
		c := execTestCommand()
		mutate(&c)
		return c
	}
	for name, cmd := range map[string]ExecCommand{
		"no argv":            ok(func(c *ExecCommand) { c.Argv = nil }),
		"empty argv0":        ok(func(c *ExecCommand) { c.Argv = []string{""} }),
		"a NUL in argv":      ok(func(c *ExecCommand) { c.Argv = []string{"sh", "a\x00b"} }),
		"cwd outside":        ok(func(c *ExecCommand) { c.Cwd = "/etc" }),
		"cwd with dot dot":   ok(func(c *ExecCommand) { c.Cwd = "../secrets" }),
		"cwd with a NUL":     ok(func(c *ExecCommand) { c.Cwd = "a\x00b" }),
		"env name with =":    ok(func(c *ExecCommand) { c.Env = map[string]string{"A=B": "x"} }),
		"env name with NUL":  ok(func(c *ExecCommand) { c.Env = map[string]string{"A\x00B": "x"} }),
		"env value with NUL": ok(func(c *ExecCommand) { c.Env = map[string]string{"A": "x\x00y"} }),
		"empty env name":     ok(func(c *ExecCommand) { c.Env = map[string]string{"": "x"} }),
		"detach with no log": ok(func(c *ExecCommand) { c.Detach = true }),
		"log outside":        ok(func(c *ExecCommand) { c.Detach, c.Log = true, "/etc/x.log" }),
		"log with dot dot":   ok(func(c *ExecCommand) { c.Detach, c.Log = true, "../x.log" }),
		"log without detach": ok(func(c *ExecCommand) { c.Log = "run.log" }),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateExec(cmd); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}

	for name, cmd := range map[string]ExecCommand{
		"a plain command": execTestCommand(),
		"a relative cwd":  ok(func(c *ExecCommand) { c.Cwd = "app" }),
		"an absolute cwd": ok(func(c *ExecCommand) { c.Cwd = "/workspace/app" }),
		"an ordinary env": ok(func(c *ExecCommand) { c.Env = map[string]string{"CI": "1"} }),
		"a sized tty":     ok(func(c *ExecCommand) { c.TTY, c.Cols, c.Rows = true, 80, 24 }),
		"a detached run":  ok(func(c *ExecCommand) { c.Detach, c.Log = true, "run.log" }),
		"an empty env":    ok(func(c *ExecCommand) { c.Env = map[string]string{} }),
		"an absolute log": ok(func(c *ExecCommand) { c.Detach, c.Log = true, "/workspace/run.log" }),
		// A SIZE is not the plane's to refuse: the sandbox clamps it into
		// what the pty ioctl carries, so an absent or absurd one costs a
		// default-sized terminal rather than a refusal — and refusing would
		// have meant answering "the command is not executable" about a
		// window dimension.
		"a tty with no size":        ok(func(c *ExecCommand) { c.TTY = true }),
		"a tty with an absurd size": ok(func(c *ExecCommand) { c.TTY, c.Cols, c.Rows = true, 1<<20, -4 }),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateExec(cmd); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

// TestExecRefusesAMalformedCommandBeforeTheBroker: a shape error never
// reaches a runner and never leaves an audit record.
func TestExecRefusesAMalformedCommandBeforeTheBroker(t *testing.T) {
	fx := newExecFixture(t)
	err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		ExecCommand{SessionID: "sess_example"}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	if calls, _, _ := fx.execs.seen(); calls != 0 {
		t.Fatal("a malformed exec reached the broker")
	}
	if fx.events.calls != 0 {
		t.Fatal("a malformed exec was audited")
	}
}

// TestExecStartMessage is the first-message contract: the spec travels in the
// opening message and only there, and nothing else opens an exec.
func TestExecStartMessage(t *testing.T) {
	spec := &runner.ExecSpec{Argv: []string{"git", "status"}, Cwd: "app",
		Detach: true, LogPath: "run.log", TTY: true, Cols: 80, Rows: 24,
		Env: map[string]string{"CI": "1"}}
	got, err := ExecStartMessage(terminal.ClientMessage{
		Type: terminal.TypeExecStart, Exec: spec}, "sess_example")
	if err != nil {
		t.Fatal(err)
	}
	if got.SessionID != "sess_example" || got.Cwd != "app" || !got.Detach ||
		got.Log != "run.log" || !got.TTY || got.Cols != 80 || got.Env["CI"] != "1" {
		t.Fatalf("mapped to %+v", got)
	}

	for name, m := range map[string]terminal.ClientMessage{
		"a resize":                {Type: "resize", Cols: 80, Rows: 24},
		"stdin":                   {Type: "stdin", Data: []byte("rm -rf /")},
		"a claim":                 {Type: terminal.TypeClaim},
		"exec_start with no spec": {Type: terminal.TypeExecStart},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ExecStartMessage(m, "sess_example"); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

// TestExecAuditIsRenderedWholeAndIsStillClean is the same claim as above,
// applied to a spec whose every field is a distinctive marker. It exists
// because the natural way to write that assertion — listing the fields the
// test already knows about — would keep passing if a NEW field on
// control.Event ever started carrying one of them.
func TestExecAuditIsRenderedWholeAndIsStillClean(t *testing.T) {
	fx := newExecFixture(t)
	cmd := ExecCommand{
		SessionID: "sess_example",
		Argv: []string{"/opt/marker-dir/git", "marker-arg-one", "--marker-flag",
			"marker-arg-two"},
		Cwd: "marker-cwd",
		Env: map[string]string{"MARKER_NAME": "marker-value"},
		TTY: true, Cols: 4242, Rows: 2424,
	}
	if err := fx.svc.ExecCommand(context.Background(), attachmentTestScope(),
		cmd, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatal(err)
	}
	got := fx.events.snapshot()
	if len(got) != 1 || got[0].Command != "git" {
		t.Fatalf("recorded %+v, want one event naming git", got)
	}
	rendered := fmt.Sprintf("%+v", got[0])
	for _, marker := range []string{
		"marker-dir", "marker-arg-one", "--marker-flag", "marker-arg-two",
		"marker-cwd", "MARKER_NAME", "marker-value", "4242", "2424", "/opt",
	} {
		if strings.Contains(rendered, marker) {
			t.Fatalf("the audit event carried %q: %s", marker, rendered)
		}
	}
}
