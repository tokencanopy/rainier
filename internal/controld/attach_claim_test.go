// internal/controld/attach_claim_test.go
//
// The mid-attach claim, through the whole self-hosted stack: a client socket
// at controld, the runner's own dial-back, a scripted sessiond behind it, and
// the real ownerOrAdmin policy deciding who may drive. Nothing here fakes the
// keeper or the plane — the defect these tests exist for lived precisely in
// the seam between the client's authorized request and the runner's dial-back,
// which no fake of either end can show.
package controld

import (
	"context"
	"sync"
	"testing"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// attachAs dials one negotiated attach, sends the opening resize the protocol
// requires, and returns the socket with its opening ownership answer read.
func attachAs(t *testing.T, fx *attachFixture, query, token string) (*websocket.Conn, terminal.ServerMessage) {
	t.Helper()
	c, _, err := dialAttach(t, fx.ts, fx.id, query, token)
	if err != nil {
		t.Fatalf("dial attach %q: %v", query, err)
	}
	t.Cleanup(func() { c.CloseNow() })
	writeClient(t, c, terminal.ClientMessage{Type: "resize", Cols: 120, Rows: 40})
	return c, readOwnership(t, c)
}

// readOwnership reads until the next ownership message, skipping the terminal
// traffic that legitimately interleaves with it: the sandbox's snapshot and
// output arrive on the same socket and on their own schedule, and a test that
// insisted the next frame be an answer would be asserting an ordering the
// plane does not promise — the two directions are independent.
func readOwnership(t *testing.T, c *websocket.Conn) terminal.ServerMessage {
	t.Helper()
	for range 32 {
		m := readServer(t, c)
		switch m.Type {
		case terminal.TypeAttached, terminal.TypeStale, terminal.TypeControlChanged:
			return m
		}
	}
	t.Fatal("no ownership message arrived in 32 frames")
	return terminal.ServerMessage{}
}

// takeControl sends the frame `rainier attach --take` and the in-session
// take-control key both send. It is spelled out here rather than imported
// because internal/attachio owns the decision to send it and keeps it
// unexported; that package's own tests pin the frame (a claim carrying the
// generation this client was last told), and these pin what the server does
// with it.
func takeControl(t *testing.T, c *websocket.Conn, expected uint64) {
	t.Helper()
	writeClient(t, c, terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(expected)})
}

// ---------------------------------------------------------------------------
// the regression
// ---------------------------------------------------------------------------

// TestAMidAttachClaimTakesControl is the bug: a claim asks the host's policy
// on the context it is CALLED with, and a mid-attach claim is called on the
// runner's dial-back request — which is authenticated as a runner and carries
// no user at all. ownerOrAdmin resolves the acting user from the context
// before any rule runs, so it refused every claim, and a session's own
// creator could not take control of their own terminal from a second device:
// `--take` and the take-control key were answered `stale` forever.
//
// The whole stack is real for exactly that reason. The generation is the
// store's, the policy is the installation's, and the claim arrives on the
// socket the splice is actually reading.
func TestAMidAttachClaimTakesControl(t *testing.T) {
	fx := newAttachFixture(t)

	// The laptop attaches first and holds control at generation 1.
	laptop, first := attachAs(t, fx, "?control=v1", fx.tok)
	if first.Type != terminal.TypeAttached || first.Mode != terminal.ModeControl ||
		first.Generation.Value() != 1 {
		t.Fatalf("the laptop's opening answer = %+v, want attached as control at 1", first)
	}
	if open := fx.sd.nextOpen(t); open.Mode != terminal.ModeControl || open.Gen != 1 {
		t.Fatalf("the laptop's FrameOpen carried %q at %d, want control at 1", open.Mode, open.Gen)
	}

	// The phone attaches under that live lease presenting nothing, so it
	// watches — the honest answer, and the state `--take` and the
	// take-control key exist to get out of.
	phone, second := attachAs(t, fx, "?control=v1", fx.tok)
	if second.Type != terminal.TypeAttached || second.Mode != terminal.ModeView ||
		second.Generation.Value() != 1 {
		t.Fatalf("the phone's opening answer = %+v, want attached as view at 1", second)
	}
	if open := fx.sd.nextOpen(t); open.Mode != terminal.ModeView || open.Gen != 1 {
		t.Fatalf("the phone's FrameOpen carried %q at %d, want view at 1", open.Mode, open.Gen)
	}

	// One press of the take-control key.
	takeControl(t, phone, second.Generation.Value())

	// It takes control, at a generation the store advanced, and the sandbox
	// has the phone's new binding before the phone is told anything.
	if m := fx.sd.nextControl(t); m.Mode != terminal.ModeControl || m.Generation.Value() != 2 {
		t.Fatalf("the sandbox was told %q at %d, want control at 2", m.Mode, m.Generation.Value())
	}
	if m := readOwnership(t, phone); m.Type != terminal.TypeAttached ||
		m.Mode != terminal.ModeControl || m.Generation.Value() != 2 {
		t.Fatalf("the claim was answered %+v, want attached as control at 2", m)
	}

	// And the laptop is told it was displaced, so its user can take it back
	// with one press rather than discovering the number has moved.
	if m := readOwnership(t, laptop); m.Type != terminal.TypeControlChanged ||
		m.Mode != terminal.ModeView || m.Generation.Value() != 2 {
		t.Fatalf("the laptop was told %+v, want control_changed to view at 2", m)
	}
}

// TestTakeAtAttachAndTheTakeControlKeyBothTakeControl walks the two routes
// `rainier attach --take` actually uses, in the order the CLI uses them: it
// asks for control presenting the generation it can see (the query-string
// half, settled before the upgrade), and when it comes back a viewer anyway —
// which is what happens when it cannot see one, the ordinary case on a second
// device — it spends its one claim (the mid-attach half). Only the second was
// broken, and a test that covered only the first would have passed throughout.
func TestTakeAtAttachAndTheTakeControlKeyBothTakeControl(t *testing.T) {
	fx := newAttachFixture(t)

	laptop, first := attachAs(t, fx, "?control=v1", fx.tok)
	if first.Mode != terminal.ModeControl {
		t.Fatalf("the laptop's opening answer = %+v, want control", first)
	}
	fx.sd.nextOpen(t)

	// `--take` from a device that HAS seen the generation: presenting the one
	// in force claims it even under a live lease, at the door.
	taker, answer := attachAs(t, fx, "?control=v1&expected=1", fx.tok)
	if answer.Type != terminal.TypeAttached || answer.Mode != terminal.ModeControl ||
		answer.Generation.Value() != 2 {
		t.Fatalf("--take with the generation in force = %+v, want attached as control at 2", answer)
	}
	fx.sd.nextOpen(t)
	if m := readOwnership(t, laptop); m.Type != terminal.TypeControlChanged || m.Mode != terminal.ModeView {
		t.Fatalf("the displaced laptop was told %+v, want control_changed to view", m)
	}

	// The laptop now does what a person does next: one press of the
	// take-control key, from the generation it was just told.
	takeControl(t, laptop, 2)
	if m := readOwnership(t, laptop); m.Type != terminal.TypeAttached ||
		m.Mode != terminal.ModeControl || m.Generation.Value() != 3 {
		t.Fatalf("the take-control key was answered %+v, want attached as control at 3", m)
	}
	if m := readOwnership(t, taker); m.Type != terminal.TypeControlChanged ||
		m.Mode != terminal.ModeView || m.Generation.Value() != 3 {
		t.Fatalf("the displaced taker was told %+v, want control_changed to view at 3", m)
	}

	// The store is the authority, so the row itself has moved twice.
	row, err := fx.st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(fx.id))
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ControllerGeneration != 3 {
		t.Fatalf("the row's controller generation = %d, want 3", row.ControllerGeneration)
	}
}

// ---------------------------------------------------------------------------
// which identity the policy is asked about
// ---------------------------------------------------------------------------

// policyAsk is one question the host's policy was asked, and — the point of
// the whole fixture — the identity the context carried when it was asked.
type policyAsk struct {
	mode control.AttachmentMode
	user string // the login the context resolved to, or "" for nobody
}

// recordingPolicy is controlapp.AttachmentPolicy over the real installation
// policy, recording every question and the identity the context carried. It
// can also refuse the controller, which is the one thing ownerOrAdmin cannot
// express (self-hosted answers both questions the same way) and which every
// hosted policy can.
type recordingPolicy struct {
	mu             sync.Mutex
	asked          []policyAsk
	denyController bool
}

var _ controlapp.AttachmentPolicy = (*recordingPolicy)(nil)

func (p *recordingPolicy) AuthorizeAttachment(ctx context.Context, scope control.Scope,
	r control.Resource, mode control.AttachmentMode) error {
	ask := policyAsk{mode: mode}
	if u, ok := userFromContext(ctx); ok {
		ask.user = u.Login
	}
	p.mu.Lock()
	p.asked = append(p.asked, ask)
	deny := p.denyController && mode == control.AttachmentController
	p.mu.Unlock()
	if deny {
		return control.ErrDenied
	}
	return (ownerOrAdmin{}).AuthorizeAttachment(ctx, scope, r, mode)
}

func (p *recordingPolicy) questions() []policyAsk {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]policyAsk(nil), p.asked...)
}

func (p *recordingPolicy) refuseController(deny bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.denyController = deny
}

// withRecordingPolicy recomposes the fixture's attachment service over policy,
// leaving every other adapter — the store, the transport, the broker, the
// events — exactly as compose built them. It is the seam a hosted policy
// occupies, stood up in the one place a self-hosted test can reach it.
func withRecordingPolicy(t *testing.T, fx *attachFixture) *recordingPolicy {
	t.Helper()
	policy := &recordingPolicy{}
	svc, err := controlapp.NewAttachmentService(controlapp.AttachmentOptions{
		Authorizer: ownerOrAdmin{}, Policy: policy, Sessions: fx.st.Sessions(),
		Transport: fx.s.transport, Broker: fx.s.broker, Events: fx.st,
		Clock: fx.s.clock, IDs: idGenerator{}, UnitOfWork: fx.st,
	})
	if err != nil {
		t.Fatalf("composing an attachment service over a recording policy: %v", err)
	}
	fx.s.attachments = svc
	return policy
}

// TestAMidAttachClaimIsAuthorizedAgainstTheAttachingUser is the defect stated
// as the policy sees it. The question itself was always right —
// AttachmentController, asked live, once per claim — and it was asked about
// nobody, because it was asked on a context authenticated as a runner. A
// policy that reads the caller's identity out of the context (every hosted
// one: internal/cell/authz reads the current workspace role that way) can only
// answer such a question with a refusal.
func TestAMidAttachClaimIsAuthorizedAgainstTheAttachingUser(t *testing.T) {
	fx := newAttachFixture(t)
	policy := withRecordingPolicy(t, fx)

	_, first := attachAs(t, fx, "?control=v1", fx.tok)
	if first.Mode != terminal.ModeControl {
		t.Fatalf("the laptop's opening answer = %+v, want control", first)
	}
	fx.sd.nextOpen(t)
	phone, second := attachAs(t, fx, "?control=v1", fx.tok)
	if second.Mode != terminal.ModeView {
		t.Fatalf("the phone's opening answer = %+v, want view", second)
	}
	fx.sd.nextOpen(t)

	before := len(policy.questions())
	takeControl(t, phone, second.Generation.Value())
	if m := readOwnership(t, phone); m.Type != terminal.TypeAttached || m.Mode != terminal.ModeControl {
		t.Fatalf("the claim was answered %+v, want attached as control", m)
	}

	// One question, about the controller, about the person whose attach this
	// is — and not about nobody.
	asked := policy.questions()[before:]
	if len(asked) != 1 {
		t.Fatalf("the policy was asked %v for one claim; want exactly one question", asked)
	}
	if asked[0].mode != control.AttachmentController {
		t.Fatalf("the claim asked about mode %q, want the controller", asked[0].mode)
	}
	if asked[0].user != "alice" {
		t.Fatalf("the claim was authorized against %q, want the attaching user alice", asked[0].user)
	}
}

// TestAViewOnlyPrincipalsClaimIsStillRefused is the other half of the fix: it
// must carry the identity through without weakening what the policy is then
// allowed to say about it. A host that grants viewing and not driving admits
// the attach as a viewer, and its claim is refused — with the store never
// touched, because the service already carried that answer down to the plane.
func TestAViewOnlyPrincipalsClaimIsStillRefused(t *testing.T) {
	fx := newAttachFixture(t)
	policy := withRecordingPolicy(t, fx)
	policy.refuseController(true)

	phone, opening := attachAs(t, fx, "?control=v1", fx.tok)
	if opening.Type != terminal.TypeAttached || opening.Mode != terminal.ModeView {
		t.Fatalf("a view-only principal's plain attach = %+v, want attached as view", opening)
	}
	fx.sd.nextOpen(t)

	takeControl(t, phone, opening.Generation.Value())
	if m := readOwnership(t, phone); m.Type != terminal.TypeStale {
		t.Fatalf("a view-only principal's claim was answered %+v, want stale", m)
	}
	row, err := fx.st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(fx.id))
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ControllerGeneration != 0 {
		t.Fatalf("the row's generation = %d; a refused claim must move nothing", row.ControllerGeneration)
	}
}

// TestAGrantRevokedMidAttachIsRefusedAtTheNextPress pins the property the
// design bought by asking the policy per claim instead of caching the answer:
// the identity is captured at the door, the QUESTION is not. A grant the host
// withdraws while the attach is open is honoured at the next press of the
// take-control key.
func TestAGrantRevokedMidAttachIsRefusedAtTheNextPress(t *testing.T) {
	fx := newAttachFixture(t)
	policy := withRecordingPolicy(t, fx)

	_, first := attachAs(t, fx, "?control=v1", fx.tok)
	if first.Mode != terminal.ModeControl {
		t.Fatalf("the laptop's opening answer = %+v, want control", first)
	}
	fx.sd.nextOpen(t)
	phone, second := attachAs(t, fx, "?control=v1", fx.tok)
	if second.Mode != terminal.ModeView {
		t.Fatalf("the phone's opening answer = %+v, want view", second)
	}
	fx.sd.nextOpen(t)

	// The host withdraws the grant this attach was admitted under.
	policy.refuseController(true)

	takeControl(t, phone, second.Generation.Value())
	if m := readOwnership(t, phone); m.Type != terminal.TypeStale {
		t.Fatalf("a claim under a withdrawn grant was answered %+v, want stale", m)
	}
	row, err := fx.st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(fx.id))
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ControllerGeneration != 1 {
		t.Fatalf("the row's generation = %d, want the 1 the laptop holds", row.ControllerGeneration)
	}
}
