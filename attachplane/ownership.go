package attachplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// This file is the plane's half of conditional controller ownership: what one
// attach holds, how it hands control over, and how it finds out that somebody
// took it. The authority itself is not here — it is the repository's, reached
// through the control.ControllerLeaseKeeper the application handed this attach
// on its AttachTarget. The plane never sees a repository and never decides who
// may attach; it carries a decision and fences what contradicts it.

// ownership is one attach's controller state on this replica. Every attach a
// NEW plane serves has one, negotiated or not, because a new plane stamps
// every frame it forwards with the generation the attach holds — that is what
// makes an unstamped frame arriving at a sandbox mean "an older plane" and
// nothing else.
//
// negotiated is the narrower fact: this client asked for conditional
// ownership and can therefore be told things. An unnegotiated attach is
// stamped and fenced like any other, and is sent nothing it would not
// understand.
type ownership struct {
	plane      *Plane
	session    control.SessionID
	negotiated bool
	keeper     control.ControllerLeaseKeeper
	stream     control.TerminalStream

	// handoff serialises the install-and-wait pairs on this attach. The
	// client pump (a claim, a release) and the heartbeat (a demotion after a
	// stale renew) can both reach one, and they share one acknowledgement
	// channel: two of them in flight at once would consume each other's
	// answer and one would then wait out the whole timeout.
	handoff sync.Mutex

	mu     sync.Mutex
	mode   string
	gen    uint64
	runner runnerConn
	ack    chan uint64
}

func newOwnership(p *Plane, target control.AttachTarget) *ownership {
	mode := terminal.ModeControl
	if target.Mode == control.AttachmentViewer {
		mode = terminal.ModeView
	}
	return &ownership{
		plane:      p,
		session:    target.SessionID,
		negotiated: target.Controller != nil,
		keeper:     target.Controller,
		mode:       mode,
		gen:        target.ControllerGeneration,
		ack:        make(chan uint64, 1),
	}
}

func (o *ownership) get() (mode string, gen uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.mode, o.gen
}

// advance moves this attach to (mode, gen) unless that would move it
// BACKWARDS, and reports whether it moved. A generation older than the one
// this attach already holds names a decision something has since superseded,
// and acting on it would announce control that has already gone elsewhere.
//
// The window it closes belongs to a claim. A claim advances the generation in
// the store and then waits, for as long as the acknowledgement timeout
// allows, for the sandbox to confirm the binding; a second claim can win
// inside that wait. Without this, the first claim wakes up and overwrites its
// own displacement.
func (o *ownership) advance(mode string, gen uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if gen < o.gen {
		return false
	}
	o.mode, o.gen = mode, gen
	return true
}

// demoteTo makes this attach a viewer, at the newer of the generation it
// already holds and gen, and returns the generation it now holds. It is the
// one transition that cannot refuse: being wrong about the number is
// survivable, believing you still have control is not, so a demotion whose
// generation could not be read still demotes.
func (o *ownership) demoteTo(gen uint64) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if gen > o.gen {
		o.gen = gen
	}
	o.mode = terminal.ModeView
	return o.gen
}

// displaceTo moves this attach to gen because a peer took control, doing the
// check and the write under ONE hold of this attach's own lock. Split in two,
// a claim landing between them overwrites the displacement it lost to and
// tells its client it has control.
//
// It returns the mode this attach WAS in — a controller has to be fenced in
// its sandbox before anybody is told anything, a viewer only carries a new
// number — and whether it moved at all. An attach already at or past gen is
// left alone: naming it a generation it has passed would walk its client
// backwards.
func (o *ownership) displaceTo(gen uint64) (was string, moved bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gen >= gen {
		return o.mode, false
	}
	was, o.mode, o.gen = o.mode, terminal.ModeView, gen
	return was, true
}

func (o *ownership) controlling() bool {
	mode, _ := o.get()
	return mode == terminal.ModeControl
}

// bindRunner hands this ownership the socket its sandbox is on, once the
// dial-back has claimed the pairing. Until then there is nowhere to install a
// binding, which is why a handoff cannot happen before the splice starts.
func (o *ownership) bindRunner(c runnerConn) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.runner = c
}

// send writes one ownership message to the client, and only to a client that
// asked for them. An unnegotiated attach receives exactly today's message
// set, which is the whole of what it can decode.
func (o *ownership) send(ctx context.Context, m terminal.ServerMessage) {
	if !o.negotiated {
		return
	}
	_ = o.stream.Send(ctx, m)
}

// stamp puts this attach's generation on an outgoing client frame, so the
// sandbox can tell a frame sent under the current binding from one sent under
// a binding that has since been superseded. It is applied to a legacy
// client's frames too: the client cannot stamp them, the plane can, and a
// frame that reaches a sandbox unstamped then means an older plane and
// nothing else.
//
// It OVERWRITES whatever the client put there. The plane's view of what this
// attach holds is the fresher one, and a fence a client could write its own
// value into would be no fence at all.
func (o *ownership) stamp(m terminal.ClientMessage) terminal.ClientMessage {
	_, gen := o.get()
	m.Generation = terminal.GenOf(gen)
	return m
}

// mayForward reports whether a client frame that writes to the terminal
// should go on the wire at all. This is not the fence — the fence is at the
// pty, where the bytes would execute — it is the plane declining to carry
// what it already knows will be discarded.
func (o *ownership) mayForward() bool { return o.controlling() }

// install tells the sandbox what this attachment now is. It goes out as an
// ordinary client frame on this attachment's own socket, so it is ordered
// against this attachment's other frames by the transport rather than by
// anything the plane has to arrange.
func (o *ownership) install(ctx context.Context, mode string, gen uint64) error {
	o.mu.Lock()
	runner := o.runner
	o.mu.Unlock()
	if runner == nil {
		return errAttachNotSpliced
	}
	raw, err := json.Marshal(terminal.ClientMessage{
		Type: terminal.TypeControl, Mode: mode, Generation: terminal.GenOf(gen)})
	if err != nil {
		return err
	}
	return runner.Write(ctx, raw)
}

// installAndWait installs a binding and waits for the sandbox to say it has
// it. That wait is the whole reason control_ack exists: two attachments reach
// one sandbox over one conn but on independent paths, so the plane cannot
// order a displacement against a keystroke the previous controller has
// already sent. Not answering the taker until the sandbox has the new
// generation means every frame from the old controller that arrives after
// this point is fenced, and every frame that arrives before it arrived while
// the handoff had not happened for anybody.
//
// A sandbox that predates this protocol never acks. Waiting forever on it
// would be a plane requiring something an old sandbox cannot supply, so the
// wait is bounded and the handoff proceeds without it — fenced at the plane
// alone, which is what that pairing can offer.
func (o *ownership) installAndWait(ctx context.Context, mode string, gen uint64) error {
	o.handoff.Lock()
	defer o.handoff.Unlock()
	// Drop an acknowledgement left over from a handoff that already gave up
	// on it: it answers a question nobody is asking any more, and consuming
	// it here would otherwise cost this handoff the whole timeout.
	select {
	case <-o.ack:
	default:
	}
	if err := o.install(ctx, mode, gen); err != nil {
		return err
	}
	timer := time.NewTimer(o.plane.ackTimeout)
	defer timer.Stop()
	for {
		select {
		case got := <-o.ack:
			if got == gen {
				return nil
			}
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// acked delivers a sandbox's acknowledgement to whoever is waiting on it. A
// non-blocking send: an ack nobody is waiting for is an ack for a handoff
// that has already timed out, and dropping it is right.
func (o *ownership) acked(gen uint64) {
	select {
	case o.ack <- gen:
	default:
	}
}

// claim runs one take-over: advance the generation, install the new binding
// in the sandbox, wait for it, and only then tell this client it has control.
// A claim that lost the race is answered with the generation that actually
// exists now, which is the one this client would have to claim from to try
// again — never an error, because losing a race is an answer.
func (o *ownership) claim(ctx context.Context, expected uint64) {
	if !o.negotiated || o.keeper == nil {
		return
	}
	gen, err := o.keeper.Claim(ctx, expected)
	if err != nil {
		o.sendStale(ctx)
		return
	}
	_ = o.installAndWait(ctx, terminal.ModeControl, gen)
	if !o.advance(terminal.ModeControl, gen) {
		// Somebody claimed over this one while it was waiting for the
		// sandbox. The generation it won was real and is already superseded,
		// so it gets the answer a lost race gets — and the binding this
		// claim installed is re-pointed at what this attach actually is now,
		// so the sandbox's copy agrees with the plane's. The pty fence had
		// already made that binding inert, at a generation the session has
		// passed; this is what stops it lingering until the next handoff.
		mode, current := o.get()
		_ = o.install(ctx, mode, current)
		o.sendStale(ctx)
		return
	}
	o.plane.displace(ctx, o, gen, true)
	o.send(ctx, terminal.ServerMessage{
		Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(gen)})
}

// sendStale answers a refused claim with the current generation. A read that
// fails still gets an answer: the client must learn its claim did not land,
// and a "stale" with no generation is still true.
func (o *ownership) sendStale(ctx context.Context) {
	var current uint64
	if o.keeper != nil {
		if gen, _, err := o.keeper.State(ctx); err == nil {
			current = gen
		}
	}
	o.send(ctx, terminal.ServerMessage{Type: terminal.TypeStale, Generation: terminal.GenOf(current)})
}

// release gives up control without giving up the attach: the generation
// advances, which frees the lease and fences everything this attach has
// already sent, and the sandbox is told this attachment is a viewer now.
//
// Advancing is what makes the fence real. A release that only vacated the
// lease would leave this attachment's binding still matching the sandbox's
// generation, so its next keystroke — or one already on the wire — would
// still execute.
func (o *ownership) release(ctx context.Context) {
	if o.keeper == nil {
		return
	}
	mode, gen := o.get()
	if mode != terminal.ModeControl {
		return
	}
	_ = o.keeper.Release(ctx, gen)
	o.plane.displace(ctx, o, o.demote(ctx), false)
}

// demote turns this attach into a viewer and says so, in that order: the
// sandbox learns the current generation and that this attachment is not the
// controller under it, then the client is told. It returns the generation it
// demoted to.
//
// A demotion whose current generation cannot be read still demotes — being
// wrong about the number is survivable, believing you still have control is
// not — but it falls back to the generation this attach already held rather
// than to zero. A claim from a stale-but-real generation is refused exactly
// as a claim from zero would be, while the common case, where the read merely
// timed out, still leaves the client able to take control in one press.
func (o *ownership) demote(ctx context.Context) uint64 {
	_, current := o.get()
	if o.keeper != nil {
		if gen, _, err := o.keeper.State(ctx); err == nil {
			current = gen
		}
	}
	_ = o.installAndWait(ctx, terminal.ModeView, current)
	current = o.demoteTo(current)
	o.send(ctx, terminal.ServerMessage{
		Type: terminal.TypeControlChanged, Mode: terminal.ModeView, Generation: terminal.GenOf(current)})
	return current
}

// heartbeat renews the lease while this attach holds control, on the stream's
// own liveness cadence. It is also how a controller displaced by an attach on
// ANOTHER replica finds out: nothing pushes it a message, its own renewal
// simply stops being accepted, and it demotes itself.
func (o *ownership) heartbeat(ctx context.Context) {
	if o.keeper == nil {
		return
	}
	t := time.NewTicker(o.plane.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			mode, gen := o.get()
			if mode != terminal.ModeControl {
				continue
			}
			err := o.keeper.Renew(ctx, gen)
			if errors.Is(err, control.ErrStale) {
				o.demote(ctx)
				continue
			}
			// Any other failure is the store being briefly unusable. The
			// lease outlives several heartbeats for exactly this reason, so
			// the right answer is to try again rather than to drop a
			// controller because one write timed out.
		}
	}
}

// finish ends this attach's ownership. A controller that leaves releases at
// once, so the next attach claims with no click rather than waiting out a
// lease its holder has already walked away from.
//
// It runs on a context of its own: the attach's context is cancelled by the
// very disconnect that brings us here, and a release that needs the store is
// the last thing this attach owes everybody else.
func (o *ownership) finish() {
	o.plane.owners.remove(o)
	if o.keeper == nil || !o.controlling() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	_, gen := o.get()
	_ = o.keeper.Release(ctx, gen)
	// And the devices still watching learn the generation they would have to
	// claim from, so the first press of the take-control key takes it rather
	// than discovering the number has moved.
	if current, _, err := o.keeper.State(ctx); err == nil {
		o.plane.displace(ctx, o, current, false)
	}
}

// ---------------------------------------------------------------------------
// the replica's live attaches, by session
// ---------------------------------------------------------------------------

// ownerTable is the attaches this replica is serving, grouped by session, so
// that a take-over can tell the device it displaced immediately instead of
// leaving it to notice at its next heartbeat. It is a courtesy, not the
// mechanism: the heartbeat is what makes displacement work across replicas,
// and the pty fence is what makes it safe either way.
type ownerTable struct {
	mu sync.Mutex
	m  map[control.SessionID]map[*ownership]struct{}
}

func newOwnerTable() *ownerTable {
	return &ownerTable{m: map[control.SessionID]map[*ownership]struct{}{}}
}

func (t *ownerTable) add(o *ownership) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m[o.session] == nil {
		t.m[o.session] = map[*ownership]struct{}{}
	}
	t.m[o.session][o] = struct{}{}
}

func (t *ownerTable) remove(o *ownership) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m[o.session], o)
	if len(t.m[o.session]) == 0 {
		delete(t.m, o.session)
	}
}

// peers returns the other attaches this replica is serving for one session.
func (t *ownerTable) peers(o *ownership) []*ownership {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*ownership, 0, len(t.m[o.session]))
	for other := range t.m[o.session] {
		if other != o {
			out = append(out, other)
		}
	}
	return out
}

// displace tells every other attach on this replica that the generation has
// moved to gen. A peer that believed it was the controller is demoted, which
// is the notice this exists for; a peer that was already a viewer is told the
// number, which is what lets ONE press of its take-control key take control
// rather than discovering that the generation it saw at attach time is gone.
// Without that, every viewer's first press after any handoff — including a
// controller simply leaving — is answered "somebody else got there first"
// about a session nobody is using.
//
// wait says whether to hold until each demoted peer's sandbox has confirmed
// the new binding. A take-over waits, because the taker must not be told it
// has control while a keystroke the previous controller has already sent
// could still execute; a release does not, because the generation it is
// announcing has already moved and there is nobody it could be racing.
func (p *Plane) displace(ctx context.Context, winner *ownership, gen uint64, wait bool) {
	if gen == 0 {
		return
	}
	for _, other := range p.owners.peers(winner) {
		// The check and the write are ONE step, under that peer's own lock:
		// between them, a claim of its own could otherwise land and overwrite
		// the displacement it has just lost to.
		was, moved := other.displaceTo(gen)
		if !moved {
			continue
		}
		if was == terminal.ModeControl {
			// It believed it was typing, so its sandbox has to be told before
			// anybody is told anything else.
			if wait {
				_ = other.installAndWait(ctx, terminal.ModeView, gen)
			} else {
				_ = other.install(ctx, terminal.ModeView, gen)
			}
		}
		// A viewer stays a viewer; only the number it would claim from
		// changes, and its client reads that silently.
		other.send(ctx, terminal.ServerMessage{
			Type: terminal.TypeControlChanged, Mode: terminal.ModeView, Generation: terminal.GenOf(gen)})
	}
}

// ---------------------------------------------------------------------------
// what the client asked for
// ---------------------------------------------------------------------------

// Ownership is the conditional-ownership request a client made, read off its
// attach URL's query string. A host reads it before it upgrades the socket
// and puts it on the control.AttachTerminal command; the zero value is a
// client that asked for nothing, which is today's attach.
type Ownership struct {
	Negotiated bool
	Mode       control.AttachmentMode
	Expected   uint64
}

// RequestedOwnership reads the three attach parameters off q. Everything
// about it is forgiving in the same direction: an unknown capability, an
// unknown mode and an unparseable generation all read as "not that", because
// the alternative is refusing an attach over a query string the client cannot
// fix and does not know it got wrong. The application decides what the
// request is worth; this only says what was asked.
func RequestedOwnership(q url.Values) Ownership {
	if q.Get(terminal.ParamControl) != terminal.CapabilityControl {
		// Not a negotiating client. It gets today's semantics, which is
		// unconditional control — the only thing it can be.
		return Ownership{Mode: control.AttachmentController}
	}
	o := Ownership{Negotiated: true, Mode: control.AttachmentController}
	if q.Get(terminal.ParamMode) == terminal.ModeView {
		o.Mode = control.AttachmentViewer
	}
	o.Expected = terminal.Gen(q.Get(terminal.ParamExpected)).Value()
	return o
}
