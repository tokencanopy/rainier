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
// negotiated and mayClaim are two narrower facts, and they are separate on
// purpose. negotiated says this client asked for conditional ownership and
// can therefore be told things; an unnegotiated attach is stamped and fenced
// like any other, and is sent nothing it would not understand. mayClaim says
// the application authorized this client for the controller it would become
// — a host policy that grants viewing without driving sets the first and not
// the second, and such a client must still be told its mode and generation.
type ownership struct {
	plane      *Plane
	session    control.SessionID
	negotiated bool
	mayClaim   bool
	keeper     control.ControllerLeaseKeeper
	stream     control.TerminalStream

	// announce serialises everything that TELLS this client what it is, and
	// holds the state read and the message that reports it together. Without
	// that, a claim answers itself with a mode it read before it spent
	// seconds displacing its peers, while a peer that won inside that wait
	// has already sent the correction — the two land in the wrong order and
	// the client is left believing it has control it does not have. Nothing
	// corrects that: a client that believes it is the controller sends no
	// claim, and this attach's heartbeat renews nothing, because the plane
	// knows it is a viewer.
	//
	// It is only ever taken alone. A claim finishes displacing every peer
	// before it announces anything about itself, and a displacement takes the
	// peer's announce and no other, so two attaches can never each hold one
	// and wait for the other's.
	//
	// It is a channel rather than a sync.Mutex because ACQUIRING it has to be
	// bounded too. A message to a client that has stopped reading is held for
	// as long as that socket's own write deadline allows, and a peer
	// displacing this attach needs this hold: a mutex would put the peer's
	// handoff behind that client, which is the whole failure this round
	// removed from the writes themselves.
	announce chan struct{}

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
	// told records that this attach's client has been sent an ownership
	// message, which in practice means its opening `attached` has gone out.
	// An attach is registered in the owner table BEFORE that — it has to be,
	// or a take-over cannot see it — and the read of its first message that
	// sits between the two is bounded only by attachFirstMsgTimeout. So a
	// handoff elsewhere on the session can reach an attach that has not been
	// told what it is yet, and a courtesy notice arriving first would be the
	// opening answer as far as that client is concerned. See displace.
	told bool
}

func newOwnership(p *Plane, target control.AttachTarget) *ownership {
	mode := terminal.ModeControl
	if target.Mode == control.AttachmentViewer {
		mode = terminal.ModeView
	}
	// Negotiated is read forgivingly in the one direction that cannot cost
	// anybody anything. A target carrying a keeper but not the flag is a
	// composer written before the flag existed, and a keeper has always meant
	// exactly this; reading it as unnegotiated would silently stop telling
	// that client its mode, its generation and every handoff, and from the
	// client's end it would look like an older plane.
	//
	// MayClaim is read the other way: it is a privilege, and one that needs a
	// keeper to exercise, so it is never inferred and never wider than what
	// the target actually carries.
	return &ownership{
		plane:      p,
		session:    target.SessionID,
		negotiated: target.Negotiated || target.Controller != nil,
		mayClaim:   target.MayClaim && target.Controller != nil,
		keeper:     target.Controller,
		mode:       mode,
		gen:        target.ControllerGeneration,
		announce:   make(chan struct{}, 1),
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

// demoteTo makes this attach a viewer at the newer of the generation it
// already holds and gen, reporting the generation it now holds and whether it
// moved. from is the generation the demotion was DECIDED at: a demotion is an
// answer about one generation, and an attach that has since moved past that
// generation has left the question behind.
//
// Every other transition refuses a backwards move (advance, displaceTo), and
// this one used to be the exception — the justification being that getting
// the number wrong is survivable while believing you still have control is
// not. That is true of the NUMBER and was applied to the MODE. A heartbeat
// demotion decided at generation N can spend a store read and a bounded
// sandbox wait in flight, and this attach can win N+1 in that window through
// a claim of its own; demoting it then leaves the plane saying viewer while
// the store says this attach holds the lease — nobody can type until the
// lease expires. Which is the outcome the unconditional write was there to
// prevent, arrived at from the other side.
//
// Being wrong about the number is still survivable, so a demotion whose
// generation could not be read still demotes at the generation this attach
// already held.
func (o *ownership) demoteTo(from, gen uint64) (uint64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gen > from {
		return o.gen, false
	}
	if gen > o.gen {
		o.gen = gen
	}
	o.mode = terminal.ModeView
	return o.gen, true
}

// displaceTo moves this attach to gen because a peer took control, doing the
// check and the write under ONE hold of this attach's own lock. Split in two,
// a claim landing between them overwrites the displacement it lost to and
// tells its client it has control.
//
// It returns the mode this attach WAS in when it moved, the mode it still IS
// in when it did not — a controller has to be fenced in its sandbox before
// anybody is told anything, a viewer only carries a new number — and whether
// it moved at all. Only the moved case is read: see displace. An attach already at or past gen is
// left alone: writing a generation it has passed over its state would walk it
// backwards. Being left alone is a statement about its STATE and not about
// what its client has heard; the two were conflated once, and displace says
// what that cost.
func (o *ownership) displaceTo(gen uint64) (was string, moved bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gen >= gen {
		return o.mode, false
	}
	was, o.mode, o.gen = o.mode, terminal.ModeView, gen
	return was, true
}

// spokenTo reports whether anything has been said to this attach's client
// yet. See the told field and displace.
func (o *ownership) spokenTo() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.told
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
	// The CALLER's context decides how long this may take, and the two kinds
	// of caller want opposite things. A message this attach is owed — the
	// opening `attached`, the answer to its own claim — carries no deadline
	// of its own and is bounded only by the socket's write deadline, because
	// dropping it leaves a client believing something untrue. A message about
	// somebody else's handoff carries one acknowledgement timeout, because it
	// is a courtesy: the mechanism behind it is the peer's own heartbeat and
	// its fence at the pty, and a handoff must never wait on a client that
	// has stopped reading.
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
	// Bounded like every other peer-facing step. A runner socket that is not
	// draining must not hold a handoff open: the generation has already moved
	// in the store, this attachment's pty fence is already in force, and a
	// write that cannot land inside one acknowledgement timeout is not going
	// to make the handoff any safer by landing later.
	ctx, cancel := o.plane.step(ctx)
	defer cancel()
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
	if !o.negotiated {
		return
	}
	if mode, gen := o.get(); mode == terminal.ModeControl {
		// A claim from the attach this plane already believes is the
		// controller. Taking it at face value would advance the generation,
		// fence the keystrokes this client has already sent, re-install a
		// binding the sandbox has and spend an acknowledgement timeout on
		// every peer — to arrive at the answer the client already had. The
		// first-party CLI refuses to send one (internal/attachio's own
		// claim()); nothing at the plane stopped another client, and a claim
		// is client-triggered and unlimited by design.
		//
		// But the BELIEF is checked before it is answered, with a read that
		// advances nothing. A controller displaced by an attach on another
		// replica is told nothing at all: it reads `control` here until its
		// own heartbeat renewal is refused, up to one interval later, and
		// this press is its user's only way out of that. Answering it from
		// memory would confirm control that has already gone elsewhere and
		// close the one door out.
		//
		// A read that fails leaves the belief standing: the store being
		// briefly unusable is not evidence that this attach lost anything,
		// and the heartbeat is what settles it either way.
		if o.keeper != nil {
			if current, _, err := o.keeper.State(ctx); err == nil && current != gen {
				_ = o.sendStale(ctx)
				return
			}
		}
		// It is still the controller, and it is told so — because a
		// take-control key that produces nothing at all is the one outcome a
		// client cannot tell from a broken connection.
		o.announceAs(ctx, terminal.TypeAttached, 0)
		return
	}
	if !o.mayClaim || o.keeper == nil {
		// This client may watch and may not drive: the host's policy answers
		// differently for a viewer and a controller, which is the whole
		// reason those are two facts on the target. It is told its claim did
		// not land, in the words a lost race already uses — a refusal of its
		// own would be a message every client would have to learn for an
		// answer that is already "you are still a viewer" — and the store is
		// never touched, so an unauthorized claim costs nothing anywhere.
		//
		// A negotiated attach is ANSWERED either way. Returning in silence
		// would leave the take-control key doing nothing at all, which is the
		// one outcome a client cannot tell from a broken connection.
		o.announceAs(ctx, terminal.TypeStale, 0)
		return
	}
	gen, err := o.keeper.Claim(ctx, expected)
	if err != nil {
		_ = o.sendStale(ctx)
		return
	}
	if err := o.installAndWait(ctx, terminal.ModeControl, gen); err != nil {
		// The generation is this attach's and its sandbox has no binding for
		// it. Taking control here would produce a controller nothing can
		// repair: the pty discards every keystroke, because that attachment's
		// binding still says what it said before, and the heartbeat renews
		// happily — the lease genuinely IS this attach's — so the plane never
		// notices and the client's own take-control key is answered out of
		// the same wrong state.
		//
		// So give the generation back. The lease is vacated and advanced,
		// which frees it for whoever asks next (including this client, on its
		// next press), and the answer names the generation that now exists.
		// A sandbox that simply never acknowledges is NOT this case: that is
		// an older sessiond, installAndWait returns nil for it, and the
		// handoff proceeds fenced at the plane alone.
		_ = o.keeper.Release(ctx, gen)
		// And everybody else is told the number that now exists. The
		// generation moved TWICE for this claim — once to win it, once to
		// give it back — and the attach that was the controller when it
		// started is still `control` in the plane, still forwarded for, until
		// its own heartbeat renewal is refused up to one interval later.
		// Every viewer's next press is refused once too, for want of a number
		// nobody sent them: the exact case displace's own doc says the viewer
		// notice exists for.
		//
		// The answer goes out FIRST, on its own line because the order
		// matters: this client asked a question and the fan-out behind it can
		// spend an acknowledgement timeout per peer.
		current := o.sendStale(ctx)
		// And it WAITS, like a take-over and unlike a release. This is the
		// only fan-out in the module that can demote a peer which genuinely
		// holds control — release and finish both demote the caller first, so
		// their peers are already viewers — and a peer flipped to `view` in
		// the plane before its sandbox has the matching binding is a peer the
		// NEXT taker will not wait for: displaceTo hands that taker
		// `was == view`, it skips installAndWait, and it is answered while
		// the old binding is still in flight. The pty's own generation fence
		// still holds, so nothing executes twice; what would not hold is the
		// ordering this module promises.
		o.plane.displace(ctx, o, current, true)
		return
	}
	if o.advance(terminal.ModeControl, gen) {
		o.plane.displace(ctx, o, gen, true)
	}
	// And only now is this client told, because only now is it true. There
	// are TWO waits between winning the generation and answering — this
	// attach's own acknowledgement, and then one per displaced peer's
	// sandbox, each up to the acknowledgement timeout — and another claim can
	// win inside either. Both are answered by the same question, asked once,
	// at the end and inside the hold that sends the answer: does this attach
	// still hold the generation it won?
	mode, current := o.announceAs(ctx, terminal.TypeAttached, gen)
	if mode == terminal.ModeControl && current == gen {
		return
	}
	// It does not: something won inside one of those waits, and the client
	// has just been told so. The binding this claim installed is re-pointed
	// at what this attach actually is, so the sandbox's copy agrees with the
	// plane's — the pty fence had already made it inert, at a generation the
	// session has passed, and this is what stops it lingering until the next
	// handoff.
	//
	// It happens OUTSIDE the announce hold. A sandbox that is slow to take
	// this write would otherwise hold this attach's announce mutex, and the
	// first thing a peer displacing this attach needs is that mutex — so one
	// slow sandbox would stall somebody else's handoff. Reading the state a
	// second time is safe: claims on one attach are handled inline on its own
	// client pump, so nothing can promote this attach between the two reads.
	_ = o.install(ctx, mode, current)
}

// announceAs sends ONE ownership message, and it is the only place any of
// them is sent. It takes the announce hold, reads the mode and the generation
// under it, and reports what it READ — no caller can assert a mode it did not
// read, because no caller supplies one. Three rounds of review found that
// defect on three different paths; this is the version of the fix that cannot
// recur.
//
// won is the generation a claim answer is answering at, and zero on every
// other path. Both of the decisions that depend on the state are made HERE,
// under the hold that writes the message:
//
//   - a claim that no longer holds what it won is answered the way a lost
//     race is answered, because that is what it is now;
//   - a `stale` that would tell a LIVE controller it is a viewer is sent as
//     `attached control` instead. A client that reads `controller.generation`
//     and claims from a value it no longer holds is refused by the store, and
//     answering that refusal with `stale` would make the session's own
//     controller stop typing for the rest of its lease.
//
// It returns the mode and generation it reported.
func (o *ownership) announceAs(ctx context.Context, typ string, won uint64) (string, uint64) {
	select {
	case o.announce <- struct{}{}:
	case <-ctx.Done():
		// Somebody else is mid-sentence to this client and ctx says we are
		// out of time — a peer's displacement giving up on a client that is
		// not draining. The message this would have sent is a courtesy in
		// every path that carries a deadline; the caller's own paths carry
		// none, so they wait.
		return o.get()
	}
	defer func() { <-o.announce }()
	o.mu.Lock()
	mode, gen := o.mode, o.gen
	o.told = true
	o.mu.Unlock()
	switch {
	case typ == terminal.TypeAttached && won != 0 && (mode != terminal.ModeControl || gen != won):
		typ = terminal.TypeStale
	case typ == terminal.TypeStale && mode == terminal.ModeControl:
		typ = terminal.TypeAttached
	}
	m := terminal.ServerMessage{Type: typ, Generation: terminal.GenOf(gen)}
	if typ != terminal.TypeStale {
		// `stale` carries a generation and nothing else: it is a refusal, not
		// a statement about what this attach is, and it has never carried a
		// mode on the wire.
		m.Mode = mode
	}
	o.send(ctx, m)
	return mode, gen
}

// sendStale answers a refused claim with the current generation, and returns
// the generation it answered with. A read that fails still gets an answer: the
// client must learn its claim did not land.
//
// It falls back to the generation this attach already holds rather than to
// zero, for the reason demote does. Zero is a generation no row can ever be
// at, so a client told it presents zero on every later claim and is refused
// every time — stranded by one store read that timed out, until it detaches
// and attaches again. A stale-but-real generation is refused exactly as zero
// would be, and in the common case, where the read merely timed out, it is
// still the number that takes control in one press.
func (o *ownership) sendStale(ctx context.Context) uint64 {
	_, current := o.get()
	if o.keeper != nil {
		if gen, _, err := o.keeper.State(ctx); err == nil {
			current = gen
		}
	}
	// Record what the store says before answering, so the answer is read out
	// of this attach's state rather than asserted over it. displaceTo is the
	// right write for exactly the two cases this has: an attach BEHIND the
	// store lost the race and is a viewer at the current generation, and an
	// attach at or past it is left alone — which is a controller whose client
	// claimed with an `expected` it no longer holds, and which announceAs
	// then answers with the control it still has.
	o.displaceTo(current)
	_, gen := o.announceAs(ctx, terminal.TypeStale, 0)
	return gen
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
	o.plane.displace(ctx, o, o.demote(ctx, gen), false)
}

// demote turns this attach into a viewer and says so: the plane stops
// forwarding for it, the sandbox learns the current generation and that this
// attachment is not the controller under it, and then the client is told what
// this attach now IS. It returns the generation it demoted to.
//
// from is the generation the demotion was DECIDED at — the one the heartbeat's
// renewal was refused for, or the one a release gave up — and not a generation
// read here. The window this guard exists for opens the moment that decision
// is made, and reading it again at the top of this function would leave the
// first part of it uncovered. It does nothing to an attach that has since
// moved past from; see demoteTo.
//
// A demotion whose current generation cannot be read still demotes — being
// wrong about the number is survivable, believing you still have control is
// not — but it falls back to the generation this attach already held rather
// than to zero. A claim from a stale-but-real generation is refused exactly
// as a claim from zero would be, while the common case, where the read merely
// timed out, still leaves the client able to take control in one press.
func (o *ownership) demote(ctx context.Context, from uint64) uint64 {
	current := from
	if o.keeper != nil {
		if gen, _, err := o.keeper.State(ctx); err == nil {
			current = gen
		}
	}
	// The plane's own half of the fence goes first: it is a single locked
	// write that cannot fail, and it stops this attach being forwarded for at
	// once. The sandbox is told only if the demotion still stands — installing
	// a viewer binding for an attach that has since WON a newer generation
	// would fence the controller it just became.
	if gen, moved := o.demoteTo(from, current); moved {
		_ = o.installAndWait(ctx, terminal.ModeView, gen)
	}
	_, gen := o.announceAs(ctx, terminal.TypeControlChanged, 0)
	return gen
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
				// Demote from the generation the renewal was refused FOR. A
				// claim of this attach's own can land while the refusal is
				// still in flight, and a demotion that re-read the generation
				// here would be about the one it just won.
				o.demote(ctx, gen)
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
	// ONE read, as release does. Split in two, this attach's own heartbeat
	// demotion can complete between them — it holds `control` for the whole
	// of its bounded wait on the sandbox — and the second read then returns
	// the generation the NEW controller holds. Releasing that advances past
	// somebody who legitimately has control, on their own generation, from an
	// attach that is walking out of the door.
	mode, gen := o.get()
	if o.keeper == nil || mode != terminal.ModeControl {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
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
// EVERY peer is told, including one whose state is already at gen. The notice
// reports what that attach is, read under its own announce hold, so telling a
// peer something it already knows costs one small frame and says nothing
// untrue — while not telling it, on the theory that its state moving and its
// client being told are the same event, is how a displaced controller ended
// up watching a screen that said it had control.
//
// wait says whether to hold until each demoted peer's sandbox has confirmed
// the new binding. A take-over waits, because the taker must not be told it
// has control while a keystroke the previous controller has already sent
// could still execute; a release does not, because the generation it is
// announcing has already moved and there is nobody it could be racing.
//
// It is a BOUNDED FAN-OUT, and that is the whole of its liveness. Every peer
// is reached on its own goroutine, so one peer's socket is never in front of
// another's, and every peer-facing step inside it carries its own deadline,
// so a peer that cannot be reached never holds the taker. Walked serially
// with unbounded writes — which is what this was — a single client that had
// stopped reading (a closed lid, a zero window, a paused tab: no RST, so
// nothing closes it) froze every handoff on the session: the taker's own
// claim had already advanced the store, so nobody could type, and the taker
// was never told it had won.
//
// It still returns only when the fan-out is done, because the contract's
// ordering depends on it: the taker is not told it has control until the
// displaced controllers' sandboxes have the new binding. What changed is the
// bound — two acknowledgement timeouts in total, rather than none at all,
// once per peer, in a queue.
func (p *Plane) displace(ctx context.Context, winner *ownership, gen uint64, wait bool) {
	var wg sync.WaitGroup
	// Zero needs no guard of its own: no row is ever at generation zero, so
	// every attach is already at or past it and displaceTo refuses each one.
	for _, other := range p.owners.peers(winner) {
		// The check and the write are ONE step, under that peer's own lock:
		// between them, a claim of its own could otherwise land and overwrite
		// the displacement it has just lost to. It stays on this goroutine,
		// ahead of the fan-out: every displaced peer stops being forwarded
		// for at once, whatever its socket is doing.
		//
		// What it decides is whether this peer's SANDBOX needs a new binding,
		// and nothing else. It used to decide whether the peer's CLIENT heard
		// anything either — `moved == false` was read as "already at gen,
		// therefore already told" — and those are not the same fact. Two
		// paths move an attach's own state and then spend real time before
		// announcing it: `demote`, which holds `installAndWait` for up to one
		// acknowledgement timeout against a sandbox that never answers, and
		// `sendStale`, which can wait out a client's whole write budget. A
		// take-over landing in either gap skipped that peer entirely and
		// answered the taker, so two screens said "you have control" until
		// the skipped peer's own wait timed out.
		was, moved := other.displaceTo(gen)
		if !moved && !other.spokenTo() {
			// A peer that has not been told what it is yet, and whose state
			// this handoff did not move. Its opening `attached` is still
			// coming and will name exactly what this notice would — while
			// arriving FIRST would make a courtesy notice that peer's
			// opening answer, and a client reads its first answer
			// differently from a later one ("you are watching" against
			// "somebody took control from you").
			//
			// This is not the skip the fix above removed. That one asserted
			// that a peer's state being current meant its client was
			// current; this one is about a client that has been told
			// nothing, and it holds only when there is nothing to tell.
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if moved && was == terminal.ModeControl {
				// It believed it was typing, so its sandbox has to be told
				// before anybody is told anything else. The deadline for
				// this step lives inside it — install bounds its own write,
				// installAndWait bounds its own wait — so wrapping another
				// one around the pair here would cap at one timeout what is
				// already at most one timeout: a write that fails takes the
				// wait with it.
				if wait {
					_ = other.installAndWait(ctx, terminal.ModeView, gen)
				} else {
					_ = other.install(ctx, terminal.ModeView, gen)
				}
			}
			// A viewer stays a viewer; only the number it would claim from
			// changes, and its client reads that silently. What the notice
			// SAYS is read at send time, not asserted here: a peer that won
			// its own claim inside this loop is told it has control, rather
			// than being told it is a viewer and then corrected.
			//
			// That is also what makes it safe to send this to a peer already
			// at gen. announceAs reads the mode and the number under the
			// announce hold and reports what it read, so the notice names
			// that peer's real state and cannot walk its client backwards;
			// a client that is already there folds it to no notice at all.
			//
			// Under its own deadline, like every other step: this notice is
			// the one thing in a handoff that goes to a client, and a client
			// that cannot take it is exactly the client that must not hold
			// the taker.
			nctx, cancel := p.step(ctx)
			defer cancel()
			other.announceAs(nctx, terminal.TypeControlChanged, 0)
		}()
	}
	wg.Wait()
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
