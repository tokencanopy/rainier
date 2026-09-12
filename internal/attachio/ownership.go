package attachio

import (
	"fmt"
	"io"
	"sync"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ownership is what one attach knows about who may type: the mode the server
// granted it, the generation that mode is held under, and whether the server
// ever answered at all.
//
// "Never answered" is a real state and not a failure. An installed client
// talks to whatever plane is deployed, and a plane that predates conditional
// ownership simply does not send an `attached`. That client must then behave
// exactly as it did before — type unconditionally, forward Ctrl-\ as an
// ordinary byte, stamp nothing — which is what settled=false means here.
type ownership struct {
	asked bool // this attach advertised the capability
	// shared is what the server said its attachment policy is, read off the
	// session view before this attach dialed. It changes the COPY and nothing
	// else: under a shared policy no peer holds anything, so "another device
	// has control" is false and the key it offers cannot succeed.
	shared bool
	// others is how many other terminals could already type when this attach
	// was prepared, for the one line it opens with. Zero is silence.
	others int
	// neverClaim is --view, which is NOT the same fact as asking for view
	// mode: a plain attach reconnecting after it was superseded also asks for
	// view mode, and that device must keep its take-control key and be told
	// it is viewing. See Options.NeverClaim.
	neverClaim bool
	take       bool // --take: claim once if it comes back a viewer

	mu sync.Mutex
	// settled is the one bit an answer sets, and it means both things it
	// could mean at once: an ownership message has arrived, and the server
	// therefore speaks conditional ownership. Nothing else can set it — a
	// plane that speaks it says so by saying anything at all — so tracking
	// the two separately was tracking one fact twice.
	settled bool
	mode    string
	gen     uint64
	claimed bool // the one --take claim has been spent
}

func newOwnership(o Options) *ownership {
	own := &ownership{asked: o.Control, shared: o.Shared, others: o.OtherTypers,
		neverClaim: o.NeverClaim, take: o.Take, gen: o.Expected}
	if o.Control && o.Mode == terminal.ModeView {
		// --view is the user's instruction, not a request the server may
		// ignore. Holding it locally from the first byte is what makes it
		// true before any answer arrives, and true at all against a plane
		// that never answers — which is the plane that would otherwise
		// admit this attach as an unconditional controller and let it type.
		own.mode = terminal.ModeView
	}
	return own
}

// state returns the current mode and generation.
func (o *ownership) state() (string, uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.mode, o.gen
}

// mayType reports whether this attach should send input at all. An attach
// that never negotiated types — it is the only thing it can do — and a viewer
// does not, because the plane would drop it and the pty would discard it.
func (o *ownership) mayType() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.mode != terminal.ModeView
}

// stamp returns the generation to put on an outgoing frame: zero — and so
// absent from the wire — until a server has answered.
func (o *ownership) stamp() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.settled {
		return 0
	}
	return o.gen
}

// keyActive reports whether Ctrl-\ is this client's take-control key on this
// attach, or just another byte to forward. It becomes the key only once a
// server has proved it speaks conditional ownership, so a user attached to an
// older plane keeps every byte they had.
func (o *ownership) keyActive() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.settled
}

// observe folds one ownership message into this attach's state and returns
// the notice a person should see, if any. It is the whole of the client's
// ownership policy, in one place, so that "what did the CLI decide" is a
// question with one answer.
func (o *ownership) observe(m terminal.ServerMessage) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	was := o.mode
	first := !o.settled
	o.settled = true
	switch m.Type {
	case terminal.TypeAttached:
		o.mode = m.Mode
		o.gen = m.Generation.Value()
	case terminal.TypeStale:
		o.mode = terminal.ModeView
		o.gen = m.Generation.Value()
	case terminal.TypeControlChanged:
		o.mode = m.Mode
		o.gen = m.Generation.Value()
	default:
		return ""
	}
	if o.shared {
		// The take-over copy is wrong under a shared policy in every one of
		// its clauses — no other device holds anything, nothing was taken, and
		// the key it offers cannot succeed — so that policy's copy is decided
		// on its own, out of the same three facts.
		return o.sharedNotice(m, was, first)
	}
	switch m.Type {
	case terminal.TypeAttached:
		switch {
		case m.Mode == terminal.ModeView && first:
			if o.neverClaim {
				// --view asked to watch. Being told it is watching is not
				// news, and "another device has control" might not even be
				// true. A plain attach that RECONNECTED as a viewer also asked
				// for view mode, but it was superseded while its network was
				// out, and the contract says it comes back as a viewer and
				// says so.
				return ""
			}
			return NoticeViewing // the opening answer: somebody else is typing
		case m.Mode == terminal.ModeView:
			return NoticeTaken
		case was == terminal.ModeView:
			return NoticeHaveControl // a claim landed
		}
		return "" // the ordinary case: control, first thing, silently
	case terminal.TypeStale:
		return NoticeStale
	case terminal.TypeControlChanged:
		switch {
		case m.Mode == terminal.ModeView && was != terminal.ModeView:
			return NoticeTaken
		case m.Mode == terminal.ModeControl && was == terminal.ModeView:
			// The generation moved and this attach came out of it holding
			// control: a claim of its own won while somebody else's handoff
			// was announcing to it. The `attached` that answers that claim
			// arrives after this and will find the mode already `control`, so
			// this is the only place the person can be told.
			return NoticeHaveControl
		}
		return ""
	}
	return ""
}

// sharedNotice is the client's copy under control.PolicyShared. Callers hold
// o.mu and have already folded m into the state; was and first are what it was
// before.
//
// Four answers, and no fifth:
//
//   - the opening answer as a typer is SILENCE, or the one line naming the
//     other terminals that may type;
//   - the opening answer as a viewer is "this terminal may not type", which is
//     a host policy's answer about this person rather than a race — so it
//     offers no key. --view asked for it and is told nothing;
//   - becoming a typer again says so, which is the answer to a release this
//     client made or a revocation that ended;
//   - losing the ability to type says so without offering a key that cannot
//     take it back: under this policy no peer can do that to this attach, only
//     its own release and a plane-side revocation.
func (o *ownership) sharedNotice(m terminal.ServerMessage, was string, first bool) string {
	typing := o.mode != terminal.ModeView
	switch {
	case first && typing:
		return SharedNotice(o.others)
	case first:
		if o.neverClaim {
			return ""
		}
		return NoticeViewOnly
	case typing && was == terminal.ModeView:
		return NoticeHaveControl
	case !typing && was != terminal.ModeView:
		return NoticeViewOnly
	}
	return ""
}

// takeOnce reports whether --take should spend its one claim now, and marks
// it spent. A client that re-claimed every time it was refused would be two
// devices fighting over a keyboard instead of one person deciding.
//
// The FIRST answer spends it, whatever that answer said. --take is a flag on
// one attach, and an attach that opened holding control has already had what
// it asked for; leaving the claim unspent would fire it minutes later, when
// somebody else takes control, as a snatch-back nobody pressed a key for.
func (o *ownership) takeOnce() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.take || o.claimed || !o.settled {
		return false
	}
	o.claimed = true
	return o.mode == terminal.ModeView
}

// claim reports the claim this attach should send when the user presses the
// take-control key, and whether to send one at all. A client that already has
// control sends nothing: re-claiming from yourself would advance the
// generation and fence your own keystrokes for no reason anybody asked for.
//
// Neither does a --view attach, which is what makes "watch without ever
// claiming control" true rather than aspirational. The plane would honour the
// claim — a view-mode attach whose principal may drive is authorized to take
// control mid-attach, which is what a reconnecting controller admitted as a
// viewer depends on, so the service cannot tell the two apart — and --view is
// this user's instruction to their own client, so it is held here, in the one
// place that decides what this client claims. It covers the take-control key
// and --take's single claim alike.
//
// It reads neverClaim, not the requested mode. A plain attach that came back a
// viewer after a disconnect asks for view mode too, and taking its
// take-control key away for the rest of the process — silently, with no way
// back but detaching — is not what "reconnect is conditional" means.
//
// Ctrl-\ is therefore swallowed under --view rather than forwarded: a --view
// attach sends no input at all, so forwarding it would reach the same nowhere
// with more moving parts. Nothing is printed, which is what the key already
// does on a device that has control.
func (o *ownership) claim() (terminal.ClientMessage, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.neverClaim || !o.settled || o.mode == terminal.ModeControl {
		return terminal.ClientMessage{}, false
	}
	return terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(o.gen)}, true
}

// isOwnershipMessage reports whether a server message belongs to ownership
// rather than to the terminal. They are handled and never rendered: none of
// them is screen content.
func isOwnershipMessage(t string) bool {
	switch t {
	case terminal.TypeAttached, terminal.TypeStale, terminal.TypeControlChanged:
		return true
	}
	return false
}

// writeNotice prints one ownership line the way every other status line this
// package writes is printed: CR LF framed, because the terminal is in raw
// mode and a bare newline would leave the cursor mid-row.
func writeNotice(w io.Writer, notice string) {
	fmt.Fprintf(w, "\r\n%s\r\n", notice)
}

// controlKey is a key this package interprets rather than forwards.
type controlKey int

const (
	keyNone controlKey = iota
	keyDetach
	keyTake
)

// scanKeys finds the first byte in buf this package interprets, and says
// which one it is. It is pure, so the part with no socket in it is testable
// on its own — the same reason ScanDetach is.
func scanKeys(buf []byte, takeActive bool) (int, controlKey) {
	for i, b := range buf {
		switch {
		case b == detachKey:
			return i, keyDetach
		case b == takeKey && takeActive:
			return i, keyTake
		}
	}
	return -1, keyNone
}
