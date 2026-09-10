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
	asked     bool // this attach advertised the capability
	askedView bool // --view: this attach asked never to claim
	take      bool // --take: claim once if it comes back a viewer

	mu       sync.Mutex
	settled  bool // the server answered, so it speaks conditional ownership
	answered bool // an ownership message has arrived, whatever it said
	mode     string
	gen      uint64
	claimed  bool // the one --take claim has been spent
}

func newOwnership(o Options) *ownership {
	own := &ownership{asked: o.Control, take: o.Take, gen: o.Expected}
	if o.Control && o.Mode == terminal.ModeView {
		// --view is the user's instruction, not a request the server may
		// ignore. Holding it locally from the first byte is what makes it
		// true before any answer arrives, and true at all against a plane
		// that never answers — which is the plane that would otherwise
		// admit this attach as an unconditional controller and let it type.
		own.askedView = true
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
	first := !o.answered
	o.settled, o.answered = true, true
	switch m.Type {
	case terminal.TypeAttached:
		o.mode = m.Mode
		o.gen = m.Generation.Value()
		switch {
		case m.Mode == terminal.ModeView && first:
			if o.askedView {
				// It asked to watch. Being told it is watching is not news,
				// and "another device has control" might not even be true.
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
		o.mode = terminal.ModeView
		o.gen = m.Generation.Value()
		return NoticeStale
	case terminal.TypeControlChanged:
		o.mode = m.Mode
		o.gen = m.Generation.Value()
		if m.Mode == terminal.ModeView && was != terminal.ModeView {
			return NoticeTaken
		}
		return ""
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
	if !o.take || o.claimed || !o.answered {
		return false
	}
	o.claimed = true
	return o.mode == terminal.ModeView
}

// claim reports the claim this attach should send when the user presses the
// take-control key, and whether to send one at all. A client that already has
// control sends nothing: re-claiming from yourself would advance the
// generation and fence your own keystrokes for no reason anybody asked for.
func (o *ownership) claim() (terminal.ClientMessage, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.settled || o.mode == terminal.ModeControl {
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
