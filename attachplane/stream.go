package attachplane

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// This file is the client half of the plane behind one control port: the
// client's websocket as control.TerminalStream. Everything above it — who may
// attach, in which mode, and whether the session is attachable at all — is the
// attachment service's; everything below is this replica's sockets. Neither
// side reads a terminal message for anything but forwarding it, and no
// message, no byte of it, and no length of it is ever logged.

// ---------------------------------------------------------------------------
// the close vocabulary
// ---------------------------------------------------------------------------

// The four reasons a client socket is closed with, as sentinels rather than
// sentences, because the sentence is wsTerminalStream.Close's to pick: a
// close reason is a fixed, 123-byte-bounded string on the wire, and none of
// these may ever quote what the client or the runner said.
var (
	// errAttachFirstMessage is a client that did not open with the resize the
	// protocol requires (or did not open at all before the first-message
	// timeout). It is the client's own protocol error, so it closes with a
	// policy violation rather than an invitation to retry.
	errAttachFirstMessage = errors.New("controld: the first attach message must be a resize")
	// errAttachNoDialBack is the pairing TTL: the runner took the dial_attach
	// and never came back for the client.
	errAttachNoDialBack = errors.New("controld: no dial-back within the pairing TTL")
	// errAttachIDCollision is the refusal to overwrite another client's
	// parked pairing (see attachTable.park).
	errAttachIDCollision = errors.New("controld: attach id collision")
	// errAttachNotSpliced is a handoff attempted before the runner's
	// dial-back arrived: there is no sandbox socket to install a binding on
	// yet. Claims are only read inside the splice, which binds that socket
	// first, so it is unreachable from a client — it is what a peer's
	// displacement gets when that peer is still waiting for its own
	// dial-back, and the answer is to proceed: the generation has already
	// moved, and the binding rides the frame that opens that attachment.
	errAttachNotSpliced = errors.New("controld: the attach is not spliced yet")
	// errAttachEnded is an attach that ran and is over — one side of the
	// splice stopped, and the other is being closed after it.
	errAttachEnded = errors.New("controld: the attach ended")
)

// ---------------------------------------------------------------------------
// control.TerminalStream over the client socket
// ---------------------------------------------------------------------------

const (
	// defaultClientWriteBase is the part of one message's budget that does
	// not depend on its size. See Send: it is not a liveness budget for a
	// handoff (Plane.step is that, and it is seconds), it is the point at
	// which a socket that has taken NOTHING is closed rather than held.
	defaultClientWriteBase = 60 * time.Second
	// defaultClientWriteRate is the throughput a client has to sustain on
	// the payload of a large frame to keep its socket. It buys the rest of
	// the budget: without it the base would be a whole-write deadline, and
	// the largest frame attachReadLimit allows would demand ≈273 KB/s of a
	// client that is making perfectly steady progress — more than a 2 Mbit/s
	// link has. At this rate that frame gets 60s + 256s instead.
	//
	// It is deliberately a floor rather than an estimate of anybody's link.
	// A client slower than this over sixteen megabytes is not going to
	// render the scrollback either.
	defaultClientWriteRate = 64 << 10 // bytes per second
)

// ClientStream wraps an accepted client websocket as the control.TerminalStream
// the application (and this plane's broker) speaks. It also sets the socket's
// read limit: a snapshot replaying a large scrollback is the biggest frame
// either direction of the splice ever carries, and the plane owns that policy
// for both halves of it.
//
// The caller keeps the socket's own lifetime — a handler that accepted it
// still defers its CloseNow — and hands the reason it ends with to Close.
func ClientStream(c *websocket.Conn) control.TerminalStream {
	return clientStream(c, defaultClientWriteBase, defaultClientWriteRate)
}

// clientStream is the same over a write budget the caller picks, which is how
// a test drives the wedged-client path without spending a minute on it. The
// two knobs are FIELDS rather than package variables: nothing in the package
// takes t.Parallel() today, and a package variable three tests write is a
// race waiting for the first one that does.
func clientStream(c *websocket.Conn, base time.Duration, rate int) wsTerminalStream {
	c.SetReadLimit(attachReadLimit)
	return wsTerminalStream{c: c, once: &sync.Once{}, base: base, rate: rate}
}

// wsTerminalStream is the typed adapter between the client's websocket and
// control.TerminalStream. relay.Conn is raw frames; the port carries whole
// terminal.ClientMessage/ServerMessage values, and protocol/terminal IS the
// wire format, so the decode/encode across this boundary is lossless.
//
// close is the socket's one closing step, held by a sync.Once because two
// owners can reach it: the broker (a protocol error, or the pairing TTL) and
// the attachment service or the handler above it (any refusal after the
// upgrade). Whichever arrives first names the reason; a second close would
// otherwise fail on an already-closed socket and log a line saying nothing.
type wsTerminalStream struct {
	c    *websocket.Conn
	once *sync.Once
	// base and rate are this stream's write budget; see budget and Send.
	base time.Duration
	rate int
}

// budget is how long ONE message carrying payload bytes of terminal data may
// take. The fixed part is what a socket that has taken nothing gets; the rest
// is the time that payload needs at the slowest rate this stream will keep a
// client for. A message with no payload — every ownership message, every
// acknowledgement — gets exactly the base.
func (s wsTerminalStream) budget(payload int) time.Duration {
	if s.rate <= 0 {
		return s.base
	}
	return s.base + time.Duration(payload)*time.Second/time.Duration(s.rate)
}

var _ control.TerminalStream = wsTerminalStream{}

// Receive reads one client message. A read failure is the caller's to
// interpret: it is a client that hung up as often as it is a malformed frame,
// and neither is worth a log line.
func (s wsTerminalStream) Receive(ctx context.Context) (terminal.ClientMessage, error) {
	var m terminal.ClientMessage
	if err := wsjson.Read(ctx, s.c, &m); err != nil {
		return terminal.ClientMessage{}, err
	}
	return m, nil
}

// Send writes one server message to the client, under a deadline of its own.
//
// The deadline is what keeps a client that has stopped READING from holding
// the plane that is writing to it. A socket whose peer never drains it (a
// closed lid, a TCP zero window, a paused browser tab) accepts a little and
// then accepts nothing, and no RST ever arrives to end it — so an unbounded
// write to it is an unbounded wait, in whatever goroutine happened to be
// carrying it. The plane's handoffs no longer wait on any single client
// (Plane.step bounds those), and this is the other half: the socket itself is
// eventually closed rather than held open forever with a writer parked on it.
//
// It is deliberately far above any frame a live client could be slow with —
// the biggest thing this stream ever carries is a snapshot replaying a large
// scrollback, and a slow link must be able to take one, which is why the
// budget SCALES with the payload rather than being one number for every
// frame. What it catches is a peer that is not draining at all.
func (s wsTerminalStream) Send(ctx context.Context, m terminal.ServerMessage) error {
	wctx, cancel := context.WithTimeout(ctx, s.budget(len(m.Data)))
	defer cancel()
	if err := wsjson.Write(wctx, s.c, m); err != nil {
		if wctx.Err() != nil && ctx.Err() == nil {
			// THIS stream's budget ran out, not the caller's: the socket is
			// still open and has taken nothing for a minute, so end it.
			// CloseNow rather than a close frame, because a peer that will
			// not read a message will not read a close reason either — and it
			// takes the one close, so a later Close does not log a failure
			// that says nothing.
			//
			// coder/websocket also tears a connection down when a write
			// that is IN FLIGHT runs out of time, so on that path this is
			// belt and braces. It is here for the path the library cannot
			// see — a write that never acquired the conn's write lock, and
			// so never reached the socket at all — and because the rule this
			// stream owes its caller ("a client that has taken nothing for a
			// minute is closed") should not rest on another package's
			// implementation detail. TestAWedgedClientIsClosedRatherThanHeld
			// pins the property; which of the two closes does it is not
			// something a test can, or should, tell apart.
			//
			// The caller's own deadline expiring is a different thing
			// entirely. A handoff's courtesy notice carries seconds, and a
			// websocket serialises its writes, so such a notice queued behind
			// a large snapshot expires while waiting for the write lock —
			// without the socket having failed at anything. Closing there
			// would let one take-over on the session disconnect a client that
			// is merely reading a scrollback over a slow link.
			s.once.Do(func() { _ = s.c.CloseNow() })
		}
		return err
	}
	return nil
}

// Close ends the socket with the one close code and fixed reason its error
// maps to. Everything the service reports — and every failure the broker has
// no specific word for — is "try again later": the client's own next attach
// is the remedy, and the reason says which dependency to blame without
// quoting anybody.
func (s wsTerminalStream) Close(err error) error {
	code, reason := attachCloseReason(err)
	s.once.Do(func() { closeAttach(s.c, code, reason) })
	return nil
}

// attachCloseReason is the whole mapping, in one place: a client that broke
// the protocol or may not be here is a policy violation, and everything else
// is a retryable close naming the dependency.
func attachCloseReason(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, errAttachFirstMessage):
		return websocket.StatusPolicyViolation, "first attach message must be resize"
	case errors.Is(err, control.ErrDenied):
		return websocket.StatusPolicyViolation, "not authorized to attach to this session"
	case errors.Is(err, errAttachIDCollision):
		return websocket.StatusInternalError, "attach id collision"
	case errors.Is(err, errAttachNoDialBack):
		return websocket.StatusTryAgainLater, "runner did not dial back"
	case errors.Is(err, control.ErrConflict):
		return websocket.StatusTryAgainLater, "session not ready"
	case errors.Is(err, errAttachEnded):
		return websocket.StatusTryAgainLater, "the attach ended"
	default:
		return websocket.StatusTryAgainLater, "runner unreachable"
	}
}

// closeAttach closes an attach socket with a reason the protocol can actually
// carry. Close reasons cap at 123 bytes and a wrapped read error can exceed
// that; an over-long reason makes coder/websocket drop the close frame
// entirely, leaving the peer with a bare EOF instead of the diagnostic.
// Same discipline as closeRunner on the runner plane.
//
// It logs through the standard logger rather than a plane's Logf: a stream
// outlives no plane in particular (ClientStream is called on a socket before
// any broker sees it), and this line reports a socket that could not be
// closed, never anything either end said.
func closeAttach(c *websocket.Conn, code websocket.StatusCode, reason string) {
	const maxReason = 123
	if len(reason) > maxReason {
		reason = strings.ToValidUTF8(reason[:maxReason], "")
	}
	if err := c.Close(code, reason); err != nil {
		log.Printf("controld: closing attach socket: %v", err)
	}
}

// ---------------------------------------------------------------------------
// the attach cursor
// ---------------------------------------------------------------------------

// sinceKey carries the client's `since` from the HTTP handler to the broker.
// control.AttachTarget names the session, the placement and the two
// generations — not the cursor — and control is frozen, so the cursor travels
// beside the command rather than in it.
type sinceKey struct{}

// WithSince returns ctx carrying since as the attach cursor. The handler that
// parsed it sets it; the broker reads it back with Since when it mints the
// dial_attach.
func WithSince(ctx context.Context, since uint64) context.Context {
	return context.WithValue(ctx, sinceKey{}, since)
}

// Since is the cursor the handler parsed, or 0 — the same answer a malformed
// `since` gets on the wire, which costs a full replay and never an error the
// client could act on.
func Since(ctx context.Context) uint64 {
	since, _ := ctx.Value(sinceKey{}).(uint64)
	return since
}
