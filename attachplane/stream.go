package attachplane

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"

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
	// errAttachEnded is an attach that ran and is over — one side of the
	// splice stopped, and the other is being closed after it.
	errAttachEnded = errors.New("controld: the attach ended")
)

// ---------------------------------------------------------------------------
// control.TerminalStream over the client socket
// ---------------------------------------------------------------------------

// ClientStream wraps an accepted client websocket as the control.TerminalStream
// the application (and this plane's broker) speaks. It also sets the socket's
// read limit: a snapshot replaying a large scrollback is the biggest frame
// either direction of the splice ever carries, and the plane owns that policy
// for both halves of it.
//
// The caller keeps the socket's own lifetime — a handler that accepted it
// still defers its CloseNow — and hands the reason it ends with to Close.
func ClientStream(c *websocket.Conn) control.TerminalStream {
	c.SetReadLimit(attachReadLimit)
	return wsTerminalStream{c: c, once: &sync.Once{}}
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

// Send writes one server message to the client.
func (s wsTerminalStream) Send(ctx context.Context, m terminal.ServerMessage) error {
	return wsjson.Write(ctx, s.c, m)
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
