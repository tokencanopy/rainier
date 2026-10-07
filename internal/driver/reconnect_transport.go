package driver

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

var errGuestStream = errors.New("unavailable")

// AuthorizeGuestConnection exchanges one bounded challenge/proof on an already
// admitted guest stream using the runner's original-connection authorization
// port. session is the driver's assignment, never data read from the guest.
// Failure closes the stream and returns zero authority with a fixed code.
//
// Success does NOT write accepted/configuration, replace a relay, or transfer
// ownership of c. The caller must fence the original instance/placement/epoch,
// acquire fresh configuration and install the relay before publishing readiness.
// Keep the same Conn for that handoff: it may buffer bytes after the proof.
// There is deliberately no shipping listener caller until those gates exist.
//
// The caller must bound peer admission before invoking this synchronously. Like
// GuestReconnectHost, this helper does not detach a host that ignores context.
func AuthorizeGuestConnection(ctx context.Context, host GuestReconnectHost, session string, c relay.Conn) (runner.GuestReconnectAcceptResponse, error) {
	var zero runner.GuestReconnectAcceptResponse
	if c == nil {
		return zero, errGuestStream
	}
	success := false
	defer func() {
		if !success {
			_ = c.Close()
		}
	}()
	if host == nil || !guestStreamSession(session) || ctx.Err() != nil {
		return zero, errGuestStream
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	var invoked, proved, invalid atomic.Bool
	accepted, err := host.AuthorizeGuestReconnect(ctx, session, func(proofCtx context.Context, challenge runner.GuestReconnectChallenge) (string, error) {
		if !invoked.CompareAndSwap(false, true) {
			invalid.Store(true)
			return "", errGuestStream
		}
		body, _ := json.Marshal(challenge)
		if _, err := runner.DecodeGuestReconnectChallenge(body); err != nil || challenge.SessionID != session {
			invalid.Store(true)
			return "", errGuestStream
		}
		// Both the outer transport budget and the host's connection-bound proof
		// context must remain live. Neither may extend the other.
		pctx, pcancel := context.WithCancel(ctx)
		defer pcancel()
		pstop := context.AfterFunc(proofCtx, pcancel)
		defer pstop()
		if proofCtx.Err() != nil {
			return "", errGuestStream
		}
		if relay.WriteGuestReconnectFrame(pctx, c, relay.KindGuestReconnectChallenge, body) != nil {
			return "", errGuestStream
		}
		event, err := relay.ReadGuestReconnectFrame(pctx, c)
		if err != nil || event.Kind != relay.KindGuestReconnectProof {
			return "", errGuestStream
		}
		proof, err := runner.DecodeGuestReconnectAcceptRequest(event.Payload)
		if err != nil || proof.AttemptID != challenge.AttemptID || pctx.Err() != nil || proofCtx.Err() != nil {
			return "", errGuestStream
		}
		proved.Store(true)
		return proof.Signature, nil
	})
	if err != nil || !proved.Load() || invalid.Load() || ctx.Err() != nil {
		return zero, guestStreamError(err)
	}
	body, _ := json.Marshal(accepted)
	accepted, err = runner.DecodeGuestReconnectAcceptResponse(body)
	if err != nil || ctx.Err() != nil {
		return zero, errGuestStream
	}
	success = true
	return accepted, nil
}

func guestStreamError(err error) error {
	if err != nil {
		switch err.Error() {
		case "invalid", "expired", "fenced":
			return errors.New(err.Error())
		}
	}
	return errGuestStream
}
func guestStreamSession(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for i := range id {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}
