package driver

import (
	"context"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// GuestReconnectProof exchanges a validated challenge with the guest and returns
// its canonical signature. It must honor ctx, bound unauthenticated frames and
// never send configuration or replace the active relay. Errors are not exposed.
type GuestReconnectProof func(context.Context, runner.GuestReconnectChallenge) (string, error)

// GuestReconnectHost is an optional authorization port for a future reconnect
// listener. Implementing it does not advertise or enable reconnect capability.
type GuestReconnectHost interface {
	// AuthorizeGuestReconnect performs challenge and acceptance on one accepted
	// control connection, within a single five-second budget. sessionID is the
	// driver's assignment, never guest input. Proof is called only after challenge
	// scope is checked. Failures return a zero response and one fixed error code:
	// invalid, expired, fenced or unavailable. Success is committed authority, not
	// permission to skip fresh configuration or old-relay fencing.
	AuthorizeGuestReconnect(context.Context, string, GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error)
}
