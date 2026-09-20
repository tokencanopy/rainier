package control

import (
	"context"
	"errors"
	"time"
)

// SessionBootstrap is the control plane's whole memory of one minted session
// bootstrap token: the hash of the token, the placement it was minted under,
// and when it stops being one. The token itself is put on the wire once and
// never stored — a store that held it would be a store that could hand an
// environment's secrets to anyone who could read a row.
//
// There is no "consumed" field here because a caller never writes one: the
// spend is the store's own atomic step (ConsumeSessionBootstrap), and a
// record a caller could mark spent is a record two callers could both spend.
type SessionBootstrap struct {
	// Hash is the hex-encoded SHA-256 of the token. Hex rather than raw
	// bytes so a durable store keeps one comparable column and no encoding
	// decision of its own — the same shape the bearer-token hash already has.
	Hash string
	// PlacementGeneration is the generation the sandbox this token was minted
	// for belongs to. It is the fence: a session re-placed onto another
	// runner has moved past it, and the token minted for the sandbox that no
	// longer exists stops being an answer.
	PlacementGeneration uint64
	// ExpiresAt is when the token stops being one, whatever else is true of
	// it. Absolute rather than a TTL because the store is the authority on
	// "now" for every replica reading the row.
	ExpiresAt time.Time
}

// The four ways a bootstrap exchange is refused. They are four sentinels
// rather than one because each names a different fact about the fleet, and a
// single "no" would make a routine cold resume (fenced) and a replay attempt
// (spent) indistinguishable in an operator's log — while the sandbox still
// gets the same closed answer either way.
//
// None of them, and nothing wrapping them, may carry a token or a secret
// value: a refusal names a condition and a session, never a credential.
var (
	// ErrBootstrapUnknown reports that the session has no minted token whose
	// hash matches the one presented. It is also the answer for ANOTHER
	// session's token, because the lookup is keyed by the session the
	// placement guard read and never by anything in the request.
	ErrBootstrapUnknown = errors.New("control: bootstrap token unknown")

	// ErrBootstrapSpent reports a token that matched and has already been
	// exchanged. Single-use is the design's decision: a second boot inside
	// one VM asks for a second mint rather than replaying the first token.
	ErrBootstrapSpent = errors.New("control: bootstrap token already used")

	// ErrBootstrapExpired reports a token whose ExpiresAt has passed.
	ErrBootstrapExpired = errors.New("control: bootstrap token expired")

	// ErrBootstrapFenced reports a token whose PlacementGeneration is not the
	// row's current one: the session has been placed again since, so the
	// sandbox this token was minted for is not the sandbox asking.
	ErrBootstrapFenced = errors.New("control: bootstrap token superseded by a newer placement")
)

// SessionBootstrapStore is the persistence behind the one-shot token a
// microVM session exchanges for its environment's decrypted secrets. It is
// its own port, sized to two methods, for the reason every other port here is
// sized to its job: minting and spending a capability has nothing to do with
// a session's lifecycle, and a repository that could do both would put "hand
// out this workspace's secrets" one method away from "list sessions".
type SessionBootstrapStore interface {
	// PutSessionBootstrap records b as id's ONLY acceptable token, replacing
	// whatever was recorded before — a fresh mint invalidates its
	// predecessor, which is what makes a cold resume's new token the only one
	// that works. ErrInvalid on an empty workspace, session, or hash.
	PutSessionBootstrap(ctx context.Context, ws WorkspaceID, id SessionID, b SessionBootstrap) error

	// ConsumeSessionBootstrap spends id's token, atomically with respect to
	// every other caller, and reports which of the four refusals applies when
	// it cannot. gen is the session row's CURRENT placement generation, read
	// by the caller under the same guard that authorized the request, and now
	// is the caller's clock.
	//
	// The atomicity is the whole contract: two exchanges racing on one token
	// must produce exactly one success, or single-use means nothing. An
	// implementation that reads the record and then writes it back is not
	// this method, whatever its tests say on an idle machine.
	//
	// It returns nothing but an error on purpose. The VALUES the token buys
	// are not the store's to know — they are resolved above it, from the
	// environment, by the one component that holds the secrets key.
	ConsumeSessionBootstrap(ctx context.Context, ws WorkspaceID, id SessionID, hash string, gen uint64, now time.Time) error
}
