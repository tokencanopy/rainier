package controlapp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// GuestReconnect authorizes continuity of an enrolled guest. Callers supply an
// authenticated workload scope and (for hosted use) hold current membership and
// policy authorization. This service does not attach relays or persist RAM.
// Driver capability advertisement must wait for both host and guest integration.
type GuestReconnect struct {
	Store control.GuestReconnectStore
	Clock control.Clock
}

func (s GuestReconnect) available() bool { return s.Store != nil && s.Clock != nil }

// Enroll binds a process-memory public key to a fresh, single-use boot token.
// Private keys never cross this interface. Failed enrollment cannot downgrade
// to a reconnect path that merely replays the old token.
func (s GuestReconnect) Enroll(ctx context.Context, b control.GuestReconnectScope, token, bootEpoch, publicKey string) error {
	if !s.available() {
		return control.ErrUnavailable
	}
	if len(publicKey) != 43 || len(token) != 43 {
		return control.ErrReconnectInvalid
	}
	key, err := base64.RawURLEncoding.Strict().DecodeString(publicKey)
	if err != nil || len(key) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(key) != publicKey || len(token) != 43 {
		return control.ErrReconnectInvalid
	}
	// Use the same field validation as the signed protocol, not a second
	// interpretation of which boot identities can later be proved.
	probe := reconnectChallenge(b, control.GuestReconnectIdentity{BootEpoch: bootEpoch}, control.GuestReconnectAttempt{ID: "validate", Challenge: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"})
	if _, err := probe.SigningMessage(); err != nil {
		return control.ErrReconnectInvalid
	}
	return s.Store.EnrollGuest(ctx, b, HashSessionBootstrapToken(token), control.GuestReconnectIdentity{BootEpoch: bootEpoch, PublicKey: publicKey}, s.Clock.Now())
}

// Begin mints a bounded pending attempt. It cannot evict an active relay or
// replace a bootstrap token. A new attempt invalidates only the prior challenge.
func (s GuestReconnect) Begin(ctx context.Context, b control.GuestReconnectScope) (runner.GuestReconnectChallenge, error) {
	if !s.available() {
		return runner.GuestReconnectChallenge{}, control.ErrUnavailable
	}
	attemptID, err := reconnectRandom()
	if err != nil {
		return runner.GuestReconnectChallenge{}, err
	}
	nonce, err := reconnectRandom()
	if err != nil {
		return runner.GuestReconnectChallenge{}, err
	}
	// PostgreSQL stores microseconds. Issuance and returned deadline use that
	// same precision so a database round trip cannot change the CAS operand.
	now := s.Clock.Now().UTC().Truncate(time.Microsecond)
	a := control.GuestReconnectAttempt{ID: attemptID, Challenge: nonce, ExpiresAt: now.Add(5 * time.Second)}
	id, err := s.Store.BeginGuestReconnect(ctx, b, a, now)
	if err != nil {
		return runner.GuestReconnectChallenge{}, err
	}
	c := reconnectChallenge(b, id, a)
	if _, err := c.SigningMessage(); err != nil {
		return runner.GuestReconnectChallenge{}, control.ErrReconnectInvalid
	}
	return c, nil
}

// Accept verifies the exact durable challenge, then atomically spends it,
// advances connection epoch and replaces the bootstrap capability. A lost reply
// must start a NEW challenge; no successful response is cached or replayable.
func (s GuestReconnect) Accept(ctx context.Context, b control.GuestReconnectScope, attemptID, signature string) (uint64, string, error) {
	if !s.available() {
		return 0, "", control.ErrUnavailable
	}
	if len(attemptID) != 43 || len(signature) != 86 {
		return 0, "", control.ErrReconnectInvalid
	}
	id, a, err := s.Store.ReadGuestReconnect(ctx, b, attemptID)
	if err != nil {
		return 0, "", err
	}
	if err := reconnectChallenge(b, id, a).VerifyProof(id.PublicKey, signature); err != nil {
		return 0, "", control.ErrReconnectInvalid
	}
	token, err := reconnectRandom()
	if err != nil {
		return 0, "", err
	}
	now := s.Clock.Now()
	epoch, err := s.Store.ConsumeGuestReconnect(ctx, b, id, a, control.SessionBootstrap{Hash: HashSessionBootstrapToken(token), PlacementGeneration: b.PlacementGeneration, ExpiresAt: now.Add(SessionBootstrapTTL)}, now)
	if err != nil {
		return 0, "", err
	}
	return epoch, token, nil
}

func reconnectChallenge(b control.GuestReconnectScope, id control.GuestReconnectIdentity, a control.GuestReconnectAttempt) runner.GuestReconnectChallenge {
	return runner.GuestReconnectChallenge{Protocol: runner.GuestReconnectProtocol, SessionID: string(b.SessionID), BootEpoch: id.BootEpoch, HostIncarnation: strconv.FormatUint(b.ConnectionGeneration, 10), AttemptID: a.ID, PlacementGeneration: b.PlacementGeneration, Challenge: a.Challenge}
}

func reconnectRandom() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", control.ErrUnavailable
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
