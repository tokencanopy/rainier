package controlapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// The session bootstrap token: what a microVM session presents, once, to be
// given the environment secrets its Spec.Env deliberately does not carry.
// See docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md §3.
//
// It is opaque to every hop that carries it — not a JWT, carrying no claims —
// because the control plane holds the state. What a hop can do with it is
// therefore exactly nothing except hand it on.

// SessionBootstrapTTL is how long a minted token lives. It must cover a base
// microVM claim, a guest boot and one round trip; the note's open question 1
// is whether 120 seconds is the right number, and until Phase 1 measures it
// this constant is the one place to change it.
const SessionBootstrapTTL = 120 * time.Second

// sessionBootstrapTokenBytes is the token's entropy: 32 bytes from
// crypto/rand. Base64url-encoded it is 43 characters with no padding, and it
// is never truncated, prefixed or decorated — a token with a readable prefix
// is a token somebody grep's a log for.
const sessionBootstrapTokenBytes = 32

// HashSessionBootstrapToken is the one-way function the store keeps a token
// under: hex-encoded SHA-256, the same shape the bearer-token hash already
// has. It is exported because the mint and the exchange are answered in two
// different packages and must agree byte for byte; a second spelling would be
// a token that can never be redeemed.
func HashSessionBootstrapToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// SessionBootstrapMinter mints and records one session's bootstrap token.
//
// It is a value rather than a service because it holds no policy: the TTL is
// a constant above it, the fence is the caller's placement generation, and
// the only decision it makes is that the plaintext leaves here and is stored
// nowhere. Two callers share it — the scheduler, at create, and the host's
// answer to a runner's mint_session_bootstrap on a cold resume — and they
// share it precisely so a resume's token cannot end up minted differently
// from a create's.
type SessionBootstrapMinter struct {
	Store control.SessionBootstrapStore
	Clock control.Clock
}

// Mint returns a fresh token for id and records its hash against gen with an
// expiry SessionBootstrapTTL from now, replacing any token id already had.
//
// The plaintext is returned and never retained: it exists in this process
// only long enough to be copied into the message that carries it, which is
// the same rule the git credential mint follows.
//
// A store that cannot record the hash is a hard failure, not a token minted
// anyway. The alternative — handing a sandbox a capability nothing can ever
// verify — is a create that boots and then cannot get its secrets, reported
// as a healthy session.
func (m SessionBootstrapMinter) Mint(ctx context.Context, ws control.WorkspaceID, id control.SessionID, gen uint64) (string, error) {
	if m.Store == nil || m.Clock == nil {
		return "", errors.New("controlapp: this control plane cannot mint a session bootstrap token")
	}
	b := make([]byte, sessionBootstrapTokenBytes)
	if _, err := rand.Read(b); err != nil {
		// A broken entropy source has no safe fallback: every caller needs an
		// unguessable capability, and a predictable one is worse than none.
		return "", errors.New("controlapp: the session bootstrap token could not be minted")
	}
	token := base64.RawURLEncoding.EncodeToString(b)
	if err := m.Store.PutSessionBootstrap(ctx, ws, id, control.SessionBootstrap{
		Hash:                HashSessionBootstrapToken(token),
		PlacementGeneration: gen,
		ExpiresAt:           m.Clock.Now().Add(SessionBootstrapTTL),
	}); err != nil {
		return "", err
	}
	return token, nil
}

// SessionBootstrapRefusal renders one of control's four bootstrap sentinels
// as the sentence a sandbox is given, and reports whether err was one of
// them at all.
//
// The four sentences are deliberately different from each other and
// deliberately identical in what they withhold: none names a token, a secret,
// a name, or another session. They say which condition holds so that a person
// reading a failed boot can tell "this token has already been used" from
// "this session was placed again while it was booting" — two facts with
// completely different remedies — and nothing more.
//
// Anything that is not one of the four is NOT rendered here: the caller
// answers with its own flat sentence, because an error nobody wrote to be
// shown may quote a row, a column, or a value.
func SessionBootstrapRefusal(err error) (string, bool) {
	switch {
	case errors.Is(err, control.ErrBootstrapUnknown):
		return "this session has no bootstrap token matching the one presented", true
	case errors.Is(err, control.ErrBootstrapSpent):
		return "this session's bootstrap token has already been exchanged", true
	case errors.Is(err, control.ErrBootstrapExpired):
		return "this session's bootstrap token has expired", true
	case errors.Is(err, control.ErrBootstrapFenced):
		return "this session has been placed again since its bootstrap token was minted", true
	}
	return "", false
}
