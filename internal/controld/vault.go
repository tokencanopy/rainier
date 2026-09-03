// internal/controld/vault.go
package controld

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// The credential vault is the whole behavior layer over the credentials
// table: seal on the way in (storeGitHubCredential), open on the way out
// (mintGitCredential), and one lazy transition back (rejectCredential). It is
// pure over (Store, Config.SecretsKey) — no GitHub call lives here, by
// design (spec §4.2).
//
// The mint is OPTIMISTIC: a stored credential is handed out without asking
// GitHub whether it still works. Verifying per mint would add a round-trip to
// every git operation inside every session and still lose the race with a
// revocation that happens a second later. Verification happens where it is
// cheap and meaningful instead — at login, at refresh, and lazily when a real
// git operation observes an auth failure and reports it back
// (rejectCredential), which makes the NEXT operation refuse clearly and shows
// up in `rainier creds` immediately.
//
// Secret hygiene is the invariant every function here is written around: the
// token appears in exactly two places, the plaintext argument on the way in
// and the return value on the way out. It is never logged, never wrapped into
// an error, and never rendered into a response — the errors below carry an
// action for the human and nothing else.

// githubProvider is the provider key for GitHub credentials, and the only
// provider v0 stores. It exists as a constant so a typo can't create a second,
// silently-never-minted row.
const githubProvider = "github"

// ErrCredentialNeedsRefresh is what a mint refuses with when the stored
// credential has been observed to fail: the value is still there, but
// something rejected it, so using it again would just fail again. The message
// names the exact command that fixes it — every named-action error in this
// plan says this same string.
var ErrCredentialNeedsRefresh = errors.New(`github credential needs refresh — run: rainier login --refresh github`)

// ErrCredentialMissing is the other refusal: no credential at all for this
// user. It is deliberately distinct from ErrCredentialNeedsRefresh because
// the action differs — a first login, not a refresh — and a caller that
// conflated them would tell a user to refresh something they never had.
var ErrCredentialMissing = errors.New(`no github credential — run: rainier login`)

// storeGitHubCredential seals token under the fleet secrets key and upserts
// it as userID's GitHub credential, valid as of now, recording the scopes
// GitHub reported for it (informational — nothing branches on them; the
// login response warns about a missing `repo` and stores the credential
// anyway).
//
// The upsert is a whole-row replace, which is what makes `rainier login
// --refresh github` work: a row that a rejection left in needs_refresh comes
// back valid because the value behind it is new.
func (s *Server) storeGitHubCredential(ctx context.Context, userID, token, scopes string) error {
	ciphertext, nonce, err := Seal(s.cfg.SecretsKey, []byte(token))
	if err != nil {
		// Seal fails only when the OS entropy source is broken, and its
		// error says nothing about the plaintext — but wrap it with a
		// message that says nothing about it either, since this error is
		// logged by the caller.
		return fmt.Errorf("sealing the github credential: %w", err)
	}
	now := time.Now()
	return s.st.UpsertCredential(ctx, Credential{
		UserID:     userID,
		Provider:   githubProvider,
		Ciphertext: ciphertext,
		Nonce:      nonce,
		Status:     CredentialValid,
		Scopes:     scopes,
		ObtainedAt: now,
		// Just verified: GitHub itself accepted this token for the /user
		// call the exchange made a moment ago.
		LastVerifiedAt: now,
		LastUsedAt:     now,
		UpdatedAt:      now,
	})
}

// mintGitCredential returns userID's GitHub access token for one use, or the
// named-action error explaining why it can't. It is the read side of the
// vault and the only place a stored credential is ever unsealed.
//
// last_used_at is stamped on the way out, best-effort: it is bookkeeping for
// `rainier creds`, and failing a git operation because a timestamp write
// failed would trade a working clone for a tidier row. updated_at
// deliberately does not move — a mint is a read, and the edit clock is what
// tells "used" from "changed".
func (s *Server) mintGitCredential(ctx context.Context, userID string) (string, error) {
	c, err := s.st.GetCredential(ctx, userID, githubProvider)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return "", ErrCredentialMissing
		}
		return "", fmt.Errorf("loading the github credential: %w", err)
	}
	if c.Status != CredentialValid {
		return "", ErrCredentialNeedsRefresh
	}

	token, err := Open(s.cfg.SecretsKey, c.Ciphertext, c.Nonce)
	if err != nil {
		// Open's error is errSecretAuth — flat, and carrying no plaintext
		// (GCM authenticates before it has anything to hand back). This
		// happens when the fleet key changed under a stored row, which is an
		// operator-visible condition, not something the session can fix.
		return "", fmt.Errorf("opening the github credential: %w", err)
	}

	if err := s.st.TouchCredentialUsed(ctx, userID, githubProvider); err != nil {
		log.Printf("controld: stamping github credential use for user %s: %v", userID, err)
	}
	return string(token), nil
}

// rejectCredential records that provider rejected userID's credential: the
// row flips to needs_refresh, so the next mint refuses with the named action
// instead of handing out a token that is known not to work, and `rainier
// creds` shows the state right away.
//
// It returns nothing on purpose. Every caller is already on a failure path
// (a git operation whose stderr smelled like auth), has nothing useful to do
// with a second error, and must not turn "we couldn't record the rejection"
// into a different failure than the one the user actually hit. The flip is
// logged either way, because a credential silently going stale is exactly the
// thing an operator needs to see in the log.
func (s *Server) rejectCredential(ctx context.Context, userID, provider string) {
	err := s.st.SetCredentialStatus(ctx, userID, provider, CredentialNeedsRefresh)
	switch {
	case errors.Is(err, control.ErrNotFound):
		// Nothing to flip. A rejection for a credential that isn't there is
		// already the state we'd be moving toward, so this is a no-op, not a
		// problem.
		log.Printf("controld: %s credential rejected for user %s, but no credential is stored", provider, userID)
	case err != nil:
		log.Printf("controld: marking the %s credential needs_refresh for user %s: %v", provider, userID, err)
	default:
		log.Printf("controld: %s credential for user %s marked needs_refresh after an observed auth failure", provider, userID)
	}
}
