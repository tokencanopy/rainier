// internal/controld/agentvault.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
)

// The self-hosted agent credential store: controlapp.AgentCredentialStore
// over the agent_credentials table and seal.go. It is the vault's second
// tenant — the git credential is the first (vault.go) — and it is written
// under exactly the same rule: the plaintext appears in two places, the
// argument on the way in and the return value on the way out, and NOWHERE
// else. Not in a log line, not in an error, not wrapped into one, not in a
// metric, not in test output. Every error below says what happened and never
// what it happened to.
//
// What this file adds over vault.go is BINDING. A git credential is one row
// per (user, provider) and nothing distinguishes one version of it from
// another; an agent credential set has a version, the sandbox holds a copy of
// it, and custody hands versions back and forth over an RPC a runner speaks.
// So the seal binds the identity into the ciphertext itself:
//
//	aad = user + "\x00" + provider + "\x00" + decimal(version)
//
// A row copied to another person, to another provider, or edited to claim
// another version does not decrypt — it fails authentication, exactly as a
// wrong key does, and the failure is indistinguishable from one. The NULs
// keep ("a", "bc") and ("ab", "c") apart, the same reason AgentHomeVolume has
// one.
//
// Which makes the ORDER of a put the whole design problem: the version is
// assigned by the write, and the bytes must be sealed for the version the row
// will have BEFORE the write can carry them. The resolution is a
// compare-and-set rather than a read-then-write: the vault reads the current
// version, seals for one past it, and hands the store a row the store accepts
// only if that is still one past what it holds (Store.PutAgentCredential). A
// racing put loses the CAS and loops — it re-reads, re-seals for the version
// it now sees, and tries again — so a blob sealed for a version the row never
// had can never be stored, and last-writer-wins still holds. The alternative,
// letting the store assign the version and sealing afterwards, would leave a
// window in which the row's ciphertext and its version disagree, and every
// fetch in that window would fail authentication for a credential nothing was
// wrong with.

// agentPutAttempts bounds the compare-and-set loop. Contention here is two
// sessions of ONE person writing ONE provider's set in the same instant,
// which is already the rare case A4 probed; three retries past the first
// attempt is far beyond what that costs, and a bound is what keeps a
// misbehaving store from turning a put into a spin.
const agentPutAttempts = 4

// AgentVault is the self-hosted controlapp.AgentCredentialStore: plaintext
// file maps at its edges, sealed rows behind it.
type AgentVault struct {
	rows AgentCredentialRows
	key  [32]byte
}

var _ controlapp.AgentCredentialStore = (*AgentVault)(nil)

// NewAgentVault builds the store over rows and the fleet secrets key. It
// takes the narrow four-method port rather than the whole store: custody has
// no business reaching a bearer token, a secret, or a session row.
func NewAgentVault(rows AgentCredentialRows, key [32]byte) *AgentVault {
	return &AgentVault{rows: rows, key: key}
}

// agentCredentialBlob is the sealed plaintext: the provider's files by name.
// Go encodes a []byte value as base64, so the JSON is the design's
// {"files": {name: base64}} without a conversion of its own.
//
// It is a named type rather than an inline map so the wrapper object is
// stable: a later field (a stamp, a format marker) can be added to the
// plaintext without every previously sealed row becoming unreadable, which a
// bare map would not have allowed.
type agentCredentialBlob struct {
	Files map[string][]byte `json:"files"`
}

// errAgentCredentialSeal is every cryptographic and encoding failure on this
// path, flattened to one flat sentence. The distinction between "the fleet
// key changed under this row", "the stored bytes were tampered with", and
// "the plaintext would not encode" is not something a caller can act on
// differently, and the underlying errors are the ones whose text could quote
// a value.
var errAgentCredentialSeal = errors.New("controld: the agent credential could not be sealed or opened")

// FetchAgentCredentials opens userID's set for provider. A set nobody has put
// — or one a revoke destroyed — is version 0 with no files and no error: "you
// have not logged this agent in" is an answer, and the sandbox starts the
// agent anyway so the person can log in.
//
// A row that will not open is NOT reported as version 0. That would silently
// tell a person who is logged in that they are not, and would hide exactly
// the operator-visible condition (a changed fleet key, an edited row) worth
// seeing.
func (v *AgentVault) FetchAgentCredentials(ctx context.Context, user control.ActorID, provider string) (controlapp.AgentCredentialSet, error) {
	row, err := v.rows.GetAgentCredential(ctx, string(user), provider)
	switch {
	case errors.Is(err, control.ErrNotFound):
		return controlapp.AgentCredentialSet{}, nil
	case err != nil:
		return controlapp.AgentCredentialSet{}, err
	}
	plaintext, err := OpenAAD(v.key, row.Ciphertext, row.Nonce,
		agentCredentialAAD(string(user), provider, row.Version))
	if err != nil {
		return controlapp.AgentCredentialSet{}, errAgentCredentialSeal
	}
	var blob agentCredentialBlob
	if err := json.Unmarshal(plaintext, &blob); err != nil {
		// The error is DROPPED rather than wrapped: json's own message quotes
		// the bytes it choked on, and those bytes are the credential.
		return controlapp.AgentCredentialSet{}, errAgentCredentialSeal
	}
	files := blob.Files
	if files == nil {
		files = map[string][]byte{}
	}
	return controlapp.AgentCredentialSet{Version: row.Version, Files: files}, nil
}

// PutAgentCredentials seals files for the version the row is about to have
// and stores them, returning that version. See this file's header for why the
// read, the seal, and the write are one compare-and-set loop rather than
// three independent steps.
func (v *AgentVault) PutAgentCredentials(ctx context.Context, user control.ActorID, provider string, files map[string][]byte) (uint64, error) {
	// Encoded once, outside the loop: the plaintext does not depend on the
	// version, only the seal does, so a retry re-seals without rebuilding it.
	plaintext, err := json.Marshal(agentCredentialBlob{Files: nonNilFiles(files)})
	if err != nil {
		return 0, errAgentCredentialSeal
	}

	for attempt := 0; attempt < agentPutAttempts; attempt++ {
		var stored uint64
		cur, err := v.rows.GetAgentCredential(ctx, string(user), provider)
		switch {
		case err == nil:
			stored = cur.Version
		case errors.Is(err, control.ErrNotFound):
			// stored stays 0: the first put of a set is version 1.
		default:
			return 0, err
		}

		next := stored + 1
		ciphertext, nonce, err := SealAAD(v.key, plaintext,
			agentCredentialAAD(string(user), provider, next))
		if err != nil {
			return 0, errAgentCredentialSeal
		}
		version, err := v.rows.PutAgentCredential(ctx, AgentCredential{
			UserID: string(user), Provider: provider,
			Ciphertext: ciphertext, Nonce: nonce, Version: next,
		})
		if errors.Is(err, control.ErrConflict) {
			// Somebody else's put landed between the read and the write. Our
			// bytes are sealed for a version that is now taken, so they are
			// discarded and re-sealed against what the row actually holds.
			continue
		}
		if err != nil {
			return 0, err
		}
		return version, nil
	}
	return 0, control.ErrConflict
}

// RevokeAgentCredentials destroys the set. It is idempotent by construction:
// the store's delete reports nothing for a row that is not there, because
// "there is no credential" is the state the caller asked for either way.
func (v *AgentVault) RevokeAgentCredentials(ctx context.Context, user control.ActorID, provider string) error {
	return v.rows.DeleteAgentCredential(ctx, string(user), provider)
}

// ListAgentCredentials renders one status per stored set. It never opens a
// row — the store's listing does not even read the sealed columns — so there
// is no path here on which a listing could produce a byte of a credential.
func (v *AgentVault) ListAgentCredentials(ctx context.Context, user control.ActorID) ([]controlapp.AgentCredentialStatus, error) {
	rows, err := v.rows.ListAgentCredentials(ctx, string(user))
	if err != nil {
		return nil, err
	}
	out := make([]controlapp.AgentCredentialStatus, 0, len(rows))
	for _, r := range rows {
		out = append(out, controlapp.AgentCredentialStatus{
			Provider: r.Provider, Version: r.Version, UpdatedAt: r.UpdatedAt,
		})
	}
	return out, nil
}

// agentCredentialAAD builds the bytes a row is bound to. It is a function
// rather than three inlined concatenations so the seal side and the open side
// cannot drift: one spelling, used twice.
func agentCredentialAAD(user, provider string, version uint64) []byte {
	return []byte(user + "\x00" + provider + "\x00" + strconv.FormatUint(version, 10))
}

// nonNilFiles turns a nil map into an empty one, so an empty set seals as
// {"files":{}} rather than {"files":null}. A put of no files is a real state
// — the agent removed its own credential file — and it must round-trip as
// "logged in, holding nothing" rather than as a JSON null somebody has to
// decide what to do with.
func nonNilFiles(files map[string][]byte) map[string][]byte {
	if files == nil {
		return map[string][]byte{}
	}
	return files
}
