// internal/controld/seal.go
package controld

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// Secrets are sealed with AES-256-GCM under a single fleet-wide key the
// operator supplies as RAINIER_SECRETS_KEY (design §3): the store only ever
// holds ciphertext and a nonce, and the key never leaves controld's memory.
// Losing the key loses secret values and nothing else — every other row in
// the database is plaintext.
//
// This file is the whole cryptographic surface, deliberately: stdlib only,
// one key, one algorithm, no key derivation and no versioning byte. v0 has
// exactly one key, and a scheme nobody can misread is worth more here than
// one that's ready for a rotation story that hasn't been designed yet
// (design §5 makes v0 rotation a manual re-PUT).
//
// Seal/Open are the plain pair a secret and a git credential use. SealAAD/
// OpenAAD are the same primitive with additional authenticated data — bytes
// that are neither encrypted nor stored, but without which the ciphertext
// does not open. That is how the agent vault binds a row to the (user,
// provider, version) it belongs to. Seal and Open ARE SealAAD and OpenAAD
// with a nil aad, so there is still exactly one code path and every value
// sealed before the pair existed still opens.
const (
	// secretsKeyHexLen is the exact length of RAINIER_SECRETS_KEY: 64 hex
	// characters, which is 32 bytes, which is AES-256. Anything else is a
	// configuration error, never a shorter key silently accepted.
	secretsKeyHexLen = 64
	// secretsNonceLen is AES-GCM's standard nonce size, and the only one
	// this package accepts on the way back in.
	secretsNonceLen = 12
)

// errSecretAuth is what Open returns for every authentication failure —
// wrong key, tampered ciphertext, tampered nonce. The distinction is not
// something a caller can act on differently, and the underlying detail says
// nothing useful, so one flat error keeps the log line honest and quiet.
var errSecretAuth = errors.New("controld: secret failed authentication (wrong key, or the stored value was tampered with)")

// ParseSecretsKey parses the operator-supplied RAINIER_SECRETS_KEY: exactly
// 64 lowercase-or-uppercase hex characters, decoding to the 32-byte AES-256
// key. Its errors are printed at startup and must therefore never echo the
// key material itself — they say what was wrong with the shape, not what was
// supplied.
//
// An all-zero key is rejected too. It is a fine 32 bytes as far as hex is
// concerned, but Config's zero value is how "no key configured" is
// represented, so accepting it would produce the one failure an operator
// cannot debug: a key that was definitely set, and a server that insists no
// key was set.
func ParseSecretsKey(hexKey string) ([32]byte, error) {
	var key [32]byte
	if len(hexKey) != secretsKeyHexLen {
		return key, fmt.Errorf("controld: RAINIER_SECRETS_KEY must be exactly %d hex characters (32 bytes), got %d",
			secretsKeyHexLen, len(hexKey))
	}
	raw, err := hex.DecodeString(hexKey)
	if err != nil {
		// hex.DecodeString's own error quotes the offending byte; that is
		// key material, so it is dropped rather than wrapped.
		return [32]byte{}, errors.New("controld: RAINIER_SECRETS_KEY must be hex characters only (generate one with: openssl rand -hex 32)")
	}
	copy(key[:], raw)
	if key == ([32]byte{}) {
		return [32]byte{}, errors.New("controld: RAINIER_SECRETS_KEY must not be all zeros (it is indistinguishable from an unset key)")
	}
	return key, nil
}

// Seal encrypts plaintext under key with AES-256-GCM, returning the
// ciphertext (which carries GCM's authentication tag) and the fresh 12-byte
// nonce it was sealed with. Both are opaque bytes to the store.
//
// The nonce is drawn from crypto/rand on every call and never reused: a
// repeated nonce under one GCM key is a total break, not a degradation, so
// this function has no "caller supplies the nonce" variant to misuse.
func Seal(key [32]byte, plaintext []byte) (ciphertext, nonce []byte, err error) {
	return SealAAD(key, plaintext, nil)
}

// SealAAD is Seal with additional authenticated data: aad is not encrypted
// and not stored, but the ciphertext will not open without exactly the same
// bytes. It is how a sealed row is BOUND to the identity it was sealed for —
// the agent vault binds (user, provider, version), so a row copied to another
// person, another provider, or another version is not a credential, it is
// noise that fails authentication (agentvault.go).
//
// A nil aad is identical to Seal, which is why the two share this
// implementation rather than growing a second one: a value sealed before this
// function existed still opens, and always will.
func SealAAD(key [32]byte, plaintext, aad []byte) (ciphertext, nonce []byte, err error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}
	nonce = make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("controld: generating secret nonce: %w", err)
	}
	return aead.Seal(nil, nonce, plaintext, aad), nonce, nil
}

// Open decrypts and authenticates a value sealed by Seal. Every failure —
// the wrong key, a flipped ciphertext bit, a swapped or wrong-sized nonce —
// is errSecretAuth, and no failure ever returns partial plaintext: GCM
// authenticates before this function has anything to hand back.
func Open(key [32]byte, ciphertext, nonce []byte) ([]byte, error) {
	return OpenAAD(key, ciphertext, nonce, nil)
}

// OpenAAD is Open for a value sealed by SealAAD: it authenticates aad along
// with the ciphertext, so the WRONG aad is indistinguishable from the wrong
// key or a tampered byte — one flat errSecretAuth, and never partial
// plaintext.
func OpenAAD(key [32]byte, ciphertext, nonce, aad []byte) ([]byte, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(nonce) != aead.NonceSize() {
		return nil, errSecretAuth
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, errSecretAuth
	}
	return plaintext, nil
}

// newGCM builds the AES-256-GCM AEAD for key. Both constructors can only
// fail on a wrong key size, which a [32]byte makes unrepresentable — the
// errors are wrapped rather than ignored anyway, since silently returning a
// nil AEAD would be far worse than a startup-visible error.
func newGCM(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("controld: building AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("controld: building AES-GCM: %w", err)
	}
	return aead, nil
}
