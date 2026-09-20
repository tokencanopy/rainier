package checkpoint

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
)

// cryptoRand is the default random source for every value this package draws:
// the data key, the frame nonce prefix, the content object's attempt suffix, and
// a key-wrapping nonce. It is a variable rather than a direct use of rand.Reader
// only so the golden-manifest test can hand a deterministic stream to the two
// constructors that accept one; nothing exported can change it.
var cryptoRand io.Reader = rand.Reader

// The envelope, two levels, which is what the tenancy specification's §11 asks
// for:
//
//  1. A DATA KEY, 32 bytes from crypto/rand, generated per checkpoint, used for
//     exactly one content object and one manifest, never stored.
//  2. Two purpose-separated subkeys derived from it with HKDF-SHA256, so that
//     the manifest tag's nonce can never collide with a frame nonce under one
//     key. The authenticated context is the HKDF salt, which is a binding that
//     costs nothing.
//  3. The data key WRAPPED by a key service under a versioned key reference,
//     with the authenticated context as additional data. Only the wrapped form
//     is stored, in the manifest.
//
// The construction below — AES-256-GCM, a fresh 12-byte nonce per seal, AAD
// binding an identity into the ciphertext, one flat authentication error — is
// deliberately the same one internal/controld/seal.go uses, and this is a COPY
// of it rather than a call into it. A public package may not import internal/
// (scripts/check-public-control.sh), and inverting the dependency so that the
// credential vault seals through this package would change the credential path
// in a change that is meant to be additive. Both spellings are annotated so the
// next reader sees two spellings of one construction and not two constructions.
// The design note's §14 keeps the consolidation open.

const (
	// dekLen is the data key: AES-256.
	dekLen = 32
	// nonceLen is AES-GCM's standard nonce size, and the only one accepted back.
	nonceLen = 12
	// tagLen is AES-GCM's authentication tag, which Seal appends.
	tagLen = 16
	// noncePrefixLen is the random per-checkpoint half of a frame nonce; the
	// other four bytes are the frame index.
	noncePrefixLen = nonceLen - 4
	// maxKeyRefLen bounds a key reference. A cloud KMS resource name is long;
	// an unbounded string in a manifest is longer.
	maxKeyRefLen = 512
)

// KeyRef is an opaque, versioned reference to a key-encryption key. This
// package never interprets one: self-hosted Rainier spells it something like
// "fleet/secrets/v1" and a hosted cell spells it a cloud KMS resource name, and
// neither spelling reaches the format's logic.
type KeyRef string

// Wrapper is the key service seam: the whole of what this package needs from a
// key hierarchy. It is deliberately two methods over opaque bytes, so that a
// KMS, an HSM, a per-workspace KEK hierarchy, and the static key below are all
// expressible and none of them is in this repository.
//
// Both methods take the authenticated context as aad. An implementation MUST
// pass it to its own authenticated encryption — a wrapper that ignores aad
// silently removes one of the three bindings that make a cross-context copy
// fail. The other two (the manifest tag and every frame's AAD) still hold, which
// is why there are three.
type Wrapper interface {
	// Wrap encrypts dek under ref, binding aad, and returns the wrapped key
	// together with the CONCRETE reference it actually used. ref may be an
	// alias; used must be the exact version, because that is what goes in the
	// manifest and what Unwrap will be asked for. That one return value is the
	// whole versioned-key story: rotation changes what an alias resolves to, old
	// checkpoints keep naming the old version, and Unwrap never guesses.
	Wrap(ctx context.Context, ref KeyRef, dek, aad []byte) (wrapped []byte, used KeyRef, err error)

	// Unwrap decrypts a key wrapped under the exact reference ref, verifying
	// aad. It fails closed: a reference this destination cannot resolve is
	// ErrKeyUnavailable, and everything else is ErrAuth. This call is where a
	// destination's authorization and key readiness are actually proven, so an
	// implementation that cannot reach the key version in its region must return
	// an error rather than falling back.
	Unwrap(ctx context.Context, ref KeyRef, wrapped, aad []byte) (dek []byte, err error)
}

// StaticKeyWrapper is the one implementation this package ships: a single
// 32-byte key under a single reference. It is what self-hosted Rainier and
// every test use, and it is the readable thing a reviewer can check the format
// against without a cloud account.
//
// It is not a key hierarchy. Rotation is a new reference and a new wrapper, and
// old checkpoints under the old reference stop opening — which is exactly the
// crypto-shred the deletion story (design note §10) relies on, and exactly the
// wrong behavior for a fleet that wanted to keep reading them. A hosted cell
// uses a real key service.
type StaticKeyWrapper struct {
	ref  KeyRef
	key  [dekLen]byte
	rand io.Reader
}

var _ Wrapper = (*StaticKeyWrapper)(nil)

// NewStaticKeyWrapper builds the wrapper for one key under one reference.
//
// An all-zero key is refused for the reason ParseSecretsKey refuses one: it is
// a fine 32 bytes, and it is also how a zero value looks, so accepting it
// produces a deployment that definitely configured a key and a library that
// insists no key was configured.
func NewStaticKeyWrapper(ref KeyRef, key [dekLen]byte) (*StaticKeyWrapper, error) {
	return newStaticKeyWrapper(ref, key, nil)
}

// newStaticKeyWrapper is NewStaticKeyWrapper with the random source injectable,
// which is how the golden-manifest test gets a byte-for-byte deterministic
// manifest. Unexported: a caller outside this package gets crypto/rand.
func newStaticKeyWrapper(ref KeyRef, key [dekLen]byte, r io.Reader) (*StaticKeyWrapper, error) {
	if ref == "" {
		return nil, fmt.Errorf("%w: a key reference is required", ErrInvalid)
	}
	if len(ref) > maxKeyRefLen {
		return nil, fmt.Errorf("%w: the key reference is over the %d character limit", ErrInvalid, maxKeyRefLen)
	}
	if key == ([dekLen]byte{}) {
		return nil, fmt.Errorf("%w: the wrapping key must not be all zeros (it is indistinguishable from an unset key)", ErrInvalid)
	}
	if r == nil {
		r = cryptoRand
	}
	return &StaticKeyWrapper{ref: ref, key: key, rand: r}, nil
}

// Ref is the reference this wrapper wraps under. A host uses it to fill
// WriterOptions.KeyRef without spelling the same string twice.
func (w *StaticKeyWrapper) Ref() KeyRef { return w.ref }

// Wrap seals dek under the static key. An empty ref means "whatever this
// wrapper wraps under"; any other reference than its own is ErrKeyUnavailable
// rather than a silent substitution.
func (w *StaticKeyWrapper) Wrap(ctx context.Context, ref KeyRef, dek, aad []byte) ([]byte, KeyRef, error) {
	if ref != "" && ref != w.ref {
		return nil, "", ErrKeyUnavailable
	}
	if len(dek) != dekLen {
		return nil, "", fmt.Errorf("%w: a data key is %d bytes", ErrInvalid, dekLen)
	}
	aead, err := newGCM(w.key[:])
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(w.rand, nonce); err != nil {
		// The error says nothing about the key or the plaintext, and a broken
		// entropy source is the only way here.
		return nil, "", fmt.Errorf("checkpoint: generating a key-wrapping nonce: %w", err)
	}
	// nonce || ciphertext, one opaque blob, because the manifest has one field
	// for it and a caller that had to keep two in step would eventually not.
	out := make([]byte, 0, nonceLen+len(dek)+tagLen)
	out = append(out, nonce...)
	return aead.Seal(out, nonce, dek, aad), w.ref, nil
}

// Unwrap opens a key wrapped by Wrap. Every failure is ErrAuth except a
// reference it does not hold, which is ErrKeyUnavailable — the distinction is
// worth making because the two have different operator actions: one is a
// tampered or misdirected checkpoint, the other is a key that has not been
// replicated to this region yet.
func (w *StaticKeyWrapper) Unwrap(ctx context.Context, ref KeyRef, wrapped, aad []byte) ([]byte, error) {
	if ref != w.ref {
		return nil, ErrKeyUnavailable
	}
	if len(wrapped) < nonceLen+tagLen {
		return nil, ErrAuth
	}
	aead, err := newGCM(w.key[:])
	if err != nil {
		return nil, err
	}
	dek, err := aead.Open(nil, wrapped[:nonceLen], wrapped[nonceLen:], aad)
	if err != nil {
		return nil, ErrAuth
	}
	if len(dek) != dekLen {
		// A wrapped blob of the wrong length authenticated, which means the key
		// service is handing back something that is not a data key. Refused
		// flatly rather than derived from.
		wipe(dek)
		return nil, ErrAuth
	}
	return dek, nil
}

// deriveSubkeys splits the data key into the content key and the manifest key.
// Domain separation is the point: the manifest tag and the frames must never be
// able to share a (key, nonce) pair, and deriving two keys is cheaper to reason
// about than reserving a nonce range. The authenticated context is the salt, so
// the subkeys are bound to it as well as the wrapped key being bound to it.
func deriveSubkeys(dek []byte, c Context) (content, manifest []byte, err error) {
	if len(dek) != dekLen {
		return nil, nil, fmt.Errorf("%w: a data key is %d bytes", ErrInvalid, dekLen)
	}
	salt := c.Bytes()
	content, err = hkdf.Key(sha256.New, dek, salt, FormatVersion+" content", dekLen)
	if err != nil {
		return nil, nil, fmt.Errorf("checkpoint: deriving the content key: %w", err)
	}
	manifest, err = hkdf.Key(sha256.New, dek, salt, FormatVersion+" manifest", dekLen)
	if err != nil {
		wipe(content)
		return nil, nil, fmt.Errorf("checkpoint: deriving the manifest key: %w", err)
	}
	return content, manifest, nil
}

// newGCM builds AES-256-GCM over key. Both constructors can only fail on a
// wrong key length, which every caller here has already made impossible; the
// errors are wrapped anyway, because returning a nil AEAD would be far worse.
func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != dekLen {
		return nil, fmt.Errorf("%w: an AES-256 key is %d bytes", ErrInvalid, dekLen)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("checkpoint: building the AES cipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("checkpoint: building AES-GCM")
	}
	return aead, nil
}

// wipe overwrites key material once it is no longer needed. Go makes no promise
// that a copy was not left somewhere by the compiler or the collector, so this
// is hygiene rather than a guarantee — it shortens the window in which a heap
// dump of a checkpointing process contains a data key, and that is worth three
// lines. subtle is used so the write cannot be optimized away as dead.
func wipe(b []byte) {
	if len(b) > 0 {
		subtle.ConstantTimeCopy(1, b, make([]byte, len(b)))
	}
}
