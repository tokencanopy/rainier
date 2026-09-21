package checkpoint

import "errors"

// The whole error vocabulary, in one file, because the rule that governs it is
// one rule: an error from this package says what happened and never what it
// happened to. No sentinel below, and no sentence wrapped around one, may carry
// a path, a file name, a symlink target, a file byte, a key, a wrapped key or a
// nonce. These errors travel from sessiond through runnerd into a session's
// error column and an operator's log, where the tenancy specification's §15.1
// prohibited-logging list applies, and paths, diffs and file contents are on it.
//
// The cost is real and is not hidden: a refusal about the source tree can name
// only the offending entry's ORDINAL and its kind, which is enough to find with
// a local walk of the same tree and is not a path. The design note's §14 keeps
// a content-scoped diagnostic channel open as the alternative rather than
// pretending the trade is free.
var (
	// ErrAuth is every cryptographic failure: the wrong key, the wrong
	// authenticated context, a flipped bit in the manifest, a flipped bit in a
	// frame. One flat error, for the reason internal/controld/seal.go gives for
	// errSecretAuth — the distinction is not actionable, and the underlying
	// detail is exactly the part that could quote a value. No failure ever
	// returns partial plaintext; GCM authenticates before there is anything to
	// hand back.
	ErrAuth = errors.New("checkpoint: the checkpoint failed authentication (wrong key, wrong context, or tampered bytes)")

	// ErrManifest is a strict-parse refusal: a missing field, an unknown field,
	// a field out of range, a self-inconsistent set of lengths. The manifest is
	// the only part of a checkpoint that is read before anything is
	// authenticated, so it is the part parsed most suspiciously.
	ErrManifest = errors.New("checkpoint: the manifest is not a well-formed checkpoint manifest")

	// ErrFormatVersion is a manifest that names a format this build does not
	// implement. It is checked before anything else and it refuses rather than
	// guessing: there is no minor version and no field a reader may ignore,
	// because "ignore what you don't understand" is how a format acquires an
	// unauthenticated extension point.
	ErrFormatVersion = errors.New("checkpoint: the manifest states a checkpoint format version this build does not implement")

	// ErrContextMismatch is the cheap refusal before any key is touched: the
	// manifest says it belongs to another workspace, session or generation.
	// It leaks nothing the plaintext manifest did not already say, and it turns
	// the ordinary operational mistake — restoring the wrong checkpoint — into a
	// sentence rather than an authentication failure.
	ErrContextMismatch = errors.New("checkpoint: the manifest belongs to another workspace, session, or checkpoint generation")

	// ErrTruncated is a content object that ends before the frame count the
	// manifest committed to. It is the failure mode of an upload that died, and
	// it is caught twice: here, and by the ciphertext length and digest.
	ErrTruncated = errors.New("checkpoint: the content object ends before the frame count the manifest committed to")

	// ErrTrailingData is a content object that continues past its last frame.
	ErrTrailingData = errors.New("checkpoint: the content object continues past the frame count the manifest committed to")

	// ErrExists is a lost put-if-absent: a checkpoint is already committed at
	// this generation. The loser of the race is told so rather than being
	// allowed to believe it committed, and it never touches the winner's
	// objects.
	ErrExists = errors.New("checkpoint: a checkpoint is already committed at this generation")

	// ErrNotFound is no committed checkpoint at this generation. A BlobStore
	// reports a missing object with it, and this package passes it through.
	ErrNotFound = errors.New("checkpoint: no checkpoint is committed at this generation")

	// ErrNoAuthorization is a programming error with its own sentinel: a Reader
	// was constructed without an authorization hook. It is refused at
	// construction so that no path through this package can be the place where
	// authorizing a restore was forgotten.
	ErrNoAuthorization = errors.New("checkpoint: no authorization hook was supplied; a restore may not proceed unauthorized")

	// ErrNotAuthorized wraps whatever the authorization hook refused with.
	ErrNotAuthorized = errors.New("checkpoint: the destination is not authorized to restore this checkpoint")

	// ErrKeyUnavailable is a key reference this destination cannot resolve. It
	// is the "key readiness" half of the restore precondition, and a Wrapper
	// fails closed with it rather than trying a key it has.
	ErrKeyUnavailable = errors.New("checkpoint: the key version this checkpoint names is not available here")

	// ErrTargetNotEmpty refuses a restore into a directory that already has
	// something in it.
	ErrTargetNotEmpty = errors.New("checkpoint: the restore target exists and is not empty")

	// ErrRestore is the destination file system refusing to hold the tree: out
	// of space, out of permission, a name it cannot represent. The checkpoint is
	// fine; the target is not.
	ErrRestore = errors.New("checkpoint: the checkpoint could not be written to the restore target")

	// ErrSource is a source tree that cannot be checkpointed as it stands: an
	// entry kind the format does not carry, a symlink that leaves the tree, a
	// file whose size changed under the walk because the source was not
	// quiesced.
	ErrSource = errors.New("checkpoint: the source tree cannot be checkpointed as it stands")

	// ErrEntry is a decoded entry that is not a valid tree entry — a name that
	// is not a clean relative path, an entry kind the format does not carry.
	// Distinct from ErrSource because the blame is opposite: ErrSource is about
	// a local tree, ErrEntry about bytes that came out of a checkpoint.
	ErrEntry = errors.New("checkpoint: a checkpoint entry is not a valid tree entry")

	// ErrMismatch is a stream that decrypted and decoded but is not the tree
	// the manifest committed to: a different entry count, byte total, or tree
	// digest. Every byte authenticated, and the aggregates still disagree, which
	// means this package has a bug or the manifest and content came from
	// different writes.
	ErrMismatch = errors.New("checkpoint: the content does not match the aggregates the manifest committed to")

	// ErrTooLarge is a source tree that needs more frames than the format
	// allows. At the default frame size the ceiling is petabytes; it exists so
	// the frame index cannot silently wrap.
	ErrTooLarge = errors.New("checkpoint: the source tree needs more frames than the checkpoint format allows")

	// ErrInvalid is a bad argument from this package's own caller: a malformed
	// context, an out-of-range option, a source with no file system.
	ErrInvalid = errors.New("checkpoint: invalid argument")
)
