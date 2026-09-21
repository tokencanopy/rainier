// Package checkpoint implements Rainier's portable workspace checkpoint: the
// durable, provider-neutral representation of a cold-suspended session's
// workspace. Its canonical import path is
// github.com/tokencanopy/rainier/checkpoint.
//
// A checkpoint is two objects in a blob store. The content object is the
// session's filesystem tree as a tar stream, cut into fixed-size frames, each
// frame sealed with AES-256-GCM under a data key that exists for one
// checkpoint and is stored only in wrapped form. The manifest object is small
// plaintext JSON that names the content object, the algorithms, the versioned
// key reference, the wrapped data key, and the digests and counts a restore is
// checked against — and nothing else. It holds no path, no file name, no
// symlink target and no file byte, because those are session content
// (rainier-cloud tenancy specification §4.2) and a plaintext sidecar listing
// them would be a leak no careful logging downstream could undo.
//
// The manifest is written with put-if-absent, and that single conditional
// write is the atomic commit the durability barrier rests on: before it there
// is no checkpoint at a generation, after it there is exactly one, forever.
//
// The design note is docs/design/2026-09-20-portable-workspace-checkpoint.md.
// It argues every choice summarized here, including the ones that were close.
//
// # The name
//
// Unqualified "checkpoint" is unambiguous because Rainier has no other kind:
// ADR-0003 §2.2 forbids serializing guest memory for an authenticated session,
// so there is no process checkpoint for this name to be confused with. If that
// ever changes, this package keeps the name and the other one qualifies itself.
//
// # The durability barrier
//
// The order below is the design, not a suggestion. A caller may not report a
// cold suspension successful, or release the workspace, until Verify has
// returned:
//
//	w, _ := checkpoint.NewWriter(store, keys, checkpoint.WriterOptions{KeyRef: ref})
//	res, err := w.Write(ctx, c, checkpoint.DirSource("/workspace"))
//	if err != nil { return err }          // nothing was committed
//
//	r, _ := checkpoint.NewReader(store, keys, checkpoint.ReaderOptions{
//	        Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error {
//	                // The producer is the principal; a destination puts its
//	                // real check here and this library cannot let it forget to.
//	                return nil
//	        },
//	})
//	rep, err := r.Verify(ctx, c)          // reads the committed bytes back
//	if err != nil { return err }          // the disk stays; reconciliation retries
//	_ = res.ManifestKey; _ = rep.Summary  // record, then release the workspace
//
// Verify reads through the blob store rather than checking the bytes the
// writer still had in memory, because the claim being tested is that the
// committed object is restorable, and an in-memory check tests the encoder
// against itself.
//
// DirSource takes no exclusion argument there because it does not need one.
// DefaultExclusions — the Rainier-owned paths inside a workspace, starting with
// .rainier — is applied by every Write, and an argument a caller does pass is
// ADDED to that set rather than replacing it.
//
// # Neutrality
//
// The package is provider-neutral, transport-neutral and policy-neutral, each
// as a hard boundary enforced by scripts/check-public-control.sh:
//
//   - No storage backend but the in-memory one. GCS, S3 and anything else live
//     behind BlobStore, whose PutIfAbsent is the only semantics the format
//     needs from a provider.
//   - No key service but a static-key wrapper. KMS, rotation policy, key
//     hierarchies and regional replication live behind Wrapper, which returns
//     the concrete key version it used so a manifest never has to guess.
//   - No authorization policy. ReaderOptions.Authorize is required and runs
//     before the data key is unwrapped, before a content byte is read, and
//     before the target directory is touched. What it decides is the control
//     plane's; that it runs is this package's.
//   - No scheduling, retention, checkpoint-age policy, or upload retry.
//
// # Memory
//
// Nothing here is O(entries) or O(tree bytes). A write or a restore holds one
// frame's plaintext, one frame's ciphertext, a 32 KiB copy buffer, one entry
// header and a few hash states — under 3 MiB at the default frame size, for a
// tree of any size. There is deliberately no API that returns a list of
// entries, and no io.ReadAll anywhere except the manifest, which is capped.
//
// # Errors
//
// Every error is one of this package's typed sentinels wrapped in a flat
// sentence. No error carries a path, a file name, a symlink target, a file
// byte, a key, a wrapped key or a nonce: these errors travel from sessiond
// through runnerd to a session's error column, and the tenancy specification
// §15.1 forbids paths and content there. A refusal about a source entry names
// the entry's ordinal and kind instead, which is findable with a local walk and
// is not a path.
//
// Every cryptographic failure is one flat ErrAuth — wrong key, wrong context,
// tampered bytes — for the reason internal/controld/seal.go gives for its own
// errSecretAuth: the distinction is not something a caller can act on
// differently, and the underlying detail is not safe to print.
package checkpoint
