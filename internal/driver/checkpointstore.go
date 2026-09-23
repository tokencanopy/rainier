// internal/driver/checkpointstore.go
//
// The self-hosted half of the portable workspace checkpoint: where the objects
// go on a runner that has no cloud behind it, and where its key comes from.
//
// Both are deliberately small and deliberately here rather than in the
// checkpoint package. That package ships one storage backend (an in-memory map)
// and one key wrapper (a static key) on purpose — "restorable on another
// qualified provider" (PRD §10) is a property it keeps by never having met a
// provider — so a directory on a host is this repository's own concern, behind
// the same two ports a GCS bucket and a KMS go behind.
package driver

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/tokencanopy/rainier/checkpoint"
)

// SelfHostedCheckpointKeyRef is the key reference a self-hosted runner's
// checkpoints are wrapped under. It is a constant because there is exactly one
// key — rotation for a static wrapper is a new reference and a new file, and old
// checkpoints under the old one stop opening, which is the crypto-shred the
// deletion story rests on and the wrong behaviour for a fleet that wanted to
// keep reading them. A cell uses a real key service.
const SelfHostedCheckpointKeyRef checkpoint.KeyRef = "selfhosted/checkpoint/v1"

// checkpointDirMode is the mode of every directory this store creates: the
// runner's own. A checkpoint is ciphertext, but the object LAYOUT names a
// workspace and a session (see the checkpoint package's storage layout), and a
// directory listing that tells a local user which sessions this host holds is a
// disclosure with nothing to buy it.
const checkpointDirMode fs.FileMode = 0o700

// DirBlobStore is a checkpoint.BlobStore in a directory on this host.
//
// It exists so a self-hosted runner can cold suspend at all. It is not a
// pretend cloud: there is no replication, no lifecycle policy and no
// cross-region anything, and a host that loses this directory has lost the
// checkpoints in it. What it does implement exactly is the one semantic the
// format's atomicity rests on — a conditional create that either produces the
// WHOLE object or leaves nothing at the key.
type DirBlobStore struct{ root string }

var _ checkpoint.BlobStore = (*DirBlobStore)(nil)

// NewDirBlobStore prepares dir as a checkpoint store, creating it 0700 if it is
// not there.
//
// It fails rather than falling back, for the reason every other microVM
// preflight does: a runner that started with nowhere to put a checkpoint would
// accept placements and then fail every cold suspend, with a tenant's work
// still inside a VM it cannot terminate.
func NewDirBlobStore(dir string) (*DirBlobStore, error) {
	if dir == "" {
		return nil, errors.New("microvm: a checkpoint store directory is required (--checkpoint-store-dir)")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("microvm: resolving the checkpoint store directory: %w", err)
	}
	if err := os.MkdirAll(abs, checkpointDirMode); err != nil {
		return nil, fmt.Errorf("microvm: creating the checkpoint store directory: %w", err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("microvm: reading the checkpoint store directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("microvm: the checkpoint store path %s is not a directory", abs)
	}
	// A directory that was already there keeps whatever mode it was made with —
	// MkdirAll is a no-op on one — and the default umask makes that 0755. It is
	// tightened rather than refused: this directory is the runner's own by
	// definition, the layout inside it names a workspace and a session for
	// every checkpoint on this host (see checkpointDirMode), and an operator who
	// mkdir'd it before starting the runner has made an ordinary mistake rather
	// than a decision.
	if info.Mode().Perm()&0o077 != 0 {
		log.Printf("microvm: the checkpoint store directory %s was mode %04o; tightening it to 0700, because its layout names every session this host has checkpointed",
			abs, info.Mode().Perm())
		if err := os.Chmod(abs, checkpointDirMode); err != nil {
			return nil, fmt.Errorf("microvm: tightening the checkpoint store directory's mode: %w", err)
		}
	}
	return &DirBlobStore{root: abs}, nil
}

// PutIfAbsent writes the object at key, and either the whole of it appears or
// nothing does.
//
// The mechanism is a temporary file in the SAME directory, fsynced, then
// link(2)ed into place. link is what makes this conditional: it fails with
// EEXIST against an existing name, where rename would silently replace one —
// and replacing a committed manifest is the one operation this format has no
// answer for, because the data key inside it is the only way to read the
// content object it names.
//
// The temporary file is removed on every path, so a failed write leaves the
// directory as it found it.
func (s *DirBlobStore) PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%w: %s", checkpoint.ErrExists, "the object is already present")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checkpoint store: reading the object: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, checkpointDirMode); err != nil {
		return fmt.Errorf("checkpoint store: creating the object's directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".partial-*")
	if err != nil {
		return fmt.Errorf("checkpoint store: creating a temporary object: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if err := write(tmp); err != nil {
		tmp.Close()
		// The writer's own error, unwrapped: it is the checkpoint package's,
		// which is written to carry no path and no content, and a caller
		// matching on its sentinels must still be able to.
		return err
	}
	// Both fsyncs are the durability half of the barrier. ADR-0003 §4.4 says a
	// cold suspend may not report success until the manifest is committed, and
	// an object still in this host's page cache is not committed to anything: a
	// power loss between here and the VM's termination would lose the only copy
	// of a workspace whose VM was told it was safe to end.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("checkpoint store: syncing the object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("checkpoint store: closing the object: %w", err)
	}
	if err := os.Link(tmpName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %s", checkpoint.ErrExists, "the object is already present")
		}
		return fmt.Errorf("checkpoint store: committing the object: %w", err)
	}
	committed = true
	_ = os.Remove(tmpName)
	return syncDir(dir)
}

// Open returns a reader over the object at key.
func (s *DirBlobStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, checkpoint.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint store: opening the object: %w", err)
	}
	return f, nil
}

// Delete removes the object at key. A missing object is success, because
// deletion is retried and a ledger that cannot report "already gone" as done
// never converges.
func (s *DirBlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checkpoint store: deleting the object: %w", err)
	}
	return nil
}

// path turns a storage key into a file path under this store's root.
//
// Every element is checked, even though the keys this store sees are built by
// the checkpoint package from identifiers it has already validated. That is the
// last-hop rule the rest of this repository applies at every syscall boundary:
// the check performed by the layer above is a check you are trusting, and what
// is on the other side of this one is os.Remove.
func (s *DirBlobStore) path(key string) (string, error) {
	if key == "" || len(key) > maxCheckpointKeyLen {
		return "", fmt.Errorf("checkpoint store: the object key is empty or over %d characters", maxCheckpointKeyLen)
	}
	elems := strings.Split(key, "/")
	for _, e := range elems {
		switch {
		case e == "", e == ".", e == "..":
			return "", errors.New("checkpoint store: the object key has an empty, \".\" or \"..\" element")
		case strings.ContainsAny(e, "\x00\\"):
			return "", errors.New("checkpoint store: the object key has an element with a NUL or a backslash in it")
		}
	}
	return filepath.Join(append([]string{s.root}, elems...)...), nil
}

// maxCheckpointKeyLen bounds a key, so that a path this store builds is a
// bounded string. It is over the format's own maximum (a 512-byte prefix plus
// the layout) and under any filesystem's.
const maxCheckpointKeyLen = 1024

// syncDir fsyncs a directory, which is what makes a newly linked name survive a
// power loss. It is best effort on a filesystem that refuses the call (some do
// for a directory opened read-only) and an error otherwise.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("checkpoint store: opening the object's directory to sync it: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		if errors.Is(err, os.ErrInvalid) || errors.Is(err, fs.ErrPermission) {
			return nil
		}
		return fmt.Errorf("checkpoint store: syncing the object's directory: %w", err)
	}
	return nil
}

// LoadCheckpointKey reads the 32-byte key a self-hosted runner wraps its
// checkpoints under, and refuses everything about the file that is not right.
//
// The refusals are the point. This one file is the difference between a
// checkpoint that is ciphertext and a checkpoint anybody who can read the store
// directory can open, and every condition below is one a real deployment gets
// wrong at some point:
//
//   - a file that is not there, or is not a regular file;
//   - a mode any other user on this host can read (a key is a key);
//   - something that is not 32 bytes, in either spelling this accepts;
//   - all zeros, which is a fine 32 bytes and is also how an unset key looks —
//     refused for the reason ParseSecretsKey refuses one.
//
// Two spellings are accepted because both are what operators actually produce:
// 64 hex characters (`openssl rand -hex 32`, and what fits in a secret manager
// or a Kubernetes Secret) and 32 raw bytes (`openssl rand 32 > key`). Trailing
// whitespace is trimmed from the hex form, because a file written by `echo` has
// a newline in it and refusing that would be refusing the most likely correct
// key in the world.
func LoadCheckpointKey(path string) ([32]byte, error) {
	var key [32]byte
	if path == "" {
		return key, errors.New("microvm: a checkpoint key file is required (--checkpoint-key-file)")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return key, fmt.Errorf("microvm: reading the checkpoint key file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return key, fmt.Errorf("microvm: the checkpoint key file %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return key, fmt.Errorf("microvm: the checkpoint key file %s is mode %04o; it must not be readable by group or other (0600)",
			path, info.Mode().Perm())
	}
	if err := checkKeyOwner(path, info); err != nil {
		return key, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return key, fmt.Errorf("microvm: reading the checkpoint key file: %w", err)
	}
	switch trimmed := strings.TrimSpace(string(raw)); {
	case len(trimmed) == hex.EncodedLen(len(key)):
		decoded, err := hex.DecodeString(trimmed)
		if err != nil {
			// The error is not wrapped: hex quotes the byte it choked on, and
			// that byte is key material.
			return key, fmt.Errorf("microvm: the checkpoint key file %s is %d characters but is not hex", path, len(trimmed))
		}
		copy(key[:], decoded)
	case len(raw) == len(key):
		copy(key[:], raw)
	default:
		return key, fmt.Errorf("microvm: the checkpoint key file %s holds %d bytes; it must be 32 raw bytes or 64 hex characters",
			path, len(raw))
	}
	if key == ([32]byte{}) {
		return key, fmt.Errorf("microvm: the checkpoint key file %s is all zeros, which is indistinguishable from an unset key", path)
	}
	return key, nil
}
