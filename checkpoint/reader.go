package checkpoint

import (
	"archive/tar"
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ReaderOptions configures a Reader.
type ReaderOptions struct {
	// Prefix is the storage prefix the checkpoint was written under. It must
	// match the Writer's.
	Prefix string

	// Authorize is how a destination proves it is allowed to read this
	// checkpoint. It is REQUIRED: NewReader refuses a nil hook with
	// ErrNoAuthorization, so that no path through this package can be the place
	// where authorizing a restore was forgotten.
	//
	// It runs after the manifest has been parsed and identity-checked, and before
	// the data key is unwrapped, before one content byte is read, and before the
	// target directory is touched. What it decides is the control plane's
	// business — the tenancy specification's §4 and §8.2 define the policy and it
	// lives in the cell. That it runs, and runs first, is this package's.
	//
	// The producer running the durability barrier's own restore test is the
	// principal that just wrote the checkpoint, so its hook returns nil. That is
	// not a loophole: the library cannot judge a policy, only guarantee the step
	// exists and is named.
	//
	// Whatever error it returns is wrapped alongside ErrNotAuthorized. Its text
	// is the caller's own, and this package's no-content rule applies to what the
	// caller puts in it.
	Authorize func(ctx context.Context, c Context, m Manifest) error
}

// Reader verifies and restores checkpoints. Like Writer it holds no
// per-checkpoint state and is safe to use concurrently for different contexts.
type Reader struct {
	store BlobStore
	keys  Wrapper
	opts  ReaderOptions
}

// NewReader validates the options once. A nil Authorize fails HERE rather than
// at the first restore, which is the difference between an impossible mistake
// and a latent one.
func NewReader(store BlobStore, keys Wrapper, opts ReaderOptions) (*Reader, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: a blob store is required", ErrInvalid)
	}
	if keys == nil {
		return nil, fmt.Errorf("%w: a key wrapper is required", ErrInvalid)
	}
	if err := validPrefix(opts.Prefix); err != nil {
		return nil, err
	}
	if opts.Authorize == nil {
		return nil, ErrNoAuthorization
	}
	return &Reader{store: store, keys: keys, opts: opts}, nil
}

// Preflight is what a destination learns before it reads any content.
type Preflight struct {
	ManifestKey string
	ContentKey  string
	Manifest    Manifest
	Summary     Summary
}

// Report is what a verification or a restore observed, recomputed from the
// stream rather than copied from the manifest. Every field here was also
// compared against the manifest before the call returned, so a Report that
// exists is a Report that agreed.
type Report struct {
	Summary      Summary
	Entries      int64
	Dirs         int64
	Files        int64
	Symlinks     int64
	FileBytes    int64
	PlainBytes   int64
	ContentBytes int64
	TreeDigest   string
	// Restored is true when the tree was written to a target directory, false
	// for a Verify. It is here so a caller logging one line can tell the
	// durability barrier's restore test from an actual restore.
	Restored bool
}

// Preflight parses and authenticates the manifest, proves this destination can
// unwrap the checkpoint's data key, and returns what a restore would do —
// without reading one byte of the content object.
//
// It is the call a placement decision makes when PRD §4.3 filters capacity by
// "storage and key readiness": a destination that cannot reach the key version
// in its region fails here, in one small request, before anything is scheduled.
// Restore begins by doing exactly the same work, so preflighting is an
// optimization for a scheduler and never a check the restore path skips.
func (r *Reader) Preflight(ctx context.Context, c Context) (Preflight, error) {
	o, err := r.open(ctx, c)
	if err != nil {
		return Preflight{}, err
	}
	wipe(o.kContent)
	return Preflight{
		ManifestKey: manifestKey(r.opts.Prefix, c),
		ContentKey:  o.manifest.ContentKey,
		Manifest:    o.manifest,
		Summary:     o.manifest.Summary(),
	}, nil
}

// Verify is the restore test. It decrypts every frame, checks every integrity
// value the manifest committed to, and walks the tree's structure under the same
// rules a restore applies — and writes nothing.
//
// It is what the durability barrier's fourth step calls, and it reads through the
// blob store, because the claim being tested is that the COMMITTED object is
// restorable. Verifying the bytes the writer still had in memory would test the
// encoder against itself.
//
// It is not a second copy: one sequential read, constant memory, no tree on disk.
func (r *Reader) Verify(ctx context.Context, c Context) (Report, error) {
	return r.consume(ctx, c, "")
}

// Restore writes the checkpoint's tree into target, which must be empty or
// absent, and checks the same aggregates Verify checks while it does it.
//
// A failure part-way through leaves what it had written; the caller deletes the
// target and retries. Rolling back would mean either a second pass, which this
// package does not have the memory budget for, or a staging directory, which is
// the caller's choice to make because only the caller knows what filesystem it
// has.
func (r *Reader) Restore(ctx context.Context, c Context, target string) (Report, error) {
	if target == "" {
		return Report{}, fmt.Errorf("%w: a restore target is required", ErrInvalid)
	}
	return r.consume(ctx, c, target)
}

// Delete removes the checkpoint, manifest FIRST.
//
// The order is the design. The data key exists only in the manifest, wrapped, so
// deleting the manifest makes the content object unreadable by anyone, including
// Rainier, immediately and irreversibly — which means an interrupted deletion
// leaves the unreadable state and never the readable one, and a deletion ledger
// reporting the content object as pending is not reporting a disclosure.
//
// A missing manifest is success: an orphan content object without its manifest is
// unreadable garbage, and a ledger that cannot report "already gone" as done
// never converges.
func (r *Reader) Delete(ctx context.Context, c Context) error {
	m, err := r.readManifest(ctx, c)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := r.opts.Authorize(ctx, c, m); err != nil {
		return fmt.Errorf("%w: %w", ErrNotAuthorized, err)
	}
	if err := r.store.Delete(ctx, manifestKey(r.opts.Prefix, c)); err != nil {
		return fmt.Errorf("checkpoint: deleting the manifest: %w", err)
	}
	if err := r.store.Delete(ctx, m.ContentKey); err != nil {
		return fmt.Errorf("checkpoint: deleting the content object: %w", err)
	}
	return nil
}

// readManifest is everything that can be checked without a key: the object is
// there, it parses strictly, it claims the context the caller expects, and its
// content key lives under this checkpoint's own prefix.
func (r *Reader) readManifest(ctx context.Context, c Context) (Manifest, error) {
	if err := c.Validate(); err != nil {
		return Manifest{}, err
	}
	rc, err := r.store.Open(ctx, manifestKey(r.opts.Prefix, c))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Manifest{}, ErrNotFound
		}
		return Manifest{}, fmt.Errorf("checkpoint: opening the manifest: %w", err)
	}
	defer rc.Close()

	// One byte past the cap, so an oversized manifest is refused rather than
	// silently truncated into a parse error.
	b, err := io.ReadAll(io.LimitReader(rc, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("checkpoint: reading the manifest (%s)", fsCategory(err))
	}
	m, err := ParseManifest(b)
	if err != nil {
		return Manifest{}, err
	}
	// The manifest's own identity is compared against the context the CALLER
	// supplied, and is otherwise never consulted. See Context's documentation for
	// why that direction is the whole property.
	if m.Workspace != c.Workspace || m.Session != c.Session || m.Generation != c.Generation {
		return Manifest{}, ErrContextMismatch
	}
	if err := r.checkContentKey(c, m.ContentKey); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// checkContentKey holds the manifest to the layout: one content object, in this
// checkpoint's own generation directory, with an attempt suffix and no path
// elements of its own. The field is authenticated, so this is not what stops a
// redirect; it is what stops the layout from drifting.
func (r *Reader) checkContentKey(c Context, key string) error {
	want := generationDir(r.opts.Prefix, c) + "/" + contentObject + "."
	suffix, ok := strings.CutPrefix(key, want)
	if !ok || suffix == "" || strings.Contains(suffix, "/") {
		return fmt.Errorf("%w: the content key is not this checkpoint's content object", ErrManifest)
	}
	return nil
}

// opened is an authenticated checkpoint: the manifest, and the content subkey.
type opened struct {
	manifest Manifest
	kContent []byte
}

// open is the full precondition chain, in the order the design note's §8
// specifies: parse, identity, AUTHORIZE, unwrap, manifest tag. Nothing after
// this function reads content, and nothing before it holds a key.
func (r *Reader) open(ctx context.Context, c Context) (opened, error) {
	m, err := r.readManifest(ctx, c)
	if err != nil {
		return opened{}, err
	}
	if err := r.opts.Authorize(ctx, c, m); err != nil {
		return opened{}, fmt.Errorf("%w: %w", ErrNotAuthorized, err)
	}

	wrapped, err := base64.StdEncoding.DecodeString(m.WrappedKey)
	if err != nil {
		return opened{}, fmt.Errorf("%w: the wrapped key is not base64", ErrManifest)
	}
	dek, err := r.keys.Unwrap(ctx, KeyRef(m.KeyRef), wrapped, c.aad(purposeKey))
	if err != nil {
		// Passed through rather than flattened to ErrAuth: a key service that is
		// unreachable, throttled, or missing a key version must not be
		// indistinguishable from a tampered checkpoint, because the operator
		// actions are opposite. A wrapper's OWN authentication failure already
		// satisfies errors.Is(err, ErrAuth) through this wrapping.
		return opened{}, fmt.Errorf("checkpoint: unwrapping the checkpoint key: %w", err)
	}
	defer wipe(dek)

	kContent, kManifest, err := deriveSubkeys(dek, c)
	if err != nil {
		return opened{}, err
	}
	defer wipe(kManifest)
	if err := m.checkAuth(kManifest, c); err != nil {
		wipe(kContent)
		return opened{}, err
	}
	return opened{manifest: m, kContent: kContent}, nil
}

// consume is Verify and Restore: the same code, with target == "" meaning
// "write nothing". They are one function on purpose — the note claims Verify
// validates every entry under the restore path's rules, and the cheapest way to
// keep that claim true is for there to be one set of rules and one walk.
func (r *Reader) consume(ctx context.Context, c Context, target string) (Report, error) {
	o, err := r.open(ctx, c)
	if err != nil {
		return Report{}, err
	}
	defer wipe(o.kContent)
	m := o.manifest

	if target != "" {
		if err := prepareTarget(target); err != nil {
			return Report{}, err
		}
	}

	rc, err := r.store.Open(ctx, m.ContentKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// The manifest committed, the content object is gone. That is a
			// broken checkpoint, not a missing one, and it is named as such.
			return Report{}, fmt.Errorf("%w: the manifest names a content object that is not in the store", ErrTruncated)
		}
		return Report{}, fmt.Errorf("checkpoint: opening the content object: %w", err)
	}
	defer rc.Close()

	noncePrefix, err := base64.StdEncoding.DecodeString(m.NoncePrefix)
	if err != nil || len(noncePrefix) != noncePrefixLen {
		return Report{}, fmt.Errorf("%w: the nonce prefix is not base64 of %d bytes", ErrManifest, noncePrefixLen)
	}

	hr := &hashReader{r: rc, h: sha256.New()}
	fr, err := newFrameReader(hr, c, o.kContent, noncePrefix, m.FrameSize, m.Frames, m.PlainBytes)
	if err != nil {
		return Report{}, err
	}

	var st streamStats
	if err := consumeStream(tar.NewReader(fr), target, &st); err != nil {
		// The frame layer's own refusal outranks whatever the tar layer made of
		// it. archive/tar sees a failed frame as a malformed archive and says so,
		// and reporting "entry 1 could not be decoded" for a flipped ciphertext
		// bit would name the wrong thing entirely.
		if fault := fr.fault(); fault != nil {
			return Report{}, fault
		}
		return Report{}, err
	}
	// tar stops at the end-of-archive marker and may leave the trailing padding
	// unread; the padding is plaintext this checkpoint committed to, so it is
	// drained and counted rather than assumed.
	if _, err := io.Copy(io.Discard, fr); err != nil {
		if fault := fr.fault(); fault != nil {
			return Report{}, fault
		}
		return Report{}, err
	}

	// Snapshot the ciphertext accounting BEFORE probing for trailing data, so the
	// probe's byte cannot land in either.
	gotBytes, gotDigest := hr.n, hex.EncodeToString(hr.h.Sum(nil))
	var probe [1]byte
	if n, _ := hr.Read(probe[:]); n > 0 {
		return Report{}, ErrTrailingData
	}

	switch {
	case fr.plain != m.PlainBytes:
		return Report{}, ErrTruncated
	case gotBytes != m.ContentBytes:
		return Report{}, ErrTruncated
	case gotDigest != m.ContentDigest:
		return Report{}, ErrAuth
	case st.entries != m.Entries, st.fileBytes != m.FileBytes:
		return Report{}, ErrMismatch
	}
	tree := st.tree.sum()
	if tree != m.TreeDigest {
		return Report{}, ErrMismatch
	}

	return Report{
		Summary:      m.Summary(),
		Entries:      st.entries,
		Dirs:         st.dirs,
		Files:        st.files,
		Symlinks:     st.symlinks,
		FileBytes:    st.fileBytes,
		PlainBytes:   fr.plain,
		ContentBytes: gotBytes,
		TreeDigest:   tree,
		Restored:     target != "",
	}, nil
}

// prepareTarget refuses anything but an empty or absent directory. An error from
// the file system is reduced to its errno, never its path.
func prepareTarget(target string) error {
	fi, err := os.Stat(target)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(target, 0o755); err != nil {
			return fmt.Errorf("%w: the restore target could not be created (%s)", ErrRestore, fsCategory(err))
		}
		return nil
	case err != nil:
		return fmt.Errorf("%w: the restore target could not be inspected (%s)", ErrRestore, fsCategory(err))
	case !fi.IsDir():
		return fmt.Errorf("%w: the restore target is not a directory", ErrTargetNotEmpty)
	}
	ents, err := os.ReadDir(target)
	if err != nil {
		return fmt.Errorf("%w: the restore target could not be read (%s)", ErrRestore, fsCategory(err))
	}
	if len(ents) > 0 {
		return ErrTargetNotEmpty
	}
	return nil
}

// streamStats is the reader's O(1) accounting.
type streamStats struct {
	entries   int64
	dirs      int64
	files     int64
	symlinks  int64
	fileBytes int64
	tree      *treeHasher
}

// consumeStream walks the decoded entry stream, applying every rule, and writes
// into target when target is not empty.
func consumeStream(tr *tar.Reader, target string, st *streamStats) error {
	st.tree = newTreeHasher()
	buf := make([]byte, copyBufSize)
	entryDigest := sha256.New()
	ordinal := int64(0)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			// Every byte that got here was authenticated, so a malformed entry
			// stream means this package wrote one. Named as a bad entry rather
			// than as an authentication failure, which it is not.
			return fmt.Errorf("%w: entry %d could not be decoded", ErrEntry, ordinal+1)
		}
		ordinal++
		name := hdr.Name
		if err := validEntryName(name); err != nil {
			return fmt.Errorf("%w: entry %d is not a valid tree entry (%s)", ErrEntry, ordinal, err)
		}
		st.entries++

		switch hdr.Typeflag {
		case tar.TypeDir:
			if hdr.Size != 0 {
				return fmt.Errorf("%w: entry %d is a directory with a byte count", ErrEntry, ordinal)
			}
			mode, err := entryPerm(hdr.Mode, ordinal)
			if err != nil {
				return err
			}
			st.dirs++
			st.tree.dir(name, mode)
			if target == "" {
				continue
			}
			p, err := entryPath(target, name, ordinal)
			if err != nil {
				return err
			}
			if err := makeDir(p, mode); err != nil {
				return fmt.Errorf("%w: entry %d could not be created (%s)", ErrRestore, ordinal, fsCategory(err))
			}

		case tar.TypeSymlink:
			if hdr.Size != 0 {
				return fmt.Errorf("%w: entry %d is a symbolic link with a byte count", ErrEntry, ordinal)
			}
			if err := checkLink(name, hdr.Linkname); err != nil {
				return fmt.Errorf("%w: entry %d is a symbolic link that is not allowed (%s)", ErrEntry, ordinal, err)
			}
			st.symlinks++
			st.tree.symlink(name, hdr.Linkname)
			if target == "" {
				continue
			}
			p, err := entryPath(target, name, ordinal)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return fmt.Errorf("%w: entry %d's parent could not be created (%s)", ErrRestore, ordinal, fsCategory(err))
			}
			if err := os.Symlink(hdr.Linkname, p); err != nil {
				return fmt.Errorf("%w: entry %d could not be created (%s)", ErrRestore, ordinal, fsCategory(err))
			}

		case tar.TypeReg:
			if hdr.Size < 0 {
				return fmt.Errorf("%w: entry %d has a negative byte count", ErrEntry, ordinal)
			}
			mode, err := entryPerm(hdr.Mode, ordinal)
			if err != nil {
				return err
			}
			var dst string
			if target != "" {
				if dst, err = entryPath(target, name, ordinal); err != nil {
					return err
				}
			}
			entryDigest.Reset()
			n, err := restoreFile(dst, tr, hdr, mode, ordinal, buf, entryDigest)
			if err != nil {
				return err
			}
			if n != hdr.Size {
				return fmt.Errorf("%w: entry %d is shorter than its header", ErrTruncated, ordinal)
			}
			st.files++
			st.fileBytes += n
			st.tree.file(name, mode, n, hdr.ModTime.UnixNano(), entryDigest.Sum(nil))

		default:
			// Hard links included, for the reason protocol/workspace refuses one:
			// a hard link's target is resolved by the kernel against paths that
			// already exist, so honoring one would mean reasoning about what is on
			// disk rather than about the stream. This package's Writer never
			// produces one — an in-tree hard link travels as an independent file,
			// which loses link identity and is documented as such — so seeing one
			// here means the stream did not come from this Writer.
			return fmt.Errorf("%w: entry %d is not a regular file, directory or symbolic link", ErrEntry, ordinal)
		}
	}
}

// entryPerm refuses a mode with anything outside the permission bits. Setuid,
// setgid and sticky are dropped by the Writer (see the design note's §4.3), so
// one arriving here did not come from it.
func entryPerm(mode int64, ordinal int64) (fs.FileMode, error) {
	if mode < 0 || mode&^0o777 != 0 {
		return 0, fmt.Errorf("%w: entry %d has mode bits outside the permission bits", ErrEntry, ordinal)
	}
	return fs.FileMode(mode), nil
}

// entryPath joins a validated name onto target and checks containment again.
// validEntryName has already made an escape unrepresentable; this is the check
// that does not trust it, for the reason protocol/workspace's Resolve
// re-derives containment: the last hop before a syscall trusts nobody.
func entryPath(target, name string, ordinal int64) (string, error) {
	clean := filepath.Clean(target)
	p := filepath.Join(clean, filepath.FromSlash(name))
	if p != clean && !strings.HasPrefix(p, clean+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: entry %d would land outside the restore target", ErrEntry, ordinal)
	}
	return p, nil
}

// makeDir creates a directory with exactly the recorded permissions. Mkdir's
// mode is masked by the process umask, so the mode is set explicitly afterwards.
// A directory that is already there — created as an earlier entry's parent — is
// chmodded into place rather than refused; anything else already there is an
// error, which is what keeps a duplicate entry from quietly winning.
func makeDir(p string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := os.Mkdir(p, mode); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return err
		}
		fi, statErr := os.Lstat(p)
		if statErr != nil {
			return statErr
		}
		if !fi.IsDir() {
			return fs.ErrExist
		}
	}
	return os.Chmod(p, mode)
}

// restoreFile hashes an entry's bytes and, when dst is not empty, writes them,
// returning how many bytes it consumed. It builds its own errors because it is
// the one place where a read failure (a broken checkpoint) and a write failure
// (a destination that cannot hold it) need different sentinels.
//
// O_EXCL is deliberate and does two jobs: it refuses to follow a symbolic link
// that is already at the destination, and it makes a duplicated entry name an
// error rather than an overwrite.
func restoreFile(dst string, tr io.Reader, hdr *tar.Header, mode fs.FileMode, ordinal int64, buf []byte, digest io.Writer) (int64, error) {
	if dst == "" {
		n, err := io.CopyBuffer(digest, io.LimitReader(tr, hdr.Size), buf)
		if err != nil {
			return n, fmt.Errorf("%w: entry %d could not be read", ErrTruncated, ordinal)
		}
		return n, nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return 0, fmt.Errorf("%w: entry %d's parent could not be created (%s)", ErrRestore, ordinal, fsCategory(err))
	}
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return 0, fmt.Errorf("%w: entry %d could not be created (%s)", ErrRestore, ordinal, fsCategory(err))
	}
	n, err := io.CopyBuffer(io.MultiWriter(f, digest), io.LimitReader(tr, hdr.Size), buf)
	if err != nil {
		f.Close()
		return n, fmt.Errorf("%w: entry %d could not be written (%s)", ErrRestore, ordinal, fsCategory(err))
	}
	if err := f.Close(); err != nil {
		return n, fmt.Errorf("%w: entry %d could not be closed (%s)", ErrRestore, ordinal, fsCategory(err))
	}
	// Chmod after the write, because O_CREATE's mode is masked by the umask.
	if err := os.Chmod(dst, mode); err != nil {
		return n, fmt.Errorf("%w: entry %d's mode could not be set (%s)", ErrRestore, ordinal, fsCategory(err))
	}
	if err := os.Chtimes(dst, hdr.ModTime, hdr.ModTime); err != nil {
		return n, fmt.Errorf("%w: entry %d's modification time could not be set (%s)", ErrRestore, ordinal, fsCategory(err))
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// the frame reader
// ---------------------------------------------------------------------------

// frameReader is the frame writer in reverse, and it trusts the manifest's frame
// count rather than guessing where the stream ends. That is what makes the final
// flag work: with an authenticated count, frame i's AAD is known to say final if
// and only if i is the last, so a truncated stream runs out of bytes instead of
// finding a plausible ending.
type frameReader struct {
	src        io.Reader
	aead       cipher.AEAD
	c          Context
	prefix     []byte
	frameSize  int64
	frames     uint32
	plainTotal int64

	ct  []byte // reusable ciphertext buffer, frameSize+tagLen
	pt  []byte // the current frame's plaintext, backed by a frameSize array
	pos int

	idx   uint32
	plain int64
	err   error
}

func newFrameReader(src io.Reader, c Context, key, noncePrefix []byte, frameSize int64, frames uint32, plainTotal int64) (*frameReader, error) {
	if frameSize < MinFrameSize || frameSize > MaxFrameSize {
		return nil, fmt.Errorf("%w: the frame size is outside [%d, %d]", ErrManifest, MinFrameSize, MaxFrameSize)
	}
	if frames == 0 {
		return nil, fmt.Errorf("%w: a checkpoint has at least one frame", ErrManifest)
	}
	if len(noncePrefix) != noncePrefixLen {
		return nil, fmt.Errorf("%w: the nonce prefix is %d bytes", ErrManifest, noncePrefixLen)
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	return &frameReader{
		src:        src,
		aead:       aead,
		c:          c,
		prefix:     noncePrefix,
		frameSize:  frameSize,
		frames:     frames,
		plainTotal: plainTotal,
		ct:         make([]byte, frameSize+tagLen),
		pt:         make([]byte, 0, frameSize),
	}, nil
}

// fault is the frame layer's own sticky failure, if it had one. It exists
// because archive/tar sits above this reader and turns any read error into "the
// archive is malformed", which is the wrong sentence for a flipped ciphertext bit
// and the wrong error for a caller to branch on.
func (f *frameReader) fault() error {
	if f.err == nil || errors.Is(f.err, io.EOF) {
		return nil
	}
	return f.err
}

func (f *frameReader) Read(p []byte) (int, error) {
	for f.pos == len(f.pt) {
		if f.err != nil {
			return 0, f.err
		}
		if err := f.next(); err != nil {
			f.err = err
			return 0, err
		}
	}
	n := copy(p, f.pt[f.pos:])
	f.pos += n
	return n, nil
}

func (f *frameReader) next() error {
	if f.idx == f.frames {
		return io.EOF
	}
	remaining := f.plainTotal - f.plain
	want := min(f.frameSize, remaining)
	if want <= 0 {
		// The manifest's own consistency checks make this unreachable; it is here
		// because an unreachable branch that returns an error is better than one
		// that reads a negative length.
		return fmt.Errorf("%w: the frame count and plaintext length disagree", ErrManifest)
	}
	need := want + tagLen
	if _, err := io.ReadFull(f.src, f.ct[:need]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrTruncated
		}
		return fmt.Errorf("checkpoint: reading the content object (%s)", fsCategory(err))
	}

	var nonce [nonceLen]byte
	copy(nonce[:], f.prefix)
	binary.BigEndian.PutUint32(nonce[noncePrefixLen:], f.idx)

	pt, err := f.aead.Open(f.pt[:0], nonce[:], f.ct[:need], f.c.frameAAD(f.idx, f.idx == f.frames-1))
	if err != nil {
		return ErrAuth
	}
	f.pt = pt
	f.pos = 0
	f.idx++
	f.plain += int64(len(pt))
	return nil
}
