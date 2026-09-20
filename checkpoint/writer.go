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
	"hash"
	"io"
	"io/fs"
	"time"
)

// WriterOptions configures a Writer. Everything in it is stable across
// checkpoints of one deployment; per-checkpoint values are arguments to Write.
type WriterOptions struct {
	// Prefix is the storage prefix every object goes under. It may be empty.
	Prefix string

	// KeyRef is the key-encryption key to wrap the data key under. It may be an
	// alias — rotation is the Wrapper's business — and whatever concrete version
	// the Wrapper reports is what the manifest records. An empty KeyRef means
	// "whatever the Wrapper wraps under by default", which is only useful with a
	// single-key wrapper.
	KeyRef KeyRef

	// FrameSize is the plaintext frame size. Zero means DefaultFrameSize.
	FrameSize int64

	// rand and now are test seams. Unexported, so a caller outside this package
	// gets crypto/rand and time.Now and cannot get anything else: a checkpoint
	// whose data key came from a caller-supplied stream is a checkpoint whose
	// confidentiality is a caller's bug.
	rand io.Reader
	now  func() time.Time
}

// Writer produces checkpoints. It holds no per-checkpoint state, so one Writer
// serves every session on a host and Write is safe to call concurrently for
// different contexts.
type Writer struct {
	store BlobStore
	keys  Wrapper
	opts  WriterOptions
}

// NewWriter validates the options once, so that Write's failures are about the
// tree and the store rather than about configuration.
func NewWriter(store BlobStore, keys Wrapper, opts WriterOptions) (*Writer, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: a blob store is required", ErrInvalid)
	}
	if keys == nil {
		return nil, fmt.Errorf("%w: a key wrapper is required", ErrInvalid)
	}
	if err := validPrefix(opts.Prefix); err != nil {
		return nil, err
	}
	if len(opts.KeyRef) > maxKeyRefLen {
		return nil, fmt.Errorf("%w: the key reference is over the %d character limit", ErrInvalid, maxKeyRefLen)
	}
	if opts.FrameSize == 0 {
		opts.FrameSize = DefaultFrameSize
	}
	if opts.FrameSize < MinFrameSize || opts.FrameSize > MaxFrameSize {
		return nil, fmt.Errorf("%w: the frame size is outside [%d, %d]", ErrInvalid, MinFrameSize, MaxFrameSize)
	}
	if opts.rand == nil {
		opts.rand = cryptoRand
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	return &Writer{store: store, keys: keys, opts: opts}, nil
}

// Result is what a committed checkpoint leaves behind: the two object keys a
// deletion ledger needs, and the manifest a freshness view needs.
type Result struct {
	ManifestKey string
	ContentKey  string
	Manifest    Manifest
}

// Write produces the checkpoint for c from src and commits it. In order:
//
//  1. Draw the data key, the frame nonce prefix, and the content object's
//     attempt suffix — all from one read of the random source, so that the draw
//     order is structural rather than incidental.
//  2. Derive the content and manifest subkeys; wrap the data key under the
//     configured key reference with the authenticated context as additional data.
//  3. Stream the tree into the content object with one put-if-absent, computing
//     every aggregate the manifest needs during that single pass.
//  4. Seal and commit the manifest with a second put-if-absent. THAT call is the
//     atomic commit: before it there is no checkpoint at this generation, after
//     it there is exactly one.
//
// A caller may not report a cold suspension successful on Write alone. The
// durability barrier is Write followed by Reader.Verify, which reads the
// committed bytes back; see this package's documentation.
//
// The source must be quiesced. Write does not enforce that — ADR-0003 §4.4 puts
// quiescing on the caller — but it detects the violation: a file whose size
// changed between the walk's stat and the read fails the whole checkpoint,
// because a tar header has already promised a length and a stream that
// disagreed with it would be a corrupt checkpoint that verified.
func (w *Writer) Write(ctx context.Context, c Context, src Source) (Result, error) {
	if err := c.Validate(); err != nil {
		return Result{}, err
	}
	if err := src.validate(); err != nil {
		return Result{}, err
	}

	seed := make([]byte, dekLen+noncePrefixLen+attemptLen)
	if _, err := io.ReadFull(w.opts.rand, seed); err != nil {
		return Result{}, fmt.Errorf("checkpoint: drawing the checkpoint's random values: %w", err)
	}
	dek := seed[:dekLen]
	noncePrefix := seed[dekLen : dekLen+noncePrefixLen]
	attempt := hex.EncodeToString(seed[dekLen+noncePrefixLen:])
	defer wipe(seed)

	kContent, kManifest, err := deriveSubkeys(dek, c)
	if err != nil {
		return Result{}, err
	}
	defer wipe(kContent)
	defer wipe(kManifest)

	wrapped, usedRef, err := w.keys.Wrap(ctx, w.opts.KeyRef, dek, c.aad(purposeKey))
	if err != nil {
		return Result{}, fmt.Errorf("checkpoint: wrapping the checkpoint key: %w", err)
	}
	switch {
	case usedRef == "":
		return Result{}, fmt.Errorf("%w: the key wrapper did not report the key version it used", ErrInvalid)
	case len(usedRef) > maxKeyRefLen:
		return Result{}, fmt.Errorf("%w: the key wrapper reported a reference over the %d character limit", ErrInvalid, maxKeyRefLen)
	case len(wrapped) < dekLen:
		return Result{}, fmt.Errorf("%w: the key wrapper returned an implausibly short wrapped key", ErrInvalid)
	}

	contentKey := contentKeyFor(w.opts.Prefix, c, attempt)
	var st writeStats
	err = w.store.PutIfAbsent(ctx, contentKey, func(dst io.Writer) error {
		return writeContent(dst, c, kContent, noncePrefix, w.opts.FrameSize, src, &st)
	})
	if err != nil {
		return Result{}, err
	}

	m := Manifest{
		Version:       FormatVersion,
		Workspace:     c.Workspace,
		Session:       c.Session,
		Generation:    c.Generation,
		CreatedAt:     formatCreatedAt(w.opts.now()),
		Cipher:        cipherName,
		KDF:           kdfName,
		Compression:   compressionNone,
		KeyRef:        string(usedRef),
		WrappedKey:    base64.StdEncoding.EncodeToString(wrapped),
		ContentKey:    contentKey,
		FrameSize:     w.opts.FrameSize,
		Frames:        st.frames,
		NoncePrefix:   base64.StdEncoding.EncodeToString(noncePrefix),
		ContentBytes:  st.contentBytes,
		ContentDigest: st.contentDigest,
		PlainBytes:    st.plainBytes,
		Entries:       st.entries,
		FileBytes:     st.fileBytes,
		Skipped:       st.skipped,
		TreeDigest:    st.treeDigest,
	}
	if err := m.seal(kManifest, c, w.opts.rand); err != nil {
		return Result{}, err
	}
	// The writer holds itself to the reader's parser. A manifest this package
	// produced that its own ParseManifest would refuse is a bug worth failing the
	// suspend for, not one worth discovering at restore.
	if err := m.validate(); err != nil {
		return Result{}, err
	}
	enc, err := m.Encode()
	if err != nil {
		return Result{}, err
	}

	mk := manifestKey(w.opts.Prefix, c)
	if err := w.store.PutIfAbsent(ctx, mk, func(dst io.Writer) error {
		_, err := dst.Write(enc)
		return err
	}); err != nil {
		// A lost race leaves the winner's checkpoint entirely alone, including
		// this attempt's orphaned content object: it is unreadable without this
		// manifest's wrapped key, so leaving it for the retention sweeper costs
		// storage and discloses nothing, while deleting it here would mean a
		// reconciler racing itself could delete the object it had just committed.
		return Result{}, err
	}
	return Result{ManifestKey: mk, ContentKey: contentKey, Manifest: m}, nil
}

// entryModTime is the modification time a REGULAR FILE's entry carries:
// truncated to the second, deliberately and by this package rather than by
// archive/tar.
//
// tar's own header holds whole seconds. Sub-second precision is representable
// only as a PAX extended record, which costs a 1 KiB block PER ENTRY — a
// million-file tree would pay a gigabyte for nanoseconds no build tool asks for,
// and archive/tar rounds to the second on its own unless a format is named. So
// the truncation happens here, where it is visible, where the tree digest can use
// the same value, and where a restore reproduces exactly what was recorded. A
// header whose name is too long for USTAR still becomes a PAX entry; that is
// archive/tar's choice and it is per-entry rather than universal.
func entryModTime(info fs.FileInfo) time.Time {
	return info.ModTime().Truncate(time.Second)
}

// unsetModTime is what a directory's and a symbolic link's entry carries
// instead: the Unix epoch, which is a zero in a tar header and needs no PAX
// record.
//
// Neither one is restored — a directory's mtime is a function of the order its
// children were written, and os.Chtimes on a symlink is not in the standard
// library — so carrying the source's value would make the PLAINTEXT stream depend
// on something the format does not preserve. With these two pinned to a constant,
// the plaintext is a function of exactly what the tree digest commits to: two
// checkpoints of the same tree have byte-identical plaintext (their ciphertext
// still differs, because the data key is fresh per checkpoint), re-checkpointing
// a restored tree reproduces the same tree digest and the same plaintext length,
// and a future differential format has something stable to diff against.
func unsetModTime() time.Time { return time.Unix(0, 0).UTC() }

// writeStats is the single pass's output: everything the manifest commits to
// that is only knowable after the tree has been streamed. It is O(1), which is
// the whole reason the manifest is written second.
type writeStats struct {
	frames        uint32
	contentBytes  int64
	contentDigest string
	plainBytes    int64
	entries       int64
	fileBytes     int64
	skipped       int64
	treeDigest    string
}

// writeContent streams src into dst as the encrypted content object, filling st.
//
// The stack is: tar.Writer -> frameWriter -> (counter, SHA-256) -> the store's
// writer. Nothing in it buffers more than one frame.
func writeContent(dst io.Writer, c Context, key, noncePrefix []byte, frameSize int64, src Source, st *writeStats) error {
	counted := &countingWriter{w: dst}
	digest := sha256.New()
	fw, err := newFrameWriter(io.MultiWriter(counted, digest), c, key, noncePrefix, frameSize)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(fw)
	if err := walkTree(src, tw, st); err != nil {
		return err
	}
	// tar's end-of-archive marker, then the final frame. Both must succeed before
	// the aggregates are recorded: a Close error here means the content object is
	// incomplete, and PutIfAbsent's contract says an errored write leaves nothing.
	if err := tw.Close(); err != nil {
		return fmt.Errorf("checkpoint: closing the checkpoint entry stream: %w", err)
	}
	if err := fw.Close(); err != nil {
		return err
	}
	st.frames = fw.frames
	st.plainBytes = fw.plain
	st.contentBytes = counted.n
	st.contentDigest = hex.EncodeToString(digest.Sum(nil))
	return nil
}

// walkTree writes every entry of src into tw, in fs.WalkDir order, which is
// lexical and therefore deterministic.
//
// Refusals name the entry's ORDINAL and its kind, never its path — see errors.go
// for why, and the design note's §14 for what it costs.
func walkTree(src Source, tw *tar.Writer, st *writeStats) error {
	buf := make([]byte, copyBufSize)
	tree := newTreeHasher()
	entryDigest := sha256.New()
	ordinal := int64(0)

	err := fs.WalkDir(src.FS, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			// fs.WalkDir hands back the file system's own error, which is an
			// *fs.PathError carrying the path. Reduced, never forwarded.
			return fmt.Errorf("%w: entry %d could not be read (%s)", ErrSource, ordinal+1, fsCategory(err))
		}
		if name == "." {
			return nil // the root is the destination, not an entry
		}
		if src.excluded(name) {
			if d.IsDir() {
				// Pruned, not filtered: the directory is never opened and nothing
				// inside it is ever read, which is what makes an exclusion a
				// guarantee about the store rather than about the archive.
				return fs.SkipDir
			}
			return nil
		}
		ordinal++
		if err := validEntryName(name); err != nil {
			return fmt.Errorf("%w: entry %d is not a valid tree entry (%s)", ErrSource, ordinal, err)
		}

		switch {
		case d.IsDir():
			info, err := d.Info()
			if err != nil {
				return fmt.Errorf("%w: entry %d could not be inspected (%s)", ErrSource, ordinal, fsCategory(err))
			}
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeDir,
				Name:     name,
				Mode:     int64(info.Mode().Perm()),
				ModTime:  unsetModTime(),
			}); err != nil {
				return fmt.Errorf("%w: entry %d could not be written (%s)", ErrSource, ordinal, fsCategory(err))
			}
			st.entries++
			tree.dir(name, info.Mode())
			return nil

		case d.Type()&fs.ModeSymlink != 0:
			target, err := fs.ReadLink(src.FS, name)
			if err != nil {
				// Either the file system does not implement fs.ReadLinkFS, in
				// which case a symlink cannot travel as a link and must not be
				// silently followed or dropped, or the read failed.
				return fmt.Errorf("%w: entry %d is a symbolic link this source cannot read (%s)",
					ErrSource, ordinal, fsCategory(err))
			}
			if err := checkLink(name, target); err != nil {
				return fmt.Errorf("%w: entry %d is a symbolic link that is not allowed (%s)", ErrSource, ordinal, err)
			}
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     name,
				Linkname: target,
				ModTime:  unsetModTime(),
			}); err != nil {
				return fmt.Errorf("%w: entry %d could not be written (%s)", ErrSource, ordinal, fsCategory(err))
			}
			st.entries++
			tree.symlink(name, target)
			return nil

		case d.Type().IsRegular():
			info, err := d.Info()
			if err != nil {
				return fmt.Errorf("%w: entry %d could not be inspected (%s)", ErrSource, ordinal, fsCategory(err))
			}
			// A separate function, not an inline body with a defer: a deferred
			// Close inside a WalkDir callback would hold one descriptor per file
			// until the whole walk finished, which is O(entries) of exactly the
			// resource this package promises to keep constant.
			digest, err := writeFileEntry(tw, src.FS, name, info, buf, entryDigest)
			if err != nil {
				return fmt.Errorf("%w: entry %d %s", ErrSource, ordinal, err)
			}
			st.entries++
			st.fileBytes += info.Size()
			tree.file(name, info.Mode(), info.Size(), entryModTime(info).UnixNano(), digest)
			return nil

		default:
			// A socket, a device node, a FIFO. Skipped and counted rather than
			// refused: protocol/workspace refuses one because a transfer the user
			// asked for should fail on a surprise, but a stray dev-server socket
			// must not be able to defeat the durability barrier. The count is in
			// the manifest, so this is never silent.
			st.skipped++
			return nil
		}
	})
	if err != nil {
		return err
	}
	st.treeDigest = tree.sum()
	return nil
}

// writeFileEntry writes one regular file's header and bytes, returning the
// file's SHA-256. Its errors are fragments — "could not be opened (permission
// denied)" — that walkTree prefixes with the ordinal, so that the ordinal is
// stamped in exactly one place and no path ever is.
func writeFileEntry(tw *tar.Writer, fsys fs.FS, name string, info fs.FileInfo, buf []byte, digest hash.Hash) ([]byte, error) {
	size := info.Size()
	if err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     name,
		Mode:     int64(info.Mode().Perm()),
		Size:     size,
		ModTime:  entryModTime(info),
	}); err != nil {
		return nil, fmt.Errorf("could not be written (%s)", fsCategory(err))
	}
	f, err := fsys.Open(name)
	if err != nil {
		return nil, fmt.Errorf("could not be opened (%s)", fsCategory(err))
	}
	defer f.Close()

	digest.Reset()
	// Bounded by the size the header already promised, so a file that shrank
	// under the walk fails rather than desynchronizing the stream.
	n, err := io.CopyBuffer(io.MultiWriter(tw, digest), io.LimitReader(f, size), buf)
	if err != nil {
		return nil, fmt.Errorf("could not be read (%s)", fsCategory(err))
	}
	if n != size {
		return nil, errors.New("shrank while it was being read; the source was not quiesced")
	}
	// And one byte past the promise, to catch a file that GREW: the tar entry
	// would otherwise be a silent truncation that verified.
	var probe [1]byte
	if k, _ := f.Read(probe[:]); k > 0 {
		return nil, errors.New("grew while it was being read; the source was not quiesced")
	}
	return digest.Sum(nil), nil
}

// ---------------------------------------------------------------------------
// the frame writer
// ---------------------------------------------------------------------------

// frameWriter cuts a plaintext stream into fixed-size frames and seals each one.
//
// The subtlety is the FINAL flag. A frame's AAD says whether it is the last, so
// a truncated stream cannot present its last surviving frame as the end — which
// means a full buffer cannot be sealed when it fills, because whether it is the
// last frame is not yet known. So a full buffer is held until either another byte
// arrives (seal it as non-final) or Close is called (seal it as final). That is
// the whole trick, and it is why this type buffers exactly one frame and never
// two.
type frameWriter struct {
	dst       io.Writer
	aead      cipher.AEAD
	c         Context
	prefix    []byte
	frameSize int

	buf []byte // pending plaintext, 0..frameSize
	out []byte // reusable ciphertext buffer

	frames uint32
	plain  int64
	closed bool
}

func newFrameWriter(dst io.Writer, c Context, key, noncePrefix []byte, frameSize int64) (*frameWriter, error) {
	if frameSize < MinFrameSize || frameSize > MaxFrameSize {
		return nil, fmt.Errorf("%w: the frame size is outside [%d, %d]", ErrInvalid, MinFrameSize, MaxFrameSize)
	}
	if len(noncePrefix) != noncePrefixLen {
		return nil, fmt.Errorf("%w: the nonce prefix is %d bytes", ErrInvalid, noncePrefixLen)
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	return &frameWriter{
		dst:       dst,
		aead:      aead,
		c:         c,
		prefix:    noncePrefix,
		frameSize: int(frameSize),
		buf:       make([]byte, 0, int(frameSize)),
		out:       make([]byte, 0, int(frameSize)+tagLen),
	}, nil
}

func (f *frameWriter) Write(p []byte) (int, error) {
	if f.closed {
		return 0, errors.New("checkpoint: write after the content stream was closed")
	}
	total := len(p)
	for len(p) > 0 {
		if len(f.buf) == f.frameSize {
			if err := f.flush(false); err != nil {
				return total - len(p), err
			}
		}
		n := min(f.frameSize-len(f.buf), len(p))
		f.buf = append(f.buf, p[:n]...)
		p = p[n:]
	}
	f.plain += int64(total)
	return total, nil
}

// Close seals whatever is pending as the final frame. It is always at least one
// frame: a tar stream is never empty, and even an empty one would produce a
// final frame rather than a zero-frame checkpoint no reader could describe.
func (f *frameWriter) Close() error {
	if f.closed {
		return nil
	}
	if err := f.flush(true); err != nil {
		return err
	}
	f.closed = true
	return nil
}

func (f *frameWriter) flush(final bool) error {
	if f.frames == ^uint32(0) {
		return ErrTooLarge
	}
	var nonce [nonceLen]byte
	copy(nonce[:], f.prefix)
	binary.BigEndian.PutUint32(nonce[noncePrefixLen:], f.frames)

	ct := f.aead.Seal(f.out[:0], nonce[:], f.buf, f.c.frameAAD(f.frames, final))
	if _, err := f.dst.Write(ct); err != nil {
		return fmt.Errorf("checkpoint: writing the content object: %w", err)
	}
	f.frames++
	f.buf = f.buf[:0]
	return nil
}

// hashReader counts and hashes everything read through it, which is how the
// ciphertext digest and length are checked without a second pass over the object.
type hashReader struct {
	r io.Reader
	h hash.Hash
	n int64
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.h.Write(p[:n])
		h.n += int64(n)
	}
	return n, err
}
