package wstream

import (
	"archive/tar"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"
)

// FS is the host's end: a workspace stream, presented as the fs.FS the
// checkpoint writer walks.
//
// It is a CURSOR, not a file system. The stream is sequential and one-way, so
// this type can answer a question about the entry the cursor is at or about any
// entry ahead of it, and never about one behind it. That is sound only because
// the walk order is known in advance: the checkpoint writer walks in fs.WalkDir
// order, this type's ReadDir hands back children in the order that produces,
// and the index at the head of the stream is required to be in exactly that
// order — so every question the writer asks is about the entry the cursor is at
// or the next one. Anything else is a stream that was not in walk order, and is
// refused rather than answered wrongly.
//
// What it holds: the tree's shape (a name and a kind per entry, bounded by
// Limits), one tar header, and the underlying reader. NO file content, ever,
// and no buffer proportional to a file. A workspace of any size passes through
// a 32 KiB copy buffer in the writer above it.
//
// It is NOT safe for concurrent use. One walk, one goroutine — which is what
// the checkpoint writer does, and the cursor could not mean anything otherwise.
type FS struct {
	lim Limits

	// order is every entry the index named, in stream order, INCLUDING the ones
	// this host excludes: the tar carries them too, and the cursor's position is
	// a position in the tar.
	order []idxEntry
	// nodes is the tree the walk sees, which is order minus the exclusions.
	// Keyed by name; "." is the root and is synthesized.
	nodes map[string]*node

	tr *tar.Reader
	// pos is how many tar entries have been consumed. After seek(i) it is i+1
	// and cur is entry i.
	pos int
	cur *tar.Header
	// err is sticky: a stream that has been found malformed stays malformed,
	// and every later call reports the same sentence rather than a second,
	// stranger one derived from a desynchronized cursor.
	err error
}

type idxEntry struct {
	name     string
	kind     byte
	excluded bool
}

// node is one entry of the tree the walk sees. It holds no content and no
// header: everything but the shape comes from the tar when the cursor arrives.
type node struct {
	name     string
	kind     byte
	pos      int // its position in the stream, which is what the cursor seeks to
	children []*node
}

var (
	_ fs.FS         = (*FS)(nil)
	_ fs.StatFS     = (*FS)(nil)
	_ fs.ReadDirFS  = (*FS)(nil)
	_ fs.ReadLinkFS = (*FS)(nil)
)

// NewFS reads the stream's header and index, and returns the tree they
// describe. It reads no tar entry: the content follows the index on the same
// reader, and the first seek is what starts consuming it.
//
// r is read to exactly the end of the index and no further, so a caller that
// wants the rest of the stream for itself (to count its bytes, say) wraps r
// rather than sharing it.
func NewFS(r io.Reader, lim Limits) (*FS, error) {
	lim, err := lim.resolve()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: there is no stream to read", ErrFormat)
	}
	// Bounded before anything is parsed: every limit below is about a number the
	// GUEST chose, and the guest is the untrusted end of this hop.
	br := bufio.NewReaderSize(&limitReader{r: r, left: lim.MaxTotalBytes}, 64<<10)

	magic := make([]byte, len(Magic))
	if _, err := io.ReadFull(br, magic); err != nil {
		return nil, fmt.Errorf("%w: the stream ended before its header", ErrTruncated)
	}
	if string(magic) != Magic {
		return nil, fmt.Errorf("%w: the stream does not begin with this format's header", ErrFormat)
	}

	f := &FS{lim: lim, nodes: map[string]*node{}}
	f.nodes["."] = &node{name: ".", kind: KindDir, pos: -1}
	if err := f.readIndex(br); err != nil {
		return nil, err
	}
	f.tr = tar.NewReader(br)
	return f, nil
}

// readIndex parses the index section and builds the tree, refusing anything
// that is not a strict fs.WalkDir ordering of a valid tree.
//
// The ordering is CHECKED here rather than discovered later, and that is what
// makes the cursor safe: after this returns, the order the walk will ask for
// entries in is known to be the order they arrive in.
func (f *FS) readIndex(br *bufio.Reader) error {
	// last is the previous entry's name, which is how depth-first order is
	// checked: an entry's parent must be the previous entry or one of its
	// ancestors, or the walk jumped sideways into a directory it had left.
	last := ""
	var indexBytes int64
	for {
		n, err := binary.ReadUvarint(br)
		if err != nil {
			return fmt.Errorf("%w: the stream ended inside its index", ErrTruncated)
		}
		if n == 0 {
			return nil
		}
		if n > maxRecordLen {
			return fmt.Errorf("%w: an index record claims %d bytes", ErrFormat, n)
		}
		indexBytes += int64(n)
		if indexBytes > f.lim.MaxIndexBytes {
			return fmt.Errorf("%w: the index is over %d bytes", ErrLimit, f.lim.MaxIndexBytes)
		}
		if int64(len(f.order)) >= f.lim.MaxEntries {
			return fmt.Errorf("%w: the index names more than %d entries", ErrLimit, f.lim.MaxEntries)
		}
		rec := make([]byte, n)
		if _, err := io.ReadFull(br, rec); err != nil {
			return fmt.Errorf("%w: the stream ended inside an index record", ErrTruncated)
		}
		kind, name := rec[0], string(rec[1:])
		ord := int64(len(f.order)) + 1
		switch kind {
		case KindDir, KindFile, KindSymlink:
		default:
			return fmt.Errorf("%w: entry %d names a kind this format does not carry", ErrFormat, ord)
		}
		if err := validName(name); err != nil {
			return fmt.Errorf("%w: entry %d is not a valid name (%s)", ErrEntry, ord, err)
		}
		if err := f.addIndexEntry(ord, name, kind, last); err != nil {
			return err
		}
		last = name
	}
}

// addIndexEntry places one index record in the tree, or refuses it.
func (f *FS) addIndexEntry(ord int64, name string, kind byte, last string) error {
	pos := len(f.order)
	excluded := f.lim.excluded(name)
	f.order = append(f.order, idxEntry{name: name, kind: kind, excluded: excluded})
	if excluded {
		// Dropped from the tree entirely — it is not something the checkpoint
		// writer may ever see — but kept in `order`, because the tar carries it
		// and the cursor has to walk past it.
		return nil
	}
	if _, dup := f.nodes[name]; dup {
		return fmt.Errorf("%w: entry %d is named twice", ErrFormat, ord)
	}
	parent := path.Dir(name)
	p, ok := f.nodes[parent]
	if !ok || p.kind != KindDir {
		// Either the guest named a child before its directory, or the directory
		// is excluded and this entry claims not to be — both are a tree this
		// host will not assemble.
		return fmt.Errorf("%w: entry %d is not inside a directory the stream already named", ErrFormat, ord)
	}
	// Depth first: the parent must be the previous entry or one of its
	// ancestors. Without this, "a/x, b, a/y" would build a tree whose walk
	// order is not the stream's, and the cursor would be asked to go back.
	if last != "" && parent != "." && parent != last && !strings.HasPrefix(last, parent+"/") {
		return fmt.Errorf("%w: entry %d is out of depth-first order", ErrFormat, ord)
	}
	// And lexical within the directory, which is the order fs.WalkDir visits
	// children in and therefore the order ReadDir must return them in.
	if len(p.children) > 0 && path.Base(name) <= path.Base(p.children[len(p.children)-1].name) {
		return fmt.Errorf("%w: entry %d is out of lexical order within its directory", ErrFormat, ord)
	}
	n := &node{name: name, kind: kind, pos: pos}
	f.nodes[name] = n
	p.children = append(p.children, n)
	return nil
}

// Indexed is how many entries the guest's index named, excluded ones included.
// It is what a caller compares against the count in the guest's end marker: the
// two are statements about the same list, made by the two ends of the hop.
func (f *FS) Indexed() int { return len(f.order) }

// Entries is how many entries the TREE presents — Indexed minus whatever this
// host excluded. It is smaller than Indexed exactly when a guest sent something
// the host would not carry, which is worth a line in a log.
func (f *FS) Entries() int { return len(f.nodes) - 1 }

// Open returns the entry at name. For a regular file it is the file's bytes,
// readable exactly once and only while the cursor is on it; for a directory it
// is a listing.
func (f *FS) Open(name string) (fs.File, error) {
	n, err := f.lookup("open", name)
	if err != nil {
		return nil, err
	}
	if n.name == "." {
		return &dirFile{fs: f, node: n, info: rootInfo{}}, nil
	}
	if err := f.seek(n.pos); err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	info, err := f.info(n)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	if n.kind != KindFile {
		return &dirFile{fs: f, node: n, info: info}, nil
	}
	return &streamFile{fs: f, node: n, info: info, at: f.pos, left: f.cur.Size}, nil
}

// Stat is the entry's metadata, read off the tar header when the cursor
// arrives at it. It does not follow symbolic links — there is nothing to
// follow to, and the writer above wants the link.
func (f *FS) Stat(name string) (fs.FileInfo, error) { return f.Lstat(name) }

// Lstat is fs.ReadLinkFS's half of the same thing.
func (f *FS) Lstat(name string) (fs.FileInfo, error) {
	n, err := f.lookup("stat", name)
	if err != nil {
		return nil, err
	}
	if n.name == "." {
		return rootInfo{}, nil
	}
	if err := f.seek(n.pos); err != nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	info, err := f.info(n)
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	return info, nil
}

// ReadLink is what makes a symbolic link travel as a link: without it the
// checkpoint writer refuses the entry rather than following it.
func (f *FS) ReadLink(name string) (string, error) {
	n, err := f.lookup("readlink", name)
	if err != nil {
		return "", err
	}
	if n.kind != KindSymlink {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	if err := f.seek(n.pos); err != nil {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: err}
	}
	return f.cur.Linkname, nil
}

// ReadDir lists a directory's children FROM THE INDEX, without moving the
// cursor. That is the whole reason the index exists: fs.WalkDir asks for every
// child of a directory before it visits the first of them, and the answer is
// spread across the entire stream.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	n, err := f.lookup("readdir", name)
	if err != nil {
		return nil, err
	}
	if n.kind != KindDir {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: errors.New("not a directory")}
	}
	out := make([]fs.DirEntry, 0, len(n.children))
	for _, c := range n.children {
		out = append(out, &dirEntry{fs: f, node: c})
	}
	// Already lexical by construction (addIndexEntry refuses anything else);
	// sorted again because fs.ReadDir's contract is the sort, not our
	// construction, and a contract the caller relies on is not a place to save
	// a comparison.
	slices.SortFunc(out, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return out, nil
}

func (f *FS) lookup(op, name string) (*node, error) {
	if f.err != nil {
		return nil, &fs.PathError{Op: op, Path: name, Err: f.err}
	}
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	n, ok := f.nodes[name]
	if !ok {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	return n, nil
}

// seek moves the cursor to the entry at stream position i, consuming — and
// discarding — everything between. It never moves backwards: a stream is
// one-way, and a request for an entry already passed is a walk that did not
// happen in the order the index promised.
func (f *FS) seek(i int) error {
	if f.err != nil {
		return f.err
	}
	if f.pos > i+1 {
		return f.fail(fmt.Errorf("%w: the tree was walked out of the order the stream is in", ErrFormat))
	}
	for f.pos <= i {
		if err := f.next(); err != nil {
			return err
		}
	}
	return nil
}

// next consumes one tar entry and checks it against the index. tar.Reader.Next
// discards whatever is left of the previous entry's body, which is what makes
// skipping an excluded entry free.
func (f *FS) next() error {
	hdr, err := f.tr.Next()
	if errors.Is(err, io.EOF) {
		return f.fail(fmt.Errorf("%w: the stream ended before the entries its index named", ErrTruncated))
	}
	if err != nil {
		if errors.Is(err, errStreamTooLarge) {
			return f.fail(fmt.Errorf("%w: the stream is over %d bytes", ErrLimit, f.lim.MaxTotalBytes))
		}
		return f.fail(fmt.Errorf("%w: an entry's header could not be read", ErrFormat))
	}
	want := f.order[f.pos]
	ord := int64(f.pos) + 1
	// A directory's name is compared with a trailing slash removed: tar's own
	// convention allows one and this format's writer emits none, and a
	// disagreement about a slash is not worth failing a suspend over.
	got := hdr.Name
	if hdr.Typeflag == tar.TypeDir {
		got = strings.TrimSuffix(got, "/")
	}
	if got != want.name {
		return f.fail(fmt.Errorf("%w: entry %d is not the entry the index named there", ErrFormat, ord))
	}
	if err := f.checkHeader(hdr, want, ord); err != nil {
		return f.fail(err)
	}
	f.cur = hdr
	f.pos++
	return nil
}

// checkHeader applies every rule this host enforces on one entry. It runs for
// EXCLUDED entries too: an excluded entry's body is still bytes on this
// connection, and its size still counts against the stream.
func (f *FS) checkHeader(hdr *tar.Header, want idxEntry, ord int64) error {
	var kind byte
	switch hdr.Typeflag {
	case tar.TypeDir:
		kind = KindDir
	case tar.TypeReg:
		kind = KindFile
	case tar.TypeSymlink:
		kind = KindSymlink
	default:
		// A hard link, a device node, a FIFO, a GNU extension. The writer of
		// this format never produces one, and none of them is a thing a
		// workspace checkpoint carries.
		return fmt.Errorf("%w: entry %d is not a regular file, directory or symbolic link", ErrEntry, ord)
	}
	if kind != want.kind {
		return fmt.Errorf("%w: entry %d is not the kind the index named", ErrFormat, ord)
	}
	for k := range hdr.PAXRecords {
		// A sparse entry's defining property is a header size far larger than
		// the bytes behind it, which is the one thing a bounded reader cannot
		// tolerate. archive/tar implements them; this format does not have them.
		if strings.HasPrefix(k, "GNU.sparse.") {
			return fmt.Errorf("%w: entry %d is a sparse entry", ErrEntry, ord)
		}
	}
	if hdr.Mode < 0 || hdr.Mode&^0o777 != 0 {
		// Setuid, setgid and sticky are dropped by the writer of this format
		// and by the checkpoint writer above it. One arriving here did not come
		// from either.
		return fmt.Errorf("%w: entry %d has mode bits outside the permission bits", ErrEntry, ord)
	}
	switch kind {
	case KindFile:
		if hdr.Size < 0 || hdr.Size > f.lim.MaxEntryBytes {
			return fmt.Errorf("%w: entry %d is larger than %d bytes", ErrLimit, ord, f.lim.MaxEntryBytes)
		}
	case KindSymlink:
		if err := validLink(want.name, hdr.Linkname); err != nil {
			return fmt.Errorf("%w: entry %d is a symbolic link that is not allowed (%s)", ErrEntry, ord, err)
		}
	case KindDir:
		if hdr.Size != 0 {
			return fmt.Errorf("%w: entry %d is a directory with a body", ErrFormat, ord)
		}
	}
	return nil
}

// fail records a stream as unusable. Everything afterwards reports the same
// sentence: a cursor that has lost its place cannot produce a second, more
// specific diagnosis, only a stranger one.
func (f *FS) fail(err error) error {
	if f.err == nil {
		f.err = err
	}
	return f.err
}

// info is the FileInfo for the entry the cursor is on.
func (f *FS) info(n *node) (fs.FileInfo, error) {
	if f.cur == nil || f.pos != n.pos+1 {
		return nil, f.fail(fmt.Errorf("%w: the tree was walked out of the order the stream is in", ErrFormat))
	}
	return &entryInfo{hdr: f.cur, name: path.Base(n.name), kind: n.kind}, nil
}

// ---------------------------------------------------------------------------
// the fs.File and fs.DirEntry implementations
// ---------------------------------------------------------------------------

// streamFile is one regular file's bytes, read straight off the tar reader.
// There is no buffer: what the caller reads is what the connection delivers.
type streamFile struct {
	fs   *FS
	node *node
	info fs.FileInfo
	// at is the cursor position this file was opened at. A read after the
	// cursor has moved on is refused rather than served from whatever the
	// stream is now pointing at.
	at   int
	left int64
}

func (s *streamFile) Stat() (fs.FileInfo, error) { return s.info, nil }
func (s *streamFile) Close() error               { return nil }

func (s *streamFile) Read(p []byte) (int, error) {
	if s.fs.err != nil {
		return 0, s.fs.err
	}
	if s.fs.pos != s.at {
		return 0, s.fs.fail(fmt.Errorf("%w: an entry was read after the stream had moved past it", ErrFormat))
	}
	if s.left == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > s.left {
		p = p[:s.left]
	}
	n, err := s.fs.tr.Read(p)
	s.left -= int64(n)
	if errors.Is(err, io.EOF) && s.left > 0 {
		return n, s.fs.fail(fmt.Errorf("%w: an entry's body ended before the length its header promised", ErrTruncated))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		if errors.Is(err, errStreamTooLarge) {
			return n, s.fs.fail(fmt.Errorf("%w: the stream is over %d bytes", ErrLimit, s.fs.lim.MaxTotalBytes))
		}
		return n, s.fs.fail(fmt.Errorf("%w: an entry's body could not be read", ErrTruncated))
	}
	return n, nil
}

// dirFile is a directory (and, for completeness, a symbolic link) opened as a
// file. Reading one is an error rather than an empty file, which is what an
// operating system does and what keeps a caller that opened the wrong thing
// from silently checkpointing nothing.
type dirFile struct {
	fs   *FS
	node *node
	info fs.FileInfo
	read int
}

func (d *dirFile) Stat() (fs.FileInfo, error) { return d.info, nil }
func (d *dirFile) Close() error               { return nil }

func (d *dirFile) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.node.name, Err: errors.New("is a directory")}
}

func (d *dirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	all, err := d.fs.ReadDir(d.node.name)
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		d.read = len(all)
		return all, nil
	}
	if d.read >= len(all) {
		return nil, io.EOF
	}
	end := min(d.read+n, len(all))
	out := all[d.read:end]
	d.read = end
	return out, nil
}

// dirEntry is one child in a listing. Its Info is LAZY — it moves the cursor —
// because the walk asks for a directory's children long before it visits them,
// and a listing that stat'ed each child would have to read the whole stream to
// answer.
type dirEntry struct {
	fs   *FS
	node *node
}

func (d *dirEntry) Name() string { return path.Base(d.node.name) }
func (d *dirEntry) IsDir() bool  { return d.node.kind == KindDir }

func (d *dirEntry) Type() fs.FileMode {
	switch d.node.kind {
	case KindDir:
		return fs.ModeDir
	case KindSymlink:
		return fs.ModeSymlink
	default:
		return 0
	}
}

func (d *dirEntry) Info() (fs.FileInfo, error) { return d.fs.Lstat(d.node.name) }

// entryInfo is a tar header, as an fs.FileInfo. Nothing here is computed: the
// mode, the size and the modification time are what the header said, so that
// what the checkpoint records is what the guest sent.
type entryInfo struct {
	hdr  *tar.Header
	name string
	kind byte
}

func (e *entryInfo) Name() string       { return e.name }
func (e *entryInfo) Size() int64        { return e.hdr.Size }
func (e *entryInfo) ModTime() time.Time { return e.hdr.ModTime }
func (e *entryInfo) IsDir() bool        { return e.kind == KindDir }
func (e *entryInfo) Sys() any           { return nil }

func (e *entryInfo) Mode() fs.FileMode {
	mode := fs.FileMode(e.hdr.Mode) & fs.ModePerm
	switch e.kind {
	case KindDir:
		mode |= fs.ModeDir
	case KindSymlink:
		mode |= fs.ModeSymlink
	}
	return mode
}

// rootInfo is the tree's root, which the stream does not carry an entry for —
// the root is the destination rather than an entry, in this format as in the
// checkpoint one. Its mode is a plain 0755 directory, and nothing reads it: the
// checkpoint writer's walk skips ".".
type rootInfo struct{}

func (rootInfo) Name() string       { return "." }
func (rootInfo) Size() int64        { return 0 }
func (rootInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o755 }
func (rootInfo) ModTime() time.Time { return time.Unix(0, 0).UTC() }
func (rootInfo) IsDir() bool        { return true }
func (rootInfo) Sys() any           { return nil }

// limitReader bounds the whole stream, headers and bodies alike, before any of
// it is parsed. It is the outermost thing on this path on purpose: a guest that
// sends bytes forever must be stopped by a counter that no parser's opinion can
// move.
type limitReader struct {
	r    io.Reader
	left int64
}

var errStreamTooLarge = errors.New("wstream: the stream is over its byte limit")

func (l *limitReader) Read(p []byte) (int, error) {
	if l.left <= 0 {
		return 0, errStreamTooLarge
	}
	if int64(len(p)) > l.left {
		p = p[:l.left]
	}
	n, err := l.r.Read(p)
	l.left -= int64(n)
	return n, err
}
