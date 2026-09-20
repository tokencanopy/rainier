package checkpoint

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
)

// BlobStore is the whole of what a checkpoint needs from durable storage:
// create-if-absent, read, delete. Object storage everywhere implements the first
// one conditionally (GCS with x-goog-if-generation-match: 0, S3 with
// If-None-Match: *), and that conditional create is the only primitive the
// durability barrier needs — see the design note's §6.
//
// Two requirements an implementation must meet, because the format's atomicity
// rests on them rather than on anything in this package:
//
//   - PutIfAbsent either creates the WHOLE object or leaves nothing. A write
//     callback that returns an error, or a process that dies mid-upload, must not
//     leave a partial object visible at key. A resumable-upload implementation
//     aborts its session; the in-memory store below buffers and inserts on
//     success only.
//   - PutIfAbsent returns an error satisfying errors.Is(err, ErrExists) when an
//     object is already there, and never overwrites it. Open returns one
//     satisfying errors.Is(err, ErrNotFound) for a missing object.
//
// The write side is a callback rather than an io.Reader parameter so that the
// producer can be a streaming tar writer with no io.Pipe and no goroutine
// between it and the network, and so that a real implementation may use its own
// resumable-upload writer — which is the only cheap form of resumability this
// format wants (design note §4.7).
type BlobStore interface {
	PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

// ---------------------------------------------------------------------------
// the storage layout
// ---------------------------------------------------------------------------

// The layout, for one checkpoint at generation N:
//
//	<prefix>/rainier.checkpoint.v1/ws/<workspace>/sess/<session>/gen/<020d N>/manifest.json
//	<prefix>/rainier.checkpoint.v1/ws/<workspace>/sess/<session>/gen/<020d N>/content.<attempt>
//
// Every component is derived from the prefix and the Context, so a restoring
// caller needs only the two things a control plane already holds and never
// plumbs an object name through its own tables.
//
// The generation is zero-padded to 20 digits — the width of math.MaxUint64 — so
// a lexical listing of the gen/ prefix is generation order. The retention
// sweeper and the "latest verified checkpoint" view both want that, and neither
// should have to sort.
//
// The content object carries a random attempt suffix. Two writers at the same
// generation — a retry after a host died mid-upload, a reconciler racing the
// original — therefore never collide, never overwrite, and never have to reason
// about whether a half-written object was theirs. The manifest key has no
// suffix, which is what makes its put-if-absent the single atomic commit.
const (
	manifestObject = "manifest.json"
	contentObject  = "content"
	// attemptLen is the random content-key suffix, in bytes before hex.
	attemptLen = 16
	// maxPrefixLen bounds the caller's prefix, so that a storage key stays a
	// bounded string.
	maxPrefixLen = 512
	// maxContentKeyLen bounds the content key a manifest may name.
	maxContentKeyLen = 1024
)

// generationDir is the directory every object of one checkpoint lives in.
func generationDir(prefix string, c Context) string {
	var b strings.Builder
	if prefix != "" {
		b.WriteString(prefix)
		b.WriteByte('/')
	}
	fmt.Fprintf(&b, "%s/ws/%s/sess/%s/gen/%020d", FormatVersion, c.Workspace, c.Session, c.Generation)
	return b.String()
}

func manifestKey(prefix string, c Context) string {
	return generationDir(prefix, c) + "/" + manifestObject
}

func contentKeyFor(prefix string, c Context, attempt string) string {
	return generationDir(prefix, c) + "/" + contentObject + "." + attempt
}

// validPrefix refuses a prefix that would make a key ambiguous or unbounded.
func validPrefix(prefix string) error {
	if len(prefix) > maxPrefixLen {
		return fmt.Errorf("%w: the store prefix is over the %d character limit", ErrInvalid, maxPrefixLen)
	}
	if strings.HasPrefix(prefix, "/") || strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("%w: the store prefix must not begin or end with a slash", ErrInvalid)
	}
	if strings.Contains(prefix, "//") || strings.Contains(prefix, "..") {
		return fmt.Errorf("%w: the store prefix must not contain an empty element or \"..\"", ErrInvalid)
	}
	for i := 0; i < len(prefix); i++ {
		if prefix[i] < 0x20 || prefix[i] == 0x7f {
			return fmt.Errorf("%w: the store prefix contains a control character", ErrInvalid)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// the in-memory store
// ---------------------------------------------------------------------------

// MemoryStore is a BlobStore in a map. It exists for this package's tests, for
// a host's own tests, and so that the semantics PutIfAbsent promises are
// readable in one screen rather than inferred from a cloud SDK.
//
// It is the ONLY storage backend in this repository, on purpose: a checkpoint
// has to be restorable on another qualified provider (PRD §10), and the way to
// keep that true is for the format to have never met a provider.
type MemoryStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

var _ BlobStore = (*MemoryStore)(nil)

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string][]byte)}
}

// PutIfAbsent runs write into a buffer and inserts the result only if write
// returned nil and nothing is at key. Buffering is what makes it atomic, and is
// also why this store is not a production one: a real object store streams.
//
// The absence check is repeated after write returns, under the same lock the
// insert takes, so two concurrent writers cannot both see an empty key.
func (m *MemoryStore) PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	_, exists := m.objects[key]
	m.mu.Unlock()
	if exists {
		return fmt.Errorf("%w: %s", ErrExists, "the object is already present")
	}

	var buf bytes.Buffer
	if err := write(&buf); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objects[key]; exists {
		return fmt.Errorf("%w: %s", ErrExists, "the object is already present")
	}
	m.objects[key] = buf.Bytes()
	return nil
}

// Open returns a reader over the object at key.
func (m *MemoryStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// Delete removes the object at key. A missing object is success, because
// deletion is retried and a ledger that cannot report "already gone" as done
// never converges.
func (m *MemoryStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

// Keys returns every stored key, sorted. For tests.
func (m *MemoryStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.objects))
}

// Object returns a copy of the bytes at key. For tests.
func (m *MemoryStore) Object(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, false
	}
	return bytes.Clone(b), true
}

// Overwrite replaces the bytes at key, bypassing put-if-absent. It is spelled
// unattractively because that is its whole purpose: the tamper tests need to
// corrupt a stored object, and no other caller should want this.
func (m *MemoryStore) Overwrite(key string, b []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = bytes.Clone(b)
}

// countingWriter counts what passes through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
