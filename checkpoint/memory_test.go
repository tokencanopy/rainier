package checkpoint

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// discardStore is a BlobStore that counts what it is handed and keeps none of it.
// The bounded-memory test needs it: run through MemoryStore, a multi-gigabyte
// checkpoint would prove only that the machine has RAM.
type discardStore struct {
	mu    sync.Mutex
	bytes map[string]int64
}

func newDiscardStore() *discardStore { return &discardStore{bytes: make(map[string]int64)} }

func (d *discardStore) PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error {
	d.mu.Lock()
	_, exists := d.bytes[key]
	d.mu.Unlock()
	if exists {
		return ErrExists
	}
	cw := &countingWriter{w: io.Discard}
	if err := write(cw); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bytes[key] = cw.n
	return nil
}

func (d *discardStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, ErrNotFound
}

func (d *discardStore) Delete(ctx context.Context, key string) error { return nil }

func (d *discardStore) written(key string) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bytes[key]
}

// heapCeiling is what a checkpoint of any tree may cost above the baseline. At
// the default 1 MiB frame size the live set is a plaintext frame, a ciphertext
// frame, a 32 KiB copy buffer and a few hash states — under 3 MiB; measured
// peak is around 4.5 MiB once the collector's slack is counted.
//
// It is set at 12 MiB rather than at some comfortable order of magnitude above
// that, because the failure this test exists to catch is not a catastrophically
// O(tree-bytes) buffer — it is the cheap-looking one the design note names: "a
// list of entries, a map of digests". At the entry count below, a
// map[string][32]byte of path to digest is roughly 20 MiB and a []Entry slice
// similar, so a ceiling of 64 MiB would have watched either one go by.
const heapCeiling = 12 << 20

// TestBoundedMemoryOnAVeryLargeTree is the bounded-memory promise as a test
// rather than an assertion in a comment. It checkpoints a tree of several
// gigabytes and twenty thousand entries while sampling the heap, and fails if the
// peak crosses heapCeiling.
//
// The multi-gigabyte half is a sparse file, so the bytes cost address space
// rather than disk. If the environment cannot create one, the test skips: an
// environment that cannot allocate the fixture has not found a bug.
func TestBoundedMemoryOnAVeryLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("the multi-gigabyte fixture is not worth it under -short")
	}

	const sparseSize = int64(2) << 30 // 2 GiB
	// Large enough that a per-entry map or slice would cross heapCeiling on its
	// own, which is what makes the ceiling a real test of the note's §9 rather
	// than a test that the machine has RAM.
	const smallFiles = 100000

	root := t.TempDir()
	sparse := filepath.Join(root, "sparse.bin")
	f, err := os.Create(sparse)
	if err != nil {
		t.Skipf("this environment cannot create the fixture: %v", err)
	}
	if err := f.Truncate(sparseSize); err != nil {
		f.Close()
		t.Skipf("this environment cannot allocate a %d byte sparse file: %v", sparseSize, err)
	}
	if err := f.Close(); err != nil {
		t.Skipf("this environment cannot allocate the sparse file: %v", err)
	}
	if fi, err := os.Stat(sparse); err != nil || fi.Size() != sparseSize {
		t.Skipf("the sparse file is not the size it was truncated to")
	}

	// A hundred thousand entries across five hundred directories, so the entry
	// count is a real dimension of the test and not only the byte count. Every one
	// of them would be one map entry in an O(entries) implementation.
	for d := 0; d < 500; d++ {
		dir := filepath.Join(root, "many", fmt.Sprintf("d%03d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Skipf("this environment cannot create the fixture: %v", err)
		}
		for i := 0; i < smallFiles/500; i++ {
			p := filepath.Join(dir, fmt.Sprintf("f%03d.txt", i))
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Skipf("this environment cannot create the fixture: %v", err)
			}
		}
	}

	store := newDiscardStore()
	w, err := NewWriter(store, testWrapper(t), WriterOptions{KeyRef: testKeyRef})
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	var peak atomic.Uint64
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
				runtime.ReadMemStats(&ms)
				for {
					was := peak.Load()
					if ms.HeapAlloc <= was || peak.CompareAndSwap(was, ms.HeapAlloc) {
						break
					}
				}
			}
		}
	}()

	res, err := w.Write(context.Background(), testContext(), DirSource(root))
	close(done)
	sampler.Wait()
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := store.written(res.ContentKey); got < sparseSize {
		t.Fatalf("the store received %d bytes, expected at least the %d byte sparse file", got, sparseSize)
	}
	if res.Manifest.Entries < smallFiles {
		t.Fatalf("entries = %d, expected at least %d", res.Manifest.Entries, smallFiles)
	}
	if res.Manifest.Frames < uint32(sparseSize/DefaultFrameSize) {
		t.Fatalf("frames = %d, expected the stream to be cut into thousands", res.Manifest.Frames)
	}

	if peak.Load() == 0 {
		t.Skip("the heap sampler never ran; the machine is too fast for this test to mean anything")
	}
	grew := int64(peak.Load()) - int64(base.HeapAlloc)
	if grew > heapCeiling {
		t.Errorf("the heap grew by %d bytes while checkpointing %d bytes and %d entries, over the %d byte ceiling",
			grew, sparseSize, res.Manifest.Entries, heapCeiling)
	}
	t.Logf("checkpointed %d bytes and %d entries in %d frames; heap peak %d bytes above a %d byte baseline",
		store.written(res.ContentKey), res.Manifest.Entries, res.Manifest.Frames,
		grew, base.HeapAlloc)
}

// streamingStore serves an object from a file on disk rather than from a byte
// slice, so the read-side memory measurement is not dominated by a store that
// holds the whole ciphertext in the heap. It is what a real blob store looks
// like from this package's point of view.
type streamingStore struct {
	dir  string
	keys map[string]string // store key -> file path
}

func newStreamingStore(dir string) *streamingStore {
	return &streamingStore{dir: dir, keys: make(map[string]string)}
}

func (s *streamingStore) path(key string) string {
	if p, ok := s.keys[key]; ok {
		return p
	}
	p := filepath.Join(s.dir, fmt.Sprintf("obj%04d", len(s.keys)))
	s.keys[key] = p
	return p
}

func (s *streamingStore) PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error {
	if _, ok := s.keys[key]; ok {
		return ErrExists
	}
	p := s.path(key)
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<16)
	if err := write(bw); err != nil {
		delete(s.keys, key)
		os.Remove(p)
		return err
	}
	return bw.Flush()
}

func (s *streamingStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	p, ok := s.keys[key]
	if !ok {
		return nil, ErrNotFound
	}
	return os.Open(p)
}

func (s *streamingStore) Delete(ctx context.Context, key string) error {
	if p, ok := s.keys[key]; ok {
		delete(s.keys, key)
		return os.Remove(p)
	}
	return nil
}

// TestBoundedMemoryOnVerifyAndRestore is the read side of the same promise.
//
// It runs against a store that streams from disk rather than MemoryStore: a
// store that keeps the ciphertext in the heap would put its own allocation
// inside the measurement, and a number dominated by something the library does
// not control is not a measurement of the library.
func TestBoundedMemoryOnVerifyAndRestore(t *testing.T) {
	root := t.TempDir()
	const dirs, perDir = 200, 100
	for d := 0; d < dirs; d++ {
		dir := filepath.Join(root, fmt.Sprintf("d%03d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < perDir; i++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d.bin", i)),
				pseudorandom(4096), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	store := newStreamingStore(t.TempDir())
	keys := testWrapper(t)
	w, err := NewWriter(store, keys, WriterOptions{KeyRef: testKeyRef})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(store, keys, ReaderOptions{Authorize: (&authorizeRecorder{}).hook})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c := testContext()
	res, err := w.Write(ctx, c, DirSource(root))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.Manifest.Frames < 50 {
		t.Fatalf("the fixture should cross many frames, got %d", res.Manifest.Frames)
	}

	measure := func(t *testing.T, f func() error) int64 {
		t.Helper()
		runtime.GC()
		var base, after runtime.MemStats
		runtime.ReadMemStats(&base)
		if err := f(); err != nil {
			t.Fatalf("%v", err)
		}
		runtime.ReadMemStats(&after)
		return int64(after.HeapAlloc) - int64(base.HeapAlloc)
	}

	if grew := measure(t, func() error {
		_, err := r.Verify(ctx, c)
		return err
	}); grew > heapCeiling {
		t.Errorf("Verify grew the heap by %d bytes over %d entries, past the %d byte ceiling",
			grew, res.Manifest.Entries, heapCeiling)
	}
	target := filepath.Join(t.TempDir(), "r")
	if grew := measure(t, func() error {
		_, err := r.Restore(ctx, c, target)
		return err
	}); grew > heapCeiling {
		t.Errorf("Restore grew the heap by %d bytes over %d entries, past the %d byte ceiling",
			grew, res.Manifest.Entries, heapCeiling)
	}
	if _, err := os.Stat(filepath.Join(target, fmt.Sprintf("d%03d", dirs-1),
		fmt.Sprintf("f%03d.bin", perDir-1))); err != nil {
		t.Errorf("the restored tree is incomplete: %v", err)
	}
}

// TestFrameCeiling proves the frame index cannot silently wrap: a frame writer
// pushed past the last representable index refuses rather than reusing nonce
// zero. It is driven directly, because reaching the real ceiling would need
// petabytes.
func TestFrameCeiling(t *testing.T) {
	fw, err := newFrameWriter(io.Discard, testContext(), make([]byte, dekLen), make([]byte, noncePrefixLen), MinFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	fw.frames = ^uint32(0)
	if _, err := fw.Write(make([]byte, MinFrameSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Write at the frame ceiling = %v, want ErrTooLarge", err)
	}
}
