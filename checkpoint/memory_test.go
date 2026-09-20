package checkpoint

import (
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
// frame, a 32 KiB copy buffer and a few hash states — under 3 MiB. The ceiling is
// set an order of magnitude above that so the test fails on an O(tree) structure
// and not on the garbage collector's timing.
const heapCeiling = 64 << 20

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
	const smallFiles = 20000

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

	// Twenty thousand entries across two hundred directories, so the entry count
	// is a real dimension of the test and not only the byte count. Every one of
	// them would be one map entry in an O(entries) implementation.
	for d := 0; d < 200; d++ {
		dir := filepath.Join(root, "many", fmt.Sprintf("d%03d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Skipf("this environment cannot create the fixture: %v", err)
		}
		for i := 0; i < smallFiles/200; i++ {
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

// TestBoundedMemoryOnVerifyAndRestore is the read side of the same promise, at a
// size that fits in a MemoryStore so the whole round trip is exercised: many
// entries, several hundred frames, one heap ceiling.
func TestBoundedMemoryOnVerifyAndRestore(t *testing.T) {
	root := t.TempDir()
	for d := 0; d < 40; d++ {
		dir := filepath.Join(root, fmt.Sprintf("d%03d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d.bin", i)),
				pseudorandom(4096), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	h := newHarness(t, MinFrameSize)
	ctx := context.Background()
	if _, err := h.w.Write(ctx, h.c, DirSource(root)); err != nil {
		t.Fatalf("Write: %v", err)
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

	// A MemoryStore holds the whole ciphertext, so the growth measured here
	// includes that; the ceiling is what matters, not the exact number.
	if grew := measure(t, func() error {
		_, err := h.r.Verify(ctx, h.c)
		return err
	}); grew > heapCeiling {
		t.Errorf("Verify grew the heap by %d bytes, over the %d byte ceiling", grew, heapCeiling)
	}
	target := filepath.Join(t.TempDir(), "r")
	if grew := measure(t, func() error {
		_, err := h.r.Restore(ctx, h.c, target)
		return err
	}); grew > heapCeiling {
		t.Errorf("Restore grew the heap by %d bytes, over the %d byte ceiling", grew, heapCeiling)
	}
	if _, err := os.Stat(filepath.Join(target, "d039", "f099.bin")); err != nil {
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
