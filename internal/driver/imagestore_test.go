// internal/driver/imagestore_test.go
//
// The image store's own tests: what it will store, what it refuses, and what
// it leaves behind when a fetch goes wrong.
package driver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// memImageSource is an ImageSource with nothing behind it: a ref table, some
// bytes, and the three ways a real source misbehaves — handing back content
// that does not match its digest, dying mid-stream, and being slow enough that
// two callers can collide on it.
type memImageSource struct {
	mu    sync.Mutex
	index map[string]string // ref → digest
	blobs map[string][]byte // digest → what it actually serves
	opens int

	// truncateAfter, when positive, cuts every stream short after that many
	// bytes and reports a read error — a connection that dropped.
	truncateAfter int
	// gate, when non-nil, holds every Open until it is closed. It is how the
	// dedupe test keeps two fetches of one digest overlapping.
	gate chan struct{}
	// opened is closed by the first Open, so a test can wait for a fetch to be
	// genuinely in flight instead of sleeping.
	opened chan struct{}
}

func newMemSource() *memImageSource {
	return &memImageSource{index: map[string]string{}, blobs: map[string][]byte{}}
}

// publish adds a ref whose bytes hash to the digest it is published under.
func (s *memImageSource) publish(ref string, content []byte) string {
	sum := sha256.Sum256(content)
	digest := digestOf(sum[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index[ref] = digest
	s.blobs[digest] = content
	return digest
}

// publishCorrupt adds a ref whose digest is a lie: the index names the digest
// of `claimed`, and the source serves `served`.
func (s *memImageSource) publishCorrupt(ref string, claimed, served []byte) string {
	sum := sha256.Sum256(claimed)
	digest := digestOf(sum[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index[ref] = digest
	s.blobs[digest] = served
	return digest
}

func (s *memImageSource) Resolve(_ context.Context, ref string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	digest, ok := s.index[ref]
	if !ok {
		return "", errors.New("no such ref")
	}
	return digest, nil
}

func (s *memImageSource) Open(_ context.Context, digest string) (io.ReadCloser, error) {
	s.mu.Lock()
	content, ok := s.blobs[digest]
	s.opens++
	first := s.opens == 1
	gate, opened := s.gate, s.opened
	truncate := s.truncateAfter
	s.mu.Unlock()
	if !ok {
		return nil, errors.New("no such digest")
	}
	if first && opened != nil {
		close(opened)
	}
	if gate != nil {
		<-gate
	}
	if truncate > 0 {
		return io.NopCloser(&truncatedReader{data: content, cut: truncate}), nil
	}
	return io.NopCloser(strings.NewReader(string(content))), nil
}

func (s *memImageSource) openCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens
}

// truncatedReader hands out the first cut bytes and then fails, which is what
// a dropped transfer looks like to the store.
type truncatedReader struct {
	data []byte
	cut  int
	n    int
}

func (r *truncatedReader) Read(p []byte) (int, error) {
	if r.n >= r.cut {
		return 0, errors.New("connection reset while streaming the image")
	}
	n := copy(p, r.data[r.n:r.cut])
	r.n += n
	return n, nil
}

// newDirSource writes a local image directory — one "<digest>.ext4" per image
// and the index beside them — and returns the source pointed at it. It is the
// self-hosted shape, and the fixture most driver tests want.
func newDirSource(t *testing.T, images map[string][]byte) DirImageSource {
	t.Helper()
	dir := t.TempDir()
	idx := imageIndex{Images: map[string]string{}}
	for ref, content := range images {
		sum := sha256.Sum256(content)
		digest := digestOf(sum[:])
		if err := os.WriteFile(filepath.Join(dir, digest+".ext4"), content, 0o600); err != nil {
			t.Fatalf("write image %s: %v", ref, err)
		}
		idx.Images[ref] = digest
	}
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		t.Fatalf("marshal index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, imageIndexName), data, 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}
	return DirImageSource{Dir: dir}
}

func newTestStore(t *testing.T, src ImageSource) *imageStore {
	t.Helper()
	s, err := newImageStore(t.TempDir(), src)
	if err != nil {
		t.Fatalf("newImageStore: %v", err)
	}
	return s
}

// blobCount is how many images the store is holding, which is the number the
// "left nothing behind" assertions turn on.
func blobCount(t *testing.T, s *imageStore) int {
	t.Helper()
	entries, err := os.ReadDir(s.blobDir())
	if err != nil {
		t.Fatalf("read the blob directory: %v", err)
	}
	return len(entries)
}

func tempCount(t *testing.T, s *imageStore) int {
	t.Helper()
	entries, err := os.ReadDir(s.tempDir())
	if err != nil {
		t.Fatalf("read the staging directory: %v", err)
	}
	return len(entries)
}

// TestImageStoreRefusesADigestMismatch is the rule the whole store exists for:
// an environment image is the root filesystem every later session of that
// environment boots, so bytes that do not hash to the name they were fetched
// under are not stored under it — or anywhere else.
func TestImageStoreRefusesADigestMismatch(t *testing.T) {
	src := newMemSource()
	src.publishCorrupt("rainier-env:e1-aaa", []byte("the image that was reviewed"), []byte("something else entirely"))
	s := newTestStore(t, src)

	_, err := s.resolve(context.Background(), "rainier-env:e1-aaa")
	if err == nil {
		t.Fatal("resolve of an image whose bytes do not match its digest succeeded")
	}
	if !strings.Contains(err.Error(), "hash to") {
		t.Errorf("error = %q, want it to name the digest it got instead", err)
	}
	if n := blobCount(t, s); n != 0 {
		t.Errorf("a refused image left %d blob(s) in the store", n)
	}
	if n := tempCount(t, s); n != 0 {
		t.Errorf("a refused image left %d file(s) staged", n)
	}
	if _, ok := s.manifest("rainier-env:e1-aaa"); ok {
		t.Error("a refused image published a manifest")
	}
}

// TestImageStorePartialDownloadLeavesNoFile: a transfer that dies half way
// must leave nothing a later create could mistake for a cached image. The
// bytes it did receive are a PREFIX of a real image, which is the dangerous
// shape — an ext4 superblock and nothing behind it.
func TestImageStorePartialDownloadLeavesNoFile(t *testing.T) {
	src := newMemSource()
	src.truncateAfter = 4
	digest := src.publish("rainier-env:e1-aaa", []byte("a whole environment image"))
	s := newTestStore(t, src)

	if _, err := s.resolve(context.Background(), "rainier-env:e1-aaa"); err == nil {
		t.Fatal("resolve of a transfer that died mid-stream succeeded")
	}
	if _, ok := s.have(digest); ok {
		t.Error("a partial download landed in the store under its digest")
	}
	if n := blobCount(t, s); n != 0 {
		t.Errorf("a partial download left %d blob(s) in the store", n)
	}
	if n := tempCount(t, s); n != 0 {
		t.Errorf("a partial download left %d file(s) staged", n)
	}
}

// TestImageStoreFetchesADigestOnce is the concurrency rule. Two creates for
// the same cold environment arrive together routinely — a fleet places both
// halves of a workspace at once — and an image is gigabytes.
func TestImageStoreFetchesADigestOnce(t *testing.T) {
	src := newMemSource()
	src.gate = make(chan struct{})
	src.opened = make(chan struct{})
	src.publish("rainier-env:e1-aaa", []byte("an environment image"))
	s := newTestStore(t, src)

	const callers = 8
	errs := make(chan error, callers)
	// The first caller goes in and parks inside Open, so every other one
	// arrives while a fetch is genuinely in flight rather than after it.
	go func() { _, err := s.resolve(context.Background(), "rainier-env:e1-aaa"); errs <- err }()
	select {
	case <-src.opened:
	case <-time.After(5 * time.Second):
		t.Fatal("the first fetch never reached the source")
	}
	for i := 1; i < callers; i++ {
		go func() { _, err := s.resolve(context.Background(), "rainier-env:e1-aaa"); errs <- err }()
	}
	// Give the followers a moment to reach the store and find the in-flight
	// entry, then let the download finish.
	time.Sleep(50 * time.Millisecond)
	close(src.gate)

	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent resolve: %v", err)
		}
	}
	if got := src.openCount(); got != 1 {
		t.Errorf("the source was opened %d times for one digest, want 1", got)
	}
	if n := blobCount(t, s); n != 1 {
		t.Errorf("the store holds %d blob(s) after one image, want 1", n)
	}
}

// TestImageStoreManifestRoundTrip: what a ref resolves to, and what was
// recorded about it, survives being written and read back — including the
// keys-and-no-values rule the snapshot path depends on.
func TestImageStoreManifestRoundTrip(t *testing.T) {
	s := newTestStore(t, nil)
	content := []byte("an environment image")
	sum := sha256.Sum256(content)
	digest := digestOf(sum[:])
	dst, err := s.blobPath(digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, content, microvmFileMode); err != nil {
		t.Fatal(err)
	}

	want := ImageManifest{
		Ref:          "rainier-env:e1-aaa",
		Digest:       digest,
		SizeBytes:    int64(len(content)),
		InstanceID:   "mvm-1",
		EnvKeys:      []string{"RAINIER_SESSION"},
		Cmd:          []string{"/bin/bash"},
		StrippedKeys: []string{"SECRET"},
		CreatedAt:    time.Now().UTC().Truncate(time.Second),
	}
	if err := s.writeManifest(want); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	got, ok := s.manifest(want.Ref)
	if !ok {
		t.Fatal("no manifest for a ref that was just written")
	}
	if got.Digest != want.Digest || got.InstanceID != want.InstanceID || got.SizeBytes != want.SizeBytes {
		t.Errorf("manifest = %+v, want %+v", got, want)
	}
	if len(got.EnvKeys) != 1 || got.EnvKeys[0] != "RAINIER_SESSION" {
		t.Errorf("env keys = %v", got.EnvKeys)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("created at = %s, want %s", got.CreatedAt, want.CreatedAt)
	}

	// And a store with no source resolves it — the self-hosted case, where
	// every image on the host was put there by a snapshot or an operator.
	m, err := s.resolve(context.Background(), want.Ref)
	if err != nil {
		t.Fatalf("resolve a locally published ref: %v", err)
	}
	if m.Digest != digest {
		t.Errorf("resolve = %s, want %s", m.Digest, digest)
	}

	// The manifest is 0600. It names a tenant's environment on a shared host.
	path, err := s.manifestPath(want.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != microvmFileMode {
		t.Errorf("manifest mode = %#o, want %#o", fi.Mode().Perm(), microvmFileMode)
	}
}

// TestImageStoreRefusesAnUnresolvableRef pins the other half of "never a
// placeholder": with no source there is nothing to make one out of, and with a
// source that does not publish the ref there is nothing to fetch.
func TestImageStoreRefusesAnUnresolvableRef(t *testing.T) {
	ctx := context.Background()

	noSource := newTestStore(t, nil)
	_, err := noSource.resolve(ctx, "rainier-env:e1-aaa")
	if err == nil {
		t.Fatal("a store with no source resolved a ref it has never seen")
	}
	if !strings.Contains(err.Error(), "no image source") {
		t.Errorf("error = %q, want it to say this runner has no source", err)
	}

	src := newMemSource()
	src.publish("rainier-env:other", []byte("an image"))
	withSource := newTestStore(t, src)
	if _, err := withSource.resolve(ctx, "rainier-env:e1-aaa"); err == nil {
		t.Fatal("a ref the source does not publish resolved anyway")
	}
	if n := blobCount(t, withSource); n != 0 {
		t.Errorf("an unresolvable ref left %d blob(s) in the store", n)
	}
}

// TestImageStoreRefusesADigestThatNamesAPath is the path-safety half: a digest
// arrives from an index this host did not write, and it never becomes a file
// name without being parsed.
func TestImageStoreRefusesADigestThatNamesAPath(t *testing.T) {
	s := newTestStore(t, nil)
	for _, bad := range []string{
		"../../etc/passwd",
		"sha256:../../etc/passwd",
		"sha256:" + strings.Repeat("g", 64),
		"md5:d41d8cd98f00b204e9800998ecf8427e",
		"sha256:ABCD",
		"",
	} {
		if _, err := s.blobPath(bad); err == nil {
			t.Errorf("blobPath(%q) = nil error; a digest that is not one must not become a path", bad)
		}
	}
	// A valid digest in upper case is refused too: two spellings of one image
	// would be two files, and the second would never be found by the first's
	// name.
	upper := "sha256:" + strings.ToUpper(strings.Repeat("ab", 32))
	if _, err := s.blobPath(upper); err == nil {
		t.Error("an upper-case digest was accepted")
	}
}

// TestDirAndHTTPSourcesAgree runs one image through both stock sources, since
// the only difference between them is the transport and the digest check is
// the same on either side.
func TestDirAndHTTPSourcesAgree(t *testing.T) {
	ctx := context.Background()
	content := []byte("an environment image")
	dir := newDirSource(t, map[string][]byte{"rainier-env:e1-aaa": content})

	digest, err := dir.Resolve(ctx, "rainier-env:e1-aaa")
	if err != nil {
		t.Fatalf("dir resolve: %v", err)
	}
	sum := sha256.Sum256(content)
	if digest != digestOf(sum[:]) {
		t.Fatalf("dir resolve = %s, want %s", digest, digestOf(sum[:]))
	}

	srv := httptest.NewServer(http.FileServer(http.Dir(dir.Dir)))
	defer srv.Close()
	web := HTTPImageSource{Base: srv.URL}
	webDigest, err := web.Resolve(ctx, "rainier-env:e1-aaa")
	if err != nil {
		t.Fatalf("http resolve: %v", err)
	}
	if webDigest != digest {
		t.Errorf("http resolve = %s, want %s", webDigest, digest)
	}

	s := newTestStore(t, web)
	m, err := s.resolve(ctx, "rainier-env:e1-aaa")
	if err != nil {
		t.Fatalf("resolve over http: %v", err)
	}
	path, ok := s.have(m.Digest)
	if !ok {
		t.Fatal("an image fetched over http is not in the store")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Errorf("stored image = %q, want %q", got, content)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != microvmFileMode {
		t.Errorf("stored image mode = %#o, want %#o", fi.Mode().Perm(), microvmFileMode)
	}

	// A ref neither publishes is an error on both.
	if _, err := dir.Resolve(ctx, "rainier-env:absent"); err == nil {
		t.Error("the dir source resolved a ref it does not publish")
	}
	if _, err := web.Resolve(ctx, "rainier-env:absent"); err == nil {
		t.Error("the http source resolved a ref it does not publish")
	}
}
