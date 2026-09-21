// internal/driver/image.go
//
// The per-host environment image store (ADR-0003 §2.7 item 3): one ext4 file
// per environment, named by the sha256 of its bytes, plus a manifest per
// environment ref saying which digest that ref resolves to on this host.
//
// There is no OCI anywhere in here, and that is the decision rather than an
// omission. A microVM boots a block device, not a layered filesystem, so the
// host has nothing to unpack: base images are built in CI, published by
// digest, and pulled as a single file. Prepull downloads that file and
// verifies it; Snapshot publishes one the same way. Between them, `Create`
// makes a copy-on-write copy per session (see Cloner) — which is why the
// stored image is never opened for writing after it lands.
//
// Two rules hold everywhere in this file:
//
//   - A digest is checked against the BYTES, while they stream, and a file
//     that does not hash to the name it was fetched under never reaches the
//     store at all. An image is a root filesystem every later session of an
//     environment boots; a distributor that could hand over different bytes
//     under the same digest would be handing every session a rootfs nobody
//     reviewed.
//   - A ref is opaque and arrives from the control plane, so it never becomes
//     a path without going through sanitizeRef, and a digest never becomes a
//     path without being parsed first. Both are what stop a manifest from
//     naming a file outside the store.
package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	// imageDigestAlgo is the one hash this store speaks. It is spelled into
	// every digest string ("sha256:<hex>") rather than assumed, so a manifest
	// written by a future version that used something else is refused here
	// instead of being read as a sha256 that happens not to match.
	imageDigestAlgo = "sha256"

	// maxImageBytes bounds what a fetch will write before it gives up.
	//
	// The digest is only known to be wrong at the END of a download, so a
	// source that answers an endless stream under a legitimate-looking digest
	// would otherwise fill the host's NVMe — taking every other tenant's
	// session on the host down with it — before anything checked anything. A
	// session rootfs is single-digit gigabytes; 64 GiB is far above any real
	// environment and far below a disk.
	maxImageBytes = 64 << 30

	// imageIndexName is the file both stock sources publish their ref → digest
	// table in. It is the whole of "resolution" in v0: there is no registry
	// API, no manifest negotiation, and nothing about an image to discover
	// beyond which bytes an environment ref means.
	imageIndexName = "index.json"
)

// ImageManifest is what one environment ref resolves to on this host, and the
// configuration the image was published with.
//
// EnvKeys is KEYS, never values. This file outlives the session on a host that
// runs other tenants' sessions, and an environment's decrypted values have no
// business in it (the same rule VMMConfig.Env states for the instance record).
// Keys are enough for what the manifest is for: proving that what a caller
// named in stripEnv did not survive the commit.
type ImageManifest struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
	// SizeBytes is the image's apparent length, which for a sparse ext4 file
	// is not what it occupies. It is recorded because it is the one number an
	// operator staring at a full disk wants beside the digest.
	SizeBytes int64 `json:"size_bytes"`
	// InstanceID names the session whose rootfs was committed, for a snapshot.
	// Empty for a base image, which no session on this host produced.
	InstanceID string   `json:"instance_id,omitempty"`
	EnvKeys    []string `json:"env_keys,omitempty"`
	// Cmd is what the session that produced this image was running, recorded
	// as description and read by nothing on the create path — which is a
	// deliberate difference from `docker commit`, and the same conclusion the
	// Docker driver reached by the other road.
	//
	// A commit records the container's own Cmd into the image, and the session
	// that builds an environment's cache is not always a shell: a login
	// session runs the agent's login command and exits. The Docker driver has
	// to pin the BASE image's Cmd on the way in to stop every later session
	// from booting that login. Here there is nothing to pin and nothing to
	// undo — a microVM's command comes from its boot configuration over vsock,
	// per create, and an ext4 file has no CMD to inherit in the first place.
	Cmd          []string  `json:"cmd,omitempty"`
	StrippedKeys []string  `json:"stripped_keys,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// ImageSource is where this host gets an environment image it does not have.
//
// Two methods and no more: resolve a ref to a digest, and open the bytes of a
// digest. Everything else — verification, the temp file, the atomic rename,
// the concurrent-fetch dedupe — belongs to the store, so that a source cannot
// get any of it subtly wrong and no implementation is trusted to.
//
// nil is a legal state: a runner with no source serves only the images it
// already has, which is every self-hosted host and every test. It resolves
// what is in its store and reports a clear error for anything else, which is
// exactly the pre-existing "never fabricate an image" rule.
type ImageSource interface {
	// Resolve maps an environment ref to the digest of its ext4 image.
	// A ref this source does not publish is an error, never an empty digest.
	Resolve(ctx context.Context, ref string) (digest string, err error)
	// Open streams the image with this digest. The caller verifies the bytes;
	// an implementation that checked them itself would still be checked again.
	Open(ctx context.Context, digest string) (io.ReadCloser, error)
}

// imageIndex is the ref → digest table both stock sources publish.
type imageIndex struct {
	Images map[string]string `json:"images"`
}

// parseDigest splits a digest into its algorithm and hex, or refuses.
//
// It is the only way a digest becomes part of a path in this package. The hex
// is length- and alphabet-checked, so a digest read back out of a manifest
// somebody edited cannot name ".." or a file in another directory.
func parseDigest(digest string) (algo, hexDigits string, err error) {
	algo, hexDigits, ok := strings.Cut(digest, ":")
	if !ok {
		return "", "", fmt.Errorf("image digest %q is not <algorithm>:<hex>", digest)
	}
	if algo != imageDigestAlgo {
		return "", "", fmt.Errorf("image digest %q uses %q; this host stores %s images only", digest, algo, imageDigestAlgo)
	}
	if len(hexDigits) != sha256.Size*2 {
		return "", "", fmt.Errorf("image digest %q is %d hex digits, want %d", digest, len(hexDigits), sha256.Size*2)
	}
	if _, err := hex.DecodeString(hexDigits); err != nil {
		return "", "", fmt.Errorf("image digest %q is not hex: %w", digest, err)
	}
	if strings.ToLower(hexDigits) != hexDigits {
		return "", "", fmt.Errorf("image digest %q is not lower-case hex", digest)
	}
	return algo, hexDigits, nil
}

// digestOf is the store's own spelling of a digest, so the name a blob is
// stored under and the name a fetch is checked against come from one place.
func digestOf(sum []byte) string { return imageDigestAlgo + ":" + hex.EncodeToString(sum) }

// imageStore is this host's image directory.
//
// It is unexported because nothing outside this package composes one: the
// driver owns it, MicrovmOpts names the SOURCE (which a deployment does
// configure), and the layout underneath is this file's business.
type imageStore struct {
	root   string
	source ImageSource

	// mu guards inflight only. Everything else the store does is filesystem
	// work under paths derived from a digest, which is safe to do
	// concurrently — a rename onto a blob that another fetch just wrote is
	// the same bytes by definition.
	mu sync.Mutex
	// inflight dedupes concurrent fetches of one digest. Two creates for the
	// same cold environment arrive together routinely (a fleet places both
	// halves of a workspace at once), and without this they would both
	// download the same multi-gigabyte file, each into its own temp file, to
	// rename the second over the first.
	inflight map[string]*imagePull
}

// imagePull is one download other callers can wait on.
type imagePull struct {
	done chan struct{}
	err  error
}

func newImageStore(stateDir string, source ImageSource) (*imageStore, error) {
	s := &imageStore{
		root:     filepath.Join(stateDir, "images"),
		source:   source,
		inflight: map[string]*imagePull{},
	}
	for _, dir := range []string{s.blobDir(), s.refDir(), s.tempDir()} {
		if err := os.MkdirAll(dir, microvmDirMode); err != nil {
			return nil, fmt.Errorf("microvm: create the image store directory %s: %w", dir, err)
		}
	}
	return s, nil
}

func (s *imageStore) blobDir() string { return filepath.Join(s.root, "blobs", imageDigestAlgo) }
func (s *imageStore) refDir() string  { return filepath.Join(s.root, "refs") }
func (s *imageStore) tempDir() string { return filepath.Join(s.root, "tmp") }

// blobPath is where the image with this digest lives, or an error for a digest
// that does not name anything storable.
func (s *imageStore) blobPath(digest string) (string, error) {
	_, hexDigits, err := parseDigest(digest)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.blobDir(), hexDigits+".ext4"), nil
}

// have reports the path of a stored image, and whether it is there.
func (s *imageStore) have(digest string) (string, bool) {
	path, err := s.blobPath(digest)
	if err != nil {
		return "", false
	}
	fi, err := os.Stat(path)
	if err != nil || !fi.Mode().IsRegular() {
		return "", false
	}
	return path, true
}

// manifestPath is where a ref's manifest lives. The ref is sanitized, not
// escaped-and-hoped: see sanitizeRef.
func (s *imageStore) manifestPath(ref string) (string, error) {
	seg, err := sanitizeRef(ref)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.refDir(), seg+".json"), nil
}

// manifest reads what this host has recorded for a ref.
func (s *imageStore) manifest(ref string) (ImageManifest, bool) {
	path, err := s.manifestPath(ref)
	if err != nil {
		return ImageManifest{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ImageManifest{}, false
	}
	var m ImageManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return ImageManifest{}, false
	}
	if _, _, err := parseDigest(m.Digest); err != nil {
		return ImageManifest{}, false
	}
	return m, true
}

// manifestBytes returns the raw manifest a ref was published with, or nil.
// Raw, because the strip assertion reads it for values the typed form has no
// field for — see assertStrippedFromImage.
func (s *imageStore) manifestBytes(ref string) []byte {
	path, err := s.manifestPath(ref)
	if err != nil {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return data
}

// writeManifest publishes a ref, atomically.
//
// Atomically because a manifest is what a later create resolves its rootfs
// through: a half-written one is a ref that resolves to nothing, or worse to a
// truncated digest, and this file is written while sessions on this host are
// reading it.
func (s *imageStore) writeManifest(m ImageManifest) error {
	path, err := s.manifestPath(m.Ref)
	if err != nil {
		return err
	}
	if _, _, err := parseDigest(m.Digest); err != nil {
		return fmt.Errorf("publish %q: %w", m.Ref, err)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal the manifest for %q: %w", m.Ref, err)
	}
	tmp, err := os.CreateTemp(s.tempDir(), "manifest-*.json")
	if err != nil {
		return fmt.Errorf("stage the manifest for %q: %w", m.Ref, err)
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(microvmFileMode); err != nil {
		tmp.Close()
		return fmt.Errorf("restrict the manifest for %q: %w", m.Ref, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write the manifest for %q: %w", m.Ref, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write the manifest for %q: %w", m.Ref, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("publish the manifest for %q: %w", m.Ref, err)
	}
	return nil
}

// removeRef takes a ref's manifest away. The BLOB stays: another ref may
// resolve to the same digest, and a blob is only ever bytes that hash to their
// own name.
func (s *imageStore) removeRef(ref string) error {
	path, err := s.manifestPath(ref)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// resolve answers "which file on this host is ref", fetching it if this host
// has a source and does not have the image.
//
// The order is deliberate. What this host has already recorded wins, because a
// ref published HERE (a snapshot) exists in no index anywhere; only then is
// the source asked, which is the path a base image built in CI takes. An
// unresolvable ref is an error — never a placeholder, never an empty file.
func (s *imageStore) resolve(ctx context.Context, ref string) (ImageManifest, error) {
	if ref == "" {
		return ImageManifest{}, errors.New("microvm: empty image ref")
	}
	// Refused before anything else, so a ref that could never name a manifest
	// fails the same way whether or not this host has a source.
	if _, err := sanitizeRef(ref); err != nil {
		return ImageManifest{}, err
	}

	recorded, hasRecord := s.manifest(ref)
	if hasRecord {
		if _, ok := s.have(recorded.Digest); ok {
			return recorded, nil
		}
		// The ref is recorded and the blob is not there: an operator pruned
		// the store, or a previous fetch was interrupted between the two. The
		// digest is still the right one to fetch, and it is the one the
		// manifest already promised.
		if s.source != nil {
			if err := s.fetch(ctx, recorded.Digest); err == nil {
				return recorded, nil
			}
		}
	}

	if s.source == nil {
		return ImageManifest{}, fmt.Errorf(
			"microvm: image %q is not on this host and this runner has no image source to fetch it from "+
				"(--microvm-image-dir / --microvm-image-url); an environment image is an ext4 file published by digest (ADR-0003 §2.7 item 3), never something a host makes up", ref)
	}
	digest, err := s.source.Resolve(ctx, ref)
	if err != nil {
		return ImageManifest{}, fmt.Errorf("microvm: resolve image %q: %w", ref, err)
	}
	if _, _, err := parseDigest(digest); err != nil {
		return ImageManifest{}, fmt.Errorf("microvm: image source resolved %q to %w", ref, err)
	}
	if err := s.fetch(ctx, digest); err != nil {
		return ImageManifest{}, fmt.Errorf("microvm: fetch image %q: %w", ref, err)
	}
	path, ok := s.have(digest)
	if !ok {
		return ImageManifest{}, fmt.Errorf("microvm: image %q was fetched and is not in the store", ref)
	}
	size := int64(0)
	if fi, err := os.Stat(path); err == nil {
		size = fi.Size()
	}

	m := ImageManifest{Ref: ref, Digest: digest, SizeBytes: size, CreatedAt: time.Now()}
	if hasRecord && recorded.Digest == digest {
		// The same image, re-fetched. What this host recorded ABOUT it — the
		// configuration a snapshot published — is not something the index
		// knows, so it is kept rather than flattened.
		m = recorded
		m.SizeBytes = size
	}
	if err := s.writeManifest(m); err != nil {
		return ImageManifest{}, err
	}
	return m, nil
}

// fetch downloads one digest, verifies it, and lands it atomically — once,
// however many callers ask at the same moment.
func (s *imageStore) fetch(ctx context.Context, digest string) error {
	if _, ok := s.have(digest); ok {
		return nil
	}
	if s.source == nil {
		return fmt.Errorf("image %s is not on this host and there is no source to fetch it from", digest)
	}

	s.mu.Lock()
	if p, ok := s.inflight[digest]; ok {
		s.mu.Unlock()
		select {
		case <-p.done:
			// The download that was already running is this caller's answer,
			// error and all: a second attempt would fail the same way against
			// the same source, and the ONE case it would not — a transient
			// network failure — is a retry the caller above can make with its
			// own deadline rather than one this store makes silently.
			return p.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p := &imagePull{done: make(chan struct{})}
	s.inflight[digest] = p
	s.mu.Unlock()

	p.err = s.download(ctx, digest)
	s.mu.Lock()
	delete(s.inflight, digest)
	s.mu.Unlock()
	close(p.done)
	return p.err
}

// download streams one image into a temp file, hashing as it goes, and renames
// it into place only if the bytes hash to the name they were asked for.
//
// A partial or mismatched download leaves NO file — not under the digest, not
// beside it. The temp file is removed on every path out, so a fetch that died
// half way cannot be mistaken for a cached image by the next create.
func (s *imageStore) download(ctx context.Context, digest string) error {
	dst, err := s.blobPath(digest)
	if err != nil {
		return err
	}
	rc, err := s.source.Open(ctx, digest)
	if err != nil {
		return err
	}
	defer rc.Close()

	tmp, err := os.CreateTemp(s.tempDir(), "fetch-*.ext4")
	if err != nil {
		return fmt.Errorf("stage the download of %s: %w", digest, err)
	}
	// Removed unless the rename below took it; a rename leaves nothing at the
	// old name, so the deferred remove is a no-op on the happy path.
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(microvmFileMode); err != nil {
		tmp.Close()
		return fmt.Errorf("restrict the download of %s: %w", digest, err)
	}

	sum := sha256.New()
	// maxImageBytes+1 so that an image exactly at the cap still lands and one
	// byte over is caught rather than silently truncated into a file whose
	// digest would then merely "not match".
	n, err := io.Copy(io.MultiWriter(tmp, sum), io.LimitReader(rc, maxImageBytes+1))
	if err != nil {
		tmp.Close()
		return fmt.Errorf("download %s: %w", digest, err)
	}
	if n > maxImageBytes {
		tmp.Close()
		return fmt.Errorf("download %s: the source is still sending past %d bytes; an environment image is one ext4 file, not a stream", digest, int64(maxImageBytes))
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("download %s: %w", digest, err)
	}
	if got := digestOf(sum.Sum(nil)); got != digest {
		return fmt.Errorf("refusing image %s: the bytes hash to %s. An environment image is the root filesystem every session of that environment boots, so bytes that do not match the digest they were fetched under are not stored", digest, got)
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return fmt.Errorf("store %s: %w", digest, err)
	}
	return nil
}

// stage makes an empty temp file inside the store for a caller that is about
// to produce an image locally — Snapshot, cloning a session's rootfs into it.
// It is inside the store so the publish below is a rename and not a copy.
func (s *imageStore) stage(pattern string) (string, error) {
	f, err := os.CreateTemp(s.tempDir(), pattern)
	if err != nil {
		return "", fmt.Errorf("stage an image: %w", err)
	}
	name := f.Name()
	if err := f.Chmod(microvmFileMode); err != nil {
		f.Close()
		_ = os.Remove(name)
		return "", fmt.Errorf("restrict a staged image: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("stage an image: %w", err)
	}
	// Removed so the caller can create it itself: a clone wants to make the
	// destination, not to find one. What stage really reserves is the NAME.
	if err := os.Remove(name); err != nil {
		return "", fmt.Errorf("stage an image: %w", err)
	}
	return name, nil
}

// publish digests a file this host produced and moves it into the store under
// its own digest, then records ref as resolving to it.
//
// The digest is computed HERE from the bytes on disk rather than accumulated
// while they were written, because what is published must be what is stored:
// the two are the same file, and hashing it after it is complete is the only
// version of this that cannot drift from the file the next create opens.
//
// staged is consumed: it is renamed into the store, or removed. A caller that
// sees an error has nothing left to clean up.
func (s *imageStore) publish(staged string, m ImageManifest) (ImageManifest, error) {
	defer os.Remove(staged)

	f, err := os.Open(staged)
	if err != nil {
		return ImageManifest{}, fmt.Errorf("publish %q: %w", m.Ref, err)
	}
	sum := sha256.New()
	size, err := io.Copy(sum, f)
	f.Close()
	if err != nil {
		return ImageManifest{}, fmt.Errorf("publish %q: digest the image: %w", m.Ref, err)
	}
	digest := digestOf(sum.Sum(nil))
	dst, err := s.blobPath(digest)
	if err != nil {
		return ImageManifest{}, err
	}
	if _, already := s.have(digest); already {
		// The same bytes are already stored — two snapshots of an environment
		// nobody changed, which is the common case for a re-run setup. The
		// staged copy is dropped (the deferred remove takes it) and the ref is
		// pointed at what is there: identical content under one name is the
		// whole point of storing by digest.
		_ = os.Chmod(dst, microvmFileMode)
	} else if err := os.Rename(staged, dst); err != nil {
		return ImageManifest{}, fmt.Errorf("publish %q: store the image: %w", m.Ref, err)
	}

	m.Digest = digest
	m.SizeBytes = size
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	if err := s.writeManifest(m); err != nil {
		return ImageManifest{}, err
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

// DirImageSource serves images from a directory on this host: an index at
// index.json mapping environment refs to digests, and one "<digest>.ext4" file
// per image beside it.
//
// It is what a self-hosted runner points at a staging directory with, and what
// the tests use. The directory is read-only as far as this type is concerned —
// nothing here writes to it — so pointing two runners at one NFS export or one
// read-only bind mount is a deployment, not a hazard.
type DirImageSource struct{ Dir string }

func (d DirImageSource) Resolve(_ context.Context, ref string) (string, error) {
	data, err := os.ReadFile(filepath.Join(d.Dir, imageIndexName))
	if err != nil {
		return "", fmt.Errorf("read the image index in %s: %w", d.Dir, err)
	}
	return indexLookup(data, ref)
}

func (d DirImageSource) Open(_ context.Context, digest string) (io.ReadCloser, error) {
	if _, _, err := parseDigest(digest); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(d.Dir, digest+".ext4"))
	if err != nil {
		return nil, fmt.Errorf("open image %s in %s: %w", digest, d.Dir, err)
	}
	return f, nil
}

// HTTPImageSource fetches images over HTTPS from a base URL: the same index,
// and "<base>/<digest>.ext4" per image.
//
// There is no authentication here and no signature check beyond the digest,
// and both are deliberate for v0. The digest is what makes the transport
// untrusted-by-construction: a compromised mirror, a cache, or a middlebox can
// serve whatever it likes and the store will not keep it. What the URL has to
// be trusted for is the INDEX — which digest a ref means — and that is why the
// index is fetched from the same configured base rather than followed from
// anything inside an image.
type HTTPImageSource struct {
	// Base is the prefix every object hangs off, with or without a trailing
	// slash.
	Base string
	// Client is the http.Client to use. nil means a client with a timeout,
	// because http.DefaultClient has none and an image fetch that hangs holds
	// a create behind it.
	Client *http.Client
}

// httpImageTimeout bounds a whole fetch, index or image. Multi-gigabyte
// images over a regional link are minutes, not seconds; a create that has
// waited fifteen minutes for one is a create whose session the user gave up
// on long ago.
const httpImageTimeout = 15 * time.Minute

func (h HTTPImageSource) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return &http.Client{Timeout: httpImageTimeout}
}

// objectURL composes a URL under Base, escaping the object name so a digest's
// colon cannot be read as anything but part of the path.
func (h HTTPImageSource) objectURL(object string) (string, error) {
	base, err := url.Parse(h.Base)
	if err != nil {
		return "", fmt.Errorf("image base URL %q: %w", h.Base, err)
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("image base URL %q names no scheme and host", h.Base)
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + "/" + object
	return base.String(), nil
}

func (h HTTPImageSource) get(ctx context.Context, object string) (*http.Response, error) {
	u, err := h.objectURL(object)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", u, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("fetch %s: HTTP %d", u, resp.StatusCode)
	}
	return resp, nil
}

func (h HTTPImageSource) Resolve(ctx context.Context, ref string) (string, error) {
	resp, err := h.get(ctx, imageIndexName)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// The index is a small table; a bound on it is what stops a mirror from
	// answering the index request with an image.
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read the image index: %w", err)
	}
	return indexLookup(data, ref)
}

func (h HTTPImageSource) Open(ctx context.Context, digest string) (io.ReadCloser, error) {
	if _, _, err := parseDigest(digest); err != nil {
		return nil, err
	}
	resp, err := h.get(ctx, url.PathEscape(digest)+".ext4")
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// indexLookup reads one ref out of an index document.
func indexLookup(data []byte, ref string) (string, error) {
	var idx imageIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return "", fmt.Errorf("parse the image index: %w", err)
	}
	digest, ok := idx.Images[ref]
	if !ok || digest == "" {
		known := make([]string, 0, len(idx.Images))
		for k := range idx.Images {
			known = append(known, k)
		}
		slices.Sort(known)
		return "", fmt.Errorf("the image index publishes no %q (it publishes %d ref(s): %s)", ref, len(known), strings.Join(known, ", "))
	}
	if _, _, err := parseDigest(digest); err != nil {
		return "", fmt.Errorf("the image index maps %q to %w", ref, err)
	}
	return digest, nil
}
