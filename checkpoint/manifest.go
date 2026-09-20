package checkpoint

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// The manifest is the checkpoint. It is small, plaintext JSON, and every field
// in it is either operational metadata an operator may read or a value a reader
// needs before it holds a key.
//
// What it does NOT hold is the whole reason it can be plaintext: no path, no
// file name, no symlink target, no file byte. The tenancy specification's §4.2
// counts paths, branch names and diffs as session content and §15.1 forbids
// logging them, so a plaintext sidecar listing them would be a leak that no
// careful logging downstream could undo. The tree listing lives inside the
// encrypted content stream, where the tar headers are.
//
// It is authenticated even though it is plaintext: manifest_auth is the GCM tag
// of an empty plaintext under the manifest subkey, with the authenticated
// context and a canonical rendering of every other field as additional data. Two
// consequences worth stating. The tag cannot be checked without the data key,
// which cannot be unwrapped without the key service agreeing — so "is this
// manifest authentic" and "may I read this checkpoint" are one question, asked
// once. And the canonical rendering is written by hand (authInput) rather than
// by re-serializing JSON, because a MAC over json.Marshal output is a MAC over
// whatever the encoder does this year.
//
// The cost of a hand-written rendering is that a field added to the struct and
// forgotten in authInput would be UNAUTHENTICATED METADATA — the worst bug this
// format could have. TestManifestAuthInputCoversEveryField reflects over the
// struct's JSON tags and fails if any tag but the two excluded by definition is
// missing from the rendering.

const (
	// DefaultFrameSize is the plaintext frame size a Writer uses when none is
	// given. One MiB is small enough that a frame buffer is nothing next to a
	// session's memory and large enough that the 16-byte tag per frame is 0.002%
	// overhead.
	DefaultFrameSize = 1 << 20
	// MinFrameSize and MaxFrameSize bound what a manifest may ask a reader to
	// allocate. The upper bound is the one that matters: without it, a manifest
	// could ask for a gigabyte-sized frame buffer before a single byte of it had
	// been authenticated.
	MinFrameSize = 64 << 10
	MaxFrameSize = 16 << 20

	// maxManifestBytes caps the manifest read. It is the only place in this
	// package where a whole object is read into memory, so it is the only place
	// that needs a cap.
	maxManifestBytes = 64 << 10

	// cipherName, kdfName and compressionNone are the algorithm names a v1
	// manifest must state exactly. They are fields rather than assumptions so
	// that a v2 changing one of them is a one-line diff with an obvious
	// meaning — and they are strict today, so a reader seeing "zstd" refuses an
	// unimplemented format rather than ignoring a field.
	cipherName      = "AES-256-GCM"
	kdfName         = "HKDF-SHA256"
	compressionNone = "none"
)

// Manifest is the committed description of one checkpoint. Field order is the
// canonical order authInput renders in; a reordering is a format change.
type Manifest struct {
	Version    string `json:"version"`
	Workspace  string `json:"workspace"`
	Session    string `json:"session"`
	Generation uint64 `json:"generation"`
	// CreatedAt is RFC3339 in UTC at second precision. It is authenticated,
	// which is what lets the deep-dormant precondition ("a verified checkpoint
	// newer than the disk's last write", ADR-0003 §4.4) rest on it. The
	// comparison itself needs the disk's last-write time and is the control
	// plane's.
	CreatedAt string `json:"created_at"`

	Cipher      string `json:"cipher"`
	KDF         string `json:"kdf"`
	Compression string `json:"compression"`

	// KeyRef is the CONCRETE key version the data key was wrapped under, as the
	// Wrapper reported it — never an alias. Destroying this key version makes
	// every checkpoint naming it unreadable without touching object storage,
	// which is the workspace-level crypto-shred the tenancy specification's §14.2
	// deletion ledger and §16 "key-wrapper deletion" control rely on.
	KeyRef string `json:"key_ref"`
	// WrappedKey is the data key, wrapped. It is the only copy: deleting this
	// manifest makes the content object unreadable by anyone, including Rainier.
	// It is ciphertext, and it is still never logged.
	WrappedKey string `json:"wrapped_key"`

	// ContentKey is the store key of the content object, attempt suffix
	// included. It is authenticated, so it cannot be redirected; a reader also
	// checks that it lives under this checkpoint's own prefix, which keeps the
	// layout honest rather than adding security.
	ContentKey string `json:"content_key"`

	FrameSize   int64  `json:"frame_size"`
	Frames      uint32 `json:"frames"`
	NoncePrefix string `json:"nonce_prefix"`

	ContentBytes  int64  `json:"content_bytes"`
	ContentDigest string `json:"content_digest"`
	PlainBytes    int64  `json:"plain_bytes"`

	Entries   int64 `json:"entries"`
	FileBytes int64 `json:"file_bytes"`
	// Skipped counts source entries the format does not carry — sockets, device
	// nodes, FIFOs. They are skipped rather than refused so that a stray
	// dev-server socket cannot defeat the durability barrier, and counted here so
	// that "not the tree the user named" is never silent.
	Skipped int64 `json:"skipped"`

	// TreeDigest is the structural invariant a restore is checked against. See
	// treeHasher for exactly what goes into it, and why directory and symlink
	// modification times do not.
	TreeDigest string `json:"tree_digest"`

	// ManifestNonce and ManifestAuth are excluded from authInput by definition:
	// the nonce is an input to the tag and the tag is its output. Tampering with
	// either one fails authentication like tampering with anything else.
	ManifestNonce string `json:"manifest_nonce"`
	ManifestAuth  string `json:"manifest_auth"`
}

// versionProbe is the lenient first pass of ParseManifest. It exists so that a
// manifest from a FUTURE format version — which will have fields this build has
// never heard of — is refused as an unimplemented version rather than as a
// malformed manifest, which is a much more useful thing for an operator to read.
type versionProbe struct {
	Version string `json:"version"`
}

// ParseManifest decodes and validates a manifest. It is strict in every
// direction a parser can be strict, because the manifest is the one part of a
// checkpoint that is read before anything has been authenticated:
//
//   - The version is checked first, leniently, so a future format is named as
//     such.
//   - Unknown fields are refused. A v1 reader that tolerated an unknown field
//     would be tolerating an unauthenticated extension point.
//   - Trailing JSON after the object is refused.
//   - Every field is required, every enumerated field must be its one accepted
//     value, every base64 field must decode to its exact expected length, and
//     every number must be in range.
//   - The lengths must be SELF-CONSISTENT: content_bytes must equal plain_bytes
//     plus one tag per frame, and plain_bytes must fall in the half-open range
//     the frame count and frame size imply. Those two checks catch a manifest
//     that lies about its own shape without needing the key.
//
// It does not check the authenticated context, the content key's prefix, or the
// tag: those need a Context and a Wrapper, and live on Reader.
func ParseManifest(b []byte) (Manifest, error) {
	if len(b) == 0 {
		return Manifest{}, fmt.Errorf("%w: it is empty", ErrManifest)
	}
	if len(b) > maxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: it is larger than the %d byte limit", ErrManifest, maxManifestBytes)
	}

	var probe versionProbe
	if err := json.Unmarshal(b, &probe); err != nil {
		// json's own message can quote the bytes it choked on. They are manifest
		// bytes rather than content, but the habit of not forwarding a decoder's
		// text is worth keeping whole.
		return Manifest{}, fmt.Errorf("%w: it is not a JSON object", ErrManifest)
	}
	if probe.Version != FormatVersion {
		return Manifest{}, fmt.Errorf("%w: this build implements %s only", ErrFormatVersion, FormatVersion)
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("%w: it has a missing, unknown, or mistyped field", ErrManifest)
	}
	if dec.More() {
		return Manifest{}, fmt.Errorf("%w: it has trailing data after the object", ErrManifest)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (m Manifest) validate() error {
	if m.Version != FormatVersion {
		return fmt.Errorf("%w: this build implements %s only", ErrFormatVersion, FormatVersion)
	}
	if err := validID("workspace", m.Workspace); err != nil {
		return fmt.Errorf("%w: %s", ErrManifest, err)
	}
	if err := validID("session", m.Session); err != nil {
		return fmt.Errorf("%w: %s", ErrManifest, err)
	}
	if m.Generation == 0 {
		return fmt.Errorf("%w: the generation counts from 1", ErrManifest)
	}
	if _, err := parseCreatedAt(m.CreatedAt); err != nil {
		return err
	}
	if m.Cipher != cipherName {
		return fmt.Errorf("%w: this build implements %s only", ErrFormatVersion, cipherName)
	}
	if m.KDF != kdfName {
		return fmt.Errorf("%w: this build implements %s only", ErrFormatVersion, kdfName)
	}
	if m.Compression != compressionNone {
		return fmt.Errorf("%w: this build implements uncompressed content only", ErrFormatVersion)
	}
	if m.KeyRef == "" || len(m.KeyRef) > maxKeyRefLen {
		return fmt.Errorf("%w: the key reference is empty or over the %d character limit", ErrManifest, maxKeyRefLen)
	}
	if n, err := decodedLen(m.WrappedKey); err != nil || n < dekLen || n > 4096 {
		return fmt.Errorf("%w: the wrapped key is not base64 of a plausible length", ErrManifest)
	}
	if err := validContentKeyShape(m.ContentKey); err != nil {
		return err
	}
	if m.FrameSize < MinFrameSize || m.FrameSize > MaxFrameSize {
		return fmt.Errorf("%w: the frame size is outside [%d, %d]", ErrManifest, MinFrameSize, MaxFrameSize)
	}
	if m.Frames == 0 {
		return fmt.Errorf("%w: a checkpoint has at least one frame", ErrManifest)
	}
	if n, err := decodedLen(m.NoncePrefix); err != nil || n != noncePrefixLen {
		return fmt.Errorf("%w: the nonce prefix is not base64 of %d bytes", ErrManifest, noncePrefixLen)
	}
	if n, err := decodedLen(m.ManifestNonce); err != nil || n != nonceLen {
		return fmt.Errorf("%w: the manifest nonce is not base64 of %d bytes", ErrManifest, nonceLen)
	}
	if n, err := decodedLen(m.ManifestAuth); err != nil || n != tagLen {
		return fmt.Errorf("%w: the manifest tag is not base64 of %d bytes", ErrManifest, tagLen)
	}
	if err := validDigest("content digest", m.ContentDigest); err != nil {
		return err
	}
	if err := validDigest("tree digest", m.TreeDigest); err != nil {
		return err
	}

	// Self-consistency. A tar stream is never shorter than its two trailing
	// zero blocks, every frame but the last is exactly frame_size, and every
	// frame costs exactly one tag.
	if m.PlainBytes < 1024 {
		return fmt.Errorf("%w: the plaintext length is shorter than an empty tar stream", ErrManifest)
	}
	full := int64(m.Frames-1) * m.FrameSize
	if m.PlainBytes <= full || m.PlainBytes > full+m.FrameSize {
		return fmt.Errorf("%w: the plaintext length does not match the frame count and frame size", ErrManifest)
	}
	if m.ContentBytes != m.PlainBytes+int64(m.Frames)*tagLen {
		return fmt.Errorf("%w: the ciphertext length is not the plaintext length plus one tag per frame", ErrManifest)
	}
	if m.Entries < 0 || m.FileBytes < 0 || m.Skipped < 0 {
		return fmt.Errorf("%w: a count is negative", ErrManifest)
	}
	// Every entry costs at least one 512-byte tar header, and file bytes are a
	// subset of the plaintext. Both bounds are loose and both catch a manifest
	// whose counts could not describe its own stream.
	if m.Entries > m.PlainBytes/512 {
		return fmt.Errorf("%w: the entry count cannot fit in the plaintext length", ErrManifest)
	}
	if m.FileBytes > m.PlainBytes {
		return fmt.Errorf("%w: the file-byte total exceeds the plaintext length", ErrManifest)
	}
	return nil
}

// Encode renders the manifest as the bytes that are stored. Indented, because a
// manifest is a thing an operator reads, and the formatting is irrelevant to
// security: authInput is what is authenticated, not this.
func (m Manifest) Encode() ([]byte, error) {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: the manifest could not be encoded", ErrManifest)
	}
	return append(b, '\n'), nil
}

// authInput is the canonical rendering the manifest tag authenticates: the
// purpose-separated authenticated context, then every field but the nonce and
// the tag, each as 0x00 "name=" value, in the struct's declaration order.
//
// The leading NUL on every field is what makes coverage testable: the test that
// keeps this function honest searches for "\x00<json tag>=" per field.
func (m Manifest) authInput(c Context) []byte {
	b := c.aad(purposeManifest)
	add := func(name, value string) {
		b = append(b, 0)
		b = append(b, name...)
		b = append(b, '=')
		b = append(b, value...)
	}
	add("version", m.Version)
	add("workspace", m.Workspace)
	add("session", m.Session)
	add("generation", strconv.FormatUint(m.Generation, 10))
	add("created_at", m.CreatedAt)
	add("cipher", m.Cipher)
	add("kdf", m.KDF)
	add("compression", m.Compression)
	add("key_ref", m.KeyRef)
	add("wrapped_key", m.WrappedKey)
	add("content_key", m.ContentKey)
	add("frame_size", strconv.FormatInt(m.FrameSize, 10))
	add("frames", strconv.FormatUint(uint64(m.Frames), 10))
	add("nonce_prefix", m.NoncePrefix)
	add("content_bytes", strconv.FormatInt(m.ContentBytes, 10))
	add("content_digest", m.ContentDigest)
	add("plain_bytes", strconv.FormatInt(m.PlainBytes, 10))
	add("entries", strconv.FormatInt(m.Entries, 10))
	add("file_bytes", strconv.FormatInt(m.FileBytes, 10))
	add("skipped", strconv.FormatInt(m.Skipped, 10))
	add("tree_digest", m.TreeDigest)
	return b
}

// authInputExcluded is the two fields authInput cannot cover, named once so the
// coverage test and this file agree.
var authInputExcluded = []string{"manifest_nonce", "manifest_auth"}

// seal fills ManifestNonce and ManifestAuth. The plaintext is empty: this is a
// MAC, and GCM over no plaintext with everything in the AAD is the MAC this
// package already has an implementation of.
func (m *Manifest) seal(key []byte, c Context, r io.Reader) error {
	aead, err := newGCM(key)
	if err != nil {
		return err
	}
	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(r, nonce); err != nil {
		return fmt.Errorf("checkpoint: generating the manifest nonce: %w", err)
	}
	m.ManifestNonce = base64.StdEncoding.EncodeToString(nonce)
	m.ManifestAuth = ""
	tag := aead.Seal(nil, nonce, nil, m.authInput(c))
	if len(tag) != tagLen {
		return fmt.Errorf("%w: the manifest tag is the wrong length", ErrManifest)
	}
	m.ManifestAuth = base64.StdEncoding.EncodeToString(tag)
	return nil
}

// checkAuth verifies the tag. The manifest's own nonce and tag are excluded from
// the input by construction, so a caller cannot get a passing tag by editing
// them: GCM refuses a wrong nonce and a wrong tag exactly as it refuses a wrong
// field.
func (m Manifest) checkAuth(key []byte, c Context) error {
	aead, err := newGCM(key)
	if err != nil {
		return err
	}
	nonce, err := base64.StdEncoding.DecodeString(m.ManifestNonce)
	if err != nil || len(nonce) != nonceLen {
		return ErrAuth
	}
	tag, err := base64.StdEncoding.DecodeString(m.ManifestAuth)
	if err != nil || len(tag) != tagLen {
		return ErrAuth
	}
	bare := m
	bare.ManifestAuth = ""
	bare.ManifestNonce = ""
	if _, err := aead.Open(nil, nonce, tag, bare.authInput(c)); err != nil {
		return ErrAuth
	}
	return nil
}

// Summary is the content-free operational view of a manifest: what a "latest
// verified checkpoint" panel, a freshness policy, and a deletion ledger need.
// It deliberately has nowhere to put the wrapped key, the nonces or the tag,
// which is the durable way to keep them out of a log line.
type Summary struct {
	Version       string
	Workspace     string
	Session       string
	Generation    uint64
	CreatedAt     time.Time
	KeyRef        KeyRef
	Frames        uint32
	FrameSize     int64
	ContentBytes  int64
	PlainBytes    int64
	Entries       int64
	FileBytes     int64
	Skipped       int64
	ContentDigest string
	TreeDigest    string
}

// Summary renders the operational view. A manifest that reached here has been
// parsed, so CreatedAt cannot fail to parse; a zero time would still be
// preferable to a panic.
func (m Manifest) Summary() Summary {
	t, _ := parseCreatedAt(m.CreatedAt)
	return Summary{
		Version:       m.Version,
		Workspace:     m.Workspace,
		Session:       m.Session,
		Generation:    m.Generation,
		CreatedAt:     t,
		KeyRef:        KeyRef(m.KeyRef),
		Frames:        m.Frames,
		FrameSize:     m.FrameSize,
		ContentBytes:  m.ContentBytes,
		PlainBytes:    m.PlainBytes,
		Entries:       m.Entries,
		FileBytes:     m.FileBytes,
		Skipped:       m.Skipped,
		ContentDigest: m.ContentDigest,
		TreeDigest:    m.TreeDigest,
	}
}

// parseCreatedAt pins one spelling: RFC3339, UTC, second precision. A timestamp
// that could be written two ways is a timestamp two implementations eventually
// write two ways.
func parseCreatedAt(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: the creation time is not RFC3339", ErrManifest)
	}
	if formatCreatedAt(t) != s {
		return time.Time{}, fmt.Errorf("%w: the creation time is not UTC at second precision", ErrManifest)
	}
	return t.UTC(), nil
}

func formatCreatedAt(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

func decodedLen(s string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func validDigest(field, s string) error {
	if len(s) != 64 {
		return fmt.Errorf("%w: the %s is not 64 hex characters", ErrManifest, field)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return fmt.Errorf("%w: the %s is not lowercase hex", ErrManifest, field)
	}
	return nil
}

// validContentKeyShape is the shape check a manifest's content key passes before
// a Reader compares it against the prefix it expects.
func validContentKeyShape(k string) error {
	switch {
	case k == "":
		return fmt.Errorf("%w: the content key is empty", ErrManifest)
	case len(k) > maxContentKeyLen:
		return fmt.Errorf("%w: the content key is over the %d character limit", ErrManifest, maxContentKeyLen)
	case strings.HasPrefix(k, "/"), strings.HasSuffix(k, "/"):
		return fmt.Errorf("%w: the content key begins or ends with a slash", ErrManifest)
	case strings.Contains(k, "//"), strings.Contains(k, ".."):
		return fmt.Errorf("%w: the content key contains an empty element or \"..\"", ErrManifest)
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] == 0x7f {
			return fmt.Errorf("%w: the content key contains a control character", ErrManifest)
		}
	}
	return nil
}
