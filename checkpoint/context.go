package checkpoint

import (
	"fmt"
	"strconv"
	"strings"
)

// FormatVersion is the one version string this build implements. It appears in
// three places that must agree — the manifest's version field, the storage key
// prefix, and the authenticated context — so that a v2 sharing the same key
// hierarchy cannot open a v1 object's data key, and so that the one piece of
// interpretation the bytes cannot safely assert about themselves is asserted
// under the key instead.
const FormatVersion = "rainier.checkpoint.v1"

// maxIDLen bounds a workspace or session identifier. It is generous for an
// opaque id and it keeps an authenticated context, and a storage key, from
// being an unbounded string a caller supplies.
const maxIDLen = 128

// Context is the authenticated context of a checkpoint: the workspace it
// belongs to, the session inside it, and the monotonic checkpoint generation.
// The tenancy specification's §11 names exactly these three for a portable
// checkpoint, and §18 item 39 requires that ciphertext copied into a different
// one fail authenticated decryption.
//
// A caller SUPPLIES this on every read. It is never taken from the manifest —
// the manifest's own identity fields are compared against it and are otherwise
// not consulted. A library that read the context out of the manifest and then
// used it to open the manifest would make the cross-context property vacuous,
// because ciphertext would always be in "its own" context. That is the single
// easiest way to get this format wrong, so it is written down here rather than
// only in the design note.
type Context struct {
	// Workspace is the opaque workspace identifier.
	Workspace string
	// Session is the opaque session identifier. Each resumable workspace
	// belongs to one session (PRD §4.3).
	Session string
	// Generation is the monotonic checkpoint generation, counting from 1. Zero
	// is refused so that a zero-value Context is never a valid one, for the
	// reason ParseSecretsKey refuses an all-zero key: the zero value is how
	// "nothing was configured" is represented, and accepting it produces the one
	// failure nobody can debug.
	Generation uint64
}

// Validate refuses a context that could not be used unambiguously. The
// identifier rule is strict on purpose: an identifier containing a slash would
// place one session's checkpoint under another's storage prefix, and one
// containing a NUL would make the NUL-separated authenticated context
// ambiguous.
func (c Context) Validate() error {
	if err := validID("workspace", c.Workspace); err != nil {
		return err
	}
	if err := validID("session", c.Session); err != nil {
		return err
	}
	if c.Generation == 0 {
		return fmt.Errorf("%w: the checkpoint generation counts from 1", ErrInvalid)
	}
	return nil
}

// Bytes is the canonical authenticated context:
//
//	FormatVersion 0x00 workspace 0x00 session 0x00 decimal(generation)
//
// NUL separators, for the reason internal/controld's agentCredentialAAD uses
// them: they keep ("a","bc") and ("ab","c") apart, and Validate has already
// refused a component that could contain one.
//
// It is exported because a Wrapper implementation may want to record a digest
// of what it authenticated. It is not itself used as an AAD anywhere: every use
// is purpose-separated (see aad), so no ciphertext produced for one purpose can
// be presented as another.
func (c Context) Bytes() []byte {
	var b strings.Builder
	b.WriteString(FormatVersion)
	b.WriteByte(0)
	b.WriteString(c.Workspace)
	b.WriteByte(0)
	b.WriteString(c.Session)
	b.WriteByte(0)
	b.WriteString(strconv.FormatUint(c.Generation, 10))
	return []byte(b.String())
}

// The three purposes. They are separate constants rather than inline strings
// for the reason agentCredentialAAD is a function: one spelling, used on both
// the seal side and the open side, so the two cannot drift.
const (
	purposeKey      = "key"
	purposeManifest = "manifest"
	purposeFrame    = "frame"
)

// aad builds the additional authenticated data for one purpose: the canonical
// context, then the purpose, then whatever that purpose adds (a frame's index
// and final flag, a manifest's canonical field rendering), all NUL-separated.
func (c Context) aad(purpose string, tail ...string) []byte {
	b := c.Bytes()
	b = append(b, 0)
	b = append(b, purpose...)
	for _, t := range tail {
		b = append(b, 0)
		b = append(b, t...)
	}
	return b
}

// frameAAD is the AAD of frame i. The index makes a reordered or spliced frame
// fail; the final flag makes a truncated stream unable to present its last
// surviving frame as the end.
func (c Context) frameAAD(i uint32, final bool) []byte {
	f := "0"
	if final {
		f = "1"
	}
	return c.aad(purposeFrame, strconv.FormatUint(uint64(i), 10), f)
}

// validID applies the identifier rule: 1 to maxIDLen characters, beginning with
// an ASCII letter or digit, continuing with those plus '.', '_' and '-'. The
// error names which field was wrong and its length, never its value — an
// identifier is a locator rather than a secret, but runnerd already calls a
// leaked session id "a credential-shaped value" and there is nothing to gain by
// echoing it.
func validID(field, s string) error {
	if s == "" {
		return fmt.Errorf("%w: the %s identifier is empty", ErrInvalid, field)
	}
	if len(s) > maxIDLen {
		return fmt.Errorf("%w: the %s identifier is %d characters, over the %d limit",
			ErrInvalid, field, len(s), maxIDLen)
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
			continue
		case i > 0 && (ch == '.' || ch == '_' || ch == '-'):
			continue
		}
		return fmt.Errorf("%w: the %s identifier has a character outside [A-Za-z0-9][A-Za-z0-9._-]*", ErrInvalid, field)
	}
	return nil
}
