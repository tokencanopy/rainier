package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
)

// The tree rules: which entries a checkpoint carries, what a valid entry name
// is, which symbolic links are allowed, and how a tree is summarized into one
// digest. They are applied at BOTH ends — a Writer refuses a source tree that
// breaks them, and a Reader re-applies every one of them to the decoded stream
// before it writes anything — for the reason protocol/workspace gives for
// validating a transfer at both ends: a check performed by the hop before you is
// a check you are trusting.
//
// They are close cousins of protocol/workspace's checkLink and entryPath, and
// deliberately not the same code. That package is a wire protocol between two
// live endpoints where the archive is untrusted at both ends and refusing it
// whole is right; a checkpoint is a durability artifact authenticated under the
// tenant's own key, where refusing it whole means losing work. The design note's
// §11 argues the split; the rules that DO transfer are spelled the same way here
// on purpose.

const (
	// maxEntryNameLen and maxNameElementLen bound a name. Linux's own limits are
	// PATH_MAX and NAME_MAX; these are them, so a checkpoint cannot carry a name
	// no filesystem could hold.
	maxEntryNameLen   = 4096
	maxNameElementLen = 255
	// copyBufSize is the one copy buffer a write or a restore holds. It is a
	// constant because it is part of the bounded-memory promise.
	copyBufSize = 32 << 10
)

// Source is the tree to checkpoint: one file system, and any paths inside it
// that must not travel ON TOP OF the ones that never travel.
//
// DefaultExclusions is applied by every Write, unioned with AlsoExclude. That
// is a property of the API rather than of the caller's diligence: there is no
// spelling of Source — zero value, struct literal, DirSource with no extra
// arguments — that checkpoints /workspace/.rainier.
//
// One root, deliberately. PRD §10 wants "the session filesystem and native agent
// resume state" in a checkpoint while ADR-0003 §2.3 keeps the agent home out of
// one, and those hold together only if the resume state a session needs is a
// file under this root. A Writer that accepted two roots would let a caller
// quietly get half a session instead of coming back and changing the format; the
// design note's §14 keeps the question open in the open place.
type Source struct {
	// FS is the tree, rooted at the workspace. If it implements fs.ReadLinkFS —
	// as os.DirFS does — symbolic links travel as links; if it does not, a
	// symlink in the tree is refused rather than silently followed or dropped.
	FS fs.FS

	// AlsoExclude is what the caller adds to DefaultExclusions, which a Writer
	// applies whether or not this field is set: clean, relative,
	// slash-separated paths. A path matches itself and everything under it, and
	// an excluded directory is PRUNED — never opened, never read — so a file
	// inside one cannot reach the store even by accident.
	//
	// The name says "also" because the union is not optional. A zero Source is
	// not the raw tree, and there is no argument, field or option that makes it
	// one: a caller who never thought about exclusions still gets the set
	// Rainier owns, which is the only version of that promise that survives a
	// caller in a hurry.
	//
	// This is not a redaction pass. The tenancy specification's §8.2 says a
	// workload may deliberately write a credential into its own workspace, that
	// those bytes are customer content, and that "Rainier does not represent
	// redaction as a security boundary". What keeps Rainier-delivered credentials
	// out of a checkpoint is that they are not files under this root in the first
	// place (§18 item 38 is the claim being made, and no wider one). This list is
	// for whatever a caller's own policy adds on top of the Rainier-owned paths
	// DefaultExclusions already names.
	AlsoExclude []string
}

// DirSource is Source over a directory on the local filesystem. Any arguments
// after the directory are ADDED to DefaultExclusions; passing none does not
// mean "checkpoint everything".
func DirSource(dir string, alsoExclude ...string) Source {
	return Source{FS: os.DirFS(dir), AlsoExclude: alsoExclude}
}

// DefaultExclusions is the set Rainier itself owns inside a session workspace.
// Every Write applies it, so it is not something a caller opts into; it is
// exported so that a caller can SEE what will be left out, and so that a test
// comparing a source tree with a restored one can skip the same paths.
//
// It is a function rather than a package variable so that one caller cannot
// append to another caller's policy.
func DefaultExclusions() []string {
	// .rainier is where the session's own control files go (see the bootstrap
	// token note's §1, which names /workspace/.rainier/session.json). It is
	// Rainier's, not the user's work, and nothing in it should survive a resume.
	return []string{".rainier"}
}

// exclusions is the set a walk actually applies: the defaults, always, plus
// whatever the caller added. Computed once per Write rather than consulted per
// entry, because the walk's memory is supposed to be O(1) in the tree.
func (s Source) exclusions() []string {
	out := DefaultExclusions()
	for _, e := range s.AlsoExclude {
		if !slices.Contains(out, e) {
			out = append(out, e)
		}
	}
	return out
}

func (s Source) validate() error {
	if s.FS == nil {
		return fmt.Errorf("%w: the source has no file system", ErrInvalid)
	}
	for i, e := range s.AlsoExclude {
		switch {
		case e == "":
			return fmt.Errorf("%w: exclusion %d is empty", ErrInvalid, i)
		case path.IsAbs(e):
			return fmt.Errorf("%w: exclusion %d is absolute; exclusions are relative to the source root", ErrInvalid, i)
		case e != path.Clean(e):
			return fmt.Errorf("%w: exclusion %d is not a clean path", ErrInvalid, i)
		case e == ".", strings.HasPrefix(e, ".."):
			return fmt.Errorf("%w: exclusion %d does not name a path inside the source root", ErrInvalid, i)
		}
	}
	return nil
}

// excluded reports whether name is an excluded path or sits under one.
func excluded(exclusions []string, name string) bool {
	for _, e := range exclusions {
		if name == e || strings.HasPrefix(name, e+"/") {
			return true
		}
	}
	return false
}

// validEntryName is the name rule, applied to a source entry on the way in and
// to a decoded entry on the way out. A name is a clean, relative,
// slash-separated path with no "." or ".." element, no NUL, and no element or
// total over the filesystem's own limits.
//
// Cleanliness is what makes the containment check at restore a real guard rather
// than a spelling rule: "a/../../etc" does not survive path.Clean equality, and
// so never reaches a join.
func validEntryName(name string) error {
	switch {
	case name == "":
		return errors.New("the entry name is empty")
	case len(name) > maxEntryNameLen:
		return fmt.Errorf("the entry name is longer than %d bytes", maxEntryNameLen)
	case strings.ContainsRune(name, 0):
		return errors.New("the entry name contains a NUL byte")
	case path.IsAbs(name):
		return errors.New("the entry name is absolute")
	case name != path.Clean(name):
		return errors.New("the entry name is not a clean relative path")
	case name == ".":
		return errors.New("the entry name is the source root itself")
	}
	for _, elem := range strings.Split(name, "/") {
		switch {
		case elem == "", elem == ".", elem == "..":
			return errors.New("the entry name has an empty, \".\" or \"..\" element")
		case len(elem) > maxNameElementLen:
			return fmt.Errorf("the entry name has an element longer than %d bytes", maxNameElementLen)
		}
	}
	return nil
}

// checkLink is the symlink rule: a target must be relative and must resolve,
// lexically, to a path still inside the tree. It is applied at pack time as well
// as at restore time, which is the whole point — a tree containing
// "vendor/node -> /usr/bin/node" is refused before the first byte moves, in an
// error about the tree the caller actually has.
//
// It is lexical on purpose, and does not follow what the target POINTS at. A
// dangling but contained link is allowed, because real trees are full of them
// (git worktrees, node_modules/.bin) and a checkpoint that refused them would be
// a checkpoint that could not be taken.
func checkLink(name, target string) error {
	switch {
	case target == "":
		return errors.New("the symlink target is empty")
	case len(target) > maxEntryNameLen:
		return fmt.Errorf("the symlink target is longer than %d bytes", maxEntryNameLen)
	case strings.ContainsRune(target, 0):
		return errors.New("the symlink target contains a NUL byte")
	case path.IsAbs(target):
		return errors.New("the symlink target is absolute")
	}
	resolved := path.Join(path.Dir(name), target)
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return errors.New("the symlink target leaves the source tree")
	}
	return nil
}

// ---------------------------------------------------------------------------
// the tree digest
// ---------------------------------------------------------------------------

// treeHasher folds a tree into one digest, in stream order, one entry at a time
// and with no state per entry. It is what makes PRD §19's "checksum-equal
// restore" checkable without a second copy: the writer computes it from the local
// tree, the reader recomputes it from the decoded stream, and the manifest
// commits to it in between.
//
// The record is six NUL-separated fields and a newline:
//
//	kind 0x00 name 0x00 mode 0x00 size 0x00 mtime 0x00 (file digest | link target) 0x0a
//
// Directory and symlink modification times are "-" rather than a value, because
// the restorer does not set them: a directory's mtime is a function of the order
// its children were written, and os.Chtimes on a symlink is not in the standard
// library. Excluding what cannot be restored is what keeps this digest an
// equality rather than an aspiration. The bytes themselves are still
// authenticated — they are inside the encrypted frames, and the ciphertext digest
// covers every one of them; this digest is the STRUCTURAL invariant, not the only
// integrity mechanism.
type treeHasher struct {
	h hash.Hash
}

func newTreeHasher() *treeHasher { return &treeHasher{h: sha256.New()} }

func (t *treeHasher) record(kind byte, name, mode, size, mtime, tail string) {
	t.h.Write([]byte{kind, 0})
	t.h.Write([]byte(name))
	t.h.Write([]byte{0})
	t.h.Write([]byte(mode))
	t.h.Write([]byte{0})
	t.h.Write([]byte(size))
	t.h.Write([]byte{0})
	t.h.Write([]byte(mtime))
	t.h.Write([]byte{0})
	t.h.Write([]byte(tail))
	t.h.Write([]byte{'\n'})
}

func (t *treeHasher) file(name string, mode fs.FileMode, size, mtimeNano int64, digest []byte) {
	t.record('F', name,
		strconv.FormatUint(uint64(mode.Perm()), 8),
		strconv.FormatInt(size, 10),
		strconv.FormatInt(mtimeNano, 10),
		hex.EncodeToString(digest))
}

func (t *treeHasher) dir(name string, mode fs.FileMode) {
	t.record('D', name, strconv.FormatUint(uint64(mode.Perm()), 8), "0", "-", "-")
}

func (t *treeHasher) symlink(name, target string) {
	t.record('L', name, "-", "0", "-", target)
}

func (t *treeHasher) sum() string { return hex.EncodeToString(t.h.Sum(nil)) }

// ---------------------------------------------------------------------------
// error hygiene for file-system failures
// ---------------------------------------------------------------------------

// fsCategory reduces a file-system error to the part that is safe to print. An
// *fs.PathError and an *os.LinkError both carry the path that failed — which is
// session content — wrapped around an errno that carries nothing. Unwrapping to
// the errno keeps the diagnostic ("no space left on device", "permission
// denied") and drops the path, which is a far better trade than the usual
// choice between a leak and a shrug.
func fsCategory(err error) string {
	var pe *fs.PathError
	if errors.As(err, &pe) && pe.Err != nil {
		return pe.Err.Error()
	}
	var le *os.LinkError
	if errors.As(err, &le) && le.Err != nil {
		return le.Err.Error()
	}
	if err == nil {
		return "unknown error"
	}
	// Anything else is an error from a file system this package did not write,
	// whose text is not known to be path-free. It is reduced rather than
	// forwarded: a leak is worse than a vague sentence, and the two named cases
	// above cover every error the standard library's file systems produce.
	return "input/output error"
}
