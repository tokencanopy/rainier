package wstream

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// Magic is the stream's first bytes. It is a line so that a reader which got
// something else — a JSON error page, a truncated frame, another protocol —
// fails on the first read with a sentence rather than on the tar header with a
// puzzle.
const Magic = "rainier.wsstream.v1\n"

// The three entry kinds an index record can name. They are bytes rather than
// an enum because they are wire-visible: the guest that writes them ships
// inside a session image and the host that reads them ships with the runner, so
// the two are routinely different builds.
const (
	KindDir     byte = 'd'
	KindFile    byte = 'f'
	KindSymlink byte = 'l'
)

// The limits below are the SAME numbers on both ends. The guest applies them so
// that an oversized workspace fails as a sentence about the session; the host
// applies them because the guest is the untrusted end of this hop.
const (
	// DefaultMaxEntries bounds the index, and therefore the one O(entries)
	// structure this design has. Half a million names is a large monorepo with
	// its dependencies installed; past that a checkpoint is not the right tool
	// and a refusal is better than a host that allocates until it is killed.
	DefaultMaxEntries = 500_000
	// DefaultMaxIndexBytes bounds the same structure by BYTES, because
	// MaxEntries alone bounds it only if names are short. 16 MiB is 500,000
	// names averaging 33 bytes.
	DefaultMaxIndexBytes = 16 << 20
	// DefaultMaxEntryBytes bounds one file. 8 GiB is under the workspace disk
	// (10 GiB) and over any plausible single artifact in one.
	DefaultMaxEntryBytes = 8 << 30
	// DefaultMaxTotalBytes bounds the whole stream. The workspace disk is 10
	// GiB; 32 GiB leaves room for a disk that grows without making this the
	// check that has to be edited first.
	DefaultMaxTotalBytes = 32 << 30
	// maxNameLen and maxNameElement are the filesystem's own limits (PATH_MAX,
	// NAME_MAX). A name past them could not be restored anywhere.
	maxNameLen     = 4096
	maxNameElement = 255
	// maxRecordLen bounds ONE index record before its name is even looked at,
	// so a malformed length prefix cannot make a reader allocate.
	maxRecordLen = 1 + maxNameLen
)

// Limits is what both ends enforce. A zero value means every default; a field
// set to a negative number is refused rather than read as "no limit", because
// "no limit" is not a thing either end of this hop may choose.
type Limits struct {
	MaxEntries    int64
	MaxIndexBytes int64
	MaxEntryBytes int64
	MaxTotalBytes int64

	// Exclude is the set of paths that never travel: clean, relative,
	// slash-separated, matching themselves and everything under them. The
	// caller passes checkpoint.DefaultExclusions() (plus its own policy), and
	// both ends apply it — the guest so the bytes never leave the session, the
	// host so an excluded entry cannot reach the checkpoint writer even if a
	// guest sent one anyway.
	Exclude []string
}

// DefaultLimits is the set a caller gets for passing a zero Limits.
func DefaultLimits() Limits {
	return Limits{
		MaxEntries:    DefaultMaxEntries,
		MaxIndexBytes: DefaultMaxIndexBytes,
		MaxEntryBytes: DefaultMaxEntryBytes,
		MaxTotalBytes: DefaultMaxTotalBytes,
	}
}

// resolve folds the defaults in and refuses a nonsensical set. It is called
// once per stream, at the top, so that a limit failure is about configuration
// rather than about the tree.
func (l Limits) resolve() (Limits, error) {
	d := DefaultLimits()
	if l.MaxEntries == 0 {
		l.MaxEntries = d.MaxEntries
	}
	if l.MaxIndexBytes == 0 {
		l.MaxIndexBytes = d.MaxIndexBytes
	}
	if l.MaxEntryBytes == 0 {
		l.MaxEntryBytes = d.MaxEntryBytes
	}
	if l.MaxTotalBytes == 0 {
		l.MaxTotalBytes = d.MaxTotalBytes
	}
	for _, v := range [...]int64{l.MaxEntries, l.MaxIndexBytes, l.MaxEntryBytes, l.MaxTotalBytes} {
		if v < 0 {
			return Limits{}, fmt.Errorf("%w: a stream limit is negative", ErrLimit)
		}
	}
	for i, e := range l.Exclude {
		switch {
		case e == "", e != path.Clean(e), path.IsAbs(e), e == ".", strings.HasPrefix(e, ".."):
			// The index of the offending element, not its value: an exclusion
			// is the caller's own configuration, but it is also a path.
			return Limits{}, fmt.Errorf("%w: exclusion %d is not a clean path inside the tree", ErrLimit, i)
		}
	}
	return l, nil
}

// excluded reports whether name is an excluded path or sits under one. It is
// the same rule the checkpoint package applies, spelled here because this end
// must not depend on the other one having applied it.
func (l Limits) excluded(name string) bool {
	for _, e := range l.Exclude {
		if name == e || strings.HasPrefix(name, e+"/") {
			return true
		}
	}
	return false
}

// The error vocabulary. Every one of them is a flat sentence with no path, no
// name and no target in it — see the package comment.
var (
	// ErrLimit is a tree, or a stream, past one of the limits above.
	ErrLimit = errors.New("wstream: the workspace is past a limit this stream enforces")

	// ErrFormat is a stream that is not one: a bad magic, a length prefix that
	// does not parse, an index record with an unknown kind, a tar header where
	// the index said something else, an entry out of walk order.
	ErrFormat = errors.New("wstream: the workspace stream is malformed")

	// ErrEntry is an entry the format does not carry: a name that is not a
	// clean relative path, a symlink that leaves the tree, a device node.
	ErrEntry = errors.New("wstream: an entry is not one a workspace stream carries")

	// ErrSource is the guest's own tree refusing to be read: a file that
	// changed size under the walk (the workspace was not quiesced), a read
	// that failed.
	ErrSource = errors.New("wstream: the workspace could not be read as it stands")

	// ErrTruncated is a stream that ended before its tar did.
	ErrTruncated = errors.New("wstream: the workspace stream ended early")
)

// validName is the name rule, applied by the guest on the way out and by the
// host on the way in. It is the checkpoint package's rule, spelled again here
// for the reason that package spells protocol/workspace's again: a check the
// previous hop performed is a check you are trusting.
func validName(name string) error {
	switch {
	case name == "":
		return errors.New("the name is empty")
	case len(name) > maxNameLen:
		return fmt.Errorf("the name is longer than %d bytes", maxNameLen)
	case strings.ContainsRune(name, 0):
		return errors.New("the name contains a NUL byte")
	case path.IsAbs(name):
		return errors.New("the name is absolute")
	case name != path.Clean(name):
		return errors.New("the name is not a clean relative path")
	case name == ".":
		return errors.New("the name is the tree root itself")
	}
	for _, elem := range strings.Split(name, "/") {
		switch {
		case elem == "", elem == ".", elem == "..":
			return errors.New("the name has an empty, \".\" or \"..\" element")
		case len(elem) > maxNameElement:
			return fmt.Errorf("the name has an element longer than %d bytes", maxNameElement)
		}
	}
	return nil
}

// validLink is the symlink rule: a target must be relative and must resolve,
// lexically, to somewhere still inside the tree. Lexical on purpose — a
// dangling but contained link is ordinary in a real tree (git worktrees,
// node_modules/.bin) and a stream that refused them could not be taken.
func validLink(name, target string) error {
	switch {
	case target == "":
		return errors.New("the symlink target is empty")
	case len(target) > maxNameLen:
		return fmt.Errorf("the symlink target is longer than %d bytes", maxNameLen)
	case strings.ContainsRune(target, 0):
		return errors.New("the symlink target contains a NUL byte")
	case path.IsAbs(target):
		return errors.New("the symlink target is absolute")
	}
	resolved := path.Join(path.Dir(name), target)
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return errors.New("the symlink target leaves the tree")
	}
	return nil
}
