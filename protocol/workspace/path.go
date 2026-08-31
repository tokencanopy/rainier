package workspace

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// ---------------------------------------------------------------------------
// path rules
// ---------------------------------------------------------------------------

// ValidatePath is the SHAPE check every hop applies to a transfer path before
// passing it on: the CLI before it sends, controld before it forwards. It
// rejects an empty path, any `..` element, an embedded NUL, and any absolute
// path that is not inside WorkspaceRoot.
//
// It is deliberately not the only check. Resolve, below, re-derives
// containment against the root the file system actually has, and sessiond
// calls it on every request — because a check performed by the hop before you
// is a check you are trusting, and the last hop before a syscall trusts
// nobody.
func ValidatePath(p string) error {
	if p == "" {
		return errors.New("the transfer path is empty")
	}
	if strings.ContainsRune(p, 0) {
		return errors.New("the transfer path contains a NUL byte")
	}
	if err := noParentElements(p); err != nil {
		return err
	}
	if !path.IsAbs(p) {
		return nil
	}
	clean := path.Clean(p)
	if clean != WorkspaceRoot && !strings.HasPrefix(clean, WorkspaceRoot+"/") {
		return fmt.Errorf("transfer path %q is outside %s", p, WorkspaceRoot)
	}
	return nil
}

// Resolve turns a transfer path into the absolute path under root it names,
// refusing anything that would land outside. A relative path is joined to
// root; an absolute one is honored as given and then checked.
//
// The containment test is on the CLEANED result, which is what makes it a real
// guard rather than a spelling rule: `a/../../etc` cleans to `../etc` and
// fails the prefix test even though no single element of it looked wrong.
func Resolve(root, p string) (string, error) {
	if p == "" {
		return "", errors.New("the transfer path is empty")
	}
	if strings.ContainsRune(p, 0) {
		return "", errors.New("the transfer path contains a NUL byte")
	}
	if err := noParentElements(p); err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	abs := p
	if !filepath.IsAbs(abs) {
		abs = filepath.Join(root, abs)
	}
	abs = filepath.Clean(abs)
	if !contains(root, abs) {
		return "", fmt.Errorf("transfer path %q is outside %s", p, root)
	}
	if !containsResolved(root, abs) {
		return "", fmt.Errorf("transfer path %q leaves %s through a symbolic link", p, root)
	}
	return abs, nil
}

// containsResolved repeats the containment test with symlinks followed, which
// is the only version of it the kernel will agree with.
//
// The spelling rules above say nothing about what a name POINTS at, and the
// tree being checked is one a session's own agent writes to: `/workspace/vendor
// -> /etc` turns "vendor/passwd" into a perfectly well-formed transfer path
// that lands outside the workspace. Resolving the deepest ancestor that exists
// covers the push case too, where the destination is about to be created.
//
// A root that cannot be resolved at all (it is not there yet, or is not
// readable) falls back to the lexical answer: refusing every transfer because
// of it would turn a missing workspace into a puzzling permission error
// instead of the plain "no such directory" the next syscall gives.
//
// WHAT THIS DOES NOT COVER, deliberately: a symlink planted INSIDE the
// destination between this check and the writes that follow, or one an
// extraction walks into on a path this function never saw. Closing that needs
// openat2-style per-component opening; the boundary that actually contains a
// session is its container, and both principals here are the same person.
func containsResolved(root, p string) bool {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return true
	}
	probe := p
	for {
		real, err := filepath.EvalSymlinks(probe)
		if err == nil {
			return contains(realRoot, real)
		}
		if !os.IsNotExist(err) {
			return false
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return false // walked to the filesystem root without finding anything
		}
		probe = parent
	}
}

// noParentElements refuses a path with a `..` element. The clean-and-compare
// checks above would catch every escape on their own; this exists so the
// caller gets told what is actually wrong with the path they sent instead of a
// sentence about a directory they never mentioned.
func noParentElements(p string) error {
	for _, elem := range strings.Split(filepath.ToSlash(p), "/") {
		if elem == ".." {
			return fmt.Errorf("transfer path %q contains \"..\"", p)
		}
	}
	return nil
}

// contains reports whether p is root or sits underneath it.
func contains(root, p string) bool {
	if p == root {
		return true
	}
	return strings.HasPrefix(p, root+string(filepath.Separator))
}
