package workspace_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tokencanopy/rainier/protocol/workspace"
)

// TestValidatePath pins the shape check every hop applies before a transfer
// path travels any further: the CLI before it sends, controld before it
// forwards, sessiond before it touches a file. A path that gets past this and
// still escapes would have to escape Resolve too, which is the point of having
// both.
func TestValidatePath(t *testing.T) {
	ok := []string{
		"repo",
		"repo/sub/dir",
		"./repo",
		workspace.WorkspaceRoot,
		workspace.WorkspaceRoot + "/repo/sub",
	}
	for _, p := range ok {
		if err := workspace.ValidatePath(p); err != nil {
			t.Errorf("ValidatePath(%q) = %v, want nil", p, err)
		}
	}

	bad := []string{
		"",
		"..",
		"../etc",
		"repo/../../etc",
		"/etc/passwd",
		"/workspaceother",
		workspace.WorkspaceRoot + "/../etc",
		"repo\x00/sub",
	}
	for _, p := range bad {
		if err := workspace.ValidatePath(p); err == nil {
			t.Errorf("ValidatePath(%q) = nil, want a refusal", p)
		}
	}
}

// TestResolveStaysUnderRoot is the file-system half of the same rule: whatever
// a caller sends, the path a transfer actually opens is inside the session's
// workspace or the transfer does not happen.
func TestResolveStaysUnderRoot(t *testing.T) {
	root := t.TempDir()

	got, err := workspace.Resolve(root, "repo/sub")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if want := filepath.Join(root, "repo/sub"); got != want {
		t.Fatalf("Resolve = %q, want %q", got, want)
	}

	// An absolute path INSIDE the root is honored as given.
	abs := filepath.Join(root, "repo")
	if got, err := workspace.Resolve(root, abs); err != nil || got != abs {
		t.Fatalf("Resolve(%q) = %q, %v; want the path back unchanged", abs, got, err)
	}

	for _, p := range []string{"", "..", "../outside", "repo/../../outside", "/etc/passwd"} {
		if got, err := workspace.Resolve(root, p); err == nil {
			t.Errorf("Resolve(%q) = %q, want a refusal", p, got)
		}
	}
}

// TestResolveRefusesASymlinkOutOfTheRoot: the lexical rules above say nothing
// about what a name POINTS at, and a workspace is a directory the session's own
// agent writes to. `/workspace/vendor -> /etc` would make "vendor/passwd" a
// perfectly well-formed transfer path that lands outside the workspace.
func TestResolveRefusesASymlinkOutOfTheRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if got, err := workspace.Resolve(root, "escape"); err == nil {
		t.Fatalf("Resolve of a symlink out of the root = %q, want a refusal", got)
	}
	// And through it, at a path that does not exist yet — the push case, where
	// the destination is about to be created.
	if got, err := workspace.Resolve(root, "escape/newdir"); err == nil {
		t.Fatalf("Resolve through a symlink out of the root = %q, want a refusal", got)
	}
	// A symlink that stays inside is fine: this is a containment rule, not a
	// ban on symlinks.
	if err := os.MkdirAll(filepath.Join(root, "real"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Symlink("real", filepath.Join(root, "inside")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := workspace.Resolve(root, "inside/sub"); err != nil {
		t.Fatalf("Resolve of a symlink that stays inside the root: %v", err)
	}
	// A destination that simply does not exist yet is the ordinary push case.
	if _, err := workspace.Resolve(root, "brand/new/dir"); err != nil {
		t.Fatalf("Resolve of a path that does not exist yet: %v", err)
	}
}
