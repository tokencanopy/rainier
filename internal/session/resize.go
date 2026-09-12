package session

type Size struct{ Cols, Rows int }

// reported is one attachment's last terminal size and WHEN it reported it,
// counted in the session's own monotonic tick rather than in wall time. The
// tick is what "most recent" means here: a clock would make the rule depend on
// two sizes arriving in the same millisecond.
type reported struct {
	size Size
	at   uint64
}

// LatestSize returns the size of the most recently reported entry, and whether
// there was one at all.
//
// This is the "latest client" rule: the pty follows the most recent resize from
// any attachment that may type. It replaced smallest-per-axis when the shared
// attachment policy made several typers possible
// (docs/design/2026-09-12-shared-attachment-policy.md).
//
// Smallest was written when at most one attachment could be entitled at a time,
// where it was the identity function; with two typers it is the rule that
// leaves BOTH terminals wrong — each one rendering a screen narrower than
// itself — with no way for either person to fix it. Latest is the rule a person
// can predict: resize your window and the shell follows it. The cost is stated
// rather than hidden: the other typer's terminal then renders a screen wider
// than itself until its own next resize.
//
// Under the exclusive policy exactly one attachment is entitled at any instant,
// so this returns that attachment's size — the value smallest-per-axis returned
// too.
func LatestSize(entries []reported) (Size, bool) {
	if len(entries) == 0 {
		return Size{}, false
	}
	latest := entries[0]
	for _, e := range entries[1:] {
		if e.at > latest.at {
			latest = e
		}
	}
	return latest.size, true
}
