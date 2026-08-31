// Package workspace defines the JSON messages and security rules of Rainier's
// workspace-inspection and transfer protocol — the per-repository diff and the
// bounded push/pull file transfer — together with the path and archive rules
// both ends of a transfer apply.
//
// It is the single source of truth for these wire-visible types, shared by
// three programs that speak the protocol: cmd/rainier at one end, cmd/sessiond
// at the other, internal/controld bridging them. The shapes they exchange have
// to be ONE definition rather than three that happen to match today.
//
// The security rules are here for a stronger reason still. An archive is
// untrusted at BOTH ends — a client pushing into somebody's sandbox, and a
// sandbox answering a pull that lands in a user's home directory — so the
// checks that keep every entry inside its destination are written once and
// applied by everyone. A rule implemented twice is a rule that is eventually
// implemented once.
package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
)

// The three methods controld drives into a sandbox over the session RPC. They
// are wire words: cmd/sessiond registers handlers under exactly these names
// (plan §Global Constraints, "RPC methods v0").
const (
	MethodDiff      = "diff"
	MethodPushFiles = "push_files"
	MethodPullFiles = "pull_files"
)

// The transfer's bounds. Every one of them is enforced at more than one hop —
// the client refuses to send past them, controld refuses to forward past them,
// and sessiond refuses to write past them — because each hop is the only one
// that can be sure of the hop before it. In particular a sandbox is not
// trusted to bound what it sends UP: a pull's total is counted by controld as
// the bytes arrive.
const (
	// MaxBytes caps one transfer's COMPRESSED size, in both directions
	// (design §4.5: 256MiB, refuse over). It is a v0 number chosen to make
	// the one-shot RPC shape safe, not a statement about what a workspace may
	// hold — see the streaming note on the REST handlers.
	MaxBytes int64 = 256 << 20
	// MaxExtractBytes caps what an archive may EXPAND to. The compressed cap
	// says nothing about that: a few hundred megabytes of zeros expand to
	// terabytes, and an extract with no bound of its own would fill the disk
	// of whichever end received it.
	MaxExtractBytes int64 = 4 * MaxBytes
	// ChunkBytes is one chunk's payload. Base64 inflates it by a third, which
	// keeps a chunk under the plan's 2MiB session-RPC payload cap.
	ChunkBytes = 1 << 20
	// SyncEvery is how many chunks sessiond may hold in the page cache before
	// it fsyncs the staging file; the ack for that chunk carries synced:true.
	SyncEvery = 8
	// StatBytes caps one repository's `diff --stat` output.
	StatBytes = 64 << 10
	// WorkspaceRoot is the one directory a transfer may touch inside a
	// session — the mount the session's volume lands on. It is spelled here as
	// well as in cmd/sessiond because controld validates a path BEFORE it
	// reaches a sandbox, and a check that can only run inside the sandbox is a
	// check the sandbox has to be trusted to perform.
	WorkspaceRoot = "/workspace"
)

// ErrTooLarge is what every bound above reports. It is a sentinel so a caller
// can name the cap in its own words — the CLI says which directory was too
// big, controld says which session, sessiond says which transfer.
var ErrTooLarge = errors.New("too large")

// ---------------------------------------------------------------------------
// wire shapes
// ---------------------------------------------------------------------------

// RepoDiff is one repository's answer in a session diff: what it is, the two
// branches the comparison is between, and git's own `--stat` text.
type RepoDiff struct {
	Repo          string `json:"repo"`
	BaseBranch    string `json:"base_branch"`
	SessionBranch string `json:"session_branch"`
	Stat          string `json:"stat"`
}

// DiffAnswer is the whole session's diff, and — passed through unchanged — the
// body of GET /v0/sessions/{id}/diff. A session with no repositories answers
// an empty array, never null, which is why MarshalJSON normalizes it.
type DiffAnswer struct {
	Repos []RepoDiff `json:"repos"`
}

// MarshalJSON renders a nil Repos as `[]`. The empty answer is the COMMON one
// (every scratch session), and a client branching on `repos.length` should not
// have to also branch on null.
func (a DiffAnswer) MarshalJSON() ([]byte, error) {
	type alias DiffAnswer // no method set: avoids recursing into this method
	if a.Repos == nil {
		a.Repos = []RepoDiff{}
	}
	return json.Marshal(alias(a))
}

// PushChunk is one chunk of an upload, and the body of one POST
// /v0/sessions/{id}/files. Data is []byte so JSON carries it as base64 with no
// hand-rolled encoding at any hop.
//
// Xfer names the transfer this chunk belongs to; Path repeats on every chunk
// so that controld — which holds no per-transfer state, and must not, since a
// transfer may be answered by any replica — can validate it on every request.
type PushChunk struct {
	Xfer string `json:"xfer"`
	Path string `json:"path"`
	Seq  int    `json:"seq"`
	Data []byte `json:"data,omitempty"`
	Done bool   `json:"done,omitempty"`
}

// PushAck answers one PushChunk. Synced is true on the chunks sessiond fsynced
// the staging file for (every SyncEvery chunks, and the last one): it is what a
// client may believe about durability, and nothing else in this protocol makes
// that claim.
type PushAck struct {
	Seq    int  `json:"seq"`
	Synced bool `json:"synced"`
}

// PullRequest asks for one chunk of a download. Seq 0 is what makes the
// archive: sessiond tars the path on the first request and serves slices of
// that staging file afterwards, so the bytes a pull returns are one consistent
// snapshot rather than a directory read live under the reader.
type PullRequest struct {
	Xfer string `json:"xfer"`
	Path string `json:"path"`
	Seq  int    `json:"seq"`
}

// PullChunk is one chunk of a download.
type PullChunk struct {
	Seq  int    `json:"seq"`
	Data []byte `json:"data,omitempty"`
	Done bool   `json:"done,omitempty"`
}

// ---------------------------------------------------------------------------
// small shared helpers
// ---------------------------------------------------------------------------

// HumanBytes renders a byte count the way a transfer's messages say it —
// shared so the cap in an error and the size in a progress line are spelled
// the same.
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
