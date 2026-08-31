// Package xfer is the vocabulary of Rainier's workspace-inspection RPCs — the
// per-repository diff and the bounded push/pull file transfer — together with
// the archive rules both ends of a transfer apply.
//
// It exists for the reason internal/wire and internal/relay do: three programs
// speak this protocol (cmd/rainier at one end, cmd/sessiond at the other,
// internal/controld bridging them) and the shapes they exchange have to be ONE
// definition rather than three that happen to match today.
//
// The security rules are here for a stronger reason still. An archive is
// untrusted at BOTH ends — a client pushing into somebody's sandbox, and a
// sandbox answering a pull that lands in a user's home directory — so the
// checks that keep every entry inside its destination are written once and
// applied by everyone. A rule implemented twice is a rule that is eventually
// implemented once.
package xfer

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
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

// ---------------------------------------------------------------------------
// writing an archive
// ---------------------------------------------------------------------------

// TarGz writes dir as a gzipped tar to w, returning how many COMPRESSED bytes
// it wrote, and failing with ErrTooLarge the moment that count would pass
// limit (0 means no limit).
//
// The cap is checked while writing rather than after, because nothing about an
// archive's size is knowable up front: a directory that compresses to 300MiB
// has to die mid-stream, having written no more than the cap.
//
// Only directories, regular files and symlinks travel. A socket, a device node
// or a fifo is refused rather than skipped: silently shipping a tree that is
// not the tree the user named is the worse failure, and the message says which
// entry stopped it.
//
// A symlink is held to the SAME rule here as on extraction (checkLink), which
// is what makes the refusal cheap. Both ends apply it, but only this end can
// apply it before the bytes move: a tree containing `vendor/node ->
// /usr/bin/node` would otherwise upload every one of up to 256MiB and fail on
// the last chunk, in a message about an "archive entry" the user never wrote.
// Refusing at pack time turns that into an error about a file on their own
// disk, before the first byte.
func TarGz(w io.Writer, dir string, limit int64) (int64, error) {
	lw := &limitWriter{w: w, limit: limit}
	zw := gzip.NewWriter(lw)
	tw := tar.NewWriter(zw)

	walkErr := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil // the root itself is the destination, not an entry
		}
		var link string
		if fi.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
			// Checked against the name this entry will HAVE in the archive,
			// which is the name the far end will check it under.
			if err := checkLink(filepath.ToSlash(rel), link); err != nil {
				return err
			}
		} else if !fi.Mode().IsRegular() && !fi.IsDir() {
			return fmt.Errorf("%s is not a regular file, directory or symlink; "+
				"push and pull move ordinary files only", p)
		}
		hdr, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if fi.IsDir() {
			hdr.Name += "/"
		}
		// Uname/Gname are the pusher's local account names — meaningless in a
		// sandbox and needlessly identifying. The numeric ids are dropped for
		// the same reason; extraction runs as whoever is extracting.
		hdr.Uname, hdr.Gname, hdr.Uid, hdr.Gid = "", "", 0, 0
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		// CopyN, not Copy: the header already promised hdr.Size bytes, and a
		// file being appended to under us would otherwise desynchronize the
		// archive (tar.Writer errors, but only after the damage).
		if _, err := io.CopyN(tw, f, hdr.Size); err != nil {
			return fmt.Errorf("reading %s: %w", p, err)
		}
		return nil
	})
	if walkErr != nil {
		return lw.n, walkErr
	}
	if err := tw.Close(); err != nil {
		return lw.n, err
	}
	if err := zw.Close(); err != nil {
		return lw.n, err
	}
	return lw.n, nil
}

// limitWriter fails the whole archive as soon as one Write would carry the
// total past limit.
type limitWriter struct {
	w     io.Writer
	n     int64
	limit int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.limit > 0 && l.n+int64(len(p)) > l.limit {
		return 0, fmt.Errorf("the archive is larger than the %s transfer limit: %w",
			HumanBytes(l.limit), ErrTooLarge)
	}
	n, err := l.w.Write(p)
	l.n += int64(n)
	return n, err
}

// ---------------------------------------------------------------------------
// reading an archive
// ---------------------------------------------------------------------------

// UntarGz extracts the gzipped tar at archive into dest, refusing to write
// anything at all unless every entry passes first.
//
// TWO PASSES, deliberately. The archive comes from the other end of a
// transfer, which is never trusted: a tar whose fortieth entry is
// `../../etc/cron.d/x` would, extracted straight through, have already
// written thirty-nine files by the time the escape is noticed. Reading the
// file twice costs one more decompression and buys "an archive is accepted
// whole or not at all", which is the property the rest of the system relies
// on. (An I/O failure DURING the second pass — a full disk — can still leave
// files behind; that is a local accident, not a hostile archive, and the
// caller's error says so.)
//
// limit bounds the UNCOMPRESSED total; pass MaxExtractBytes unless you have a
// reason not to.
func UntarGz(archive, dest string, limit int64) error {
	if err := scanArchive(archive, dest, limit); err != nil {
		return err
	}
	return extractArchive(archive, dest)
}

// openArchive opens one pass over the archive.
func openArchive(archive string) (*os.File, *tar.Reader, func(), error) {
	f, err := os.Open(archive)
	if err != nil {
		return nil, nil, nil, err
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, nil, nil, fmt.Errorf("reading the archive: %w", err)
	}
	return f, tar.NewReader(zr), func() { zr.Close(); f.Close() }, nil
}

// scanArchive is the validating pass: every entry's name, type and target, and
// the running uncompressed total.
func scanArchive(archive, dest string, limit int64) error {
	_, tr, closeAll, err := openArchive(archive)
	if err != nil {
		return err
	}
	defer closeAll()

	var total int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading the archive: %w", err)
		}
		if _, err := entryPath(dest, hdr.Name); err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir, tar.TypeReg:
			total += hdr.Size
			if limit > 0 && total > limit {
				return fmt.Errorf("the archive expands to more than %s: %w", HumanBytes(limit), ErrTooLarge)
			}
		case tar.TypeSymlink:
			if err := checkLink(hdr.Name, hdr.Linkname); err != nil {
				return err
			}
		default:
			// Hard links included: a hard link's target is resolved by the
			// kernel against paths that already exist, so honoring one would
			// mean reasoning about what is on disk rather than about the
			// archive. Refused, like every other exotic entry.
			return fmt.Errorf("archive entry %q is not a regular file, directory or symlink", hdr.Name)
		}
	}
}

// extractArchive is the writing pass. It re-validates every entry as it goes:
// the two passes read the same file, but a check that only runs in the pass
// that does not write is one refactor away from not running at all.
func extractArchive(archive, dest string) error {
	_, tr, closeAll, err := openArchive(archive)
	if err != nil {
		return err
	}
	defer closeAll()

	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading the archive: %w", err)
		}
		target, err := entryPath(dest, hdr.Name)
		if err != nil {
			return err
		}
		if target == "" {
			continue // the archive's own root entry
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			// |0o700: the archive's own bits are kept, but an extraction has
			// to be able to write the entries that land INSIDE this directory,
			// and a tree carrying a mode like 0555 would otherwise fail
			// halfway through with a permission error about a file the user
			// never mentioned.
			if err := os.MkdirAll(target, entryMode(hdr, 0o755)|0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := writeFile(target, tr, hdr, entryMode(hdr, 0o644)); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := checkLink(hdr.Name, hdr.Linkname); err != nil {
				return err
			}
			// Replaced rather than merged: os.Symlink fails on an existing
			// name, and a push over a tree that already has one must land the
			// archive's version, not the older one.
			if err := os.RemoveAll(target); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("archive entry %q is not a regular file, directory or symlink", hdr.Name)
		}
	}
}

// writeFile lands one regular entry, creating its parents (an archive is not
// required to carry directory entries before the files in them).
func writeFile(target string, tr *tar.Reader, hdr *tar.Header, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	// CopyN bounded by the header's own size: the scanning pass added exactly
	// this many bytes to the total it checked against the limit, so copying
	// anything else here would extract past a bound that was already approved.
	// A short read is an error, not a short file — CopyN returns nil only when
	// it moved exactly the bytes the header promised, and an archive that
	// cannot keep that promise is truncated.
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		f.Close()
		return fmt.Errorf("extracting %q: %w", hdr.Name, err)
	}
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if !hdr.ModTime.IsZero() {
		// Best effort: a tree whose mtimes survive is a tree `make` behaves
		// sanely in, but a filesystem that will not set them is not a reason
		// to fail a transfer that has already landed.
		_ = os.Chtimes(target, time.Now(), hdr.ModTime)
	}
	return nil
}

// entryMode reads an entry's permission bits, falling back to def for an
// archive that carries none (mode 0 would otherwise extract a file nobody,
// including its owner, can read).
func entryMode(hdr *tar.Header, def os.FileMode) os.FileMode {
	mode := os.FileMode(hdr.Mode).Perm()
	if mode == 0 {
		return def
	}
	return mode
}

// entryPath resolves one archive entry against the destination, refusing every
// name that would land outside it. An empty result means the entry IS the
// destination (a "./" root entry), which is nothing to write.
func entryPath(dest, name string) (string, error) {
	clean := path.Clean(filepath.ToSlash(name))
	if clean == "." || clean == "/" {
		return "", nil
	}
	if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("archive entry %q escapes the destination directory", name)
	}
	target := filepath.Join(dest, filepath.FromSlash(clean))
	// Belt and braces: Join cleans, and the test above already rules out every
	// escape, but this is the line that would still hold if the one above were
	// ever loosened.
	if !contains(filepath.Clean(dest), target) {
		return "", fmt.Errorf("archive entry %q escapes the destination directory", name)
	}
	return target, nil
}

// checkLink refuses a symlink whose target leaves the extraction root.
//
// The check is on the link's TEXT, resolved against the directory the link
// itself lands in — which is what the kernel will do when something follows
// it. An absolute target is refused outright: inside a sandbox it would point
// at the container's own root, and on a client machine at the user's.
func checkLink(name, link string) error {
	if link == "" {
		return fmt.Errorf("archive entry %q is a symlink with no target", name)
	}
	if path.IsAbs(filepath.ToSlash(link)) {
		return fmt.Errorf("archive entry %q is a symlink to the absolute path %q", name, link)
	}
	resolved := path.Join(path.Dir(path.Clean(filepath.ToSlash(name))), filepath.ToSlash(link))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("archive entry %q is a symlink to %q, outside the destination directory", name, link)
	}
	return nil
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
