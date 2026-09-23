// internal/driver/microvm_checkpoint.go
//
// The durability barrier: a cold suspend does not succeed until this session's
// workspace is a committed, verified checkpoint.
//
// ADR-0003 §4.4, quoted, because everything here is an attempt to satisfy
// exactly it: "Suspend(warm=false) must not report success, and the host must
// not release the workspace slot, until the portable checkpoint upload
// completes and its manifest is committed atomically."
//
// The shape is in docs/design/2026-09-23-workspace-checkpoint-wiring.md. In one
// paragraph: the guest streams its workspace over the relay conn it already
// has, this file feeds that stream to the checkpoint writer as an fs.FS, the
// writer encrypts it frame by frame into a blob store, and only once the
// manifest has committed AND been verified does the VM get terminated. Nothing
// plaintext is written to host disk on this path: the bytes go from a socket,
// through a 32 KiB copy buffer, into a frame, into the store.
package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/tokencanopy/rainier/checkpoint"
	"github.com/tokencanopy/rainier/internal/wstream"
)

// CheckpointOpts is what a host must give this driver before a cold suspend can
// produce a checkpoint. It is all ports and policy — a blob store, a key
// wrapper, a key reference, limits — so that a self-hosted runner and a hosted
// cell differ in what they pass here and in nothing else.
//
// A nil *CheckpointOpts on MicrovmOpts is a driver that does not checkpoint:
// the local dev surface and the driver contract suite, which have no store and
// no key. Its cold suspend is what it was before this existed. That is a
// deliberate seam and not a fallback — runnerd REQUIRES both flags for
// --driver=microvm (see cmd/runnerd), so no production path reaches it.
type CheckpointOpts struct {
	// Store is where the two objects of a checkpoint go. The local-directory
	// store is in this package (DirBlobStore); GCS is rainier-cloud's.
	Store checkpoint.BlobStore
	// Keys wraps each checkpoint's data key. StaticKeyWrapper for self-hosted,
	// a KMS for a cell.
	Keys checkpoint.Wrapper
	// KeyRef is the key-encryption key to wrap under. It may be an alias; what
	// the wrapper reports is what the manifest records.
	KeyRef checkpoint.KeyRef
	// Prefix is the storage prefix every object goes under. It may be empty.
	Prefix string
	// Limits are what this host enforces on the guest's stream. The zero value
	// is wstream's defaults, plus the checkpoint package's own exclusions,
	// which are added by this driver and cannot be removed by configuration.
	Limits wstream.Limits
	// VerifyTimeout bounds the restore test that follows the commit. Zero means
	// defaultCheckpointVerifyTimeout.
	VerifyTimeout time.Duration
	// OwnerUID and OwnerGID are the user a RESTORED workspace is given to
	// before it becomes a filesystem: the uid and gid the guest's agent runs
	// as, which is the session image's own user (1000:1000 for Rainier's).
	//
	// They are configuration and not a constant because a checkpoint records
	// modes and not owners — a uid is not portable across hosts, and the format
	// is meant to be — so the ownership has to come from somewhere, and the
	// only party that knows which user this environment's image runs as is the
	// operator who built it.
	//
	// 0, 0 means "leave every restored file owned by whoever restored it". That
	// is right for a host whose guest runs as root and for a test with no
	// privilege to give a file away, and wrong for every production microVM
	// host — see chownTree for what it costs.
	OwnerUID int
	OwnerGID int
}

// defaultCheckpointVerifyTimeout bounds Verify: one sequential read of the
// object that was just written, through the same store. Ten minutes is far
// above a local directory and above regional object storage for a workspace
// the guest was able to stream inside its own budget.
const defaultCheckpointVerifyTimeout = 10 * time.Minute

// maxStreamTrailerBytes is how much of the stream may follow the last tree
// entry the checkpoint writer read.
//
// There is always SOME: a tar ends with two zero blocks the reader above never
// asks for, because it stops at the last entry the index named. Those bytes
// still have to be drained, or the guest blocks writing them and never sends
// the end marker the barrier is waiting for. A megabyte is a thousand times the
// trailer and still a bound.
const maxStreamTrailerBytes = 1 << 20

// checkpointStage names which step of the barrier failed. It travels into a
// session's error column, so it is a WORD and never a path, a name or a value
// (tenancy §15.1) — the cost of that rule is that an operator gets a stage and
// a count instead of a file, which is the same trade the checkpoint library
// makes and for the same reason.
type checkpointStage string

const (
	stageConfig checkpointStage = "config"
	stageStream checkpointStage = "stream"
	stageWrite  checkpointStage = "write"
	stageVerify checkpointStage = "verify"
)

// checkpointResult is what a committed checkpoint leaves on the instance
// record.
type checkpointResult struct {
	Generation  uint64
	ManifestKey string
	Entries     int64
	Bytes       int64
	Nonce       uint64
}

// CheckpointsColdSuspend reports whether this driver's cold suspend runs the
// workspace checkpoint handshake itself.
//
// runnerd asks before it sends its own cold suspend notice: the notice and the
// stream that follows it are one handshake with one nonce, and two notices
// would have the guest quiesce twice and stream into a suspend nobody is
// reading. See runnerd.Op.
func (m *Microvm) CheckpointsColdSuspend() bool { return m.ckpt != nil }

// checkpointOnSuspend is the barrier as Suspend reaches it: claim the instance,
// read what the checkpoint needs off the record, run it with the mutex
// released, and record the result.
//
// A session with no workspace disk — a create that carried no session id, which
// the driver contract's own fixtures do — has nothing to checkpoint and is not
// an error: there is no workspace, so there is no work to lose.
func (m *Microvm) checkpointOnSuspend(ctx context.Context, id string) error {
	m.mu.Lock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no such id %s", id)
	}
	if inst.checkpointing {
		m.mu.Unlock()
		return fmt.Errorf("cold suspend of %s: a workspace checkpoint is already in flight for this instance", id)
	}
	sessionID, volume := inst.SessionID, inst.Volume
	generation := inst.CheckpointGeneration + 1
	hasWorkspace := inst.Cfg.WorkspaceDiskPath != ""
	inst.checkpointing = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if e, ok := m.instances[id]; ok {
			e.checkpointing = false
		}
		m.mu.Unlock()
	}()

	if !hasWorkspace || sessionID == "" {
		log.Printf("microvm: %s has no workspace disk; there is nothing to checkpoint", id)
		return nil
	}

	res, err := m.checkpointWorkspace(ctx, id, sessionID, volume, generation)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok = m.instances[id]
	if !ok {
		// A Destroy raced the checkpoint. The objects are committed and
		// verified and are left exactly where they are: deleting a tenant's
		// only copy of their work because the record went away is the one
		// outcome this barrier exists to prevent, and the retention sweeper
		// above this driver is what removes a checkpoint on purpose.
		return fmt.Errorf("no such id %s", id)
	}
	inst.CheckpointGeneration = res.Generation
	inst.CheckpointKey = res.ManifestKey
	inst.CheckpointAt = time.Now().UTC().Format(time.RFC3339)
	inst.bump()
	return nil
}

// checkpointWorkspace is the barrier, in the order the barrier is:
//
//  1. ask the guest to cold suspend and read the workspace it streams back;
//  2. present that stream as the fs.FS the checkpoint writer walks;
//  3. write — which encrypts, uploads, and commits the manifest with one
//     put-if-absent, the atomic commit §4.4 rests on;
//  4. VERIFY, by reading the committed object back through the store, because
//     the claim being made is that what is stored is restorable and checking
//     the bytes still in memory would test the encoder against itself;
//  5. tell the guest, and only then let the caller terminate the VM.
//
// Any failure before (4) returns and the VM is untouched: the caller has not
// stopped it, the workspace disk is where it was, and runnerd's own
// settleFailedColdSuspend rolls the session back to running. Failure AFTER the
// commit still fails the suspend — a checkpoint that cannot be read back is not
// one — and leaves the committed objects for the sweeper rather than deleting
// them, because a reconciler racing itself would then be able to delete the
// checkpoint it had just written.
func (m *Microvm) checkpointWorkspace(ctx context.Context, id, sessionID, volume string, generation uint64) (checkpointResult, error) {
	opts := m.ckpt
	host := m.currentHost()
	if host == nil {
		return checkpointResult{}, m.checkpointErr(id, stageConfig,
			errors.New("this driver has no runner above it to stream the workspace over"))
	}
	c := checkpoint.Context{Workspace: volume, Session: sessionID, Generation: generation}
	if err := c.Validate(); err != nil {
		return checkpointResult{}, m.checkpointErr(id, stageConfig, err)
	}
	w, err := checkpoint.NewWriter(opts.Store, opts.Keys, checkpoint.WriterOptions{
		Prefix: opts.Prefix,
		KeyRef: opts.KeyRef,
	})
	if err != nil {
		return checkpointResult{}, m.checkpointErr(id, stageConfig, err)
	}
	r, err := checkpoint.NewReader(opts.Store, opts.Keys, checkpoint.ReaderOptions{
		Prefix: opts.Prefix,
		// The producer running its own restore test is the principal that just
		// wrote the checkpoint, so this hook has nothing to decide. It is not a
		// loophole — the library cannot judge a policy, only guarantee the step
		// exists — and a DESTINATION restoring somebody else's checkpoint puts
		// its real check here (see restoreWorkspace, which is the same
		// principal on the same host).
		Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil },
	})
	if err != nil {
		return checkpointResult{}, m.checkpointErr(id, stageConfig, err)
	}

	// The stream, on its own goroutine, writing into a pipe the checkpoint
	// writer reads. There is no buffer between them and no temporary file: the
	// pipe is what makes the guest's write rate the store's, which is the
	// back-pressure that keeps a workspace from ever existing on this host.
	pr, pw := io.Pipe()
	type streamOutcome struct {
		rep WorkspaceStream
		err error
	}
	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := host.StreamWorkspace(ctx, sessionID, pw)
		// Closing the write end is what ends the reader below, with this
		// error if there was one — so a guest that died mid-stream surfaces as
		// a failed checkpoint rather than as a reader waiting for bytes that
		// are not coming.
		_ = pw.CloseWithError(err)
		done <- streamOutcome{rep: rep, err: err}
	}()
	// Whatever happens below, the stream goroutine is unblocked and waited for
	// before this returns: a suspend that left one running would have a guest
	// still streaming into a pipe nobody reads, holding the hub's read loop.
	fail := func(stage checkpointStage, err error) (checkpointResult, error) {
		_ = pr.CloseWithError(err)
		out := <-done
		// Which of the two errors is the CAUSE. A guest that died mid-stream is
		// seen here as a malformed or truncated stream — the reader's honest
		// account of a pipe that closed under it — and reporting that would
		// send somebody looking at a parser when the sandbox is what went away.
		// A store that refused is the opposite: the stream then fails BECAUSE
		// this end stopped taking bytes, and naming the stream would hide the
		// outage. So the stream's own error wins exactly when the local one is
		// about the bytes.
		if out.err != nil && (stage == stageStream || isStreamError(err)) {
			return checkpointResult{}, m.checkpointErr(id, stageStream, out.err)
		}
		return checkpointResult{}, m.checkpointErr(id, stage, err)
	}

	lim := opts.Limits
	// The exclusions are ADDED here, by this driver, in the one place every
	// checkpoint of a workspace passes through. A host that configured none
	// still gets the set the checkpoint library owns.
	lim.Exclude = append(append([]string(nil), lim.Exclude...), checkpoint.DefaultExclusions()...)
	fsys, err := wstream.NewFS(pr, lim)
	if err != nil {
		return fail(stageStream, err)
	}
	res, err := w.Write(ctx, c, checkpoint.Source{FS: fsys})
	if err != nil {
		// Before the manifest's put-if-absent, so there is no checkpoint at
		// this generation and the counter does not advance.
		return fail(stageWrite, err)
	}

	// The tar's trailing blocks, which the walk above never asked for. Drained
	// rather than ignored: the guest is blocked writing them, and it cannot
	// send the end marker the barrier is waiting for until they are gone.
	if n, _ := io.Copy(io.Discard, io.LimitReader(pr, maxStreamTrailerBytes+1)); n > maxStreamTrailerBytes {
		return fail(stageStream, fmt.Errorf("the guest sent more than %d bytes past the tree it described", maxStreamTrailerBytes))
	}
	_ = pr.Close()
	out := <-done
	if out.err != nil {
		return checkpointResult{}, m.checkpointErr(id, stageStream, out.err)
	}
	if out.rep.Entries != int64(fsys.Indexed()) {
		return checkpointResult{}, m.checkpointErr(id, stageStream,
			fmt.Errorf("the guest reported %d entries and described %d", out.rep.Entries, fsys.Indexed()))
	}

	// The restore test, on a context of its own: the caller's may be nearly
	// spent by a long stream, and a verify that is skipped for lack of time is
	// a durability barrier that is not one.
	verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opts.verifyTimeout())
	defer cancel()
	rep, err := r.Verify(verifyCtx, c)
	if err != nil {
		return checkpointResult{}, m.checkpointErr(id, stageVerify, err)
	}
	log.Printf("microvm: %s checkpointed its workspace at generation %d: %d entries, %d bytes of tree, %d frames",
		id, generation, rep.Entries, rep.FileBytes, rep.Summary.Frames)

	host.CheckpointCommitted(sessionID, out.rep.Nonce)
	return checkpointResult{
		Generation:  generation,
		ManifestKey: res.ManifestKey,
		Entries:     rep.Entries,
		Bytes:       out.rep.Bytes,
		Nonce:       out.rep.Nonce,
	}, nil
}

// isStreamError reports whether err is about the BYTES the guest sent rather
// than about what this host did with them. It is the whole of how the barrier
// tells a sandbox that went away from a store that would not take an object,
// which are the two failures with opposite operator actions.
// The checkpoint sentinels are in the list beside this package's own, and they
// have to be: the library reduces a file system's error to a CATEGORY rather
// than wrapping it (checkpoint/tree.go's fsCategory, which exists so that no
// path can travel out in an error), so a stream that failed under its walk
// arrives here as ErrSource with the sentence flattened into text. ErrSource
// and ErrEntry both mean "the bytes I was given are the problem", which for
// this caller means the stream.
func isStreamError(err error) bool {
	for _, s := range []error{
		checkpoint.ErrSource, checkpoint.ErrEntry,
		wstream.ErrFormat, wstream.ErrEntry, wstream.ErrTruncated, wstream.ErrLimit, wstream.ErrSource,
	} {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// checkpointErr is the one shape a failed barrier reports: which stage, and the
// underlying sentence.
//
// The instance id is in it and nothing else is. A session id, a path, a file
// name, a symlink target and a file byte are all session content (tenancy
// §15.1) and this error reaches an operator's log and a session's error column;
// the checkpoint library and the stream package are both written to carry none
// of them, so what is wrapped here is safe to print.
func (m *Microvm) checkpointErr(id string, stage checkpointStage, err error) error {
	return fmt.Errorf("microvm: the cold suspend of %s failed at the checkpoint %s stage: %w", id, stage, err)
}

func (o *CheckpointOpts) verifyTimeout() time.Duration {
	if o.VerifyTimeout > 0 {
		return o.VerifyTimeout
	}
	return defaultCheckpointVerifyTimeout
}

// ---------------------------------------------------------------------------
// the deep-dormant resume
// ---------------------------------------------------------------------------

const (
	stageRestore checkpointStage = "restore"
	stageOwner   checkpointStage = "owner"
	stageMkfs    checkpointStage = "mkfs"
)

// ensureWorkspaceForResume is the deep-dormant branch of a cold resume: it does
// nothing at all unless this session's workspace image is GONE.
//
// The ordinary cold resume is unchanged and must stay that way. A parked
// session's workspace disk is right where it was (ADR-0003 §4.1's Suspend row
// persists it), the guest mounts the same block device, and restoring over it
// would replace a newer filesystem with an older checkpoint's copy of it.
//
// So the image's ABSENCE is the whole trigger, and it is the honest one: the
// only thing that removes it is RemoveWorkspace, called from above this driver
// when the session goes deep dormant.
func (m *Microvm) ensureWorkspaceForResume(ctx context.Context, id string) error {
	m.mu.Lock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no such id %s", id)
	}
	sessionID, volume := inst.SessionID, inst.Volume
	diskPath, generation := inst.Cfg.WorkspaceDiskPath, inst.CheckpointGeneration
	m.mu.Unlock()

	if diskPath == "" {
		return nil // a session with no workspace disk at all
	}
	if _, err := os.Stat(diskPath); err == nil {
		return nil // the ordinary cold resume: the disk is where it was
	} else if !errors.Is(err, os.ErrNotExist) {
		return m.restoreErr(id, stageRestore, err)
	}

	if m.ckpt == nil {
		return fmt.Errorf("cold resume of %s: this session's workspace image is gone and this runner has no checkpoint store to restore it from", id)
	}
	if generation == 0 {
		// Said plainly rather than booted around. An empty workspace that looks
		// like a successful resume is the failure a person discovers by finding
		// their work missing, which is the one outcome worth refusing for.
		return fmt.Errorf("cold resume of %s: this session's workspace image is gone and no checkpoint of it was ever committed; there is nothing to restore", id)
	}
	return m.restoreWorkspaceDisk(ctx, id, sessionID, volume, diskPath, generation)
}

// restoreWorkspaceDisk rebuilds a session's workspace image from its newest
// committed checkpoint, for the case ADR-0003 §2.3 calls deep dormant: the disk
// has been deleted and the checkpoint is the only copy of the work left.
//
// Restore into a scratch directory, chown it to the user the guest runs as,
// build an ext4 from it with `mkfs.ext4 -d`, remove the scratch directory. The
// host never mounts anything, at either end of the session's life.
//
// The scratch directory is no new exposure — the workspace image on this host
// is plaintext today, in this same state directory, with provider encryption as
// defense in depth — but it is 0700, it is never inside a jail, and it is
// removed before the guest boots, on every path including the failures.
func (m *Microvm) restoreWorkspaceDisk(ctx context.Context, id, sessionID, volume, diskPath string, generation uint64) error {
	opts := m.ckpt
	c := checkpoint.Context{Workspace: volume, Session: sessionID, Generation: generation}
	if err := c.Validate(); err != nil {
		return m.restoreErr(id, stageConfig, err)
	}
	r, err := checkpoint.NewReader(opts.Store, opts.Keys, checkpoint.ReaderOptions{
		Prefix: opts.Prefix,
		// The principal restoring is the one that wrote it, on the host that
		// holds the session. A DESTINATION in another cell puts its real check
		// here; this hook exists so that no path through the library can be the
		// place where authorizing a restore was forgotten.
		Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil },
	})
	if err != nil {
		return m.restoreErr(id, stageConfig, err)
	}

	scratch, err := m.scratchDir(id)
	if err != nil {
		return m.restoreErr(id, stageRestore, err)
	}
	// Removed on EVERY path, including a restore that failed part way through a
	// tree: a tenant's files left in a directory nobody names again would be
	// exactly the exposure this design says it does not add.
	defer func() {
		if err := os.RemoveAll(scratch); err != nil {
			log.Printf("microvm: removing the restore scratch directory of %s: %v", id, err)
		}
	}()

	rep, err := r.Restore(ctx, c, scratch)
	if err != nil {
		return m.restoreErr(id, stageRestore, err)
	}
	if err := chownTree(scratch, opts.OwnerUID, opts.OwnerGID); err != nil {
		return m.restoreErr(id, stageOwner, err)
	}
	if err := m.formatRestored(diskPath, scratch); err != nil {
		return m.restoreErr(id, stageMkfs, err)
	}
	log.Printf("microvm: %s restored its workspace from checkpoint generation %d: %d entries, %d bytes",
		id, generation, rep.Entries, rep.FileBytes)
	return nil
}

// formatRestored creates the image file and populates it from dir, removing a
// half-built image rather than leaving one for a later boot to find with Stat
// and attach as a filesystem — which is ensureDisk's rule, applied to the one
// other place an image is made.
func (m *Microvm) formatRestored(path, dir string) error {
	if err := os.MkdirAll(filepath.Dir(path), microvmDirMode); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, microvmFileMode)
	if err != nil {
		return err
	}
	if err := f.Truncate(workspaceDiskBytes); err != nil {
		f.Close()
		_ = os.Remove(path)
		return err
	}
	f.Close()
	if err := m.format.FormatFromDir(path, dir); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// scratchDir makes this restore's private directory, 0700, under the state
// directory. The suffix is the boot counter rather than a random name so that a
// directory left behind by a host that died mid-restore is recognisably one
// instance's and is replaced by the next attempt rather than accumulating.
func (m *Microvm) scratchDir(id string) (string, error) {
	if err := checkPathSegment("instance id", id); err != nil {
		return "", err
	}
	dir := filepath.Join(m.opts.StateDir, "restore", id)
	// Whatever a previous attempt left is gone before this one starts: a
	// restore into a non-empty target is refused by the library, and a
	// half-restored tree from a host that died is not a tree to build a
	// filesystem from.
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(dir), microvmDirMode); err != nil {
		return "", err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", err
	}
	// MkdirAll and Mkdir both mask the mode with the process umask, so the
	// permission a comment claims is set explicitly rather than requested.
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

// chownTree gives every restored entry to the user the guest runs as.
//
// It is not optional on a real host, and it is the one thing about this path
// that a fake cannot show. A checkpoint records modes and not owners (the
// format is portable across hosts, and a uid is not), so a restored tree is
// owned by whoever restored it — root, on a microVM host. The guest's agent
// runs as the session image's own user, and the guest's /init chowns the
// workspace mount point only when it is EMPTY, precisely so that a resumed
// workspace's contents are left alone. So without this the session comes back
// with every one of its files owned by root and unwritable by the agent.
//
// A zero uid and gid mean "leave it", which is what a test on a machine with no
// privilege to give a file away wants, and what a host running the guest as
// root would want. Anything else is applied with Lchown, which changes the LINK
// rather than what it points at — a symlink out of the tree is impossible
// (checkLink refuses one at both ends), and following one would still be the
// wrong thing to do here.
func chownTree(root string, uid, gid int) error {
	if uid == 0 && gid == 0 {
		return nil
	}
	return filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := os.Lchown(p, uid, gid); err != nil {
			// The path is this host's scratch directory plus a name from inside
			// the workspace, so the error is reduced to its cause: a name is
			// session content (tenancy §15.1).
			var pe *fs.PathError
			if errors.As(err, &pe) && pe.Err != nil {
				return fmt.Errorf("giving a restored entry to %d:%d: %w", uid, gid, pe.Err)
			}
			return fmt.Errorf("giving a restored entry to %d:%d", uid, gid)
		}
		return nil
	})
}

func (m *Microvm) restoreErr(id string, stage checkpointStage, err error) error {
	return fmt.Errorf("microvm: the cold resume of %s failed at the checkpoint %s stage: %w", id, stage, err)
}
