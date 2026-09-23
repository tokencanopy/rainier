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
	"log"
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
		if out.err != nil && (stage == stageStream || err == nil) {
			// The stream's own failure is the better sentence when the reader's
			// error is merely "the pipe closed".
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
