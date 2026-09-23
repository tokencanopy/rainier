package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"time"

	"github.com/tokencanopy/rainier/checkpoint"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/wstream"
)

// The guest's half of the cold-suspend checkpoint: this VM is ending, and the
// only copy of the user's work that will survive it is the one this file puts
// on the wire.
//
// It runs INSIDE the cold suspend handshake, after the flush, the exec kill and
// the agent-home unmount, and before `suspend_ready` — because the host may not
// terminate the VM until the tree is out of it, and `suspend_ready` is what
// tells the host it may. See docs/design/2026-09-23-workspace-checkpoint-wiring.md.

// streamChunkBytes is one FrameStream's payload. A frame is JSON with a
// base64'd payload, so 64 KiB of workspace becomes about 87 KiB on the wire —
// comfortably under the transport's 16 MiB frame limit and large enough that
// the per-frame cost is noise beside the copy.
const streamChunkBytes = 64 << 10

// workspaceStreamer is what the cold suspend handler needs from this file: put
// the workspace on the wire under this suspend's nonce, and say what went. It
// is a function rather than an interface because there is exactly one
// implementation and the tests need to substitute a failure, not a type.
//
// A nil one is a sandbox that does not stream: the local dev surface, and a
// Docker session, which never receives a cold notice at all (runnerd sends it
// only for a driver that withholds secrets). The handler then answers
// `suspend_ready` exactly as it did before this existed, and the host's own
// stream budget is what reports that no checkpoint was taken.
type workspaceStreamer func(nonce uint64) (wstream.Report, error)

// streamWorkspaceFn builds the streamer main installs: the session's workspace
// root, streamed to the runner over the live control connection.
//
// The exclusions are the checkpoint library's own, asked for here rather than
// spelled: the bytes that must not leave this guest and the bytes the host
// would refuse are the same set, and a second spelling of it is a second thing
// to forget to update.
func streamWorkspaceFn(sender streamSender, root string) workspaceStreamer {
	return func(nonce uint64) (wstream.Report, error) {
		ctx, cancel := context.WithTimeout(context.Background(), workspaceStreamBudget)
		defer cancel()
		return streamWorkspace(ctx, sender, nonce, os.DirFS(root))
	}
}

// workspaceStreamBudget bounds the guest's own side of the copy, and it is
// deliberately longer than the host's total stream budget (30 minutes, see the
// design note's §4). The host is the end that decides a stream has failed —
// it is the one holding the checkpoint writer — and a guest that gave up first
// would turn "the host is still reading" into a truncated stream.
const workspaceStreamBudget = 45 * time.Minute

// streamSender is the one method this needs from the live connection: one
// chunk of one stream. It is the same writer every control frame goes through,
// so a chunk cannot interleave with the end marker that follows it.
type streamSender interface {
	SendStream(id uint64, chunk []byte) error
}

func streamWorkspace(ctx context.Context, sender streamSender, nonce uint64, fsys fs.FS) (wstream.Report, error) {
	w := &chunkWriter{
		send: func(chunk []byte) error { return sender.SendStream(nonce, chunk) },
		buf:  make([]byte, 0, streamChunkBytes),
	}
	rep, err := wstream.Write(ctx, fsys, w, wstream.Limits{Exclude: checkpoint.DefaultExclusions()})
	if err != nil {
		return rep, err
	}
	if err := w.Flush(); err != nil {
		return rep, err
	}
	// The counts the end marker carries are the counts of what actually
	// reached the writer, so the host's "did I receive what you sent" check is
	// against bytes this end put on the wire rather than bytes it walked.
	rep.Bytes = w.sent
	return rep, nil
}

// chunkWriter turns the stream wstream.Write produces into FrameStream
// payloads of a bounded size.
//
// It buffers exactly one chunk and never more: the whole point of this design
// is that a workspace never exists in memory on either end, and a writer that
// grew its buffer to whatever it was handed would put the guest's side of that
// promise in the hands of the copy buffer above it.
type chunkWriter struct {
	send func([]byte) error
	buf  []byte
	sent int64
}

func (c *chunkWriter) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		n := min(cap(c.buf)-len(c.buf), len(p))
		c.buf = append(c.buf, p[:n]...)
		p = p[n:]
		if len(c.buf) == cap(c.buf) {
			if err := c.Flush(); err != nil {
				return total - len(p), err
			}
		}
	}
	return total, nil
}

// Flush sends whatever is pending. An empty buffer sends nothing: a
// zero-length frame would be indistinguishable from one that was lost.
func (c *chunkWriter) Flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	if err := c.send(c.buf); err != nil {
		return fmt.Errorf("wstream: the workspace chunk could not be sent to the runner: %w", errSendChunk)
	}
	c.sent += int64(len(c.buf))
	c.buf = c.buf[:0]
	return nil
}

// errSendChunk is what a failed send becomes, and the transport's own error is
// deliberately dropped rather than wrapped: a net error quotes the socket it
// failed on, which on a microVM host is a path inside the VM's jail, and this
// sentence travels to a session's error column. There is nothing in the
// original a person could act on that "the runner is not taking bytes" does not
// already say.
var errSendChunk = errors.New("the runner is not taking the workspace stream")

// workspaceEnd is the end marker: the counts on success, the stage on failure,
// and never a path in either.
//
// A failure is reported in the `stage_failed` shape the boot chain already
// uses — a stage, an rc of -1, and a tail — because it is the same kind of
// fact: a named step of this session's lifecycle did not finish, and the
// sentence is the only thing that reaches whoever has to act on it.
func workspaceEnd(nonce uint64, rep wstream.Report, err error) relay.ControlEvent {
	ev := relay.ControlEvent{
		Kind:    relay.KindWorkspaceEnd,
		ID:      nonce,
		Entries: rep.Entries,
		Bytes:   rep.Bytes,
	}
	if err == nil {
		ev.OK = true
		return ev
	}
	ev.Stage = workspaceStreamStage
	ev.RC = -1
	ev.Tail = streamFailureTail(err)
	return ev
}

// workspaceStreamStage is the stage name a failed workspace stream reports. It
// is not one of the boot chain's stages — this one happens at the end of a
// session's life rather than the start — and it is spelled here, beside the
// event that carries it.
const workspaceStreamStage = "workspace_stream"

// streamFailureTail is the sentence that travels with a failure.
//
// Only this package's and wstream's own errors are forwarded: both are written
// to carry no path, no file name and no symlink target (wstream's package
// comment). Anything else is reduced to a flat sentence, because an error from
// a file system, a transport or the standard library is not known to be
// path-free and this text reaches an operator's log.
func streamFailureTail(err error) string {
	switch {
	case errors.Is(err, wstream.ErrLimit),
		errors.Is(err, wstream.ErrEntry),
		errors.Is(err, wstream.ErrSource),
		errors.Is(err, wstream.ErrFormat),
		errors.Is(err, errSendChunk):
		return err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Sprintf("the workspace was still being streamed after %s", workspaceStreamBudget)
	default:
		return "the workspace could not be streamed to the runner"
	}
}

// logWorkspaceEnd puts the outcome in the session's own log, which is the one
// place it survives the VM.
func logWorkspaceEnd(rep wstream.Report, err error) {
	if err != nil {
		log.Printf("the workspace stream failed after %d entries and %d bytes: %v", rep.Entries, rep.Bytes, err)
		return
	}
	log.Printf("streamed the workspace to the runner: %d entries, %d bytes, %d entries skipped as neither file, directory nor symlink",
		rep.Entries, rep.Bytes, rep.Skipped)
}

var _ io.Writer = (*chunkWriter)(nil)
