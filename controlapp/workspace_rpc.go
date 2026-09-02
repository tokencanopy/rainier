package controlapp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// sessionRPC asks the sandbox running row to perform method and waits for its
// answer, decoding a successful one into out (pass nil to ignore the body).
// It is the one downward request/response path every workspace operation
// shares: a ToRunner "session_rpc" carrying a runner.RPCEnvelope, answered by
// a FromRunner "session_req" whose envelope Method is "resp" and whose ID
// echoes the request.
//
// The sandbox is an untrusted peer, so every answer is validated before it is
// trusted: wrong session, wrong RPC ID, wrong method, or a malformed body all
// become control.ErrUnavailable, and a false verdict becomes ErrRunnerRefused
// (which wraps it) — none of them relaying any sandbox payload text.
func (s *AttachmentService) sessionRPC(ctx context.Context, row control.Session, method string, payload any, out any) error {
	id := s.rpcSeq.Add(1)
	if id == 0 {
		return control.ErrUnavailable
	}
	raw, err := rpcPayload(payload)
	if err != nil {
		return control.ErrInvalid
	}
	msg := runner.ToRunner{Type: "session_rpc", Session: string(row.ID),
		RPC: &runner.RPCEnvelope{ID: id, Method: method, Payload: raw}}
	res, err := s.transport.Dispatch(ctx, row.PoolID, row.RunnerID, msg)
	if err != nil {
		return portError(err)
	}
	if res.Type != "session_req" || res.Session != string(row.ID) || res.RPC == nil ||
		res.RPC.ID != id || res.RPC.Method != "resp" {
		return control.ErrUnavailable
	}
	if !res.RPC.OK {
		// The sandbox received the request, understood it, and declined —
		// the same fact about the far end that ErrRunnerRefused already
		// names for a lifecycle command, and a different one from "nobody
		// answered". It wraps ErrUnavailable, so a caller that knows only the
		// closed sentinel set is unaffected; a host that renders "understood
		// and declined" differently from "unreachable" can ask. The
		// sandbox's own words stay inside the transport either way.
		return ErrRunnerRefused
	}
	return decodeRPCAnswer(res.RPC.Payload, out)
}

// rpcPayload encodes a request body. A nil payload (and anything that encodes
// to JSON null) travels as no payload at all; a caller-supplied
// json.RawMessage is validated as JSON before it is forwarded.
func rpcPayload(v any) (json.RawMessage, error) {
	switch p := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(p) == 0 {
			return nil, nil
		}
		if !json.Valid(p) {
			return nil, control.ErrInvalid
		}
		return p, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if string(b) == "null" {
		return nil, nil
	}
	return b, nil
}

// decodeRPCAnswer turns one successful response body into out, rejecting an
// absent body (unless out is nil), malformed JSON, and trailing JSON.
func decodeRPCAnswer(payload json.RawMessage, out any) error {
	if out == nil {
		return nil
	}
	if len(payload) == 0 {
		return control.ErrUnavailable
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	if err := dec.Decode(out); err != nil {
		return control.ErrUnavailable
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return control.ErrUnavailable
	}
	return nil
}

// callerIOError normalizes a failure from the caller-supplied archive reader
// or writer to the closed sentinel vocabulary. Context cancellation and
// deadline propagation are preserved (the caller went away), and a permitted
// short write that made no progress stays io.ErrShortWrite; every other caller
// I/O failure is control.ErrInvalid so no filesystem path or free-form text
// from the caller's own transport reaches the answer.
func callerIOError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrShortWrite) {
		return err
	}
	return control.ErrInvalid
}

// newTransferID mints a 128-bit lowercase-hex transfer id from crypto/rand. It
// is an opaque correlation token, never a filename.
func newTransferID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// maxDiffRepos bounds how many repositories one diff answer may describe; a
// session's repository list is resolved at create and is small, so a sandbox
// answering about more is answering about a session it was not asked about.
const maxDiffRepos = 64

// maxDiffLabel bounds the three short diff fields — a repository slug and two
// branch names. They come from the session's own row by way of the sandbox,
// but "by way of the sandbox" is the part that matters: nothing on a rendered
// answer is trusted to be the length it should be.
const maxDiffLabel = 256

// WorkspaceDiff asks a running sandbox for its per-repository diff and bounds
// the untrusted answer before returning it.
func (s *AttachmentService) WorkspaceDiff(ctx context.Context, scope control.Scope, cmd control.WorkspaceDiff) (workspace.DiffAnswer, error) {
	row, err := s.authorizedSession(ctx, scope, cmd.SessionID, control.ActionDiff)
	if err != nil {
		return workspace.DiffAnswer{}, err
	}
	if row.State != control.StateRunning {
		return workspace.DiffAnswer{}, control.ErrConflict
	}
	var ans workspace.DiffAnswer
	if err := s.sessionRPC(ctx, row, workspace.MethodDiff, nil, &ans); err != nil {
		return workspace.DiffAnswer{}, err
	}
	return boundDiff(ans), nil
}

// boundDiff cuts an answer down to what this API is willing to relay, copying
// before clipping so the decoded transport object cannot alias the returned
// value.
func boundDiff(ans workspace.DiffAnswer) workspace.DiffAnswer {
	if len(ans.Repos) > maxDiffRepos {
		ans.Repos = ans.Repos[:maxDiffRepos]
	}
	out := workspace.DiffAnswer{Repos: make([]workspace.RepoDiff, 0, len(ans.Repos))}
	for _, r := range ans.Repos {
		out.Repos = append(out.Repos, workspace.RepoDiff{
			Repo:          clipTo(r.Repo, maxDiffLabel),
			BaseBranch:    clipTo(r.BaseBranch, maxDiffLabel),
			SessionBranch: clipTo(r.SessionBranch, maxDiffLabel),
			Stat:          clipTo(r.Stat, workspace.StatBytes),
		})
	}
	return out
}

// clipTo truncates s to max bytes, keeping the result valid UTF-8 — the cut
// lands mid-rune as often as not, and every one of these strings is about to
// be JSON-encoded into somebody's terminal.
func clipTo(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "")
}

// PushWorkspace streams the gzipped tar archive at Body into Path inside the
// session's workspace, one bounded chunk per RPC, without buffering the whole
// archive. The total compressed bytes (against the injected transfer bound)
// and every chunk are bounded before any byte crosses the runner seam.
func (s *AttachmentService) PushWorkspace(ctx context.Context, scope control.Scope, cmd control.PushWorkspace) error {
	row, err := s.authorizedSession(ctx, scope, cmd.SessionID, control.ActionPush)
	if err != nil {
		return err
	}
	if cmd.Body == nil {
		return control.ErrInvalid
	}
	if err := workspace.ValidatePath(cmd.Path); err != nil {
		return control.ErrInvalid
	}
	if row.State != control.StateRunning {
		return control.ErrConflict
	}
	xfer, err := newTransferID()
	if err != nil {
		return control.ErrUnavailable
	}
	eventID, err := s.newEvent()
	if err != nil {
		return err
	}

	r := bufio.NewReader(cmd.Body)
	var total int64
	for seq := 0; ; seq++ {
		data := make([]byte, workspace.ChunkBytes)
		n, rerr := io.ReadFull(r, data)
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			return callerIOError(rerr)
		}
		data = data[:n]
		done := rerr != nil
		if rerr == nil {
			// A full chunk read: peek one byte to learn whether this is the
			// last chunk without ever sending a spurious empty one.
			if _, perr := r.Peek(1); perr == io.EOF {
				done = true
			} else if perr != nil {
				return callerIOError(perr)
			}
		}
		total += int64(len(data))
		if total > s.maxTransfer {
			return control.ErrInvalid
		}
		chunk := workspace.PushChunk{Xfer: xfer, Path: cmd.Path,
			Seq: seq, Data: slices.Clone(data), Done: done}
		var ack workspace.PushAck
		if err := s.sessionRPC(ctx, row, workspace.MethodPushFiles, chunk, &ack); err != nil {
			return err
		}
		if ack.Seq != chunk.Seq {
			return control.ErrUnavailable
		}
		if done {
			if !ack.Synced {
				return control.ErrUnavailable
			}
			break
		}
	}
	if err := s.record(ctx, eventID, scope, control.ActionPush, control.Resource{
		Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID,
	}); err != nil {
		return err
	}
	return nil
}

// PullWorkspace streams the gzipped tar archive of Path out of the session's
// workspace into Body, one bounded chunk per RPC, counting the compressed
// bytes as they arrive and refusing to write past workspace.MaxBytes.
func (s *AttachmentService) PullWorkspace(ctx context.Context, scope control.Scope, cmd control.PullWorkspace) error {
	row, err := s.authorizedSession(ctx, scope, cmd.SessionID, control.ActionPull)
	if err != nil {
		return err
	}
	if cmd.Body == nil {
		return control.ErrInvalid
	}
	if err := workspace.ValidatePath(cmd.Path); err != nil {
		return control.ErrInvalid
	}
	if row.State != control.StateRunning {
		return control.ErrConflict
	}
	xfer, err := newTransferID()
	if err != nil {
		return control.ErrUnavailable
	}
	eventID, err := s.newEvent()
	if err != nil {
		return err
	}

	var total int64
	for seq := 0; ; seq++ {
		req := workspace.PullRequest{Xfer: xfer, Path: cmd.Path, Seq: seq}
		var chunk workspace.PullChunk
		if err := s.sessionRPC(ctx, row, workspace.MethodPullFiles, req, &chunk); err != nil {
			return err
		}
		if chunk.Seq != req.Seq {
			return control.ErrUnavailable
		}
		if len(chunk.Data) > workspace.ChunkBytes {
			return control.ErrInvalid
		}
		if len(chunk.Data) == 0 && !chunk.Done {
			return control.ErrUnavailable
		}
		if total+int64(len(chunk.Data)) > s.maxTransfer {
			return control.ErrInvalid
		}
		if err := writeAll(cmd.Body, chunk.Data); err != nil {
			return callerIOError(err)
		}
		total += int64(len(chunk.Data))
		if chunk.Done {
			break
		}
	}
	if err := s.record(ctx, eventID, scope, control.ActionPull, control.Resource{
		Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID,
	}); err != nil {
		return err
	}
	return nil
}

// writeAll writes data to w, looping around a permitted short write so it
// makes progress; a zero-byte write with a nil error becomes io.ErrShortWrite.
// A writer violating the io.Writer count contract (n < 0 or n > len(p)) is
// rejected as control.ErrInvalid before the slice so it cannot panic.
func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return control.ErrInvalid
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
