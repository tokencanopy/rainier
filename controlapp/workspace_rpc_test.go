package controlapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// rpcProbe is a synthetic decode target. Its fields are the only thing tests
// may assert; never decode workspace content into it.
type rpcProbe struct {
	N int `json:"n"`
}

func rpcOKReply(id uint64, payload any) runner.FromRunner {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			panic(err)
		}
		raw = b
	}
	return runner.FromRunner{
		Type:    "session_req",
		Session: "sess_example",
		RPC:     &runner.RPCEnvelope{ID: id, Method: "resp", OK: true, Payload: raw},
	}
}

func TestSessionRPCSuccess(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, rpcProbe{N: 42})
	}
	var out rpcProbe
	if err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &out); err != nil {
		t.Fatalf("sessionRPC: %v", err)
	}
	if out.N != 42 {
		t.Fatalf("decoded N = %d, want 42", out.N)
	}
	got := fx.transport.dispatched()
	if len(got) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(got))
	}
	m := got[0]
	if m.Type != "session_rpc" || m.Session != "sess_example" || m.ReqID != 0 ||
		m.RPC == nil || m.RPC.ID == 0 || m.RPC.Method != workspace.MethodDiff {
		t.Fatalf("request = %+v", m)
	}
}

func TestSessionRPCHostileResponses(t *testing.T) {
	tests := []struct {
		name  string
		reply runner.FromRunner
	}{
		{"false ok", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: false, Payload: json.RawMessage(`{"error":"synthetic"}`)}}},
		{"wrong type", runner.FromRunner{Type: "result", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true}}},
		{"wrong session", runner.FromRunner{Type: "session_req", Session: "other_session",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true}}},
		{"missing rpc", runner.FromRunner{Type: "session_req", Session: "sess_example"}},
		{"wrong envelope id", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 99, Method: "resp", OK: true}}},
		{"non-resp method", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: workspace.MethodDiff, OK: true}}},
		{"malformed json", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true, Payload: json.RawMessage("{not json")}}},
		{"empty successful payload", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true}}},
		{"trailing json", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true, Payload: json.RawMessage(`{"n":1} {"n":2}`)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newAttachmentFixture(t)
			fx.transport.replies = []runner.FromRunner{tt.reply}
			var out rpcProbe
			err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &out)
			if !errors.Is(err, control.ErrUnavailable) {
				t.Fatalf("got %v, want ErrUnavailable", err)
			}
		})
	}
}

func TestSessionRPCNilOutAcceptsAbsentPayload(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replies = []runner.FromRunner{runner.FromRunner{
		Type: "session_req", Session: "sess_example",
		RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true},
	}}
	if err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, nil); err != nil {
		t.Fatalf("nil out with absent payload: %v", err)
	}
}

func TestSessionRPCDisconnectedRunner(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.dispatchErr = control.ErrUnavailable
	err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &rpcProbe{})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestSessionRPCContextCancellation(t *testing.T) {
	fx := newAttachmentFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fx.svc.sessionRPC(ctx, runningSession(), workspace.MethodDiff, nil, &rpcProbe{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestSessionRPCInvalidRawMessage(t *testing.T) {
	fx := newAttachmentFixture(t)
	err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff,
		json.RawMessage("{not json"), &rpcProbe{})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

func TestWorkspaceDiffBounds(t *testing.T) {
	fx := newAttachmentFixture(t)
	repos := make([]workspace.RepoDiff, 65)
	for i := range repos {
		// 3-byte runes so the 256-byte clip lands mid-rune and must stay valid.
		repos[i] = workspace.RepoDiff{
			Repo:          strings.Repeat("界", 300),
			BaseBranch:    strings.Repeat("界", 300),
			SessionBranch: strings.Repeat("界", 300),
			Stat:          strings.Repeat("界", workspace.StatBytes+1),
		}
	}
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, workspace.DiffAnswer{Repos: repos})
	}
	ans, err := fx.svc.WorkspaceDiff(context.Background(), testScope(), control.WorkspaceDiff{SessionID: "sess_example"})
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(ans.Repos) != 64 {
		t.Fatalf("got %d repos, want 64", len(ans.Repos))
	}
	for i, r := range ans.Repos {
		if len(r.Repo) > 256 || !utf8.ValidString(r.Repo) {
			t.Fatalf("repo %d: len=%d valid=%v", i, len(r.Repo), utf8.ValidString(r.Repo))
		}
		if len(r.BaseBranch) > 256 || !utf8.ValidString(r.BaseBranch) {
			t.Fatalf("base %d: len=%d valid=%v", i, len(r.BaseBranch), utf8.ValidString(r.BaseBranch))
		}
		if len(r.SessionBranch) > 256 || !utf8.ValidString(r.SessionBranch) {
			t.Fatalf("session %d: len=%d valid=%v", i, len(r.SessionBranch), utf8.ValidString(r.SessionBranch))
		}
		if len(r.Stat) > workspace.StatBytes || !utf8.ValidString(r.Stat) {
			t.Fatalf("stat %d: len=%d valid=%v", i, len(r.Stat), utf8.ValidString(r.Stat))
		}
	}
}

func TestWorkspaceDiffEmptySerializesAsArray(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, workspace.DiffAnswer{})
	}
	ans, err := fx.svc.WorkspaceDiff(context.Background(), testScope(), control.WorkspaceDiff{SessionID: "sess_example"})
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	b, err := json.Marshal(ans)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"repos":[]}` {
		t.Fatalf("empty diff = %s, want {\"repos\":[]}", b)
	}
}

func TestWorkspaceDiffAuthorizesActionDiff(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, workspace.DiffAnswer{})
	}
	if _, err := fx.svc.WorkspaceDiff(context.Background(), testScope(), control.WorkspaceDiff{SessionID: "sess_example"}); err != nil {
		t.Fatalf("diff: %v", err)
	}
	if fx.auth.lastAction != control.ActionDiff {
		t.Fatalf("action = %q, want %q", fx.auth.lastAction, control.ActionDiff)
	}
}

func TestWorkspaceDiffDeniedBeforeRPC(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.auth.err = control.ErrDenied
	_, err := fx.svc.WorkspaceDiff(context.Background(), testScope(), control.WorkspaceDiff{SessionID: "sess_example"})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if len(fx.transport.dispatched()) != 0 {
		t.Fatal("denied diff reached the runner")
	}
}

func TestWorkspaceDiffRequiresRunning(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.row.State = control.StateSuspendedCold
	_, err := fx.svc.WorkspaceDiff(context.Background(), testScope(), control.WorkspaceDiff{SessionID: "sess_example"})
	if !errors.Is(err, control.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	if len(fx.transport.dispatched()) != 0 {
		t.Fatal("non-running diff reached the runner")
	}
}

// ---------------------------------------------------------------------------
// push
// ---------------------------------------------------------------------------

// pushChunkMeta decodes only the correlation fields of a pushed chunk so a
// hostile-answer test never has to carry the (possibly large) data payload
// through its own decode.
type pushChunkMeta struct {
	Seq  int  `json:"seq"`
	Done bool `json:"done"`
}

func defaultPushAck(m runner.ToRunner) runner.FromRunner {
	var meta pushChunkMeta
	_ = json.Unmarshal(m.RPC.Payload, &meta)
	return rpcOKReply(m.RPC.ID, workspace.PushAck{Seq: meta.Seq, Synced: meta.Done})
}

func dispatchedPushChunks(t *testing.T, fx *attachFixture) []workspace.PushChunk {
	t.Helper()
	var out []workspace.PushChunk
	for _, m := range fx.transport.dispatched() {
		if m.RPC == nil {
			t.Fatalf("dispatched message without RPC: %+v", m)
		}
		var c workspace.PushChunk
		if err := json.Unmarshal(m.RPC.Payload, &c); err != nil {
			t.Fatalf("decoding dispatched chunk: %v", err)
		}
		out = append(out, c)
	}
	return out
}

// repeatReader yields left copies of a 1MiB pattern without holding the whole
// MaxBytes-size stream in memory, and copies with memmove rather than a
// byte loop so the race detector does not instrument hundreds of millions of
// individual writes in the boundary tests.
type repeatReader struct {
	pattern []byte
	left    int64
}

func newRepeatReader(b byte, left int64) *repeatReader {
	return &repeatReader{pattern: bytes.Repeat([]byte{b}, 1<<20), left: left}
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.left {
		n = int(r.left)
	}
	for off := 0; off < n; {
		off += copy(p[off:n], r.pattern[:n-off])
	}
	r.left -= int64(n)
	return n, nil
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("synthetic reader failure") }

func newPushFixture(t *testing.T) *attachFixture {
	t.Helper()
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = defaultPushAck
	return fx
}

func push(t *testing.T, fx *attachFixture, body io.Reader, path string) ([]workspace.PushChunk, error) {
	t.Helper()
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: path, Body: body,
	})
	return dispatchedPushChunks(t, fx), err
}

func TestPushWorkspaceUnsafePath(t *testing.T) {
	fx := newPushFixture(t)
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "../etc", Body: strings.NewReader("x"),
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	if len(fx.transport.dispatched()) != 0 {
		t.Fatal("unsafe path reached the runner")
	}
}

func TestPushWorkspaceNilBody(t *testing.T) {
	fx := newPushFixture(t)
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst",
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestPushWorkspaceEmptyArchive(t *testing.T) {
	fx := newPushFixture(t)
	chunks, err := push(t, fx, strings.NewReader(""), "dst")
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1", len(chunks))
	}
	if chunks[0].Seq != 0 || !chunks[0].Done || len(chunks[0].Data) != 0 {
		t.Fatalf("chunk = seq=%d done=%v len=%d", chunks[0].Seq, chunks[0].Done, len(chunks[0].Data))
	}
}

func TestPushWorkspaceOneShortChunk(t *testing.T) {
	fx := newPushFixture(t)
	chunks, err := push(t, fx, strings.NewReader("hello"), "dst")
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(chunks) != 1 || !chunks[0].Done || string(chunks[0].Data) != "hello" {
		t.Fatalf("chunks = %d, done=%v", len(chunks), len(chunks) == 1 && chunks[0].Done)
	}
}

func TestPushWorkspaceExactChunk(t *testing.T) {
	fx := newPushFixture(t)
	body := bytes.NewReader(bytes.Repeat([]byte("a"), workspace.ChunkBytes))
	chunks, err := push(t, fx, body, "dst")
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(chunks) != 1 || !chunks[0].Done || len(chunks[0].Data) != workspace.ChunkBytes {
		t.Fatalf("chunks = %d, done=%v, len=%d", len(chunks), len(chunks) == 1 && chunks[0].Done, func() int {
			if len(chunks) == 1 {
				return len(chunks[0].Data)
			}
			return -1
		}())
	}
}

func TestPushWorkspaceMultipleChunks(t *testing.T) {
	fx := newPushFixture(t)
	total := workspace.ChunkBytes*2 + 17
	body := bytes.NewReader(bytes.Repeat([]byte("b"), total))
	chunks, err := push(t, fx, body, "dst")
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}
	if chunks[0].Done || chunks[1].Done || !chunks[2].Done {
		t.Fatalf("done flags = %v %v %v", chunks[0].Done, chunks[1].Done, chunks[2].Done)
	}
	if len(chunks[0].Data) != workspace.ChunkBytes || len(chunks[1].Data) != workspace.ChunkBytes || len(chunks[2].Data) != 17 {
		t.Fatalf("chunk lengths = %d %d %d", len(chunks[0].Data), len(chunks[1].Data), len(chunks[2].Data))
	}
}

// blindAck builds a correctly-sequenced, always-synced ack without decoding
// the chunk payload, so the 256MiB boundary tests never pay a per-chunk JSON
// parse. The dispatches arrive in order, so a running counter is the request
// sequence.
type blindAck struct{ n int }

func (b *blindAck) ack(m runner.ToRunner) runner.FromRunner {
	seq := b.n
	b.n++
	return rpcOKReply(m.RPC.ID, workspace.PushAck{Seq: seq, Synced: true})
}

func TestPushWorkspaceExactlyMaxBytes(t *testing.T) {
	fx := newPushFixture(t)
	acker := &blindAck{}
	fx.transport.replyFn = acker.ack
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: newRepeatReader('z', workspace.MaxBytes),
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	wantChunks := int(workspace.MaxBytes / workspace.ChunkBytes)
	if got := len(fx.transport.dispatched()); got != wantChunks {
		t.Fatalf("dispatched %d chunks, want %d", got, wantChunks)
	}
	last := fx.transport.dispatched()[wantChunks-1]
	var meta pushChunkMeta
	_ = json.Unmarshal(last.RPC.Payload, &meta)
	if !meta.Done {
		t.Fatal("final chunk not marked done")
	}
}

func TestPushWorkspaceOverMaxBytes(t *testing.T) {
	fx := newPushFixture(t)
	acker := &blindAck{}
	fx.transport.replyFn = acker.ack
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: newRepeatReader('z', workspace.MaxBytes+1),
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	// The one byte past the cap must not have been forwarded: exactly the cap's
	// worth of full chunks went out.
	wantChunks := int(workspace.MaxBytes / workspace.ChunkBytes)
	if got := len(fx.transport.dispatched()); got != wantChunks {
		t.Fatalf("dispatched %d chunks, want %d", got, wantChunks)
	}
}

func TestPushWorkspaceWrongAckSequence(t *testing.T) {
	fx := newPushFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var meta pushChunkMeta
		_ = json.Unmarshal(m.RPC.Payload, &meta)
		return rpcOKReply(m.RPC.ID, workspace.PushAck{Seq: meta.Seq + 1, Synced: true})
	}
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestPushWorkspaceUnsyncedFinalAck(t *testing.T) {
	fx := newPushFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var meta pushChunkMeta
		_ = json.Unmarshal(m.RPC.Payload, &meta)
		return rpcOKReply(m.RPC.ID, workspace.PushAck{Seq: meta.Seq, Synced: false})
	}
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestPushWorkspaceReaderError(t *testing.T) {
	fx := newPushFixture(t)
	err := fx.svc.PushWorkspace(context.Background(), testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: errorReader{},
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestPushWorkspaceContextCancellation(t *testing.T) {
	fx := newPushFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fx.svc.PushWorkspace(ctx, testScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestPushWorkspaceRecordsEventAfterFinalAck(t *testing.T) {
	fx := newPushFixture(t)
	if _, err := push(t, fx, strings.NewReader("hello"), "dst"); err != nil {
		t.Fatalf("push: %v", err)
	}
	evs := fx.events.snapshot()
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want 1", len(evs))
	}
	if evs[0].Action != control.ActionPush || evs[0].Resource.ID != "sess_example" {
		t.Fatalf("event = %+v", evs[0])
	}
}

func TestSessionRPCConcurrentOutOfOrder(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, rpcProbe{N: int(m.RPC.ID)})
	}
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out rpcProbe
			errs[i] = fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &out)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	got := fx.transport.dispatched()
	if len(got) != n {
		t.Fatalf("dispatched %d messages, want %d", len(got), n)
	}
	seen := map[uint64]bool{}
	for _, m := range got {
		if m.RPC == nil || m.RPC.ID == 0 {
			t.Fatalf("request missing id: %+v", m)
		}
		if seen[m.RPC.ID] {
			t.Fatalf("duplicate request id %d", m.RPC.ID)
		}
		seen[m.RPC.ID] = true
	}
}
