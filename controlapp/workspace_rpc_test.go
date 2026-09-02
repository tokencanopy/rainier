package controlapp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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
	if err := fx.svc.sessionRPC(context.Background(), attachmentRunningSession(), workspace.MethodDiff, nil, &out); err != nil {
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
			err := fx.svc.sessionRPC(context.Background(), attachmentRunningSession(), workspace.MethodDiff, nil, &out)
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
	if err := fx.svc.sessionRPC(context.Background(), attachmentRunningSession(), workspace.MethodDiff, nil, nil); err != nil {
		t.Fatalf("nil out with absent payload: %v", err)
	}
}

func TestSessionRPCDisconnectedRunner(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.dispatchErr = control.ErrUnavailable
	err := fx.svc.sessionRPC(context.Background(), attachmentRunningSession(), workspace.MethodDiff, nil, &rpcProbe{})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestSessionRPCContextCancellation(t *testing.T) {
	fx := newAttachmentFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fx.svc.sessionRPC(ctx, attachmentRunningSession(), workspace.MethodDiff, nil, &rpcProbe{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestSessionRPCInvalidRawMessage(t *testing.T) {
	fx := newAttachmentFixture(t)
	err := fx.svc.sessionRPC(context.Background(), attachmentRunningSession(), workspace.MethodDiff,
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
	ans, err := fx.svc.WorkspaceDiff(context.Background(), attachmentTestScope(), control.WorkspaceDiff{SessionID: "sess_example"})
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
	ans, err := fx.svc.WorkspaceDiff(context.Background(), attachmentTestScope(), control.WorkspaceDiff{SessionID: "sess_example"})
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
	if _, err := fx.svc.WorkspaceDiff(context.Background(), attachmentTestScope(), control.WorkspaceDiff{SessionID: "sess_example"}); err != nil {
		t.Fatalf("diff: %v", err)
	}
	if fx.auth.lastAction != control.ActionDiff {
		t.Fatalf("action = %q, want %q", fx.auth.lastAction, control.ActionDiff)
	}
}

func TestWorkspaceDiffDeniedBeforeRPC(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.auth.err = control.ErrDenied
	_, err := fx.svc.WorkspaceDiff(context.Background(), attachmentTestScope(), control.WorkspaceDiff{SessionID: "sess_example"})
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
	_, err := fx.svc.WorkspaceDiff(context.Background(), attachmentTestScope(), control.WorkspaceDiff{SessionID: "sess_example"})
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

func dispatchedPushChunks(t *testing.T, fx *attachmentFixture) []workspace.PushChunk {
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

func newPushFixture(t *testing.T) *attachmentFixture {
	t.Helper()
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = defaultPushAck
	return fx
}

func push(t *testing.T, fx *attachmentFixture, body io.Reader, path string) ([]workspace.PushChunk, error) {
	t.Helper()
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: path, Body: body,
	})
	return dispatchedPushChunks(t, fx), err
}

func TestPushWorkspaceUnsafePath(t *testing.T) {
	fx := newPushFixture(t)
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
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
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
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
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
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
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
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
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
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
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestPushWorkspaceReaderError(t *testing.T) {
	fx := newPushFixture(t)
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
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
	err := fx.svc.PushWorkspace(ctx, attachmentTestScope(), control.PushWorkspace{
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

func TestPushWorkspaceRecorderFailureReturnsUnavailable(t *testing.T) {
	fx := newPushFixture(t)
	fx.events.err = errors.New("synthetic recorder failure")
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	})
	if err != control.ErrUnavailable {
		t.Fatalf("got %v, want the closed ErrUnavailable sentinel", err)
	}
	if got := len(fx.transport.dispatched()); got != 1 {
		t.Fatalf("dispatched %d chunks, want 1", got)
	}
}

func TestPushWorkspaceEmptyEventIDPreventsRPC(t *testing.T) {
	fx := newPushFixture(t)
	fx.ids.eventID = ""
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if got := len(fx.transport.dispatched()); got != 0 {
		t.Fatalf("empty event ID push dispatched %d chunks, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// pull
// ---------------------------------------------------------------------------

type recordingWriter struct {
	mu       sync.Mutex
	bytes    int
	calls    int
	err      error
	shortMax int // 0 = full writes; >0 = at most this many bytes per call
	zeroOnce bool
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.err != nil {
		return 0, w.err
	}
	if w.zeroOnce {
		w.zeroOnce = false
		return 0, nil
	}
	n := len(p)
	if w.shortMax > 0 && n > w.shortMax {
		n = w.shortMax
	}
	w.bytes += n
	return n, nil
}

func (w *recordingWriter) total() (bytes, calls int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes, w.calls
}

// countWriter returns a fixed count regardless of p. Counts outside
// [0, len(p)] violate the io.Writer contract, and writeAll must reject them
// with a closed error instead of panicking while slicing.
type countWriter struct{ n int }

func (w countWriter) Write(p []byte) (int, error) { return w.n, nil }

// servePull serves the configured chunks by request sequence, ending the
// transfer once the list is exhausted.
func servePull(chunks []workspace.PullChunk) func(runner.ToRunner) runner.FromRunner {
	var mu sync.Mutex
	return func(m runner.ToRunner) runner.FromRunner {
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		mu.Lock()
		defer mu.Unlock()
		if req.Seq < len(chunks) {
			return rpcOKReply(m.RPC.ID, chunks[req.Seq])
		}
		return rpcOKReply(m.RPC.ID, workspace.PullChunk{Seq: req.Seq, Done: true})
	}
}

// pullBoundaryResponder serves full ChunkBytes chunks, ending after chunkCount
// (0 means never ends), building each response around one pre-encoded base64
// blob so the 256MiB boundary tests encode the payload only once.
func pullBoundaryResponder(chunkCount int) func(runner.ToRunner) runner.FromRunner {
	blob := bytes.Repeat([]byte("q"), workspace.ChunkBytes)
	enc := base64.StdEncoding.EncodeToString(blob)
	prefix := []byte(`{"seq":`)
	dataField := []byte(`,"data":"` + enc + `","done":`)
	doneTrue := []byte(`true}`)
	doneFalse := []byte(`false}`)
	return func(m runner.ToRunner) runner.FromRunner {
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		done := chunkCount > 0 && req.Seq == chunkCount-1
		payload := append([]byte(nil), prefix...)
		payload = strconv.AppendInt(payload, int64(req.Seq), 10)
		payload = append(payload, dataField...)
		if done {
			payload = append(payload, doneTrue...)
		} else {
			payload = append(payload, doneFalse...)
		}
		return runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: m.RPC.ID, Method: "resp", OK: true, Payload: json.RawMessage(payload)}}
	}
}

func pullTo(t *testing.T, fx *attachmentFixture, w io.Writer, path string) error {
	t.Helper()
	return fx.svc.PullWorkspace(context.Background(), attachmentTestScope(), control.PullWorkspace{
		SessionID: "sess_example", Path: path, Body: w,
	})
}

func TestPullWorkspaceUnsafePath(t *testing.T) {
	fx := newAttachmentFixture(t)
	err := fx.svc.PullWorkspace(context.Background(), attachmentTestScope(), control.PullWorkspace{
		SessionID: "sess_example", Path: "../etc", Body: &recordingWriter{},
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	if len(fx.transport.dispatched()) != 0 {
		t.Fatal("unsafe path reached the runner")
	}
}

func TestPullWorkspaceNilWriter(t *testing.T) {
	fx := newAttachmentFixture(t)
	err := fx.svc.PullWorkspace(context.Background(), attachmentTestScope(), control.PullWorkspace{
		SessionID: "sess_example", Path: "src",
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestPullWorkspaceEmptyFinalChunk(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Done: true}})
	w := &recordingWriter{}
	if err := pullTo(t, fx, w, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, calls := w.total(); got != 0 || calls != 0 {
		t.Fatalf("wrote %d bytes in %d calls, want 0/0", got, calls)
	}
	if got := len(fx.transport.dispatched()); got != 1 {
		t.Fatalf("dispatched %d requests, want 1", got)
	}
}

func TestPullWorkspaceOneChunk(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
	w := &recordingWriter{}
	if err := pullTo(t, fx, w, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, calls := w.total(); got != 5 || calls != 1 {
		t.Fatalf("wrote %d bytes in %d calls, want 5/1", got, calls)
	}
}

func TestPullWorkspaceMultipleChunks(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{
		{Seq: 0, Data: []byte("aaa"), Done: false},
		{Seq: 1, Data: []byte("bbbb"), Done: true},
	})
	w := &recordingWriter{}
	if err := pullTo(t, fx, w, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, _ := w.total(); got != 7 {
		t.Fatalf("wrote %d bytes, want 7", got)
	}
	if got := len(fx.transport.dispatched()); got != 2 {
		t.Fatalf("dispatched %d requests, want 2", got)
	}
}

func TestPullWorkspaceExactlyMaxBytes(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = pullBoundaryResponder(int(workspace.MaxBytes / workspace.ChunkBytes))
	w := &recordingWriter{}
	if err := pullTo(t, fx, w, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, _ := w.total(); got != int(workspace.MaxBytes) {
		t.Fatalf("wrote %d bytes, want %d", got, workspace.MaxBytes)
	}
	if got := len(fx.transport.dispatched()); got != int(workspace.MaxBytes/workspace.ChunkBytes) {
		t.Fatalf("dispatched %d requests, want %d", got, workspace.MaxBytes/workspace.ChunkBytes)
	}
}

func TestPullWorkspaceOverflow(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = pullBoundaryResponder(0) // never done
	w := &recordingWriter{}
	err := pullTo(t, fx, w, "src")
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	// The module refuses the chunk that would exceed the cap before writing it:
	// exactly the cap's worth of bytes landed, and one extra request was made.
	if got, _ := w.total(); got != int(workspace.MaxBytes) {
		t.Fatalf("wrote %d bytes, want %d", got, workspace.MaxBytes)
	}
	if got := len(fx.transport.dispatched()); got != int(workspace.MaxBytes/workspace.ChunkBytes)+1 {
		t.Fatalf("dispatched %d requests, want %d", got, workspace.MaxBytes/workspace.ChunkBytes+1)
	}
}

func TestPullWorkspaceWrongSequence(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		return rpcOKReply(m.RPC.ID, workspace.PullChunk{Seq: req.Seq + 1, Data: []byte("x"), Done: true})
	}
	err := pullTo(t, fx, &recordingWriter{}, "src")
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestPullWorkspaceOversizedChunk(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{
		{Seq: 0, Data: make([]byte, workspace.ChunkBytes+1), Done: true},
	})
	err := pullTo(t, fx, &recordingWriter{}, "src")
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestPullWorkspaceEmptyNonFinalChunk(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		return rpcOKReply(m.RPC.ID, workspace.PullChunk{Seq: req.Seq, Done: false})
	}
	err := pullTo(t, fx, &recordingWriter{}, "src")
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestPullWorkspaceWriterShortWrite(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
	w := &recordingWriter{shortMax: 1}
	if err := pullTo(t, fx, w, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, calls := w.total(); got != 5 || calls != 5 {
		t.Fatalf("wrote %d bytes in %d calls, want 5/5", got, calls)
	}
}

func TestPullWorkspaceWriterError(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
	w := &recordingWriter{err: errors.New("synthetic writer failure")}
	err := pullTo(t, fx, w, "src")
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestPullWorkspaceZeroWriteShortWrite(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
	w := &recordingWriter{zeroOnce: true}
	err := pullTo(t, fx, w, "src")
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("got %v, want io.ErrShortWrite", err)
	}
}

func TestWriteAllRejectsMalformedCounts(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{"negative", -1},
		{"oversized", 4}, // len("abc") == 3
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := writeAll(countWriter{n: tt.n}, []byte("abc"))
			if !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestWriteAllPartialWrites(t *testing.T) {
	w := &recordingWriter{shortMax: 1}
	if err := writeAll(w, []byte("hello")); err != nil {
		t.Fatalf("writeAll: %v", err)
	}
	if got, calls := w.total(); got != 5 || calls != 5 {
		t.Fatalf("wrote %d bytes in %d calls, want 5/5", got, calls)
	}
}

func TestPullWorkspaceMaliciousWriterCount(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{"negative", -1},
		{"oversized", 6}, // chunk is "hello" (5 bytes)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newAttachmentFixture(t)
			fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
			err := pullTo(t, fx, countWriter{n: tt.n}, "src")
			if !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestPullWorkspaceDuplicateFinalResponse(t *testing.T) {
	fx := newAttachmentFixture(t)
	// Always answer done: a second request would be a duplicate final chunk.
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		return rpcOKReply(m.RPC.ID, workspace.PullChunk{Seq: req.Seq, Data: []byte("x"), Done: true})
	}
	w := &recordingWriter{}
	if err := pullTo(t, fx, w, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got := len(fx.transport.dispatched()); got != 1 {
		t.Fatalf("dispatched %d requests, want 1 (no extra RPC after done)", got)
	}
	if got, _ := w.total(); got != 1 {
		t.Fatalf("wrote %d bytes, want 1", got)
	}
}

func TestPullWorkspaceContextCancellation(t *testing.T) {
	fx := newAttachmentFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fx.svc.PullWorkspace(ctx, attachmentTestScope(), control.PullWorkspace{
		SessionID: "sess_example", Path: "src", Body: &recordingWriter{},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestPullWorkspaceRecordsEvent(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("x"), Done: true}})
	if err := pullTo(t, fx, &recordingWriter{}, "src"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	evs := fx.events.snapshot()
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want 1", len(evs))
	}
	if evs[0].Action != control.ActionPull || evs[0].Resource.ID != "sess_example" {
		t.Fatalf("event = %+v", evs[0])
	}
}

func TestPullWorkspaceRecorderFailureReturnsUnavailable(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("x"), Done: true}})
	fx.events.err = errors.New("synthetic recorder failure")
	err := pullTo(t, fx, &recordingWriter{}, "src")
	if err != control.ErrUnavailable {
		t.Fatalf("got %v, want the closed ErrUnavailable sentinel", err)
	}
	if got := len(fx.transport.dispatched()); got != 1 {
		t.Fatalf("dispatched %d requests, want 1", got)
	}
}

func TestPullWorkspaceEmptyEventIDPreventsRPC(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("x"), Done: true}})
	fx.ids.eventID = ""
	err := pullTo(t, fx, &recordingWriter{}, "src")
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if got := len(fx.transport.dispatched()); got != 0 {
		t.Fatalf("empty event ID pull dispatched %d requests, want 0", got)
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
			errs[i] = fx.svc.sessionRPC(context.Background(), attachmentRunningSession(), workspace.MethodDiff, nil, &out)
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

// ---------------------------------------------------------------------------
// the injected transfer bound
// ---------------------------------------------------------------------------

// newBoundedAttachmentFixture is newAttachmentFixture with a host-supplied
// MaxTransferBytes. It is spelled here rather than as an option on the shared
// fixture so the bound's tests own their construction: every other test in
// this package means "the default bound", and none of them should have to say
// so.
func newBoundedAttachmentFixture(t *testing.T, bound int64) *attachmentFixture {
	t.Helper()
	fx := &attachmentFixture{
		auth:      &attachmentFakeAuthorizer{},
		policy:    &attachmentFakePolicy{},
		sessions:  &attachmentFakeSessions{found: true, row: attachmentRunningSession()},
		transport: &attachmentFakeTransport{},
		broker:    &attachmentFakeBroker{},
		events:    &attachmentFakeEvents{},
		ids:       &attachmentFakeIDs{eventID: "evt_example"},
	}
	svc, err := NewAttachmentService(AttachmentOptions{
		Authorizer:       fx.auth,
		Policy:           fx.policy,
		Sessions:         fx.sessions,
		Transport:        fx.transport,
		Broker:           fx.broker,
		Events:           fx.events,
		Clock:            attachmentFakeClock(func() time.Time { return time.Unix(0, 0) }),
		IDs:              fx.ids,
		MaxTransferBytes: bound,
	})
	if err != nil {
		t.Fatalf("NewAttachmentService: %v", err)
	}
	fx.svc = svc
	return fx
}

// TestPullRefusesBeyondInjectedBound proves the pull cap is the injected one
// rather than workspace.MaxBytes: with a 3KiB bound a sandbox answering 1KiB
// chunks forever is cut off at the fourth, before its bytes are written, and
// the refusal is the closed ErrInvalid the transfer-limit answer maps from.
func TestPullRefusesBeyondInjectedBound(t *testing.T) {
	const kib = 1 << 10
	fx := newBoundedAttachmentFixture(t, 3*kib)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		return rpcOKReply(m.RPC.ID, workspace.PullChunk{Seq: req.Seq, Data: bytes.Repeat([]byte("q"), kib)})
	}
	w := &recordingWriter{}
	if err := pullTo(t, fx, w, "src"); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	if got, _ := w.total(); got != 3*kib {
		t.Fatalf("wrote %d bytes, want exactly the %d-byte bound", got, 3*kib)
	}
	if got := len(fx.transport.dispatched()); got != 4 {
		t.Fatalf("dispatched %d requests, want 4 (the fourth is refused before its write)", got)
	}
}

// TestPushRefusesBeyondInjectedBound is the same bound on the other
// direction: a caller's archive larger than the injected limit is refused
// mid-stream rather than relayed.
func TestPushRefusesBeyondInjectedBound(t *testing.T) {
	fx := newBoundedAttachmentFixture(t, workspace.ChunkBytes)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		var c workspace.PushChunk
		_ = json.Unmarshal(m.RPC.Payload, &c)
		return rpcOKReply(m.RPC.ID, workspace.PushAck{Seq: c.Seq, Synced: c.Done})
	}
	body := bytes.NewReader(bytes.Repeat([]byte("q"), workspace.ChunkBytes+1))
	err := fx.svc.PushWorkspace(context.Background(), attachmentTestScope(), control.PushWorkspace{
		SessionID: "sess_example", Path: "src", Body: body,
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

// TestNewAttachmentServiceRejectsNegativeBound: zero means the public default,
// but a negative bound is a host that computed one wrong, and a service that
// silently relayed nothing (or everything) would hide it.
func TestNewAttachmentServiceRejectsNegativeBound(t *testing.T) {
	opts := AttachmentOptions{
		Authorizer:       &attachmentFakeAuthorizer{},
		Policy:           &attachmentFakePolicy{},
		Sessions:         &attachmentFakeSessions{},
		Transport:        &attachmentFakeTransport{},
		Broker:           &attachmentFakeBroker{},
		Events:           &attachmentFakeEvents{},
		Clock:            attachmentFakeClock(func() time.Time { return time.Unix(0, 0) }),
		IDs:              attachmentFakeIDs{eventID: "evt_example"},
		MaxTransferBytes: -1,
	}
	if _, err := NewAttachmentService(opts); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("negative bound: got %v, want ErrInvalid", err)
	}
	opts.MaxTransferBytes = 0
	svc, err := NewAttachmentService(opts)
	if err != nil {
		t.Fatalf("zero bound rejected: %v", err)
	}
	if svc.maxTransfer != workspace.MaxBytes {
		t.Fatalf("zero bound became %d, want the public default %d", svc.maxTransfer, workspace.MaxBytes)
	}
}
