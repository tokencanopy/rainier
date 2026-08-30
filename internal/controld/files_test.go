// internal/controld/files_test.go — the workspace-inspection routes in
// api.go: GET /v1/sessions/{id}/diff and the push/pull file transfer.
package controld

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"rainier/internal/rwire"
	"rainier/internal/xfer"
)

// ---------------------------------------------------------------------------
// the scripted sandbox
//
// fakeRunner is runnerd, which forwards a session RPC verbatim between its
// controld socket and the session's control channel — so a goroutine that
// answers the fake's session_rpc commands IS the sandbox on the far side of
// that forwarder (srpc_test.go makes the same argument).
// ---------------------------------------------------------------------------

// sandboxFunc answers one method. Returning an error makes an ok:false
// response carrying its text, which is the refusal path controld renders.
type sandboxFunc func(method string, payload json.RawMessage) (any, error)

// startSandbox answers every session_rpc the fake receives until the test
// ends. It drains the fake's command channel, so a test that also wants to
// assert on other commands should not use it.
func startSandbox(t *testing.T, f *fakeRunner, fn sandboxFunc) {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case cmd := <-f.cmds:
				if cmd.Type != "session_rpc" || cmd.RPC == nil {
					continue
				}
				out, err := fn(cmd.RPC.Method, cmd.RPC.Payload)
				if err != nil {
					body, _ := json.Marshal(struct {
						Error string `json:"error"`
					}{err.Error()})
					f.answerRPCRaw(cmd, false, body)
					continue
				}
				body, encErr := json.Marshal(out)
				if encErr != nil {
					return
				}
				f.answerRPCRaw(cmd, true, body)
			}
		}
	}()
}

// answerRPCRaw is answerRPC without the *testing.T: the sandbox goroutine
// outlives the test's own, and calling t.Fatalf from it would be a data race
// with the test finishing.
func (f *fakeRunner) answerRPCRaw(cmd rwire.ToRunner, ok bool, payload json.RawMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	m := rwire.FromRunner{Type: "session_req", Session: cmd.Session,
		RPC: &rwire.RPCEnvelope{ID: cmd.RPC.ID, Method: "resp", OK: ok, Payload: payload}}
	m.Used, m.Total = f.used, f.total
	// A write failure here means the test is over and the socket is gone;
	// there is nobody left to tell.
	_ = wsjson.Write(ctx, f.c, m)
}

// diffAnswerOf builds the answer a sandbox gives to `diff`.
func diffAnswerOf(repos ...xfer.RepoDiff) xfer.DiffAnswer { return xfer.DiffAnswer{Repos: repos} }

// fileStore is a sandbox that remembers what was pushed and serves it back on
// a pull — enough to prove the bytes survive the whole round trip.
type fileStore struct {
	mu     sync.Mutex
	blobs  map[string][]byte // destination path -> archive
	synced int
}

func newFileStore() *fileStore { return &fileStore{blobs: map[string][]byte{}} }

func (s *fileStore) serve(method string, payload json.RawMessage) (any, error) {
	switch method {
	case xfer.MethodPushFiles:
		var c xfer.PushChunk
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.blobs[c.Path] = append(s.blobs[c.Path], c.Data...)
		synced := c.Done || (len(s.blobs[c.Path])/xfer.ChunkBytes)%xfer.SyncEvery == 0
		if synced {
			s.synced++
		}
		return xfer.PushAck{Seq: c.Seq, Synced: synced}, nil
	case xfer.MethodPullFiles:
		var req xfer.PullRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		blob, ok := s.blobs[req.Path]
		if !ok {
			return nil, fmt.Errorf("%s does not exist in this session's workspace", req.Path)
		}
		off := req.Seq * xfer.ChunkBytes
		if off > len(blob) {
			return nil, fmt.Errorf("pull chunk %d is past the end of the archive", req.Seq)
		}
		end := min(off+xfer.ChunkBytes, len(blob))
		return xfer.PullChunk{Seq: req.Seq, Data: blob[off:end], Done: end >= len(blob)}, nil
	}
	return nil, fmt.Errorf("unknown method %q", method)
}

// transferSession seeds a running session on runner, owned by userID.
func transferSession(t *testing.T, st Store, id, runner, userID string) {
	t.Helper()
	seedSession(t, st, Session{ID: id, State: StateRunning, Runner: runner, OwnerID: userID})
}

// ---------------------------------------------------------------------------
// GET /v1/sessions/{id}/diff
// ---------------------------------------------------------------------------

// TestSessionDiff covers the four kinds this house tests every route with —
// the happy read, authZ, the response-shape pin, and the failure mapping.
func TestSessionDiff(t *testing.T) {
	t.Run("renders every repository the sandbox answered", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_diff_ok", "vm1", u.ID)

		var sawMethod string
		startSandbox(t, f, func(method string, _ json.RawMessage) (any, error) {
			sawMethod = method
			return diffAnswerOf(xfer.RepoDiff{
				Repo: "acme/widget", BaseBranch: "main", SessionBranch: "rainier/x",
				Stat: " main.go | 2 +-\n",
			}), nil
		})

		resp := doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_ok/diff", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
		}
		assertKeySet(t, raw, "repos")
		if sawMethod != xfer.MethodDiff {
			t.Fatalf("controld called %q, want %q", sawMethod, xfer.MethodDiff)
		}

		var body xfer.DiffAnswer
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body = %s", err, raw)
		}
		if len(body.Repos) != 1 {
			t.Fatalf("repos = %+v, want one", body.Repos)
		}
		if got := body.Repos[0]; got.Repo != "acme/widget" || got.BaseBranch != "main" ||
			got.SessionBranch != "rainier/x" || got.Stat != " main.go | 2 +-\n" {
			t.Fatalf("repo = %+v", got)
		}
	})

	t.Run("a session with no repositories answers an empty array", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_diff_empty", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) { return diffAnswerOf(), nil })

		resp := doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_empty/diff", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
		}
		if strings.TrimSpace(raw) != `{"repos":[]}` {
			t.Fatalf("body = %s, want an empty array (never null)", raw)
		}
	})

	t.Run("caps what a sandbox may put in one stat", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_diff_big", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) {
			return diffAnswerOf(xfer.RepoDiff{Repo: "a/b", Stat: strings.Repeat("x", 512<<10)}), nil
		})

		resp := doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_big/diff", tok, nil, nil)
		raw := readBody(t, resp)
		var body xfer.DiffAnswer
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if n := len(body.Repos[0].Stat); n > xfer.StatBytes {
			t.Fatalf("stat is %d bytes; controld must bound what a sandbox sends it, not only trust the cap inside", n)
		}
	})

	t.Run("passes the sandbox's own refusal through", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_diff_fail", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) {
			return nil, fmt.Errorf("acme/widget: fetching origin/main: fatal: couldn't find remote ref main")
		})

		resp := doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_fail/diff", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body = %s", resp.StatusCode, raw)
		}
		if !strings.Contains(raw, "couldn't find remote ref") {
			t.Fatalf("body = %s, want git's own words", raw)
		}
	})

	// TEAM-VISIBLE, deliberately: `--stat` is metadata — file paths and churn
	// counts, no content — and design §4.6 puts it on the read side of §4.4's
	// line, where GET /v1/sessions/{id} already sits. A teammate reading which
	// files another member's branch touched is the endpoint working, not a
	// leak: this fleet's posture already lets an admin attach to any session
	// and push as its owner.
	t.Run("a teammate may read another member's diff", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, _ := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_diff_teammate", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) {
			return diffAnswerOf(xfer.RepoDiff{
				Repo: "acme/widget", BaseBranch: "main", SessionBranch: "rainier/x", Stat: " main.go | 2 +-\n"}), nil
		})

		resp := doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_teammate/diff", otherTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 — the diff is a team-visible read; body = %s", resp.StatusCode, raw)
		}
		var body xfer.DiffAnswer
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body = %s", err, raw)
		}
		if len(body.Repos) != 1 || body.Repos[0].Repo != "acme/widget" {
			t.Fatalf("repos = %+v, want the owner's own answer rendered for a teammate", body.Repos)
		}
	})

	t.Run("readiness", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

		resp := doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_nope/diff", tok, nil, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown session status = %d, want 404", resp.StatusCode)
		}
		assertErrCode(t, resp, "not_found")

		seedSession(t, st, Session{ID: "sess_diff_queued", State: StateQueued, OwnerID: u.ID})
		resp = doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_queued/diff", tok, nil, nil)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("queued session status = %d, want 503", resp.StatusCode)
		}
		assertErrCode(t, resp, "session_not_ready")

		transferSession(t, st, "sess_diff_gone", "vm-gone", u.ID)
		resp = doJSON(t, ts, http.MethodGet, "/v1/sessions/sess_diff_gone/diff", tok, nil, nil)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("disconnected runner status = %d, want 502", resp.StatusCode)
		}
		assertErrCode(t, resp, "runner_unreachable")
	})
}

// ---------------------------------------------------------------------------
// POST/GET /v1/sessions/{id}/files
// ---------------------------------------------------------------------------

func TestPushFiles(t *testing.T) {
	t.Run("forwards one chunk and returns the sandbox's ack", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_push_ok", "vm1", u.ID)

		var got xfer.PushChunk
		startSandbox(t, f, func(method string, payload json.RawMessage) (any, error) {
			if method != xfer.MethodPushFiles {
				return nil, fmt.Errorf("unexpected method %q", method)
			}
			if err := json.Unmarshal(payload, &got); err != nil {
				return nil, err
			}
			return xfer.PushAck{Seq: got.Seq, Synced: true}, nil
		})

		body := xfer.PushChunk{Xfer: "t1", Path: "widget/vendor", Seq: 0, Data: []byte("hello"), Done: true}
		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_ok/files", tok, body, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
		}
		assertKeySet(t, raw, "seq", "synced")

		var ack xfer.PushAck
		if err := json.Unmarshal([]byte(raw), &ack); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if ack.Seq != 0 || !ack.Synced {
			t.Fatalf("ack = %+v", ack)
		}
		if string(got.Data) != "hello" || got.Path != "widget/vendor" || got.Xfer != "t1" || !got.Done {
			t.Fatalf("the sandbox received %+v", got)
		}
	})

	t.Run("refuses a chunk controld can see is wrong before any sandbox does", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_push_bad", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) {
			return nil, fmt.Errorf("the sandbox should never have been asked")
		})

		cases := []struct {
			name string
			body xfer.PushChunk
		}{
			{"escaping path", xfer.PushChunk{Xfer: "t", Path: "../etc", Seq: 0, Data: []byte("x")}},
			{"absolute path outside the workspace", xfer.PushChunk{Xfer: "t", Path: "/etc/cron.d", Data: []byte("x")}},
			{"no path", xfer.PushChunk{Xfer: "t", Seq: 0, Data: []byte("x")}},
			{"no transfer id", xfer.PushChunk{Path: "dst", Seq: 0, Data: []byte("x")}},
			{"negative sequence", xfer.PushChunk{Xfer: "t", Path: "dst", Seq: -1, Data: []byte("x")}},
			{"oversize chunk", xfer.PushChunk{Xfer: "t", Path: "dst", Data: make([]byte, xfer.ChunkBytes+1)}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp := doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_bad/files", tok, tc.body, nil)
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400; body = %s", resp.StatusCode, readBody(t, resp))
				}
				assertErrCode(t, resp, "invalid_request")
			})
		}
	})

	t.Run("passes the sandbox's own refusal through", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_push_refused", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) {
			return nil, fmt.Errorf("push chunk 3 arrived out of order; expected 1")
		})

		body := xfer.PushChunk{Xfer: "t", Path: "dst", Seq: 3, Data: []byte("x")}
		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_refused/files", tok, body, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body = %s", resp.StatusCode, raw)
		}
		if !strings.Contains(raw, "out of order") {
			t.Fatalf("body = %s, want the sandbox's own sentence", raw)
		}
	})

	t.Run("authorization and readiness", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		body := xfer.PushChunk{Xfer: "t", Path: "dst", Data: []byte("x")}

		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_nope/files", tok, body, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown session status = %d, want 404", resp.StatusCode)
		}

		// OWNER-OR-ADMIN, unlike the diff above: a push writes into somebody
		// else's working tree, which is the attach side of §4.4's line.
		transferSession(t, st, "sess_push_authz", "vm1", u.ID)
		resp = doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_authz/files", otherTok, body, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("pushing into another member's session status = %d, want 403", resp.StatusCode)
		}
		assertErrCode(t, resp, "forbidden")

		// An admin may, which is the same trust-your-team rule attach takes.
		_, adminTok := loginUser(t, st, "root", "admin")
		resp = doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_authz/files", adminTok, body, nil)
		if resp.StatusCode == http.StatusForbidden {
			t.Fatal("an admin was refused a push; admins reach every session")
		}

		seedSession(t, st, Session{ID: "sess_push_queued", State: StateQueued, OwnerID: u.ID})
		resp = doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_queued/files", tok, body, nil)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("queued session status = %d, want 503", resp.StatusCode)
		}
		assertErrCode(t, resp, "session_not_ready")

		transferSession(t, st, "sess_push_gone", "vm-gone", u.ID)
		resp = doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_push_gone/files", tok, body, nil)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("disconnected runner status = %d, want 502", resp.StatusCode)
		}
	})
}

func TestPullFiles(t *testing.T) {
	t.Run("streams the sandbox's archive as the response body", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_pull_ok", "vm1", u.ID)

		store := newFileStore()
		store.blobs["widget/out"] = []byte("an archive, more or less")
		startSandbox(t, f, store.serve)

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_ok/files?path=widget/out", tok, nil, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, readBody(t, resp))
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
			t.Fatalf("Content-Type = %q, want application/gzip", ct)
		}
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if string(got) != "an archive, more or less" {
			t.Fatalf("body = %q", got)
		}
	})

	t.Run("refuses a path controld can see is wrong", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_pull_bad", "vm1", u.ID)
		startSandbox(t, f, func(string, json.RawMessage) (any, error) {
			return nil, fmt.Errorf("the sandbox should never have been asked")
		})

		for _, q := range []string{"", "?path=", "?path=../etc", "?path=/etc/passwd", "?path=a/../../b"} {
			resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_bad/files"+q, tok, nil, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("path %q status = %d, want 400", q, resp.StatusCode)
			}
			assertErrCode(t, resp, "invalid_request")
		}
	})

	t.Run("a sandbox refusal before the first byte is a JSON error", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_pull_missing", "vm1", u.ID)
		startSandbox(t, f, newFileStore().serve)

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_missing/files?path=nope", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body = %s", resp.StatusCode, raw)
		}
		if !strings.Contains(raw, "does not exist") {
			t.Fatalf("body = %s, want the sandbox's own sentence", raw)
		}
	})

	t.Run("stops a sandbox that would stream forever", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_pull_flood", "vm1", u.ID)
		// The cap this replica will accept from a sandbox, lowered so the test
		// does not have to move a quarter of a gigabyte to reach it.
		s.xferMax = 4 << 20

		startSandbox(t, f, func(method string, payload json.RawMessage) (any, error) {
			var req xfer.PullRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			// Never done: a compromised sandbox answering an endless stream.
			return xfer.PullChunk{Seq: req.Seq, Data: make([]byte, xfer.ChunkBytes)}, nil
		})

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_flood/files?path=d", tok, nil, nil)
		defer resp.Body.Close()
		got, _ := io.ReadAll(resp.Body)
		if int64(len(got)) > s.xferMax {
			t.Fatalf("controld relayed %d bytes; the cap is %d — a sandbox must not be able to stream without end",
				len(got), s.xferMax)
		}
	})

	// A sandbox answering EMPTY chunks that never say done would spin this
	// loop forever without ever reaching the byte cap — the cap counts bytes,
	// and there are none. The request has to end.
	t.Run("stops a sandbox answering empty chunks", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		transferSession(t, st, "sess_pull_empty", "vm1", u.ID)
		s.xferMax = 4 << 20

		var asked atomic.Int64
		startSandbox(t, f, func(method string, payload json.RawMessage) (any, error) {
			var req xfer.PullRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			asked.Add(1)
			return xfer.PullChunk{Seq: req.Seq}, nil // no data, never done
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_empty/files?path=d", tok, nil, nil)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("the pull never ended after %d empty chunks", asked.Load())
		}
	})

	t.Run("authorization and readiness", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_nope/files?path=d", tok, nil, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown session status = %d, want 404", resp.StatusCode)
		}

		// OWNER-OR-ADMIN, unlike the diff: a pull carries the working tree's
		// raw bytes out, not the metadata `--stat` reports.
		transferSession(t, st, "sess_pull_authz", "vm1", u.ID)
		resp = doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_authz/files?path=d", otherTok, nil, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("pulling from another member's session status = %d, want 403", resp.StatusCode)
		}
		assertErrCode(t, resp, "forbidden")

		seedSession(t, st, Session{ID: "sess_pull_queued", State: StateQueued, OwnerID: u.ID})
		resp = doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_queued/files?path=d", tok, nil, nil)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("queued session status = %d, want 503", resp.StatusCode)
		}

		transferSession(t, st, "sess_pull_gone", "vm-gone", u.ID)
		resp = doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_pull_gone/files?path=d", tok, nil, nil)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("disconnected runner status = %d, want 502", resp.StatusCode)
		}
	})
}

// TestTransferRoundTrip is the whole bridge in one test: three megabytes of
// random bytes pushed chunk by chunk through the REST surface into a scripted
// sandbox, then pulled back out of it, byte for byte.
func TestTransferRoundTrip(t *testing.T) {
	s, st, ts := newTestControld(t)
	u, tok := loginUser(t, st, "alice", "member")
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	transferSession(t, st, "sess_round", "vm1", u.ID)

	store := newFileStore()
	startSandbox(t, f, store.serve)

	blob := make([]byte, 3<<20)
	if _, err := rand.Read(blob); err != nil {
		t.Fatalf("rand: %v", err)
	}

	const dest = "widget/vendor"
	for seq, off := 0, 0; off < len(blob); seq, off = seq+1, off+xfer.ChunkBytes {
		end := min(off+xfer.ChunkBytes, len(blob))
		body := xfer.PushChunk{Xfer: "round", Path: dest, Seq: seq, Data: blob[off:end], Done: end >= len(blob)}
		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_round/files", tok, body, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("chunk %d status = %d; body = %s", seq, resp.StatusCode, raw)
		}
		var ack xfer.PushAck
		if err := json.Unmarshal([]byte(raw), &ack); err != nil {
			t.Fatalf("decode ack %d: %v", seq, err)
		}
		if ack.Seq != seq {
			t.Fatalf("chunk %d was acked as %d — every chunk is acked, in order", seq, ack.Seq)
		}
	}

	store.mu.Lock()
	pushed := len(store.blobs[dest])
	store.mu.Unlock()
	if pushed != len(blob) {
		t.Fatalf("the sandbox holds %d bytes, want %d", pushed, len(blob))
	}

	resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_round/files?path="+dest, tok, nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pull status = %d; body = %s", resp.StatusCode, readBody(t, resp))
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the pull: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("pulled %d bytes, want the %d that were pushed (equal=%v)", len(got), len(blob), bytes.Equal(got, blob))
	}
}
