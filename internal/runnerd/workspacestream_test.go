// internal/runnerd/workspacestream_test.go
package runnerd

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
)

// The runner's half of the cold-suspend checkpoint. Every test here is about
// the one rule that makes the barrier a barrier: this call returns success only
// when the WHOLE workspace arrived, and every other outcome is an error the
// driver above it turns into a suspend that did not happen.

// streamScene is a runner with one registered session and the three stream
// budgets cut to something a test can spend.
func streamScene(t *testing.T) (*Server, *sandbox, string) {
	t.Helper()
	rd := New(driver.NewFake(4), "", "", "")
	rd.suspendAckWait = 500 * time.Millisecond
	rd.coldSuspendReadyWait = 500 * time.Millisecond
	rd.workspaceStartWait = time.Second
	rd.workspaceIdleWait = time.Second
	rd.workspaceTotalWait = 10 * time.Second
	srv := httptest.NewServer(rd.Handler())
	t.Cleanup(srv.Close)
	id, sb := dialSandbox(t, rd, srv)
	return rd, sb, id
}

// sendStream writes one chunk of stream id up the session's conn, the way
// sessiond's ControlSender.SendStream does.
func (s *sandbox) sendStream(t *testing.T, id uint64, chunk []byte) {
	t.Helper()
	f, err := relay.Encode(relay.Frame{Type: relay.FrameStream, AttachID: id, Payload: chunk})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.c.Write(s.ctx, websocket.MessageText, f); err != nil {
		t.Fatalf("write stream frame: %v", err)
	}
}

// coldNotice reads the cold suspend notice the runner sends and returns its
// nonce, which is also the id of the stream that answers it.
func coldNotice(t *testing.T, sb *sandbox) uint64 {
	t.Helper()
	ev := sb.nextControl(t)
	if ev.Kind != relay.KindSuspending || !ev.Cold {
		t.Fatalf("the sandbox was sent %+v, want a cold suspending notice", ev)
	}
	if ev.ID == 0 {
		t.Fatal("the cold suspend notice carries no nonce; a late answer could satisfy the next suspend")
	}
	return ev.ID
}

type streamOutcome struct {
	rep driver.WorkspaceStream
	err error
}

// TestStreamWorkspaceReadsTheWholeTreeAndChecksTheCounts is the happy path, and
// the counts are the point: the guest says how much it wrote and this end knows
// how much arrived, which is what turns "the connection went quiet" into a
// refused suspend rather than a checkpoint of half a tree.
func TestStreamWorkspaceReadsTheWholeTreeAndChecksTheCounts(t *testing.T) {
	rd, sb, id := streamScene(t)

	var got bytes.Buffer
	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, &got)
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("workspace-"))
	sb.sendStream(t, nonce, []byte("bytes"))
	sb.send(t, relay.ControlEvent{Kind: relay.KindWorkspaceEnd, ID: nonce, OK: true, Entries: 7, Bytes: 15})
	sb.answer(t, relay.KindSuspendReady, nonce)

	out := <-done
	if out.err != nil {
		t.Fatalf("the stream failed: %v", out.err)
	}
	if got.String() != "workspace-bytes" {
		t.Errorf("the runner received %q", got.String())
	}
	if out.rep.Entries != 7 || out.rep.Bytes != 15 || out.rep.Nonce != nonce {
		t.Errorf("the report is %+v, want 7 entries, 15 bytes and nonce %d", out.rep, nonce)
	}
}

// TestStreamWorkspaceRefusesCountsThatDisagree: the guest wrote more than
// arrived, which is a stream that lost bytes between the two ends. Nothing may
// be checkpointed from it.
func TestStreamWorkspaceRefusesCountsThatDisagree(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("short"))
	sb.send(t, relay.ControlEvent{Kind: relay.KindWorkspaceEnd, ID: nonce, OK: true, Entries: 3, Bytes: 4096})

	out := <-done
	if out.err == nil {
		t.Fatal("a stream that lost bytes was accepted")
	}
	if !strings.Contains(out.err.Error(), "not complete") {
		t.Errorf("the refusal is %v", out.err)
	}
}

// TestStreamWorkspaceRefusesTheGuestsOwnFailure: the sandbox could not read its
// own workspace. It reports rather than deciding — it does not know whether a
// checkpoint committed — and this end is what refuses.
func TestStreamWorkspaceRefusesTheGuestsOwnFailure(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("partial"))
	sb.send(t, relay.ControlEvent{Kind: relay.KindWorkspaceEnd, ID: nonce, Stage: "workspace_stream", RC: -1,
		Entries: 41, Bytes: 7, Tail: "the runner is not taking the workspace stream"})

	out := <-done
	if out.err == nil {
		t.Fatal("a guest that could not stream its workspace was accepted")
	}
	for _, want := range []string{"workspace_stream", "41 entries"} {
		if !strings.Contains(out.err.Error(), want) {
			t.Errorf("the refusal %q does not carry %q", out.err, want)
		}
	}
}

// TestStreamWorkspaceRefusesASandboxThatCannotAnswer. A sessiond that predates
// this vocabulary answers nothing at all, and there is no checkpoint to be had
// from one — so unlike the warm path, which shrugs and freezes anyway, this is
// a refusal.
func TestStreamWorkspaceRefusesASandboxThatCannotAnswer(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()

	coldNotice(t, sb) // heard, and deliberately never answered

	out := <-done
	if out.err == nil {
		t.Fatal("a sandbox that never acknowledged the cold suspend was accepted")
	}
	if !strings.Contains(out.err.Error(), "did not acknowledge") {
		t.Errorf("the refusal is %v", out.err)
	}
}

// TestStreamWorkspaceRefusesAStreamThatNeverStarts and
// TestStreamWorkspaceRefusesAStreamThatStalls are the two halves of the
// progress budget: one bounds the quiescing before the first chunk, the other
// bounds the gap between chunks. They are different numbers because they bound
// different work.
func TestStreamWorkspaceRefusesAStreamThatNeverStarts(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)

	out := <-done
	if out.err == nil || !strings.Contains(out.err.Error(), "did not begin streaming") {
		t.Fatalf("a sandbox that acknowledged and streamed nothing gave %v", out.err)
	}
}

func TestStreamWorkspaceRefusesAStreamThatStalls(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("a start and then nothing"))

	out := <-done
	if out.err == nil || !strings.Contains(out.err.Error(), "stopped making progress") {
		t.Fatalf("a stalled stream gave %v", out.err)
	}
}

// TestStreamWorkspaceRefusesWhenThisHostStopsTakingBytes. The stream is
// back-pressured by whatever is consuming it, so a checkpoint writer or a blob
// store that failed stalls the stream — and the failure must be named as what
// it is rather than reported as a guest that would not stream.
func TestStreamWorkspaceRefusesWhenThisHostStopsTakingBytes(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, failingWriter{})
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("anything at all"))

	out := <-done
	if out.err == nil || !strings.Contains(out.err.Error(), "could not be taken") {
		t.Fatalf("a sink that failed gave %v", out.err)
	}
}

// TestStreamWorkspaceIgnoresAnEndMarkerForAnotherSuspend: a late answer from a
// suspend that already gave up must not tell this one that a tree it never
// received is complete. That is what the nonce is for.
func TestStreamWorkspaceIgnoresAnEndMarkerForAnotherSuspend(t *testing.T) {
	rd, sb, id := streamScene(t)

	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()

	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("some bytes"))
	// The end marker of a suspend that is not this one.
	sb.send(t, relay.ControlEvent{Kind: relay.KindWorkspaceEnd, ID: nonce + 99, OK: true, Entries: 1, Bytes: 10})

	out := <-done
	if out.err == nil || !strings.Contains(out.err.Error(), "stopped making progress") {
		t.Fatalf("an end marker from another suspend gave %v", out.err)
	}
}

// TestStreamWorkspaceRefusesASessionWithNoSandbox: a session whose sandbox
// never registered cannot be checkpointed, so it is not suspended either.
func TestStreamWorkspaceRefusesASessionWithNoSandbox(t *testing.T) {
	rd := New(driver.NewFake(4), "", "", "")
	if _, err := rd.StreamWorkspace(context.Background(), "sess-nobody", new(bytes.Buffer)); err == nil {
		t.Fatal("a session with no sandbox connection was accepted")
	}
}

// TestStreamWorkspaceDropsChunksNobodyAskedFor. A guest may put a stream frame
// on the wire whenever it likes; one that matches no in-flight suspend is
// dropped rather than buffered, and the session carries on.
func TestStreamWorkspaceDropsChunksNobodyAskedFor(t *testing.T) {
	rd, sb, id := streamScene(t)

	sb.sendStream(t, 12345, []byte("a stream nobody asked for"))

	// The conn is still live and still serving: the proof is that a suspend
	// started afterwards still reaches this sandbox.
	done := make(chan streamOutcome, 1)
	go func() {
		rep, err := rd.StreamWorkspace(context.Background(), id, new(bytes.Buffer))
		done <- streamOutcome{rep, err}
	}()
	nonce := coldNotice(t, sb)
	sb.answer(t, relay.KindSuspendAck, nonce)
	sb.sendStream(t, nonce, []byte("the real one"))
	sb.send(t, relay.ControlEvent{Kind: relay.KindWorkspaceEnd, ID: nonce, OK: true, Entries: 1, Bytes: 12})
	sb.answer(t, relay.KindSuspendReady, nonce)
	if out := <-done; out.err != nil {
		t.Fatalf("the stream after an unsolicited chunk failed: %v", out.err)
	}
}

// TestAColdSuspendSendsOneNoticeWhenTheDriverCheckpoints. The notice and the
// stream that answers it are ONE handshake with one nonce, so a runner that
// sent its own notice as well would have the sandbox quiesce twice and stream
// into a suspend nobody is reading.
func TestAColdSuspendSendsOneNoticeWhenTheDriverCheckpoints(t *testing.T) {
	rd := New(&checkpointingFake{Fake: driver.NewFake(4)}, "", "", "")
	rd.suspendAckWait = 200 * time.Millisecond
	rd.coldSuspendReadyWait = 200 * time.Millisecond
	srv := httptest.NewServer(rd.Handler())
	t.Cleanup(srv.Close)
	id, sb := dialSandbox(t, rd, srv)

	if err := rd.Op(context.Background(), id, "suspend", false); err != nil {
		t.Fatalf("the cold suspend failed: %v", err)
	}

	// Nothing was sent down this conn: the driver owns the handshake.
	ctx, cancel := context.WithTimeout(sb.ctx, 300*time.Millisecond)
	defer cancel()
	if _, _, err := sb.c.Read(ctx); err == nil {
		t.Fatal("the runner sent the sandbox a notice of its own; the driver already sends one")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reading from the runner: %v", err)
	}
}

// checkpointingFake is a driver that announces the microVM capability AND says
// it runs the cold-suspend handshake itself, which is the pair runnerd reads
// before deciding whether to send a notice of its own.
type checkpointingFake struct{ *driver.Fake }

func (*checkpointingFake) Capabilities() []string         { return []string{"microvm.v1"} }
func (*checkpointingFake) CheckpointsColdSuspend() bool   { return true }
func (f *checkpointingFake) SetHost(h driver.MicrovmHost) {}

// failingWriter is a sink that cannot take a byte: a checkpoint writer whose
// store has gone away.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("the store is not taking bytes")
}

// TestClampGuestTextBoundsWhatTheSandboxChose. Both strings in the guest's own
// failure report are the SANDBOX's, and they reach an operator's log and a
// session's error column. sessiond is careful about what it puts in them; this
// hop is where that stops being something to rely on, so the bound is here and
// is tested here.
func TestClampGuestTextBoundsWhatTheSandboxChose(t *testing.T) {
	short := "mkfs"
	if got := clampGuestText(short); got != short {
		t.Errorf("a short string came back as %q", got)
	}
	exact := strings.Repeat("a", maxGuestText)
	if got := clampGuestText(exact); got != exact {
		t.Errorf("a string at the bound was truncated (%d bytes)", len(got))
	}

	over := strings.Repeat("b", maxGuestText+1)
	got := clampGuestText(over)
	if len(got) <= maxGuestText {
		t.Fatalf("the truncated form is %d bytes, which is not the truncation notice plus the bound", len(got))
	}
	if !strings.HasPrefix(got, strings.Repeat("b", maxGuestText)) {
		t.Errorf("the truncated form is not the first %d bytes: %q", maxGuestText, got)
	}
	if strings.Count(got, "b") != maxGuestText {
		t.Errorf("the truncated form carries %d bytes of the sandbox's string, want %d", strings.Count(got, "b"), maxGuestText)
	}
	// It says that it did, so nobody reads a clamped stage name as the whole
	// of what the guest reported.
	if !strings.Contains(got, "truncated") {
		t.Errorf("the truncated form does not say it was truncated: %q", got)
	}

	// And a megabyte of it — the frame limit above this is 16 MiB — comes back
	// bounded, which is the whole point.
	huge := clampGuestText(strings.Repeat("c", 1<<20))
	if len(huge) > maxGuestText+64 {
		t.Errorf("a 1 MiB guest string came back as %d bytes", len(huge))
	}
}
