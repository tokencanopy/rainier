package main

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/wstream"
)

// The guest's half of the cold-suspend checkpoint, wired the way main wires it:
// the frame arrives, the handler quiesces, the workspace goes out as a stream
// on the same connection, and the end marker and the ready answer follow it in
// that order.

// workspaceFixture builds a session workspace: a file, a directory, a symbolic
// link, and the Rainier-owned path that must never leave the guest.
func workspaceFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range map[string]string{
		"README.md":             "hello",
		"src/main.go":           "package main\n",
		".rainier/session.json": "{\"token\":\"must-not-travel\"}",
	} {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("src/main.go", filepath.Join(root, "entry")); err != nil {
		t.Fatal(err)
	}
	return root
}

// coldStreamWired is coldWired with the real streamer over root, so what these
// tests exercise is the whole journey: OnControl's dispatch, the cold branch,
// the stream, the end marker and the answers.
func coldStreamWired(t *testing.T, root string) (*rpcDispatcher, *recordingSender) {
	t.Helper()
	d := newRPCDispatcher()
	sender := &recordingSender{}
	d.online(sender)
	d.RegisterEventHandler(relay.KindSuspending, func(ev relay.ControlEvent) {
		if ev.Cold {
			quiesceCold(&recordingExecs{}, d, nil, &bootstrapper{}, ev.ID, streamWorkspaceFn(d, root))
			return
		}
		quiesceExecs(&recordingExecs{}, d, ev.ID)
	})
	return d, sender
}

// decodeStream reads a workspace stream back the way the host does: the magic,
// the index, then the tar.
func decodeStream(t *testing.T, b []byte) (index []string, tarNames map[string]*tar.Header, bodies map[string]string) {
	t.Helper()
	if !bytes.HasPrefix(b, []byte(wstream.Magic)) {
		t.Fatalf("the stream does not begin with the magic line (%d bytes)", len(b))
	}
	r := bytes.NewReader(b[len(wstream.Magic):])
	for {
		n, err := binary.ReadUvarint(r)
		if err != nil {
			t.Fatalf("reading an index length: %v", err)
		}
		if n == 0 {
			break
		}
		rec := make([]byte, n)
		if _, err := io.ReadFull(r, rec); err != nil {
			t.Fatalf("reading an index record: %v", err)
		}
		index = append(index, string(rec[0:1])+" "+string(rec[1:]))
	}
	tarNames, bodies = map[string]*tar.Header{}, map[string]string{}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return index, tarNames, bodies
		}
		if err != nil {
			t.Fatalf("reading the tar: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("reading a tar body: %v", err)
		}
		tarNames[hdr.Name] = hdr
		bodies[hdr.Name] = string(body)
	}
}

// TestAColdSuspendStreamsTheWorkspaceBeforeItAnswersReady is §4.4 from inside
// the guest: the only copy of the user's work that survives this VM is the one
// this puts on the wire, and it goes out BEFORE the answer that lets the host
// terminate the VM.
func TestAColdSuspendStreamsTheWorkspaceBeforeItAnswersReady(t *testing.T) {
	cleanEnv(t)
	root := workspaceFixture(t)
	d, sender := coldStreamWired(t, root)

	d.OnControl(suspendingFrame(t, true, 71))

	got := sender.events()
	if len(got) != 3 {
		t.Fatalf("the sandbox answered %d events, want an ack, an end marker and a ready: %+v", len(got), got)
	}
	if got[0].Kind != relay.KindSuspendAck || got[1].Kind != relay.KindWorkspaceEnd || got[2].Kind != relay.KindSuspendReady {
		t.Fatalf("the answers are %q, %q, %q; want ack, workspace_end, suspend_ready",
			got[0].Kind, got[1].Kind, got[2].Kind)
	}
	for _, ev := range got {
		if ev.ID != 71 {
			t.Fatalf("an answer carries nonce %d, want the notice's 71", ev.ID)
		}
	}
	end := got[1]
	if !end.OK {
		t.Fatalf("the end marker reports a failure: %+v", end)
	}

	raw := sender.stream(71)
	if int64(len(raw)) != end.Bytes {
		t.Errorf("the end marker claims %d bytes, the stream carried %d", end.Bytes, len(raw))
	}
	index, headers, bodies := decodeStream(t, raw)
	if int64(len(index)) != end.Entries {
		t.Errorf("the end marker claims %d entries, the index names %d", end.Entries, len(index))
	}
	// The tree, in fs.WalkDir order, which is the order the checkpoint writer
	// walks in and therefore the only order the host can read.
	want := []string{"f README.md", "l entry", "d src", "f src/main.go"}
	if len(index) != len(want) {
		t.Fatalf("the index names %v, want %v", index, want)
	}
	for i := range want {
		if index[i] != want[i] {
			t.Fatalf("the index names %v, want %v", index, want)
		}
	}
	if bodies["README.md"] != "hello" {
		t.Errorf("README.md arrived as %q", bodies["README.md"])
	}
	// A symbolic link travels as a link with its target, never followed.
	if hdr := headers["entry"]; hdr.Typeflag != tar.TypeSymlink || hdr.Linkname != "src/main.go" {
		t.Errorf("entry arrived as type %c pointing at %q, want a symlink to src/main.go", hdr.Typeflag, hdr.Linkname)
	}
}

// TestAColdSuspendStreamsNothingRainierOwns. The exclusion is a promise about
// the bytes that LEAVE the guest: /workspace/.rainier is Rainier's own control
// directory, and a bootstrap token in it must not travel into a checkpoint
// that outlives the session.
func TestAColdSuspendStreamsNothingRainierOwns(t *testing.T) {
	cleanEnv(t)
	root := workspaceFixture(t)
	d, sender := coldStreamWired(t, root)

	d.OnControl(suspendingFrame(t, true, 72))

	raw := sender.stream(72)
	if bytes.Contains(raw, []byte("must-not-travel")) {
		t.Fatal("the stream carries the contents of .rainier")
	}
	if bytes.Contains(raw, []byte(".rainier")) {
		t.Fatal("the stream names .rainier")
	}
}

// TestAFailedWorkspaceStreamReportsTheStageAndTheCounts: the conn stops taking
// bytes half way through. The guest cannot decide that the suspend has failed —
// it does not know whether a checkpoint committed — so it REPORTS, in the
// stage_failed shape, with the counts it reached and no path at all, and the
// host is what refuses the suspend.
func TestAFailedWorkspaceStreamReportsTheStageAndTheCounts(t *testing.T) {
	cleanEnv(t)
	root := workspaceFixture(t)
	// Big enough to need several chunks, so the failure lands MID-stream
	// rather than before it.
	big := bytes.Repeat([]byte("x"), 3*streamChunkBytes)
	if err := os.WriteFile(filepath.Join(root, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}

	d := newRPCDispatcher()
	sender := &recordingSender{streamErr: errors.New("write unix /srv/rainier/state/instances/i-7/v1.sock_1024: broken pipe"), afterChunks: 1}
	d.online(sender)
	d.RegisterEventHandler(relay.KindSuspending, func(ev relay.ControlEvent) {
		quiesceCold(&recordingExecs{}, d, nil, &bootstrapper{}, ev.ID, streamWorkspaceFn(d, root))
	})

	d.OnControl(suspendingFrame(t, true, 73))

	got := sender.events()
	if len(got) != 3 {
		t.Fatalf("the sandbox answered %d events, want an ack, an end marker and a ready: %+v", len(got), got)
	}
	end := got[1]
	if end.Kind != relay.KindWorkspaceEnd || end.OK {
		t.Fatalf("the end marker is %+v, want a workspace_end reporting failure", end)
	}
	if end.Stage != workspaceStreamStage || end.RC != -1 {
		t.Errorf("the failure names stage %q rc %d, want %q and -1", end.Stage, end.RC, workspaceStreamStage)
	}
	if end.Entries == 0 || end.Bytes == 0 {
		t.Errorf("the failure reports %d entries and %d bytes, want the counts it reached", end.Entries, end.Bytes)
	}
	// The transport's own error quotes the socket it failed on, which on a
	// microVM host is a path inside the VM's jail. It must not reach a
	// session's error column.
	for _, leak := range []string{root, "big.bin", "README.md", "v1.sock", "/srv/rainier"} {
		if strings.Contains(end.Tail, leak) {
			t.Errorf("the failure's tail quotes %q: %q", leak, end.Tail)
		}
	}
	if !strings.Contains(end.Tail, "not taking the workspace stream") {
		t.Errorf("the failure's tail says %q, which names no cause", end.Tail)
	}
	// And the sandbox still answers ready: the host terminates the VM when it
	// hears nothing, so silence would only make the stop slower.
	if got[2].Kind != relay.KindSuspendReady {
		t.Errorf("the last answer is %q, want suspend_ready", got[2].Kind)
	}
}

// TestAWorkspaceStreamWithNoConnectionFailsRatherThanDropping: a chunk with
// nowhere to go is a truncated workspace, and a truncated workspace that was
// reported as complete is the failure the whole barrier exists to prevent.
func TestAWorkspaceStreamWithNoConnectionFails(t *testing.T) {
	cleanEnv(t)
	root := workspaceFixture(t)
	d := newRPCDispatcher() // never online
	if _, err := streamWorkspaceFn(d, root)(74); err == nil {
		t.Fatal("streaming with no connection reported success")
	}
}

// TestTheChunkWriterHoldsOneChunk. The whole design is that a workspace never
// exists in memory on either end; a writer that grew its buffer to whatever it
// was handed would put the guest's side of that promise in the hands of
// whatever called it.
func TestTheChunkWriterHoldsOneChunk(t *testing.T) {
	var sent [][]byte
	w := &chunkWriter{
		send: func(chunk []byte) error {
			sent = append(sent, append([]byte(nil), chunk...))
			return nil
		},
		buf: make([]byte, 0, 4),
	}
	if _, err := w.Write([]byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []string{"abcd", "efgh", "ij"}
	if len(sent) != len(want) {
		t.Fatalf("the writer sent %d chunks (%q), want %v", len(sent), sent, want)
	}
	for i := range want {
		if string(sent[i]) != want[i] {
			t.Fatalf("chunk %d is %q, want %q", i, sent[i], want[i])
		}
	}
	if w.sent != 10 {
		t.Errorf("the writer counted %d bytes, want 10", w.sent)
	}
	// A second Flush sends nothing: a zero-length frame would be
	// indistinguishable from one that was lost.
	if err := w.Flush(); err != nil || len(sent) != len(want) {
		t.Errorf("flushing an empty buffer sent %d chunks (%v)", len(sent)-len(want), err)
	}
}
