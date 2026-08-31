package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tokencanopy/rainier/internal/xfer"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// sourceTree lays out a small directory to move around.
func sourceTree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "pkg"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# hi\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pkg", "big.bin"), bytes.Repeat([]byte("payload"), 1000), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return dir
}

// pushCollector is a controld that accepts the chunk protocol and keeps what
// it was sent.
type pushCollector struct {
	mu     sync.Mutex
	body   []byte
	chunks int
	xferID string
	path   string
	done   bool
	fail   string // when set, every chunk is refused with this message
}

func (p *pushCollector) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/files") {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var c xfer.PushChunk
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Errorf("decode chunk: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.fail != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]string{"code": "conflict", "message": p.fail}})
			return
		}
		if c.Seq != p.chunks {
			t.Errorf("chunk arrived with seq %d, want %d — chunks must be sequential", c.Seq, p.chunks)
		}
		if len(c.Data) > xfer.ChunkBytes {
			t.Errorf("chunk %d carried %d bytes, over the %d limit", c.Seq, len(c.Data), xfer.ChunkBytes)
		}
		if p.xferID == "" {
			p.xferID, p.path = c.Xfer, c.Path
		}
		if c.Xfer != p.xferID {
			t.Errorf("chunk %d changed the transfer id from %q to %q", c.Seq, p.xferID, c.Xfer)
		}
		if c.Path != p.path {
			t.Errorf("chunk %d changed the destination from %q to %q", c.Seq, p.path, c.Path)
		}
		p.body = append(p.body, c.Data...)
		p.chunks++
		p.done = p.done || c.Done
		json.NewEncoder(w).Encode(xfer.PushAck{Seq: c.Seq, Synced: c.Done || p.chunks%xfer.SyncEvery == 0})
	}
}

func newTestClient(t *testing.T, h http.Handler) *Client {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return &Client{Base: ts.URL, Token: "tok"}
}

// ---------------------------------------------------------------------------
// push
// ---------------------------------------------------------------------------

// TestPushStreamsAnArchiveInOrder is the client half of the chunk protocol: one
// archive, cut into chunks, each acked before the next is sent, reassembling
// on the far side into the tree that was pushed.
func TestPushStreamsAnArchiveInOrder(t *testing.T) {
	src := sourceTree(t)
	collector := &pushCollector{}
	c := newTestClient(t, collector.handler(t))

	var lastSent, lastTotal int64
	if err := Push(c, "sess_x", src, "widget/vendor", func(sent, total int64) {
		lastSent, lastTotal = sent, total
	}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()
	if !collector.done {
		t.Fatal("the last chunk did not carry done")
	}
	if collector.path != "widget/vendor" {
		t.Fatalf("destination = %q", collector.path)
	}
	if lastSent != lastTotal || lastTotal != int64(len(collector.body)) {
		t.Fatalf("progress ended at %d/%d for a %d-byte archive", lastSent, lastTotal, len(collector.body))
	}

	// What arrived is an archive of the tree that was pushed.
	archive := filepath.Join(t.TempDir(), "got.tgz")
	if err := os.WriteFile(archive, collector.body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	dest := t.TempDir()
	if err := xfer.UntarGz(archive, dest, xfer.MaxExtractBytes); err != nil {
		t.Fatalf("UntarGz what the server received: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "pkg", "big.bin"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("payload"), 1000)) {
		t.Fatal("the pushed file did not survive the round trip")
	}
}

// TestPushRefusesADirectoryOverTheCap: the cap is named, and the client is the
// first of three places that enforces it — the one that can say which
// directory before anything is sent at all.
func TestPushRefusesADirectoryOverTheCap(t *testing.T) {
	src := t.TempDir()
	blob := make([]byte, 512<<10)
	if _, err := rand.Read(blob); err != nil { // incompressible
		t.Fatalf("rand: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "blob"), blob, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("an over-cap push must not send a single chunk")
	}))
	err := pushLimited(c, "sess_x", src, "dst", nil, 64<<10)
	if err == nil {
		t.Fatal("Push of an over-cap directory returned nil")
	}
	if !strings.Contains(err.Error(), "64.0 KiB") {
		t.Fatalf("error = %v, want it to name the limit", err)
	}
	if !strings.Contains(err.Error(), src) {
		t.Fatalf("error = %v, want it to name the directory", err)
	}
}

// TestPushRefusesSomethingThatIsNotADirectory: `rainier push file.txt` is a
// mistake worth catching before a transfer starts.
func TestPushRefusesSomethingThatIsNotADirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err := Push(c, "sess_x", file, "dst", nil); err == nil {
		t.Fatal("Push of a plain file returned nil")
	}
	if err := Push(c, "sess_x", filepath.Join(t.TempDir(), "nope"), "dst", nil); err == nil {
		t.Fatal("Push of a directory that does not exist returned nil")
	}
}

// TestPushSurfacesTheServersRefusal verbatim — that sentence is usually the
// one thing that says what went wrong inside the session.
func TestPushSurfacesTheServersRefusal(t *testing.T) {
	collector := &pushCollector{fail: "push chunk 0 arrived out of order; expected 3"}
	c := newTestClient(t, collector.handler(t))
	err := Push(c, "sess_x", sourceTree(t), "dst", nil)
	if err == nil {
		t.Fatal("Push against a refusing server returned nil")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Fatalf("error = %v, want the server's own sentence", err)
	}
}

// TestPushRefusesAMismatchedAck: an ack for another chunk means the two ends
// disagree about where the transfer is, and continuing would corrupt it.
func TestPushRefusesAMismatchedAck(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(xfer.PushAck{Seq: 42})
	}))
	err := Push(c, "sess_x", sourceTree(t), "dst", nil)
	if err == nil {
		t.Fatal("Push accepted an ack for a different chunk")
	}
	if !strings.Contains(err.Error(), "42") {
		t.Fatalf("error = %v, want it to name the sequence numbers", err)
	}
}

// TestPushValidatesTheDestinationBeforeSending: the same rule controld and
// sessiond apply, applied first where the user can still fix the command.
func TestPushValidatesTheDestinationBeforeSending(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a bad destination must not reach the server")
	}))
	for _, dest := range []string{"", "../etc", "/etc/cron.d"} {
		if err := Push(c, "sess_x", sourceTree(t), dest, nil); err == nil {
			t.Errorf("Push to %q returned nil", dest)
		}
	}
}

// ---------------------------------------------------------------------------
// pull
// ---------------------------------------------------------------------------

// archiveServer answers a pull with body.
func archiveServer(t *testing.T, body []byte) *Client {
	t.Helper()
	return newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("path") == "" {
			t.Errorf("unexpected pull request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		w.Write(body)
	}))
}

func TestPullExtractsTheArchive(t *testing.T) {
	src := sourceTree(t)
	var buf bytes.Buffer
	if _, err := xfer.TarGz(&buf, src, xfer.MaxBytes); err != nil {
		t.Fatalf("TarGz: %v", err)
	}
	c := archiveServer(t, buf.Bytes())

	dest := filepath.Join(t.TempDir(), "landing")
	var received int64
	if err := Pull(c, "sess_x", "widget/out", dest, func(n, _ int64) { received = n }); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if received != int64(buf.Len()) {
		t.Fatalf("progress reported %d bytes, want %d", received, buf.Len())
	}
	got, err := os.ReadFile(filepath.Join(dest, "README.md"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "# hi\n" {
		t.Fatalf("pulled content = %q", got)
	}
}

// TestPullRefusesAHostileArchive: the archive comes from a session, and a
// session is not a trusted peer — this is the direction where an escaping
// entry would land in the USER'S home directory.
func TestPullRefusesAHostileArchive(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	body := "owned"
	if err := tw.WriteHeader(&tar.Header{
		Name: "../../escaped.txt", Mode: 0o644, Typeflag: tar.TypeReg, Size: int64(len(body))}); err != nil {
		t.Fatalf("header: %v", err)
	}
	io.WriteString(tw, body)
	tw.Close()
	zw.Close()

	c := archiveServer(t, buf.Bytes())
	parent := t.TempDir()
	dest := filepath.Join(parent, "landing")
	if err := Pull(c, "sess_x", "d", dest, nil); err == nil {
		t.Fatal("Pull accepted an archive that escapes its destination")
	}
	if _, err := os.Lstat(filepath.Join(parent, "escaped.txt")); err == nil {
		t.Fatal("an entry escaped the destination directory")
	}
}

// TestPullRefusesPastTheCap: a session that streams without end must cost the
// client an error, not its disk.
func TestPullRefusesPastTheCap(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		chunk := make([]byte, 32<<10)
		for i := 0; i < 64; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	dest := filepath.Join(t.TempDir(), "landing")
	err := pullLimited(c, "sess_x", "d", dest, nil, 64<<10)
	if err == nil {
		t.Fatal("Pull of an endless stream returned nil")
	}
	if !strings.Contains(err.Error(), "64.0 KiB") {
		t.Fatalf("error = %v, want it to name the limit", err)
	}
	if entries, _ := os.ReadDir(dest); len(entries) != 0 {
		t.Fatalf("a refused pull left %v behind", entries)
	}
}

func TestPullValidatesThePathBeforeAsking(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a bad path must not reach the server")
	}))
	for _, p := range []string{"", "../etc", "/etc/passwd"} {
		if err := Pull(c, "sess_x", p, t.TempDir(), nil); err == nil {
			t.Errorf("Pull of %q returned nil", p)
		}
	}
}

// ---------------------------------------------------------------------------
// progress
// ---------------------------------------------------------------------------

func TestProgressLine(t *testing.T) {
	cases := []struct {
		verb        string
		done, total int64
		want        string
	}{
		{"pushing", 0, 1024, "pushing 0 B / 1.0 KiB (0%)"},
		{"pushing", 512, 1024, "pushing 512 B / 1.0 KiB (50%)"},
		{"pushing", 1024, 1024, "pushing 1.0 KiB / 1.0 KiB (100%)"},
		{"pulling", 2048, 0, "pulling 2.0 KiB"},
		{"pushing", 3 << 20, 12 << 20, "pushing 3.0 MiB / 12.0 MiB (25%)"},
	}
	for _, tc := range cases {
		if got := ProgressLine(tc.verb, tc.done, tc.total); got != tc.want {
			t.Errorf("ProgressLine(%q, %d, %d) = %q, want %q", tc.verb, tc.done, tc.total, got, tc.want)
		}
	}
}
