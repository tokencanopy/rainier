package server

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// A viewer that stops responding (its transport goes silent without ever
// sending a close frame — network path dropped, laptop asleep, terminal
// killed with no chance to flush) must be detached so it no longer clamps
// session size. We simulate this by attaching a small viewer that never
// reads from its socket (so it can never answer a ping) and asserting a
// larger viewer's size is restored to the PTY within a bounded window.
func TestDeadViewerIsDetachedAndSizeRecovers(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 120, Rows: 40, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewWithKeepalive(s, 200*time.Millisecond)) // NEW ctor with fast interval for tests
	defer srv.Close()

	// big must actually announce 120x40 (the session's own configured size)
	// so its post-recovery `stty size` is distinguishable from the clamped
	// reading below. The shared dial() helper hardcodes an 80x24 resize for
	// the other server tests' 80x24 session, so it can't be reused here —
	// dialSize sends the exact size this test needs.
	big := dialSize(t, srv.URL, 120, 40)
	small := dialSize(t, srv.URL, 40, 10)
	// Cleanup only — deferred, so it runs after the assertions below, not as
	// the thing that triggers detection (see note above the deadline loop).
	defer small.CloseNow()

	// Confirm the small viewer clamped the PTY (write `stty size` and read it back on big).
	// `stty size` prints "ROWS COLS" with no leading space before ROWS, so we
	// match the full "10 40" pair rather than a bare " 10" — a bare token
	// match risks a false positive later (the clamped reading "10 40" itself
	// contains " 40" as the COLS half, which would wrongly look like the
	// post-recovery ROWS reading if we searched for " 40" alone).
	ctx := context.Background()
	wsjson.Write(ctx, big, terminal.ClientMessage{Type: "stdin", Data: []byte("stty size\n")})
	readUntil(t, big, "10 40") // rows clamped to 10, cols clamped to 40 (smallest of each)

	// Do NOT close small's transport here. coder/websocket only processes
	// (and thus auto-answers) an incoming Ping frame from inside a Read
	// call; small never calls Read after dialSize's initial resize write, so
	// it already can never pong — closing its socket would only muddy that.
	// Deliberately calling small.CloseNow() here would make this test pass
	// for the wrong reason: CloseNow performs a real local TCP close (a
	// clean FIN that the peer's kernel reports back almost instantly), which
	// the reader loop's pre-existing `if err := wsjson.Read(...); err != nil
	// { return }` path already handled before this task — that alone would
	// trigger Detach with no dependency on the ping/pong code whatsoever,
	// leaving the new keepalive logic untested. Leaving small's TCP
	// connection open but unresponsive is what actually forces detection
	// through a failed c.Ping timeout, exactly like the real dead-client
	// scenario this task fixes (network path silently gone, no FIN ever
	// sent).

	// Within a few ping intervals, the server must detect the dead pong,
	// detach small, and restore size to big's own 40x120. Poll by re-issuing
	// stty size, matching the full "40 120" pair for the same reason as above.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		wsjson.Write(ctx, big, terminal.ClientMessage{Type: "stdin", Data: []byte("stty size\n")})
		if readUntilOrTimeout(t, big, "40 120", 400*time.Millisecond) {
			return
		}
	}
	t.Fatal("size never recovered after dead viewer; liveness kick not working")
}
