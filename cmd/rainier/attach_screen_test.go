package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/creack/pty"
	"github.com/tokencanopy/rainier/internal/attachio"
	"github.com/tokencanopy/rainier/internal/cli"
	rterm "github.com/tokencanopy/rainier/internal/term"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// A TUI's next frame may be relative to its last cursor, not a full repaint.
// Drive the product retry loop over real WebSockets and a shared stdout/stderr
// PTY, then render its bytes. Merely sending diagnostics to stderr cannot pass.
func TestAttachReconnectPreservesApplicationScreen(t *testing.T) {
	for _, alternate := range []bool{false, true} {
		name := "main screen"
		if alternate {
			name = "alternate screen"
		}
		t.Run(name, func(t *testing.T) {
			var attempts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)
				wantCursor := "17"
				if attempt > 1 {
					wantCursor = "18"
				}
				if attempt > 3 {
					wantCursor = "19"
				}
				if r.URL.Query().Get("since") != wantCursor {
					t.Errorf("attempt %d lost replay cursor", attempt)
				}
				if attempt == 2 {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				if attempt > 3 {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				var first terminal.ClientMessage
				if err := wsjson.Read(ctx, c, &first); err != nil || first.Type != "resize" {
					t.Errorf("resize-first handshake: %v", err)
					return
				}
				data, seq := "\x1b[2J\x1b[HAgent working\r\nProgress: 01\x1b[2D", uint64(18)
				if alternate {
					data = "\x1b[?1049h" + data
				}
				if attempt == 3 {
					// No cursor positioning or screen reset: continue exactly
					// where the acknowledged frame left the real terminal.
					data, seq = "02", 19
				}
				if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: seq, Data: []byte(data)}); err != nil {
					t.Error(err)
					return
				}
				c.Close(websocket.StatusGoingAway, "synthetic transport interruption")
			}))
			defer ts.Close()

			master, slave, err := pty.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer master.Close()
			defer slave.Close()
			if err := pty.Setsize(master, &pty.Winsize{Cols: 60, Rows: 12}); err != nil {
				t.Fatal(err)
			}
			oldIn, oldOut, oldErr := os.Stdin, os.Stdout, os.Stderr
			os.Stdin, os.Stdout, os.Stderr = slave, slave, slave
			defer func() { os.Stdin, os.Stdout, os.Stderr = oldIn, oldOut, oldErr }()
			const finished = "CAPTURE_FINISHED_SYNTHETIC"
			captured := make(chan string, 1)
			go func() {
				var buf bytes.Buffer
				p := make([]byte, 4096)
				for {
					n, err := master.Read(p)
					buf.Write(p[:n])
					if bytes.Contains(buf.Bytes(), []byte(finished)) || err != nil {
						captured <- buf.String()
						return
					}
				}
			}()
			err = attachWithRetrySleep(cli.Config{ServerURL: ts.URL, Token: "access_synthetic"}, "sess_example", 17, func(time.Duration) {})
			var dialErr *attachio.DialError
			if !errors.As(err, &dialErr) || dialErr.Status != http.StatusForbidden || attempts.Load() != 4 {
				t.Fatalf("final refusal: err=%v attempts=%d", err, attempts.Load())
			}
			if _, err := slave.Write([]byte(finished)); err != nil {
				t.Fatal(err)
			}
			var output string
			select {
			case output = <-captured:
			case <-time.After(5 * time.Second):
				t.Fatal("terminal capture did not finish")
			}
			output = strings.Split(output, finished)[0]
			emulator := rterm.NewEmulator(60, 12)
			emulator.Feed([]byte(output))
			screen := emulator.Screen()
			for y, row := range screen.Cells {
				var line strings.Builder
				for _, cell := range row {
					line.WriteRune(cell.R)
				}
				want := ""
				if y == 0 {
					want = "Agent working"
				} else if y == 1 {
					want = "Progress: 02"
				}
				if got := strings.TrimRight(line.String(), " \x00"); got != want {
					t.Errorf("row %d = %q, want %q", y, got, want)
				}
			}
			if screen.CursorX != 12 || screen.CursorY != 1 || screen.Alt != alternate {
				t.Errorf("cursor/active screen changed: x=%d y=%d alternate=%v", screen.CursorX, screen.CursorY, screen.Alt)
			}
		})
	}
}
