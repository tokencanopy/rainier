package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/creack/pty"

	"github.com/tokencanopy/rainier/internal/attachio"
	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// Mouse modes are terminal-emulator state, not termios state. Keep them
// through a cursor-only reconnect, but never hand them back to the shell
// after a permanent reconnect failure.
func TestAttachFinalFailureClearsMouseModesWithoutBreakingReconnect(t *testing.T) {
	const resumed = "synthetic-resumed-output"
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt >= 3 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first terminal.ClientMessage
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		data := []byte("\x1b[?1003h\x1b[?1006h")
		if attempt == 2 {
			// The first output is already acknowledged: the resumed stream
			// does not replay the original terminal-mode setup.
			data = []byte(resumed)
		}
		if err := wsjson.Write(r.Context(), c, terminal.ServerMessage{Type: "output", Seq: uint64(attempt), Data: data}); err != nil {
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
	oldIn, oldOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = slave, slave
	defer func() { os.Stdin, os.Stdout = oldIn, oldOut }()

	const finished = "synthetic-capture-finished"
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
	err = attachWithRetryBudget(cli.Config{ServerURL: ts.URL, Token: "rnr_synthetic"}, "sess_synthetic", 0, func(time.Duration) {}, 0)
	var dialErr *attachio.DialError
	if !errors.As(err, &dialErr) || dialErr.Status != http.StatusUnauthorized {
		t.Fatalf("final error = %v, want unauthorized reconnect", err)
	}
	if _, err := slave.Write([]byte(finished)); err != nil {
		t.Fatal(err)
	}
	var output string
	select {
	case output = <-captured:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal output capture did not finish")
	}
	resumedAt := strings.Index(output, resumed)
	if resumedAt < 0 {
		t.Fatalf("missing resumed output in %q", output)
	}
	modes := map[string]bool{}
	for _, match := range regexp.MustCompile("\x1b\\[\\?([0-9;]+)([hl])").FindAllStringSubmatchIndex(output, -1) {
		for _, mode := range strings.Split(output[match[2]:match[3]], ";") {
			if mode != "1003" && mode != "1006" {
				continue
			}
			enabled := output[match[4]:match[5]] == "h"
			if !enabled && match[0] < resumedAt {
				t.Errorf("mouse mode %s reset during transient reconnect; resumed output does not restore it", mode)
			}
			modes[mode] = enabled
		}
	}
	for _, mode := range []string{"1003", "1006"} {
		if enabled, seen := modes[mode]; !seen || enabled {
			t.Errorf("mouse mode %s remains enabled after final reconnect failure; terminal output %q", mode, output)
		}
	}
}
