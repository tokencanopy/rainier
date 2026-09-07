package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

func TestRattachDisconnectReportsResumeCursor(t *testing.T) {
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
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
		if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: 23, Data: []byte("remote output\n")}); err != nil {
			t.Error(err)
			return
		}
		c.Close(websocket.StatusGoingAway, "synthetic disconnect")
	}))
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "run", ".", "--url", "ws"+strings.TrimPrefix(ts.URL, "http")).CombinedOutput()
	if err != nil {
		t.Fatalf("rattach: %v\n%s", err, out)
	}
	for _, want := range []string{"remote output", "[connection lost at seq 23]", "[rattach --since 23 to resume]"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("missing %q in %q", want, out)
		}
	}
	if attempts.Load() != 1 {
		t.Errorf("one-shot rattach made %d attempts", attempts.Load())
	}
}
