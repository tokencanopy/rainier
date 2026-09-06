package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/creack/pty"
	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

func TestAttachSignalHelper(t *testing.T) {
	if os.Getenv("RAINIER_ATTACH_SIGNAL_HELPER") != "1" {
		return
	}
	cfg, err := cli.Load()
	if err == nil {
		err = attachWithRetry(cfg, "sess_example", 0)
	}
	fmt.Printf("\nATTACH_RETURNED %v\n", err)
	os.Exit(0)
}

// A real Ctrl-C during a disconnected/cooked-mode interval must unwind the
// attach scope, not kill the process before its terminal cleanup can run.
func TestAttachCtrlCDuringRecoveryCleansTerminal(t *testing.T) {
	for _, phase := range []string{"upgrade blocked", "refresh blocked"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			blocked := make(chan struct{}, 1)
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v0/auth/refresh" {
					io.Copy(io.Discard, r.Body)
					blocked <- struct{}{}
					select {
					case <-r.Context().Done():
					case <-ctx.Done():
					}
					return
				}
				if attempts.Add(1) > 1 {
					if phase == "refresh blocked" {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					blocked <- struct{}{}
					select {
					case <-r.Context().Done():
					case <-ctx.Done():
					}
					return
				}
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.CloseNow()
				var first terminal.ClientMessage
				if wsjson.Read(ctx, c, &first) != nil {
					return
				}
				wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: 1, Data: []byte("\x1b[?1000h\x1b[?1006hREADY\n")})
				c.Close(websocket.StatusGoingAway, "synthetic disconnect")
			}))
			defer func() { cancel(); server.Close() }()
			master, output, done, stop := startAttachSignalProcess(t, server.URL)
			defer stop()
			select {
			case <-blocked:
			case <-ctx.Done():
				t.Fatalf("never reached %s: %s", phase, output.String())
			}
			if _, err := master.Write([]byte{3}); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Ctrl-C killed attach instead of unwinding cleanup: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Ctrl-C did not stop the pending reconnect request promptly")
			}
			// Drain the final bytes; the PTY reader may trail process exit.
			deadline := time.Now().Add(200 * time.Millisecond)
			for !strings.Contains(output.String(), "ATTACH_RETURNED") && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			out := output.String()
			if !strings.Contains(out, "ATTACH_RETURNED") {
				t.Errorf("attach never returned through its cleanup scope: %q", out)
			}
			for _, reset := range []string{"\x1b[?1000l", "\x1b[?1006l"} {
				if strings.LastIndex(out, reset) < strings.Index(out, "READY") {
					t.Errorf("missing final terminal reset %q: %q", reset, out)
				}
			}
			if strings.Contains(out, "access_signal_synthetic") || strings.Contains(out, "refresh_signal_synthetic") {
				t.Error("credentials leaked on interrupt")
			}
			saved, err := cli.Load()
			if err != nil || saved.Contexts["example"].Token != "access_signal_synthetic" || saved.Contexts["example"].RefreshToken != "refresh_signal_synthetic" {
				t.Errorf("interrupted request corrupted the saved token pair: %v", err)
			}
		})
	}
}

func TestAttachConnectedCtrlCIsForwarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	forwarded := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		var first terminal.ClientMessage
		if wsjson.Read(ctx, c, &first) != nil {
			return
		}
		wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: 1, Data: []byte("CONNECTED_READY\n")})
		var input terminal.ClientMessage
		if wsjson.Read(ctx, c, &input) != nil {
			return
		}
		forwarded <- input.Type == "stdin" && string(input.Data) == "\x03"
		wsjson.Write(ctx, c, terminal.ServerMessage{Type: "exit", ExitCode: 0})
	}))
	defer server.Close()
	master, out, done, stop := startAttachSignalProcess(t, server.URL)
	defer stop()
	for !strings.Contains(out.String(), "CONNECTED_READY") && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("no connected prompt")
	}
	master.Write([]byte{3})
	select {
	case ok := <-forwarded:
		if !ok {
			t.Error("connected Ctrl-C was not forwarded as stdin")
		}
	case <-ctx.Done():
		t.Fatal("connected Ctrl-C was intercepted locally")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("attach did not exit")
	}
}

func startAttachSignalProcess(t *testing.T, serverURL string) (*os.File, *reconnectE2EBuffer, <-chan error, func()) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("RAINIER_CONFIG", config)
	cfg := cli.Config{}
	cfg.SetContext("example", cli.Context{Server: serverURL, Token: "access_signal_synthetic", RefreshToken: "refresh_signal_synthetic", Workspace: "ws_example"})
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAttachSignalHelper$")
	cmd.Env = append(reconnectE2EEnvironment(), "RAINIER_CONFIG="+config, "RAINIER_ATTACH_SIGNAL_HELPER=1")
	master, err := pty.Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	output := &reconnectE2EBuffer{}
	readDone := make(chan struct{})
	go func() { defer close(readDone); io.Copy(output, master) }()
	done := make(chan error, 1)
	processExited := make(chan struct{})
	go func() { done <- cmd.Wait(); close(processExited) }()
	stop := func() {
		select {
		case <-processExited:
		default:
			cmd.Process.Kill()
			<-processExited
		}
		master.Close()
		<-readDone
	}
	return master, output, done, stop
}
