package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/creack/pty"
	"golang.org/x/term"

	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// TestReconnectProcessE2E builds and runs real rainier and sessiond processes.
// The local hosted-auth/route fixture is NOT a real hosted edge or gateway:
// it injects two token expiries and transport closes around sessiond's actual
// PTY, child, event log, and replay path. No Docker or live credentials are used.
// Run explicitly: RAINIER_RECONNECT_E2E=1 go test ./cmd/rainier -run '^TestReconnectProcessE2E$' -count=1 -timeout=180s
func TestReconnectProcessE2E(t *testing.T) {
	if os.Getenv("RAINIER_RECONNECT_E2E") != "1" {
		t.Skip("set RAINIER_RECONNECT_E2E=1 for built CLI/sessiond process coverage")
	}
	dir := t.TempDir()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"rainier", "sessiond"} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(dir, name), "./cmd/"+name)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, out)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// sessiond does not expose its bound address for :0; reserve an ephemeral
	// loopback port and release immediately before starting this owned process.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	reservation.Close()
	var sessionLog reconnectE2EBuffer
	script := `trap 'exit 0' TERM HUP; printf '\033[?1000h\033[?1006h'; n=0; while :; do printf 'TICK %d PID %d\n' "$n" "$$"; n=$((n+1)); sleep 0.1; done`
	sessionCmd := exec.Command(filepath.Join(dir, "sessiond"), "-listen", address, "-log", filepath.Join(dir, "session.log"), "--", "/bin/sh", "-c", script)
	sessionCmd.Env = reconnectE2EEnvironment()
	sessionCmd.Stdout, sessionCmd.Stderr = &sessionLog, &sessionLog
	if err := sessionCmd.Start(); err != nil {
		t.Fatal(err)
	}
	sessionDone := make(chan error, 1)
	go func() { sessionDone <- sessionCmd.Wait() }()
	defer func() {
		sessionCmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-sessionDone:
		case <-time.After(7 * time.Second):
			sessionCmd.Process.Kill()
			<-sessionDone
			t.Error("sessiond did not shut down gracefully")
		}
	}()
	backendURL := "ws://" + address + "/attach"
	var observer *websocket.Conn
	for ctx.Err() == nil {
		observer, _, err = websocket.Dial(ctx, backendURL+"?since="+strconv.FormatUint(terminal.SinceAll, 10), nil)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if observer == nil {
		t.Fatalf("sessiond listener: %v\n%s", err, sessionLog.String())
	}
	defer observer.CloseNow()
	if err := wsjson.Write(ctx, observer, terminal.ClientMessage{Type: "resize", Cols: 100, Rows: 30}); err != nil {
		t.Fatal(err)
	}
	var observed reconnectE2EBuffer
	var latestTick atomic.Int64
	latestTick.Store(-1)
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		for {
			var m terminal.ServerMessage
			if wsjson.Read(ctx, observer, &m) != nil {
				return
			}
			observed.Write(m.Data)
			latestTick.Store(int64(reconnectE2ELastTick(observed.String())))
		}
	}()
	defer func() { observer.CloseNow(); <-observerDone }()

	var generation, refreshes, authorized, unauthorized atomic.Int32
	var missedThrough atomic.Int64
	var lastRenderedSeq atomic.Uint64
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/auth/refresh":
			n := refreshes.Add(1)
			var body struct {
				RefreshToken string `json:"refresh_token"`
			}
			if n > 2 || r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil || body.RefreshToken != fmt.Sprintf("refresh_%d_synthetic", n-1) {
				t.Error("fixture received an unexpected/replayed refresh")
				w.WriteHeader(http.StatusForbidden)
				return
			}
			// Leave the client disconnected until the REAL child has produced
			// at least three more numbered lines, which sessiond must replay.
			target := missedThrough.Load()
			for latestTick.Load() < target {
				select {
				case <-ctx.Done():
					w.WriteHeader(http.StatusGatewayTimeout)
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
			json.NewEncoder(w).Encode(cli.TokenPair{AccessToken: fmt.Sprintf("access_%d_synthetic", n), RefreshToken: fmt.Sprintf("refresh_%d_synthetic", n)})
		case "/v0/sessions/sess_example", "/v0/sessions/sess_example/attach":
			if r.Header.Get("Rainier-Workspace") != "ws_example" {
				t.Error("workspace scope was not pinned")
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if r.Header.Get("Authorization") != fmt.Sprintf("Bearer access_%d_synthetic", generation.Load()) {
				unauthorized.Add(1)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if !strings.HasSuffix(r.URL.Path, "/attach") {
				json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_example", State: "running"}})
				return
			}
			interval := authorized.Add(1)
			if interval > 3 {
				t.Error("unexpected extra attach")
				w.WriteHeader(http.StatusForbidden)
				return
			}
			wantSince := terminal.SinceAll
			if interval > 1 {
				wantSince = lastRenderedSeq.Load()
			}
			if r.URL.Query().Get("since") != strconv.FormatUint(wantSince, 10) {
				t.Errorf("replay cursor=%q want %d", r.URL.Query().Get("since"), wantSince)
			}
			up, _, err := websocket.Dial(ctx, backendURL+"?"+r.URL.RawQuery, nil)
			if err != nil {
				t.Errorf("fixture sessiond dial: %v", err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer up.CloseNow()
			down, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer down.CloseNow()
			forwardDone := make(chan struct{})
			go func() {
				defer close(forwardDone)
				defer up.CloseNow()
				for {
					typ, payload, err := down.Read(ctx)
					if err != nil {
						return
					}
					if up.Write(ctx, typ, payload) != nil {
						return
					}
				}
			}()
			defer func() { down.CloseNow(); <-forwardDone }()
			var intervalOutput strings.Builder
			for {
				var m terminal.ServerMessage
				if wsjson.Read(ctx, up, &m) != nil {
					return
				}
				if wsjson.Write(ctx, down, m) != nil {
					return
				}
				if m.Type != "output" && m.Type != "snapshot" {
					continue
				}
				intervalOutput.Write(m.Data)
				tick := reconnectE2ELastTick(intervalOutput.String())
				threshold := 3
				if interval == 2 {
					threshold = 12
				}
				if interval < 3 && tick >= threshold {
					lastRenderedSeq.Store(m.Seq)
					missedThrough.Store(int64(tick + 3))
					generation.Add(1)
					if interval == 1 {
						down.Close(websocket.StatusPolicyViolation, "attach lease expired; reattach")
					} else {
						down.Close(websocket.StatusGoingAway, "synthetic expiry and transport interruption")
					}
					return
				}
			}
		default:
			t.Errorf("unexpected fixture route %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer fixture.Close()
	t.Setenv("RAINIER_CONFIG", filepath.Join(dir, "config.json"))
	cfg := cli.Config{}
	cfg.SetContext("example", cli.Context{Server: fixture.URL, Token: "access_0_synthetic", RefreshToken: "refresh_0_synthetic", Workspace: "ws_example"})
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	if err := pty.Setsize(master, &pty.Winsize{Cols: 100, Rows: 30}); err != nil {
		t.Fatal(err)
	}
	before, err := term.GetState(int(slave.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	// Keep the controlling session leader alive after the CLI returns, as a
	// user's shell does. macOS invalidates slave-tty ioctls when the leader exits.
	// This noninteractive wrapper does not change terminal modes itself.
	clientCmd := exec.Command("/bin/sh", "-c", `"$@"; result=$?; printf '\nLOCAL_CLI_EXIT %d\n' "$result"; IFS= read -r finish; exit "$result"`, "reconnect-e2e-shell", filepath.Join(dir, "rainier"), "attach", "sess_example", "--since", "0")
	clientCmd.Env = append(reconnectE2EEnvironment(), "RAINIER_CONFIG="+filepath.Join(dir, "config.json"), "TERM=xterm-256color")
	clientCmd.Stdin, clientCmd.Stdout, clientCmd.Stderr = slave, slave, slave
	clientCmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	if err := clientCmd.Start(); err != nil {
		t.Fatal(err)
	}
	clientDone := make(chan error, 1)
	go func() { clientDone <- clientCmd.Wait() }()
	clientExited := false
	defer func() {
		if !clientExited {
			// This group was created by this test's Setsid; never target a
			// shared terminal group or another agent's running session.
			syscall.Kill(-clientCmd.Process.Pid, syscall.SIGKILL)
			<-clientDone
		}
	}()
	var screen reconnectE2EBuffer
	readDone := make(chan struct{})
	go func() { defer close(readDone); io.Copy(&screen, master) }()
	defer func() { master.Close(); <-readDone }()
	for reconnectE2ELastTick(screen.String()) < 20 {
		if strings.Contains(screen.String(), "LOCAL_CLI_EXIT") {
			t.Fatalf("CLI returned before recovery:\n%s", screen.String())
		}
		select {
		case err := <-clientDone:
			clientExited = true
			t.Fatalf("CLI exited before recovery: %v\n%s", err, screen.String())
		case <-ctx.Done():
			t.Fatalf("waiting for replay: %v\n%s", ctx.Err(), screen.String())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if _, err := master.Write([]byte{0x1d}); err != nil {
		t.Fatal(err)
	}
	for !strings.Contains(screen.String(), "LOCAL_CLI_EXIT") && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	out := screen.String()
	if !strings.Contains(out, "LOCAL_CLI_EXIT 0") || !strings.Contains(out, "[detached at seq") {
		t.Fatalf("CLI did not detach cleanly:\n%s", out)
	}
	if refreshes.Load() != 2 || authorized.Load() != 3 || unauthorized.Load() != 2 {
		t.Errorf("refreshes=%d successful attaches=%d 401s=%d; want2,3,2", refreshes.Load(), authorized.Load(), unauthorized.Load())
	}
	matches := reconnectE2ETicks.FindAllStringSubmatch(out, -1)
	if len(matches) < 21 {
		t.Fatalf("too few real child output lines: %d", len(matches))
	}
	pid := matches[0][2]
	for i, match := range matches {
		if match[1] != strconv.Itoa(i) || match[2] != pid {
			t.Errorf("missing/duplicate output or restarted child at line%d: %v", i, match)
		}
	}
	lastOutput := reconnectE2ETicks.FindAllStringIndex(out, -1)
	for _, reset := range []string{"\x1b[?1000l", "\x1b[?1006l"} {
		if strings.LastIndex(out, reset) < lastOutput[len(lastOutput)-1][1] {
			t.Errorf("detach failed to clean terminal mode %q", reset)
		}
	}
	after, err := term.GetState(int(slave.Fd()))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Errorf("local terminal state was not restored: %v", err)
	}
	if _, err := master.Write([]byte("\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-clientDone:
		clientExited = true
		if err != nil {
			t.Fatalf("local shell exit: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("local shell did not exit")
	}
	lastLocal := reconnectE2ELastTick(out)
	for latestTick.Load() < int64(lastLocal+3) && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("real child did not continue after CLI detach")
	}
	saved, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	if saved.Contexts["example"].Token != "access_2_synthetic" || saved.Contexts["example"].Workspace != "ws_example" {
		t.Error("final hosted credentials/scope not saved")
	}
	t.Logf("real child PID remained stable through 2 expiries; %d contiguous output lines rendered once; continued after detach; synthetic auth fixture only", len(matches))
}

var reconnectE2ETicks = regexp.MustCompile(`TICK ([0-9]+) PID ([0-9]+)\r?\n`)

func reconnectE2ELastTick(s string) int {
	matches := reconnectE2ETicks.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return -1
	}
	n, _ := strconv.Atoi(matches[len(matches)-1][1])
	return n
}

type reconnectE2EBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *reconnectE2EBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *reconnectE2EBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

func reconnectE2EEnvironment() []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "RAINIER_") {
			env = append(env, entry)
		}
	}
	return env
}
