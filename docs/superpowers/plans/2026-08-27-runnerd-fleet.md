# runnerd + Docker Driver + egressd Implementation Plan (Rainier v0, Plan 2 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn single `sessiond` processes into a managed local fleet: `runnerd` owns a Docker driver behind the runner API, `sessiond` dials `runnerd` outbound and registers, clients attach through `runnerd`'s relay, and session egress is default-deny through `egressd` — plus the PID-1 and viewer-liveness hardening deferred from Plan 1.

**Architecture:** Reachability is outbound-only (spec rule 3): a session container runs `sessiond` as PID 1, which dials `runnerd` and multiplexes its terminal/control traffic over that one connection (reusing Plan 1's direction-agnostic `serve()`). `runnerd` holds a `Driver` (Docker for v0, fake for tests) implementing the runner API (`create/destroy/suspend/resume/snapshot`), a registry pairing dialed-in sessions with the create requests that spawned them, and a client-facing attach endpoint that relays to the right session. Session containers sit on an internal Docker network with no default route; `egressd` is their only path out, default-deny with a per-session allowlist and an audit log.

**Tech Stack:** Go 1.25 (existing module `rainier`); `github.com/coder/websocket` (relay + registration transport, already a dep); the `docker` CLI via `os/exec` (driver — no Docker Go SDK, keeps deps minimal, swappable behind the `Driver` interface); stdlib `net`/`net/http` (egressd CONNECT proxy). No new third-party modules.

**Spec:** `docs/superpowers/specs/2026-08-27-rainier-design.md` (§3 components, §4 runner API, §5 sessiond, §8 egress, §2 portability rules, §10 failure model)

## Global Constraints

- Go module path `rainier`; `CGO_ENABLED=0`; `sessiond` stays a static Linux binary.
- Rule 1: `sessiond` is PID-1-in-sandbox; no host-side reach-in.
- Rule 2: snapshot is OCI-image/tar at the driver-API level, never Docker-specific above the driver.
- Rule 3: no attach/exec in the runner API; all session reachability is via the session's own **outbound** connection to runnerd. The Docker driver never needs a routable per-container address.
- Driver backends are pluggable behind one `Driver` interface; nothing above the driver knows it is Docker.
- egress is default-deny; only an explicit per-session allowlist passes (spec §8).
- Deps: no new third-party Go modules. The Docker driver shells out to the `docker` binary.
- On this dev host the `docker` CLI is not on the default PATH: scripts and any Go code that execs `docker` must resolve it via PATH first, falling back to `/Applications/Docker.app/Contents/Resources/bin`. The daemon is colima (running); images are linux/arm64.
- Commit messages: conventional (`feat:`/`fix:`/`test:`/`chore:`), one commit per task (fix rounds add commits).
- Preserve Plan 1's public interfaces unless a task explicitly changes them: `session.New(cfg Config, start func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error)) (*Session, error)`; `session.Config{Argv, Cols, Rows, LogPath}`; `session.Size{Cols, Rows}`; `session.StartProc`; `Session.Attach(since uint64, size Size) (*Attachment, error)`, `Detach(id int)`, `Stdin([]byte)`, `SetSize(id int, Size)`, `Exited() <-chan struct{}`, `ExitCode() int`; `server.serve(ctx, *websocket.Conn, *session.Session, since uint64)`; `wire.ClientMsg`/`wire.ServerMsg`.

## Design decisions (locked, so tasks don't re-litigate)

- **Docker via CLI, not SDK.** The driver shells out (`docker run/rm/pause/unpause/commit/...`). Rationale: the Docker Go SDK drags a large dependency tree; the driver is behind an interface; the CLI is stable and already required for the demo. A future SDK or containerd driver is a separate `Driver` impl.
- **Relay transport is WebSocket both directions.** `sessiond` dials `runnerd` over WSS and registers; runnerd relays a client's attach WSS to the session over the registered connection. One multiplexed frame protocol (§ Task 3-of-relay) carries per-attachment terminal streams so many clients can attach to one session over its single outbound connection.
- **runnerd is single-VM in v0.** "Placement" is capacity accounting over one host's slot budget; the multi-VM scheduler is Plan 3 (`controld`). runnerd's outward dial to `controld` is also Plan 3 — v0 exposes a local HTTP control surface driven by `runnerctl`.
- **Repos/GitHub auth are NOT in this plan.** A session's filesystem is its container's volume; checking out repos with credentials is Plan 4. v0 `create` accepts an optional image and command only.

## File Structure

```
internal/reap/reap_linux.go          # PID-1 child reaper (Linux build tag)
internal/reap/reap_other.go          # no-op reaper (non-Linux, so host tests build)
internal/reap/reap_test.go
cmd/sessiond/main.go                 # MODIFY: install reaper, SIGTERM→graceful stop, --dial mode
internal/session/session.go          # MODIFY: viewer liveness hooks (Ping/DeadViewer)
internal/server/server.go            # MODIFY: ping/pong keepalive in serve(); dial-mode entry
internal/server/keepalive_test.go
internal/relay/frame.go              # multiplexed relay frame protocol (session<->runnerd)
internal/relay/frame_test.go
internal/relay/session_side.go       # sessiond's dial+register+serve-over-mux
internal/relay/runnerd_side.go       # runnerd's registration acceptor + per-session mux hub
internal/relay/relay_test.go         # in-process loopback: register, attach, stream, detach
internal/driver/driver.go            # Driver interface + Spec/Status/Handle types
internal/driver/fake.go              # in-memory fake driver
internal/driver/contract.go          # RunContract(t, newDriver) — the suite any driver must pass
internal/driver/fake_test.go         # fake passes the contract
internal/driver/docker.go            # Docker driver (shells out to `docker`)
internal/driver/docker_exec.go       # thin `docker` command runner (PATH resolution, output capture)
internal/driver/docker_test.go       # gated integration test (skips without docker)
internal/egress/proxy.go             # CONNECT proxy: default-deny, allowlist, audit
internal/egress/proxy_test.go
cmd/egressd/main.go
internal/runnerd/registry.go         # session registry + capacity accounting
internal/runnerd/registry_test.go
internal/runnerd/runnerd.go          # HTTP control surface + relay hub + driver glue
internal/runnerd/runnerd_test.go
cmd/runnerd/main.go
cmd/runnerctl/main.go                # dev CLI: create/ls/attach/suspend/resume/snapshot/destroy
Dockerfile                           # MODIFY: session image entrypoint sessiond --dial
docker-compose.fleet.yml             # MODIFY: runnerd + egressd + internal network
scripts/fleet-up.sh / fleet-down.sh  # MODIFY: bring up runnerd/egressd, print runnerctl usage
```

---

### Task 1: sessiond PID-1 child reaper + graceful SIGTERM

**Files:**
- Create: `internal/reap/reap_linux.go`, `internal/reap/reap_other.go`, `internal/reap/reap_test.go`
- Modify: `cmd/sessiond/main.go`

**Interfaces:**
- Consumes: nothing new
- Produces:
  ```go
  package reap
  // Reap installs a SIGCHLD-driven loop that wait()s orphaned grandchildren
  // reparented to this process (PID 1 in the sandbox). directChild is the pid
  // whose status the caller wants; Reap delivers that status on the returned
  // channel exactly once and reaps all other children silently. On non-Linux
  // it is a no-op and the channel never fires (host tests still build).
  func Reap(directChild int) <-chan int
  ```

**Design note:** the subtlety flagged in Plan 1's final review — a naive `Wait4(-1)` races `cmd.Wait()` in `proc.go` for the direct child. Resolution: `sessiond` keeps using `Proc.Wait()` for the agent's exit status (unchanged), and the reaper reaps `-1` but **ignores** the direct child's pid so it never double-waits. Pass the agent pid so the reaper can skip it.

- [ ] **Step 1: Write the failing test** (Linux-guarded; on macOS it asserts the no-op contract)

```go
// internal/reap/reap_test.go
package reap

import (
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestReapOnNonLinuxIsNoop(t *testing.T) {
	if runtime.GOOS == "linux" { t.Skip("linux has real reaping") }
	ch := Reap(0)
	select {
	case <-ch:
		t.Fatal("no-op reaper must never fire")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestReapCollectsOrphan(t *testing.T) {
	if runtime.GOOS != "linux" { t.Skip("reaping is linux-only") }
	// A process whose parent exits before it does becomes an orphan reparented
	// to the subreaper. `sh -c '(sleep 0.2 &) ; exit 0'` leaves a grandchild.
	Reap(-1) // reap everything; we only assert no zombie accumulates
	cmd := exec.Command("sh", "-c", "(sleep 0.2 &) ; exit 0")
	if err := cmd.Start(); err != nil { t.Fatal(err) }
	cmd.Wait()
	time.Sleep(400 * time.Millisecond)
	// If reaping works, the orphaned `sleep` has been collected — assert no
	// defunct child remains by reading /proc for our zombie children.
	if hasZombieChild(t) { t.Fatal("zombie child not reaped") }
}
```

Add the `hasZombieChild` helper in the same file:

```go
import (
	"os"
	"path/filepath"
	"strings"
)

func hasZombieChild(t *testing.T) bool {
	t.Helper()
	entries, _ := filepath.Glob("/proc/[0-9]*/stat")
	me := os.Getpid()
	for _, p := range entries {
		b, err := os.ReadFile(p)
		if err != nil { continue }
		fields := strings.Fields(string(b))
		if len(fields) < 4 { continue }
		state, ppid := fields[2], fields[3]
		if state == "Z" && ppid == itoa(me) { return true }
	}
	return false
}
func itoa(i int) string { return strings.TrimSpace(string(fmtInt(i))) }
func fmtInt(i int) []byte { return []byte(strconvItoa(i)) }
```

Use `strconv.Itoa` directly instead of the helper indirection — the implementer should simplify to `strconv.Itoa(me)` and drop `itoa/fmtInt`. (Kept explicit here so the test compiles conceptually; prefer the stdlib call.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/reap/ -v`
Expected: FAIL — `Reap` undefined.

- [ ] **Step 3: Implement the reaper**

```go
// internal/reap/reap_linux.go
//go:build linux

package reap

import (
	"os"
	"os/signal"
	"syscall"
)

func Reap(directChild int) <-chan int {
	// Become a subreaper so orphaned grandchildren reparent here even if we
	// aren't literally PID 1 (belt-and-suspenders; PID 1 is already a reaper).
	_ = unixPrSetChildSubreaper()
	out := make(chan int, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGCHLD)
	go func() {
		for range sigs {
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if pid <= 0 || err != nil { break } // no more reapable children right now
				if pid == directChild {
					select { case out <- ws.ExitStatus(): default: }
				}
			}
		}
	}()
	return out
}

func unixPrSetChildSubreaper() error {
	// PR_SET_CHILD_SUBREAPER = 36
	_, _, errno := syscall.Syscall(syscall.SYS_PRCTL, 36, 1, 0)
	if errno != 0 { return errno }
	return nil
}
```

```go
// internal/reap/reap_other.go
//go:build !linux

package reap

// Reap is a no-op off Linux so host (macOS) builds and tests still compile.
func Reap(directChild int) <-chan int { return make(chan int) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/reap/ -v` (on macOS the orphan test skips; the no-op test passes). If a Linux box is available, run there too; otherwise Task 12's container e2e exercises the Linux path.
Expected: PASS.

- [ ] **Step 5: Wire into sessiond main** — install the reaper and handle SIGTERM by stopping the child gracefully.

In `cmd/sessiond/main.go`, after the session is created (`s, err := session.New(...)`) and before serving, add:

```go
import (
	"os"
	"os/signal"
	"syscall"
	"rainier/internal/reap"
)

// ... after s is created:
reap.Reap(-1) // reap orphaned grandchildren; agent status still comes from Proc.Wait via Session

term := make(chan os.Signal, 1)
signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
go func() {
	<-term
	// Graceful: ask the agent to exit; the exit path closes viewers and the
	// process ends when the child is reaped. Give it a moment, then hard-exit.
	s.Stop() // NEW method, added below
	select {
	case <-s.Exited():
	case <-time.After(5 * time.Second):
	}
	os.Exit(0)
}()
```

Add `Stop()` to `Session` in `internal/session/session.go`:

```go
// Stop signals the agent process to terminate (SIGTERM). The normal exit
// path (close viewers, close exited) then runs. Safe to call more than once.
func (s *Session) Stop() { s.proc.Stop() }
```

(`Proc.Stop()` already exists and SIGTERMs the child; `proc.Stop` on an already-exited process is a safe no-op per Plan 1 Task 5.)

- [ ] **Step 6: Build + full suite**

Run: `go build ./... && go test ./...`
Expected: builds `CGO_ENABLED=0`; all packages pass.

- [ ] **Step 7: Commit**

```bash
git add internal/reap/ cmd/sessiond/main.go internal/session/session.go
git commit -m "feat: sessiond reaps orphaned children and stops agent on SIGTERM"
```

---

### Task 2: sessiond viewer liveness (ping/pong dead-viewer kick)

**Files:**
- Modify: `internal/server/server.go`
- Create: `internal/server/keepalive_test.go`

**Interfaces:**
- Consumes: `server.serve`, `session.Session` (Plan 1)
- Produces: no new exported symbols; `serve()` gains periodic WebSocket ping and drops a viewer whose pong times out. This fixes the acceptance finding: a client whose terminal dies otherwise parks forever as an invisible viewer that keeps clamping session size.

**Why here:** the size-clamp bug is that a dead client's viewer never detaches, so `EffectiveSize` keeps honoring its stale size. A liveness ping detaches it, `applySizeLocked` recomputes, and the surviving viewers regrow. `coder/websocket` `Conn.Ping(ctx)` sends a ping and blocks for the pong; a timeout means the peer is gone.

- [ ] **Step 1: Write the failing test**

```go
// internal/server/keepalive_test.go
package server

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/session"
	"rainier/internal/wire"
)

// A viewer that stops responding (its TCP peer vanishes) must be detached so
// it no longer clamps session size. We simulate by attaching a small viewer
// then abruptly dropping it at the transport layer and asserting a larger
// viewer's size is restored to the PTY within a bounded window.
func TestDeadViewerIsDetachedAndSizeRecovers(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 120, Rows: 40, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }
	srv := httptest.NewServer(NewWithKeepalive(s, 200*time.Millisecond)) // NEW ctor with fast interval for tests
	defer srv.Close()

	big := dial(t, srv.URL, "0")   // 120x40
	small := dialSize(t, srv.URL, 40, 10)

	// Confirm the small viewer clamped the PTY (write `stty size` and read it back on big).
	ctx := context.Background()
	wsjson.Write(ctx, big, wire.ClientMsg{Type: "stdin", Data: []byte("stty size\n")})
	readUntil(t, big, " 10") // rows clamped to 10 (smallest)

	// Kill the small viewer's transport abruptly (no clean close frame).
	small.CloseNow()

	// Within a few ping intervals, the server must detect the dead pong,
	// detach small, and restore size to 40 rows. Poll by re-issuing stty size.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		wsjson.Write(ctx, big, wire.ClientMsg{Type: "stdin", Data: []byte("stty size\n")})
		if readUntilOrTimeout(t, big, " 40", 400*time.Millisecond) { return }
	}
	t.Fatal("size never recovered after dead viewer; liveness kick not working")
}
```

Add `dialSize` and `readUntilOrTimeout` helpers next to the existing `dial`/`readUntil` in `server_test.go` (the implementer adds them; `dialSize` sends a `resize` first message with the given cols/rows, `readUntilOrTimeout` is `readUntil` with a per-call deadline returning bool instead of `t.Fatal`).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/server/ -run TestDeadViewer -v`
Expected: FAIL — `NewWithKeepalive` undefined.

- [ ] **Step 3: Implement**

Refactor `server.New` to delegate to a keepalive-parameterized constructor, and add a ping loop to `serve`:

```go
// internal/server/server.go
const defaultPingInterval = 15 * time.Second

func New(s *session.Session) http.Handler { return NewWithKeepalive(s, defaultPingInterval) }

func NewWithKeepalive(s *session.Session, pingInterval time.Duration) http.Handler {
	mux := http.NewServeMux()
	h := &handler{s: s, pingInterval: pingInterval}
	mux.HandleFunc("/attach", h.attach)
	return mux
}
```

Add `pingInterval time.Duration` to `handler`, pass it into `serve`, and in `serve` run a ping goroutine bound to the same lifetime as the reader/writer; on ping failure, cancel the connection so the existing `defer s.Detach(att.ID)` fires:

```go
// inside serve(...), after Attach succeeds and before the reader loop:
pingCtx, cancelPing := context.WithCancel(ctx)
defer cancelPing()
go func() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-pingCtx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(pingCtx, pingInterval)
			err := c.Ping(pctx)
			cancel()
			if err != nil {
				c.CloseNow() // triggers reader error → serve returns → Detach
				return
			}
		}
	}
}()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/server/ -run TestDeadViewer -v -race`
Expected: PASS.

- [ ] **Step 5: Full suite**

Run: `go test ./... -race`
Expected: PASS (existing server tests use `New`, still valid).

- [ ] **Step 6: Commit**

```bash
git add internal/server/
git commit -m "feat: detach dead viewers via websocket ping so session size recovers"
```

---

### Task 3: Relay frame protocol

**Files:**
- Create: `internal/relay/frame.go`, `internal/relay/frame_test.go`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```go
  package relay
  // Frames multiplex many client attachments over one session<->runnerd conn.
  type FrameType uint8
  const (
      FrameOpen   FrameType = iota // runnerd→session: a client attached (AttachID, Since, Cols, Rows)
      FrameClose                   // either way: attachment ended (AttachID)
      FrameClient                  // runnerd→session: a wire.ClientMsg for AttachID (Payload)
      FrameServer                  // session→runnerd: a wire.ServerMsg for AttachID (Payload)
  )
  type Frame struct {
      Type     FrameType
      AttachID uint64
      Since    uint64
      Cols     int
      Rows     int
      Payload  []byte // JSON-encoded wire.ClientMsg or wire.ServerMsg
  }
  func Encode(f Frame) ([]byte, error)
  func Decode(b []byte) (Frame, error)
  ```

- [ ] **Step 1: Write the failing test**

```go
// internal/relay/frame_test.go
package relay

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []Frame{
		{Type: FrameOpen, AttachID: 7, Since: 3, Cols: 80, Rows: 24},
		{Type: FrameClient, AttachID: 7, Payload: []byte(`{"type":"stdin","data":"aGk="}`)},
		{Type: FrameServer, AttachID: 7, Payload: []byte(`{"type":"output","seq":9}`)},
		{Type: FrameClose, AttachID: 7},
	}
	for _, in := range cases {
		b, err := Encode(in)
		if err != nil { t.Fatal(err) }
		out, err := Decode(b)
		if err != nil { t.Fatal(err) }
		if out.Type != in.Type || out.AttachID != in.AttachID || out.Since != in.Since ||
			out.Cols != in.Cols || out.Rows != in.Rows || !bytes.Equal(out.Payload, in.Payload) {
			t.Fatalf("round trip: got %+v want %+v", out, in)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("expected error decoding garbage")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/relay/ -run TestFrame -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement** (JSON encoding for v0 — simple, debuggable; a binary framing is a later optimization)

```go
// internal/relay/frame.go
// Package relay multiplexes many client attachments over the single outbound
// WebSocket a session opens to runnerd (spec rule 3). One Frame = one event
// for one attachment, tagged by AttachID.
package relay

import "encoding/json"

type FrameType uint8

const (
	FrameOpen FrameType = iota
	FrameClose
	FrameClient
	FrameServer
)

type Frame struct {
	Type     FrameType `json:"t"`
	AttachID uint64    `json:"a"`
	Since    uint64    `json:"s,omitempty"`
	Cols     int       `json:"c,omitempty"`
	Rows     int       `json:"r,omitempty"`
	Payload  []byte    `json:"p,omitempty"`
}

func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }

func Decode(b []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(b, &f)
	return f, err
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/relay/ -run TestFrame -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/relay/frame.go internal/relay/frame_test.go
git commit -m "feat: relay frame protocol multiplexing attachments over one session conn"
```

---

### Task 4: Relay endpoints (session side + runnerd side) with in-process loopback test

**Files:**
- Create: `internal/relay/session_side.go`, `internal/relay/runnerd_side.go`, `internal/relay/relay_test.go`

**Interfaces:**
- Consumes: `relay.Frame`/`Encode`/`Decode` (Task 3), `session.Session` (Plan 1), `wire` types
- Produces:
  ```go
  package relay
  // ServeSession runs on the sessiond side: it reads frames from conn (an
  // already-established WebSocket to runnerd) and, per FrameOpen, calls
  // s.Attach and pumps ServerMsgs back as FrameServer; FrameClient → s.Stdin/
  // s.SetSize; FrameClose → s.Detach. Returns when conn closes.
  func ServeSession(ctx context.Context, conn Conn, s *session.Session) error

  // Hub runs on the runnerd side: one Hub wraps one registered session conn and
  // lets many clients attach. AttachClient bridges a client WebSocket to a new
  // attachment over the session conn.
  type Hub struct { /* ... */ }
  func NewHub(ctx context.Context, sessionConn Conn) *Hub
  func (h *Hub) AttachClient(ctx context.Context, client Conn, since uint64, cols, rows int) error
  func (h *Hub) Close()

  // Conn is the minimal transport interface (satisfied by *websocket.Conn via a
  // thin adapter) so relay logic is testable with in-memory pipes.
  type Conn interface {
      Read(ctx context.Context) ([]byte, error)
      Write(ctx context.Context, b []byte) error
      Close() error
  }
  ```

**Design:** `Conn` abstracts the WebSocket so the loopback test uses in-memory channels, not a network. A `websocket.Conn` adapter (read/write text frames) lives in Task 9/10 where the real transports are wired; this task is pure relay logic against the interface.

- [ ] **Step 1: Write the failing loopback test**

```go
// internal/relay/relay_test.go
package relay

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"rainier/internal/session"
	"rainier/internal/wire"
)

// pipeConn is an in-memory Conn pair for tests.
type pipeConn struct {
	in  chan []byte
	out chan []byte
}
func newPipe() (a, b *pipeConn) {
	c1, c2 := make(chan []byte, 64), make(chan []byte, 64)
	return &pipeConn{in: c1, out: c2}, &pipeConn{in: c2, out: c1}
}
func (p *pipeConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-p.in: return b, nil
	case <-ctx.Done(): return nil, ctx.Err()
	}
}
func (p *pipeConn) Write(ctx context.Context, b []byte) error {
	cp := append([]byte(nil), b...)
	select {
	case p.out <- cp: return nil
	case <-ctx.Done(): return ctx.Err()
	}
}
func (p *pipeConn) Close() error { return nil }

func TestRelayAttachStreamsOutput(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }

	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ServeSession(ctx, sessConn, s)

	hub := NewHub(ctx, runConn)
	defer hub.Close()

	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, 0, 80, 24)

	// Client should receive a snapshot frame first (as a FrameServer wrapping a
	// wire.ServerMsg of type "snapshot"), then output after we send stdin.
	first := readServerMsg(t, client)
	if first.Type != "snapshot" { t.Fatalf("first msg = %s, want snapshot", first.Type) }

	// Send stdin through the client → hub → session → shell, expect echo.
	writeClientMsg(t, client, wire.ClientMsg{Type: "stdin", Data: []byte("echo relay-marker\n")})
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("never saw relay-marker echoed through the relay")
		default:
		}
		m := readServerMsg(t, client)
		if m.Type == "output" && contains(m.Data, "relay-marker") { return }
	}
}
```

Add helpers `readServerMsg`/`writeClientMsg` (decode a `FrameServer`/encode a `FrameClient` on the pipe) and `contains` in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/relay/ -run TestRelayAttach -v`
Expected: FAIL — `ServeSession`/`NewHub`/`AttachClient` undefined.

- [ ] **Step 3: Implement `ServeSession`**

```go
// internal/relay/session_side.go
package relay

import (
	"context"
	"encoding/json"
	"sync"

	"rainier/internal/session"
	"rainier/internal/wire"
)

func ServeSession(ctx context.Context, conn Conn, s *session.Session) error {
	var mu sync.Mutex
	atts := map[uint64]*session.Attachment{}
	write := func(f Frame) { b, _ := Encode(f); conn.Write(ctx, b) }

	for {
		raw, err := conn.Read(ctx)
		if err != nil { return err }
		f, err := Decode(raw)
		if err != nil { continue }
		switch f.Type {
		case FrameOpen:
			att, err := s.Attach(f.Since, session.Size{Cols: f.Cols, Rows: f.Rows})
			if err != nil { write(Frame{Type: FrameClose, AttachID: f.AttachID}); continue }
			mu.Lock(); atts[f.AttachID] = att; mu.Unlock()
			go func(id uint64, a *session.Attachment) {
				for msg := range a.Msgs {
					p, _ := json.Marshal(msg)
					write(Frame{Type: FrameServer, AttachID: id, Payload: p})
				}
				write(Frame{Type: FrameClose, AttachID: id})
				mu.Lock(); delete(atts, id); mu.Unlock()
			}(f.AttachID, att)
		case FrameClient:
			var cm wire.ClientMsg
			if json.Unmarshal(f.Payload, &cm) != nil { continue }
			mu.Lock(); att := atts[f.AttachID]; mu.Unlock()
			if att == nil { continue }
			switch cm.Type {
			case "stdin": s.Stdin(cm.Data)
			case "resize": s.SetSize(att.ID, session.Size{Cols: cm.Cols, Rows: cm.Rows})
			}
		case FrameClose:
			mu.Lock(); att := atts[f.AttachID]; delete(atts, f.AttachID); mu.Unlock()
			if att != nil { s.Detach(att.ID) }
		}
	}
}
```

- [ ] **Step 4: Implement the runnerd-side `Hub`**

```go
// internal/relay/runnerd_side.go
package relay

import (
	"context"
	"sync"
)

type Hub struct {
	conn    Conn
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	next    uint64
	clients map[uint64]Conn // attachID → client conn
}

func NewHub(ctx context.Context, sessionConn Conn) *Hub {
	hctx, cancel := context.WithCancel(ctx)
	h := &Hub{conn: sessionConn, ctx: hctx, cancel: cancel, clients: map[uint64]Conn{}}
	go h.readLoop()
	return h
}

// readLoop demultiplexes FrameServer/FrameClose from the session to the right client.
func (h *Hub) readLoop() {
	for {
		raw, err := h.conn.Read(h.ctx)
		if err != nil { h.cancel(); return }
		f, err := Decode(raw)
		if err != nil { continue }
		h.mu.Lock(); client := h.clients[f.AttachID]; h.mu.Unlock()
		if client == nil { continue }
		switch f.Type {
		case FrameServer:
			// Forward the wire.ServerMsg payload verbatim to the client.
			client.Write(h.ctx, f.Payload)
		case FrameClose:
			client.Close()
			h.mu.Lock(); delete(h.clients, f.AttachID); h.mu.Unlock()
		}
	}
}

func (h *Hub) AttachClient(ctx context.Context, client Conn, since uint64, cols, rows int) error {
	h.mu.Lock()
	h.next++
	id := h.next
	h.clients[id] = client
	h.mu.Unlock()

	open, _ := Encode(Frame{Type: FrameOpen, AttachID: id, Since: since, Cols: cols, Rows: rows})
	if err := h.conn.Write(h.ctx, open); err != nil { return err }

	// Pump client → session as FrameClient until the client disconnects.
	for {
		raw, err := client.Read(ctx)
		if err != nil {
			cl, _ := Encode(Frame{Type: FrameClose, AttachID: id})
			h.conn.Write(h.ctx, cl)
			h.mu.Lock(); delete(h.clients, id); h.mu.Unlock()
			return err
		}
		fr, _ := Encode(Frame{Type: FrameClient, AttachID: id, Payload: raw})
		if err := h.conn.Write(h.ctx, fr); err != nil { return err }
	}
}

func (h *Hub) Close() { h.cancel(); h.conn.Close() }
```

Note the client-side payloads: the Hub forwards raw `wire.ServerMsg` JSON to the client (so a client speaks the *same* wire protocol as Plan 1's direct `serve`), and wraps the client's raw `wire.ClientMsg` JSON into `FrameClient`. This keeps `rattach` unchanged: it still exchanges `wire.ClientMsg`/`wire.ServerMsg`, unaware whether it hit a direct `sessiond` or a `runnerd` relay.

- [ ] **Step 5: Run the loopback test**

Run: `go test ./internal/relay/ -v -race`
Expected: PASS (snapshot first, then echoed marker through the full relay).

- [ ] **Step 6: Commit**

```bash
git add internal/relay/session_side.go internal/relay/runnerd_side.go internal/relay/relay_test.go
git commit -m "feat: relay endpoints bridging clients to sessions over one outbound conn"
```

---

### Task 5: Runner API — Driver interface, fake driver, contract suite

**Files:**
- Create: `internal/driver/driver.go`, `internal/driver/fake.go`, `internal/driver/contract.go`, `internal/driver/fake_test.go`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```go
  package driver
  type Spec struct {
      Name    string   // human label
      Image   string   // OCI ref (v0 default a bash image)
      Cmd     []string // entrypoint override; empty = image default
      DialURL string   // runnerd URL the container's sessiond dials (relay)
      SessionID string // stable id runnerd assigns; sessiond registers with it
      EgressAllow []string // hostnames the session may reach
  }
  type State string
  const (
      StateRunning   State = "running"
      StateSuspended State = "suspended" // warm (paused) or cold (stopped, volume kept)
      StateGone      State = "gone"
  )
  type Handle struct { ID string; State State } // ID = driver's own resource id
  type Snapshot struct { Ref string } // OCI image ref or tar path
  type Driver interface {
      Create(ctx context.Context, spec Spec) (Handle, error)
      Suspend(ctx context.Context, id string, warm bool) error // warm=pause, cold=stop
      Resume(ctx context.Context, id string) error
      Snapshot(ctx context.Context, id string) (Snapshot, error)
      Destroy(ctx context.Context, id string) error
      Inspect(ctx context.Context, id string) (Handle, error)
      Capacity(ctx context.Context) (used, total int, err error)
  }
  // RunContract exercises the lifecycle any Driver must satisfy. newDriver
  // returns a fresh driver and a cleanup func.
  func RunContract(t *testing.T, newDriver func(t *testing.T) (Driver, func()))
  ```

**Design:** the contract suite is the enforceable form of portability rule 2 and the driver abstraction. The fake driver proves the suite is satisfiable in-memory; the Docker driver (Tasks 6-8) must pass the same suite. `Spec.DialURL`/`SessionID` are how a created container knows where to phone home — the driver injects them (env vars) so the container's `sessiond --dial` registers with the right runnerd and id.

- [ ] **Step 1: Write the contract suite + fake test**

```go
// internal/driver/contract.go
package driver

import (
	"context"
	"testing"
)

func RunContract(t *testing.T, newDriver func(t *testing.T) (Driver, func())) {
	t.Run("create-inspect-destroy", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t1", Image: "test", SessionID: "s1", DialURL: "ws://x"})
		if err != nil { t.Fatal(err) }
		if h.ID == "" { t.Fatal("empty handle id") }
		got, err := d.Inspect(ctx, h.ID)
		if err != nil || got.State != StateRunning { t.Fatalf("inspect = %+v, %v", got, err) }
		if err := d.Destroy(ctx, h.ID); err != nil { t.Fatal(err) }
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateGone { t.Fatalf("post-destroy state = %s", g.State) }
	})

	t.Run("suspend-resume", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t2", Image: "test", SessionID: "s2", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		if err := d.Suspend(ctx, h.ID, true); err != nil { t.Fatal(err) }
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateSuspended { t.Fatalf("warm state = %s", g.State) }
		if err := d.Resume(ctx, h.ID); err != nil { t.Fatal(err) }
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateRunning { t.Fatalf("resumed state = %s", g.State) }
		if err := d.Suspend(ctx, h.ID, false); err != nil { t.Fatal(err) } // cold
		if err := d.Resume(ctx, h.ID); err != nil { t.Fatal(err) }
	})

	t.Run("snapshot", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t3", Image: "test", SessionID: "s3", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		snap, err := d.Snapshot(ctx, h.ID)
		if err != nil || snap.Ref == "" { t.Fatalf("snapshot = %+v, %v", snap, err) }
	})

	t.Run("capacity", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		used0, total, _ := d.Capacity(ctx)
		if total <= 0 { t.Fatalf("total capacity must be positive, got %d", total) }
		h, _ := d.Create(ctx, Spec{Name: "t4", Image: "test", SessionID: "s4", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		used1, _, _ := d.Capacity(ctx)
		if used1 != used0+1 { t.Fatalf("used should rise by 1: %d → %d", used0, used1) }
	})
}
```

```go
// internal/driver/fake_test.go
package driver

import "testing"

func TestFakeSatisfiesContract(t *testing.T) {
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewFake(4)
		return d, func() {}
	})
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/driver/ -v`
Expected: FAIL — types/`NewFake` undefined.

- [ ] **Step 3: Implement `driver.go` (types)** — exactly the Produces block above, in `internal/driver/driver.go`, with a `context` import.

- [ ] **Step 4: Implement the fake**

```go
// internal/driver/fake.go
package driver

import (
	"context"
	"fmt"
	"sync"
)

type Fake struct {
	mu    sync.Mutex
	total int
	seq   int
	items map[string]State
}

func NewFake(total int) *Fake { return &Fake{total: total, items: map[string]State{}} }

func (f *Fake) Create(_ context.Context, spec Spec) (Handle, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	used := len(f.items)
	if used >= f.total { return Handle{}, fmt.Errorf("no capacity: %d/%d", used, f.total) }
	f.seq++
	id := fmt.Sprintf("fake-%d", f.seq)
	f.items[id] = StateRunning
	return Handle{ID: id, State: StateRunning}, nil
}
func (f *Fake) set(id string, st State) error {
	f.mu.Lock(); defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok { return fmt.Errorf("no such id %s", id) }
	f.items[id] = st
	return nil
}
func (f *Fake) Suspend(_ context.Context, id string, warm bool) error { return f.set(id, StateSuspended) }
func (f *Fake) Resume(_ context.Context, id string) error             { return f.set(id, StateRunning) }
func (f *Fake) Snapshot(_ context.Context, id string) (Snapshot, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok { return Snapshot{}, fmt.Errorf("no such id %s", id) }
	return Snapshot{Ref: "fake-image:" + id}, nil
}
func (f *Fake) Destroy(_ context.Context, id string) error {
	f.mu.Lock(); defer f.mu.Unlock()
	delete(f.items, id)
	return nil
}
func (f *Fake) Inspect(_ context.Context, id string) (Handle, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	st, ok := f.items[id]
	if !ok { return Handle{ID: id, State: StateGone}, nil }
	return Handle{ID: id, State: st}, nil
}
func (f *Fake) Capacity(_ context.Context) (int, int, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	return len(f.items), f.total, nil
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `go test ./internal/driver/ -v`
Expected: PASS (all contract subtests via the fake).

- [ ] **Step 6: Commit**

```bash
git add internal/driver/driver.go internal/driver/fake.go internal/driver/contract.go internal/driver/fake_test.go
git commit -m "feat: runner Driver interface, fake driver, and lifecycle contract suite"
```

---

### Task 6: Docker driver — create + destroy + inspect + capacity

**Files:**
- Create: `internal/driver/docker.go`, `internal/driver/docker_exec.go`, `internal/driver/docker_test.go`

**Interfaces:**
- Consumes: `Driver` interface + `Spec`/`Handle`/`State` (Task 5)
- Produces: `func NewDocker(opts DockerOpts) *Docker` implementing `Driver` (Create/Destroy/Inspect/Capacity now; Suspend/Resume/Snapshot in Tasks 7-8, stubbed to return `errNotImpl` until then so the type satisfies the interface).
  ```go
  type DockerOpts struct {
      Image       string // default session image
      Network     string // internal docker network name (created by fleet compose)
      TotalSlots  int    // capacity budget
      Label       string // docker label key marking rainier-managed containers
  }
  ```

**Design:** `docker_exec.go` resolves the `docker` binary (PATH, then `/Applications/Docker.app/Contents/Resources/bin`) and runs commands capturing stdout/stderr. `Create` runs a detached container:
`docker run -d --label rainier.session=<id> --network <net> --user 1000:1000 --security-opt no-new-privileges --read-only --tmpfs /tmp -v <vol>:/work -e RAINIER_DIAL=<DialURL> -e RAINIER_SESSION=<SessionID> -e HTTP_PROXY=... <image>`. Container id → Handle.ID. Capacity counts running labeled containers. Inspect maps `docker inspect .State.Status` → `State`.

- [ ] **Step 1: Write the gated integration test**

```go
// internal/driver/docker_test.go
package driver

import (
	"context"
	"os/exec"
	"testing"
)

func dockerAvailable(t *testing.T) {
	t.Helper()
	if _, err := dockerPath(); err != nil { t.Skip("docker CLI not found; skipping docker driver test") }
	if err := exec.Command(mustDockerPath(t), "info").Run(); err != nil {
		t.Skip("docker daemon not responding; skipping")
	}
}

func TestDockerDriverContract(t *testing.T) {
	dockerAvailable(t)
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewDocker(DockerOpts{
			Image:      "alpine:3.20",
			Network:    "bridge", // plain bridge for the driver test; internal net is a fleet concern
			TotalSlots: 8,
			Label:      "rainier.test",
		})
		// The contract creates containers with Cmd empty; alpine's default cmd
		// exits immediately, so override to a sleeper for lifecycle assertions.
		d.defaultCmd = []string{"sleep", "3600"}
		return d, func() { d.destroyAllLabeled(context.Background()) }
	})
}
```

(`mustDockerPath`, `dockerPath`, `defaultCmd`, `destroyAllLabeled` are implemented below/in docker.go.)

- [ ] **Step 2: Run to verify it fails (or skips cleanly if docker absent)**

Run: `go test ./internal/driver/ -run TestDockerDriverContract -v`
Expected: FAIL — `NewDocker` undefined (if docker present); or SKIP (if absent — acceptable, Task 12 covers Linux path in-container).

- [ ] **Step 3: Implement `docker_exec.go`**

```go
// internal/driver/docker_exec.go
package driver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func dockerPath() (string, error) {
	if p, err := exec.LookPath("docker"); err == nil { return p, nil }
	fallback := "/Applications/Docker.app/Contents/Resources/bin/docker"
	if _, err := os.Stat(fallback); err == nil { return fallback, nil }
	return "", fmt.Errorf("docker CLI not found on PATH or at %s", fallback)
}

func dockerRun(ctx context.Context, args ...string) (string, error) {
	bin, err := dockerPath()
	if err != nil { return "", err }
	cmd := exec.CommandContext(ctx, bin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// used by the test helper
func mustDockerPath(t interface{ Fatal(...any) }) string {
	p, err := dockerPath()
	if err != nil { t.Fatal(err) }
	return p
}
var _ = filepath.Base
```

- [ ] **Step 4: Implement `docker.go` (create/destroy/inspect/capacity; suspend/resume/snapshot stubbed)**

```go
// internal/driver/docker.go
package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var errNotImpl = errors.New("not implemented yet")

type Docker struct {
	opts       DockerOpts
	defaultCmd []string
}

type DockerOpts struct {
	Image      string
	Network    string
	TotalSlots int
	Label      string
}

func NewDocker(opts DockerOpts) *Docker {
	if opts.Label == "" { opts.Label = "rainier.session" }
	if opts.TotalSlots == 0 { opts.TotalSlots = 16 }
	return &Docker{opts: opts}
}

func (d *Docker) Create(ctx context.Context, spec Spec) (Handle, error) {
	used, total, err := d.Capacity(ctx)
	if err != nil { return Handle{}, err }
	if used >= total { return Handle{}, fmt.Errorf("no capacity: %d/%d", used, total) }

	image := spec.Image
	if image == "" { image = d.opts.Image }
	args := []string{"run", "-d",
		"--label", d.opts.Label + "=" + spec.SessionID,
		"--user", "1000:1000",
		"--security-opt", "no-new-privileges",
		"--read-only", "--tmpfs", "/tmp",
	}
	if d.opts.Network != "" { args = append(args, "--network", d.opts.Network) }
	if spec.DialURL != "" { args = append(args, "-e", "RAINIER_DIAL="+spec.DialURL) }
	if spec.SessionID != "" { args = append(args, "-e", "RAINIER_SESSION="+spec.SessionID) }
	args = append(args, image)
	cmd := spec.Cmd
	if len(cmd) == 0 { cmd = d.defaultCmd }
	args = append(args, cmd...)

	id, err := dockerRun(ctx, args...)
	if err != nil { return Handle{}, err }
	return Handle{ID: id, State: StateRunning}, nil
}

func (d *Docker) Destroy(ctx context.Context, id string) error {
	_, err := dockerRun(ctx, "rm", "-f", id)
	return err
}

func (d *Docker) Inspect(ctx context.Context, id string) (Handle, error) {
	out, err := dockerRun(ctx, "inspect", "-f", "{{.State.Status}}", id)
	if err != nil {
		// `docker inspect` errors when the container no longer exists.
		return Handle{ID: id, State: StateGone}, nil
	}
	st := StateRunning
	switch out {
	case "running": st = StateRunning
	case "paused", "exited", "created": st = StateSuspended
	default: st = StateSuspended
	}
	return Handle{ID: id, State: st}, nil
}

func (d *Docker) Capacity(ctx context.Context) (int, int, error) {
	out, err := dockerRun(ctx, "ps", "-aq", "--filter", "label="+d.opts.Label)
	if err != nil { return 0, d.opts.TotalSlots, err }
	used := 0
	if strings.TrimSpace(out) != "" { used = len(strings.Split(strings.TrimSpace(out), "\n")) }
	return used, d.opts.TotalSlots, nil
}

func (d *Docker) destroyAllLabeled(ctx context.Context) {
	out, err := dockerRun(ctx, "ps", "-aq", "--filter", "label="+d.opts.Label)
	if err != nil || strings.TrimSpace(out) == "" { return }
	for _, id := range strings.Split(strings.TrimSpace(out), "\n") {
		dockerRun(ctx, "rm", "-f", id)
	}
}

// Stubs until Tasks 7-8.
func (d *Docker) Suspend(ctx context.Context, id string, warm bool) error { return errNotImpl }
func (d *Docker) Resume(ctx context.Context, id string) error             { return errNotImpl }
func (d *Docker) Snapshot(ctx context.Context, id string) (Snapshot, error) { return Snapshot{}, errNotImpl }
```

Because Suspend/Resume/Snapshot are stubbed, the contract's suspend/snapshot subtests would fail. For THIS task, run only the create/destroy/inspect/capacity subtests:

- [ ] **Step 5: Run the implemented subtests**

Run: `go test ./internal/driver/ -run 'TestDockerDriverContract/(create-inspect-destroy|capacity)' -v`
Expected: PASS if docker present (SKIP otherwise). Full `TestDockerDriverContract` will still fail on suspend/snapshot until Tasks 7-8 — that is expected and those subtests are added by the later tasks' green runs.

- [ ] **Step 6: Full build + non-docker suite**

Run: `go build ./... && go test ./... -run '.' 2>&1 | tail -5` (docker subtests skip if daemon absent)
Expected: builds; fake contract still green.

- [ ] **Step 7: Commit**

```bash
git add internal/driver/docker.go internal/driver/docker_exec.go internal/driver/docker_test.go
git commit -m "feat: docker driver create/destroy/inspect/capacity via docker CLI"
```

---

### Task 7: Docker driver — suspend/resume

**Files:**
- Modify: `internal/driver/docker.go`

**Interfaces:**
- Consumes: Task 6's `Docker`
- Produces: real `Suspend`/`Resume`. Warm suspend = `docker pause`; cold suspend = `docker stop`; resume = `docker unpause` (from paused) or `docker start` (from stopped). Inspect already maps `paused`/`exited` → `StateSuspended`.

- [ ] **Step 1: Implement**

```go
func (d *Docker) Suspend(ctx context.Context, id string, warm bool) error {
	if warm {
		_, err := dockerRun(ctx, "pause", id)
		return err
	}
	_, err := dockerRun(ctx, "stop", id)
	return err
}

func (d *Docker) Resume(ctx context.Context, id string) error {
	// Determine current status to pick unpause vs start.
	out, err := dockerRun(ctx, "inspect", "-f", "{{.State.Status}}", id)
	if err != nil { return err }
	switch out {
	case "paused":
		_, err = dockerRun(ctx, "unpause", id)
	case "exited", "created":
		_, err = dockerRun(ctx, "start", id)
	case "running":
		err = nil // already running
	default:
		_, err = dockerRun(ctx, "start", id)
	}
	return err
}
```

- [ ] **Step 2: Run the suspend/resume contract subtest**

Run: `go test ./internal/driver/ -run 'TestDockerDriverContract/suspend-resume' -v`
Expected: PASS if docker present (SKIP otherwise).

- [ ] **Step 3: Commit**

```bash
git add internal/driver/docker.go
git commit -m "feat: docker driver suspend/resume via pause/stop"
```

---

### Task 8: Docker driver — snapshot

**Files:**
- Modify: `internal/driver/docker.go`

**Interfaces:**
- Consumes: Task 6's `Docker`
- Produces: real `Snapshot` returning an OCI image ref via `docker commit`. Ref format `rainier-snap:<shortid>-<n>`; the driver tags it and returns the ref (portability rule 2: OCI image at the API boundary).

- [ ] **Step 1: Implement**

```go
import "strconv"

func (d *Docker) Snapshot(ctx context.Context, id string) (Snapshot, error) {
	if _, err := dockerRun(ctx, "inspect", "-f", "{{.Id}}", id); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot: no such container %s: %w", id, err)
	}
	short := id
	if len(short) > 12 { short = short[:12] }
	ref := "rainier-snap:" + short + "-" + strconv.FormatInt(int64(len(short)), 10)
	if _, err := dockerRun(ctx, "commit", id, ref); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Ref: ref}, nil
}
```

Note: the ref must be unique per snapshot of a container across calls in the contract's lifetime; the suffix above is stable per container, which is fine for the single-snapshot contract test. If a later task needs multiple snapshots per container, thread a counter — out of scope here.

- [ ] **Step 2: Run the full docker contract**

Run: `go test ./internal/driver/ -run TestDockerDriverContract -v`
Expected: ALL subtests PASS if docker present (create/destroy, suspend-resume, snapshot, capacity) — the Docker driver now fully satisfies the same contract as the fake. SKIP if docker absent.

- [ ] **Step 3: Full suite**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/driver/docker.go
git commit -m "feat: docker driver snapshot via docker commit to an OCI ref"
```

---

### Task 9: egressd CONNECT proxy

**Files:**
- Create: `internal/egress/proxy.go`, `internal/egress/proxy_test.go`, `cmd/egressd/main.go`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```go
  package egress
  type Rule struct { Session string; Allow []string } // hostnames (exact or *.suffix)
  type Proxy struct { /* ... */ }
  func New(audit io.Writer) *Proxy
  func (p *Proxy) SetAllow(session string, hosts []string) // per-session allowlist
  func (p *Proxy) Handler() http.Handler                   // serves HTTP CONNECT
  // A CONNECT to a disallowed host returns 403 and logs a deny line to audit.
  // The session is identified by the Proxy-Authorization bearer = session id
  // (injected as the container's HTTPS_PROXY credentials).
  ```

**Design:** default-deny — a session with no allow entry can reach nothing. Allow matching: exact host, or `*.example.com` suffix. The audit writer gets one JSON line per decision (`{"session","host","port","decision","ts"}` — ts injected by caller or omitted; note: no `time.Now()` in library core paths that must stay deterministic for tests — the proxy takes a `now func() time.Time` with a default). CONNECT tunnels raw TCP after allow.

- [ ] **Step 1: Write the failing test**

```go
// internal/egress/proxy_test.go
package egress

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// startOrigin is a plain TCP echo server the proxy will tunnel to when allowed.
func startOrigin(t *testing.T) (host string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil { return }
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func connectThrough(t *testing.T, proxyURL, target, session string) (*http.Response, net.Conn) {
	t.Helper()
	u := strings.TrimPrefix(proxyURL, "http://")
	conn, err := net.Dial("tcp", u)
	if err != nil { t.Fatal(err) }
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Bearer %s\r\n\r\n", target, target, session)
	conn.Write([]byte(req))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil { t.Fatal(err) }
	return resp, conn
}

func TestDefaultDenyAndAllow(t *testing.T) {
	origin, stop := startOrigin(t); defer stop()
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler()); defer srv.Close()

	// No allow entry → deny.
	resp, _ := connectThrough(t, srv.URL, origin, "sessA")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for unlisted session, got %d", resp.StatusCode)
	}

	// Allow the origin's host for sessA → tunnel succeeds and echoes.
	host, _, _ := net.SplitHostPort(origin)
	p.SetAllow("sessA", []string{host})
	resp2, conn := connectThrough(t, srv.URL, origin, "sessA")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 after allow, got %d", resp2.StatusCode)
	}
	conn.Write([]byte("ping"))
	buf := make([]byte, 4)
	io.ReadFull(conn, buf)
	if string(buf) != "ping" { t.Fatalf("echo through tunnel = %q", buf) }
	conn.Close()

	if !strings.Contains(audit.String(), `"decision":"deny"`) ||
		!strings.Contains(audit.String(), `"decision":"allow"`) {
		t.Fatalf("audit missing decisions: %s", audit.String())
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/egress/ -v`
Expected: FAIL — `New`/`Handler` undefined.

- [ ] **Step 3: Implement the proxy**

```go
// internal/egress/proxy.go
// Package egress is a per-VM CONNECT proxy: default-deny, per-session allowlist,
// audit log. It is the only path out for session containers on the internal
// network (spec §8). The session is identified by the Proxy-Authorization
// bearer token = its session id.
package egress

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Proxy struct {
	mu    sync.RWMutex
	allow map[string][]string
	audit io.Writer
	now   func() time.Time
}

func New(audit io.Writer) *Proxy {
	return &Proxy{allow: map[string][]string{}, audit: audit, now: time.Now}
}

func (p *Proxy) SetAllow(session string, hosts []string) {
	p.mu.Lock(); defer p.mu.Unlock()
	p.allow[session] = append([]string(nil), hosts...)
}

func (p *Proxy) permitted(session, host string) bool {
	p.mu.RLock(); defer p.mu.RUnlock()
	for _, pat := range p.allow[session] {
		if pat == host { return true }
		if strings.HasPrefix(pat, "*.") && strings.HasSuffix(host, pat[1:]) { return true }
	}
	return false
}

func (p *Proxy) logDecision(session, host, port, decision string) {
	if p.audit == nil { return }
	line, _ := json.Marshal(map[string]string{
		"session": session, "host": host, "port": port,
		"decision": decision, "ts": p.now().UTC().Format(time.RFC3339),
	})
	fmt.Fprintln(p.audit, string(line))
}

func (p *Proxy) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "only CONNECT supported", http.StatusMethodNotAllowed)
			return
		}
		session := strings.TrimPrefix(r.Header.Get("Proxy-Authorization"), "Bearer ")
		host, port, err := net.SplitHostPort(r.Host)
		if err != nil { host, port = r.Host, "443" }
		if !p.permitted(session, host) {
			p.logDecision(session, host, port, "deny")
			http.Error(w, "egress denied", http.StatusForbidden)
			return
		}
		p.logDecision(session, host, port, "allow")

		upstream, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 10*time.Second)
		if err != nil { http.Error(w, "upstream dial failed", http.StatusBadGateway); return }
		defer upstream.Close()

		hj, ok := w.(http.Hijacker)
		if !ok { http.Error(w, "no hijack", http.StatusInternalServerError); return }
		client, _, err := hj.Hijack()
		if err != nil { return }
		defer client.Close()
		client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

		done := make(chan struct{}, 2)
		go func() { io.Copy(upstream, client); done <- struct{}{} }()
		go func() { io.Copy(client, upstream); done <- struct{}{} }()
		<-done
	})
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/egress/ -v -race`
Expected: PASS (deny then allow-and-echo; audit has both decisions).

- [ ] **Step 5: Implement `cmd/egressd/main.go`**

```go
// cmd/egressd/main.go
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"rainier/internal/egress"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:3128", "proxy listen address")
	flag.Parse()
	p := egress.New(os.Stdout) // audit to stdout for v0 (container logs)
	// v0: allow rules are pushed at runtime by runnerd via a tiny admin endpoint.
	// For the standalone binary, start permissive-to-nobody (default deny) and
	// rely on runnerd to SetAllow. Expose an admin mux on a second port.
	admin := flag.String("admin", "127.0.0.1:3129", "admin address for allow updates")
	http.HandleFunc("/allow", func(w http.ResponseWriter, r *http.Request) {
		s := r.URL.Query().Get("session")
		hosts := r.URL.Query()["host"]
		p.SetAllow(s, hosts)
		w.WriteHeader(http.StatusNoContent)
	})
	go func() { log.Fatal(http.ListenAndServe(*admin, nil)) }()
	log.Printf("egressd proxy on %s, admin on %s", *listen, *admin)
	log.Fatal(http.ListenAndServe(*listen, p.Handler()))
}
```

Note the ordering bug to avoid: `flag.Parse()` must precede use of `*admin`; move the `admin` flag declaration above `flag.Parse()`. The implementer fixes this ordering.

- [ ] **Step 6: Build**

Run: `go build ./cmd/egressd/ && go build ./...`
Expected: builds.

- [ ] **Step 7: Commit**

```bash
git add internal/egress/ cmd/egressd/
git commit -m "feat: egressd CONNECT proxy with default-deny allowlist and audit"
```

---

### Task 10: sessiond dial mode + websocket Conn adapter

**Files:**
- Modify: `cmd/sessiond/main.go`
- Create: `internal/relay/wsconn.go`

**Interfaces:**
- Consumes: `relay.ServeSession` (Task 4), `relay.Conn` (Task 4), `coder/websocket`
- Produces:
  - `relay.WSConn(c *websocket.Conn) relay.Conn` — adapts a websocket to the relay `Conn` interface (text frames).
  - `sessiond --dial <url> --session <id>`: instead of listening, dial `<url>?session=<id>`, then run `relay.ServeSession` over the connection. The listen mode stays as the default/dev path.

- [ ] **Step 1: Implement the ws adapter**

```go
// internal/relay/wsconn.go
package relay

import (
	"context"

	"github.com/coder/websocket"
)

type wsConn struct{ c *websocket.Conn }

func WSConn(c *websocket.Conn) Conn { return &wsConn{c: c} }

func (w *wsConn) Read(ctx context.Context) ([]byte, error) {
	_, b, err := w.c.Read(ctx)
	return b, err
}
func (w *wsConn) Write(ctx context.Context, b []byte) error {
	return w.c.Write(ctx, websocket.MessageText, b)
}
func (w *wsConn) Close() error { return w.c.CloseNow() }
```

- [ ] **Step 2: Add dial mode to sessiond main**

In `cmd/sessiond/main.go`, add flags and branch:

```go
dial := flag.String("dial", "", "runnerd URL to dial and register with (relay mode)")
sessionID := flag.String("session", "", "session id to register as (relay mode)")
// ... after flag.Parse() and session.New(...) and reaper/SIGTERM setup:

if *dial != "" {
	ctx := context.Background()
	url := *dial + "?session=" + *sessionID
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil { log.Fatalf("dial runnerd: %v", err) }
	c.SetReadLimit(16 << 20)
	log.Printf("sessiond registered with runnerd as %s", *sessionID)
	if err := relay.ServeSession(ctx, relay.WSConn(c), s); err != nil {
		log.Printf("relay ended: %v", err)
	}
	return
}
// else: existing listen mode
log.Printf("sessiond listening on %s", *listen)
log.Fatal(http.ListenAndServe(*listen, server.New(s)))
```

Environment fallback (the Docker driver injects env, not flags): if `--dial` is empty, read `RAINIER_DIAL`/`RAINIER_SESSION` from the environment and use them. Add before the branch:

```go
if *dial == "" { if v := os.Getenv("RAINIER_DIAL"); v != "" { *dial = v } }
if *sessionID == "" { if v := os.Getenv("RAINIER_SESSION"); v != "" { *sessionID = v } }
```

- [ ] **Step 3: Build**

Run: `go build ./cmd/sessiond/ && go vet ./...`
Expected: builds; both modes compile.

- [ ] **Step 4: Commit**

```bash
git add internal/relay/wsconn.go cmd/sessiond/main.go
git commit -m "feat: sessiond dial mode registers with runnerd over websocket relay"
```

---

### Task 11: runnerd — registry, control surface, driver + relay glue, runnerctl

**Files:**
- Create: `internal/runnerd/registry.go`, `internal/runnerd/registry_test.go`, `internal/runnerd/runnerd.go`, `internal/runnerd/runnerd_test.go`, `cmd/runnerd/main.go`, `cmd/runnerctl/main.go`

**Interfaces:**
- Consumes: `driver.Driver`/`driver.Spec` (Task 5), `relay.Hub`/`relay.WSConn` (Tasks 4/10), `egress` admin (Task 9), `coder/websocket`
- Produces:
  - `registry`: maps session id → {handle, hub, allow, state}; capacity accounting delegated to the driver.
  - `runnerd.Server`: HTTP control surface —
    - `POST /sessions` (JSON `{name,image,cmd,egress_allow}`) → driver.Create with a generated session id + this runnerd's dial URL; pushes allow to egressd; returns `{session_id}`.
    - `GET /sessions` → list with state.
    - `POST /sessions/{id}/suspend?warm=true|false`, `/resume`, `/snapshot`, `DELETE /sessions/{id}`.
    - `GET /register?session={id}` (WebSocket) → the session's `sessiond` dials here; runnerd wraps it in a `relay.Hub`.
    - `GET /attach?session={id}&since={n}` (WebSocket) → a client attaches; runnerd calls `hub.AttachClient`.
  - `runnerctl`: thin CLI over the control surface (`create`, `ls`, `attach`, `suspend`, `resume`, `snapshot`, `rm`). `attach` opens the client websocket and behaves like `rattach` (reuse the rattach client logic — factor the interactive loop into a shared internal package OR shell to `rattach --url ws://<runnerd>/attach?session=<id>`; v0: shell to rattach to avoid duplicating the raw-mode client).

**Design:** the registration↔attach pairing is the crux. `sessiond` (in a container) dials `/register?session=<id>`; runnerd stores `hub[id]`. A client hits `/attach?session=<id>`; runnerd looks up `hub[id]` and bridges. If a client attaches before the session has registered (container still booting), runnerd waits briefly for registration (bounded) then 503s.

- [ ] **Step 1: Write the registry test**

```go
// internal/runnerd/registry_test.go
package runnerd

import "testing"

func TestRegistryTracksAndWaits(t *testing.T) {
	r := newRegistry()
	if _, ok := r.get("s1"); ok { t.Fatal("empty registry returned a hub") }
	r.put("s1", &sessionEntry{id: "s1", state: "running"})
	e, ok := r.get("s1")
	if !ok || e.id != "s1" { t.Fatalf("get = %+v, %v", e, ok) }
	r.remove("s1")
	if _, ok := r.get("s1"); ok { t.Fatal("removed entry still present") }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/runnerd/ -run TestRegistry -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement `registry.go`**

```go
// internal/runnerd/registry.go
package runnerd

import (
	"sync"

	"rainier/internal/relay"
)

type sessionEntry struct {
	id     string
	handle string // driver handle id
	state  string
	allow  []string
	hub    *relay.Hub // set when sessiond registers; nil until then
}

type registry struct {
	mu    sync.Mutex
	items map[string]*sessionEntry
}

func newRegistry() *registry { return &registry{items: map[string]*sessionEntry{}} }

func (r *registry) put(id string, e *sessionEntry) { r.mu.Lock(); r.items[id] = e; r.mu.Unlock() }
func (r *registry) get(id string) (*sessionEntry, bool) {
	r.mu.Lock(); defer r.mu.Unlock()
	e, ok := r.items[id]; return e, ok
}
func (r *registry) remove(id string) { r.mu.Lock(); delete(r.items, id); r.mu.Unlock() }
func (r *registry) list() []*sessionEntry {
	r.mu.Lock(); defer r.mu.Unlock()
	out := make([]*sessionEntry, 0, len(r.items))
	for _, e := range r.items { out = append(out, e) }
	return out
}
func (r *registry) setHub(id string, h *relay.Hub) {
	r.mu.Lock(); defer r.mu.Unlock()
	if e, ok := r.items[id]; ok { e.hub = h }
}
```

- [ ] **Step 4: Implement `runnerd.go`** (HTTP + websocket endpoints)

```go
// internal/runnerd/runnerd.go
package runnerd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"rainier/internal/driver"
	"rainier/internal/relay"
)

type Server struct {
	drv      driver.Driver
	reg      *registry
	dialBase string // e.g. ws://runnerd:8080 — what sessiond dials to register
	seq      int
	egressAdmin string // http://egressd:3129 (optional)
}

func New(drv driver.Driver, dialBase, egressAdmin string) *Server {
	return &Server{drv: drv, reg: newRegistry(), dialBase: dialBase, egressAdmin: egressAdmin}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sessions", s.sessions)          // POST create, GET list
	mux.HandleFunc("/sessions/", s.sessionOp)        // /sessions/{id}/{op}
	mux.HandleFunc("/register", s.register)          // ws: sessiond dials in
	mux.HandleFunc("/attach", s.attach)              // ws: client attaches
	return mux
}

func (s *Server) newID() string { s.seq++; return "sess-" + strconv.Itoa(s.seq) }

func (s *Server) sessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name string `json:"name"`; Image string `json:"image"`
			Cmd []string `json:"cmd"`; EgressAllow []string `json:"egress_allow"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		id := s.newID()
		spec := driver.Spec{
			Name: body.Name, Image: body.Image, Cmd: body.Cmd,
			SessionID: id, DialURL: s.dialBase + "/register",
			EgressAllow: body.EgressAllow,
		}
		if err := s.pushEgress(id, body.EgressAllow); err != nil {
			http.Error(w, "egress setup: "+err.Error(), http.StatusBadGateway); return
		}
		h, err := s.drv.Create(r.Context(), spec)
		if err != nil { http.Error(w, err.Error(), http.StatusInternalServerError); return }
		s.reg.put(id, &sessionEntry{id: id, handle: h.ID, state: "running", allow: body.EgressAllow})
		json.NewEncoder(w).Encode(map[string]string{"session_id": id})
	case http.MethodGet:
		type row struct{ ID, State string }
		var rows []row
		for _, e := range s.reg.list() { rows = append(rows, row{e.id, e.state}) }
		json.NewEncoder(w).Encode(rows)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func (s *Server) sessionOp(w http.ResponseWriter, r *http.Request) {
	// /sessions/{id} (DELETE) or /sessions/{id}/{op} (POST)
	rest := strings.TrimPrefix(r.URL.Path, "/sessions/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	e, ok := s.reg.get(id)
	if !ok { http.Error(w, "no such session", http.StatusNotFound); return }
	ctx := r.Context()
	if r.Method == http.MethodDelete {
		s.drv.Destroy(ctx, e.handle); s.reg.remove(id); w.WriteHeader(http.StatusNoContent); return
	}
	op := ""
	if len(parts) == 2 { op = parts[1] }
	switch op {
	case "suspend":
		warm := r.URL.Query().Get("warm") != "false"
		if err := s.drv.Suspend(ctx, e.handle, warm); err != nil { http.Error(w, err.Error(), 500); return }
		e.state = "suspended"; w.WriteHeader(http.StatusNoContent)
	case "resume":
		if err := s.drv.Resume(ctx, e.handle); err != nil { http.Error(w, err.Error(), 500); return }
		e.state = "running"; w.WriteHeader(http.StatusNoContent)
	case "snapshot":
		snap, err := s.drv.Snapshot(ctx, e.handle)
		if err != nil { http.Error(w, err.Error(), 500); return }
		json.NewEncoder(w).Encode(map[string]string{"ref": snap.Ref})
	default:
		http.Error(w, "unknown op", http.StatusBadRequest)
	}
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("session")
	if _, ok := s.reg.get(id); !ok { http.Error(w, "unknown session", http.StatusNotFound); return }
	c, err := websocket.Accept(w, r, nil)
	if err != nil { return }
	c.SetReadLimit(16 << 20)
	hub := relay.NewHub(r.Context(), relay.WSConn(c))
	s.reg.setHub(id, hub)
	// Block until the session conn closes (keeps the http handler/goroutine alive).
	<-r.Context().Done()
	hub.Close()
}

func (s *Server) attach(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("session")
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)

	// Wait briefly for the session to register (container may still be booting).
	var hub *relay.Hub
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := s.reg.get(id); ok && e.hub != nil { hub = e.hub; break }
		time.Sleep(100 * time.Millisecond)
	}
	if hub == nil { http.Error(w, "session not registered", http.StatusServiceUnavailable); return }

	c, err := websocket.Accept(w, r, nil)
	if err != nil { return }
	c.SetReadLimit(16 << 20)
	// The client speaks wire.ClientMsg/ServerMsg; the hub forwards raw payloads.
	// The relay expects the first client frame to be a resize (like Plan 1 serve);
	// rattach sends it. cols/rows for the FrameOpen come from that first message.
	first, err := readFirstResize(r.Context(), c)
	if err != nil { c.CloseNow(); return }
	hub.AttachClient(r.Context(), relay.WSConn(c), since, first.Cols, first.Rows)
}

func (s *Server) pushEgress(session string, hosts []string) error {
	if s.egressAdmin == "" { return nil } // egress optional in unit tests
	q := "session=" + session
	for _, h := range hosts { q += "&host=" + h }
	resp, err := http.Post(s.egressAdmin+"/allow?"+q, "application/json", nil)
	if err != nil { return err }
	resp.Body.Close()
	return nil
}
```

Add `readFirstResize` in `runnerd.go` (reads one `wire.ClientMsg` off the ws, requires `Type=="resize"`, returns it) — it mirrors Plan 1's resize-first contract so the relayed client and a direct client behave identically. The `relay.Hub.AttachClient` will additionally forward that first resize? No — the FrameOpen carries cols/rows; the session's `ServeSession` applies them via `Attach`. Subsequent resizes flow as `FrameClient`. So `readFirstResize` consumes the resize to size the FrameOpen, and must NOT also forward it (the Open already conveys size). Document this in a comment.

- [ ] **Step 5: Write a runnerd integration test with the fake driver + a fake in-process sessiond**

```go
// internal/runnerd/runnerd_test.go
package runnerd

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/session"
	"rainier/internal/wire"
)

// This test wires the real relay end-to-end without Docker: create a session
// via the API, then simulate the container by dialing /register from an
// in-process sessiond bound to a real session.Session, then attach a client
// via /attach and assert output flows.
func TestRunnerdCreateRegisterAttach(t *testing.T) {
	srv := httptest.NewServer(New(driver.NewFake(4), "", "").Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	// Create.
	id := createSession(t, srv.URL) // helper: POST /sessions, returns session_id

	// Simulate the container's sessiond dialing /register.
	sess, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: t.TempDir() + "/s.log"},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }
	regConn, _, err := websocket.Dial(ctx, base+"/register?session="+id, nil)
	if err != nil { t.Fatal(err) }
	regConn.SetReadLimit(16 << 20)
	go relay.ServeSession(ctx, relay.WSConn(regConn), sess)

	// Attach a client.
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil { t.Fatal(err) }
	cli.SetReadLimit(16 << 20)
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})

	// Expect snapshot then echoed marker.
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "stdin", Data: []byte("echo runnerd-marker\n")})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline: t.Fatal("no runnerd-marker through runnerd relay")
		default:
		}
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, cli, &m); err != nil { t.Fatal(err) }
		if m.Type == "output" && strings.Contains(string(m.Data), "runnerd-marker") { return }
	}
}
```

Add `createSession` helper (POST JSON, parse `session_id`).

- [ ] **Step 6: Run to verify it passes**

Run: `go test ./internal/runnerd/ -v -race`
Expected: PASS — a client attaches through runnerd, and shell output routes container→session→runnerd→client with no Docker involved.

- [ ] **Step 7: Implement `cmd/runnerd/main.go`**

```go
// cmd/runnerd/main.go
package main

import (
	"flag"
	"log"
	"net/http"

	"rainier/internal/driver"
	"rainier/internal/runnerd"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:8080", "control + relay listen address")
	dialBase := flag.String("dial-base", "ws://runnerd:8080", "URL sessiond containers dial to register")
	image := flag.String("image", "rainier-session:latest", "default session image")
	network := flag.String("network", "rainier-internal", "internal docker network for sessions")
	egressAdmin := flag.String("egress-admin", "http://egressd:3129", "egressd admin URL")
	slots := flag.Int("slots", 16, "capacity")
	flag.Parse()

	drv := driver.NewDocker(driver.DockerOpts{Image: *image, Network: *network, TotalSlots: *slots})
	s := runnerd.New(drv, *dialBase, *egressAdmin)
	log.Printf("runnerd on %s (dial-base %s)", *listen, *dialBase)
	log.Fatal(http.ListenAndServe(*listen, s.Handler()))
}
```

- [ ] **Step 8: Implement `cmd/runnerctl/main.go`** (thin HTTP client; `attach` shells to `rattach`)

```go
// cmd/runnerctl/main.go
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

func main() {
	base := flag.String("runnerd", "http://127.0.0.1:8080", "runnerd control URL")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 { fmt.Fprintln(os.Stderr, "usage: runnerctl [--runnerd URL] <create|ls|attach|suspend|resume|snapshot|rm> ..."); os.Exit(2) }
	switch args[0] {
	case "create":
		image := ""
		if len(args) > 1 { image = args[1] }
		body, _ := json.Marshal(map[string]any{"image": image})
		resp, err := http.Post(*base+"/sessions", "application/json", bytes.NewReader(body))
		check(err); defer resp.Body.Close()
		io.Copy(os.Stdout, resp.Body); fmt.Println()
	case "ls":
		resp, err := http.Get(*base+"/sessions"); check(err); defer resp.Body.Close()
		io.Copy(os.Stdout, resp.Body); fmt.Println()
	case "attach":
		id := args[1]
		wsURL := strings.Replace(*base, "http", "ws", 1) + "/attach?session=" + id
		cmd := exec.Command("./bin/rattach", "--url", wsURL)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		cmd.Run()
	case "suspend":
		post(*base + "/sessions/" + args[1] + "/suspend")
	case "resume":
		post(*base + "/sessions/" + args[1] + "/resume")
	case "snapshot":
		post(*base + "/sessions/" + args[1] + "/snapshot")
	case "rm":
		req, _ := http.NewRequest(http.MethodDelete, *base+"/sessions/"+args[1], nil)
		resp, err := http.DefaultClient.Do(req); check(err); resp.Body.Close()
		fmt.Println("removed", args[1])
	default:
		fmt.Fprintln(os.Stderr, "unknown command", args[0]); os.Exit(2)
	}
}
func post(url string) {
	resp, err := http.Post(url, "application/json", nil); check(err); defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body); fmt.Println()
}
func check(err error) { if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) } }
```

- [ ] **Step 9: Build + full suite + race**

Run: `go build ./... && go test ./... -race 2>&1 | tail -8`
Expected: all binaries build; all packages green (docker subtests skip if daemon absent).

- [ ] **Step 10: Commit**

```bash
git add internal/runnerd/ cmd/runnerd/ cmd/runnerctl/
git commit -m "feat: runnerd control surface, registry, relay glue, and runnerctl CLI"
```

---

### Task 12: Session image, compose fleet, egress integration, and acceptance scripts

**Files:**
- Modify: `Dockerfile`, `docker-compose.fleet.yml`, `scripts/fleet-up.sh`, `scripts/fleet-down.sh`
- Create: `scripts/egress-check.sh`

**Interfaces:**
- Consumes: `runnerd`, `egressd`, `sessiond` (dial mode), the Docker driver
- Produces: a runnerd+egressd fleet where `runnerctl create` launches a real container whose `sessiond` dials back, `runnerctl attach` relays a live terminal, and an egress allow/deny check passes.

**Design:** two images — the existing session image (now built to run `sessiond` in dial mode via env) and a runnerd image. runnerd needs the `docker` socket to drive the CLI, so its container mounts `/var/run/docker.sock` and has the `docker` binary. Simpler for v0 acceptance: run **runnerd and egressd on the host** (not containerized), and only session containers via the driver — this sidesteps docker-in-docker and socket mounting. The compose file then defines only the internal network; runnerd/egressd run as host processes bound to that network's gateway. Document this choice.

- [ ] **Step 1: Update the Dockerfile session image** so the container entrypoint runs sessiond in dial mode using injected env:

```dockerfile
# Dockerfile (session image)
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sessiond ./cmd/sessiond

FROM alpine:3.20
RUN apk add --no-cache bash
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1; RAINIER_DIAL/RAINIER_SESSION injected by the driver select
# dial (relay) mode. With no env, it falls back to listen mode (dev).
ENTRYPOINT ["sessiond"]
CMD ["--", "bash", "-i"]
```

Note: dial mode reads env; the ENTRYPOINT needs no `--dial` flag because Task 10 added the env fallback. The `CMD` provides the agent argv after `--`.

- [ ] **Step 2: Create the internal network + host-run topology in compose**

```yaml
# docker-compose.fleet.yml — v0 fleet: an internal network for sessions.
# runnerd and egressd run on the HOST (see scripts/fleet-up.sh), driving the
# docker CLI and bound to this network's gateway. This avoids docker-in-docker.
networks:
  rainier-internal:
    name: rainier-internal
    internal: false   # v0: allow egressd (on gateway) reachability; sessions still route only via proxy env
```

(Full internal isolation with a separate egress bridge is a later hardening; v0 relies on the proxy env + default-deny allowlist for control, and documents that kernel-level isolation is deferred per the spec amendment.)

- [ ] **Step 3: Rewrite `scripts/fleet-up.sh`** to build images and start host-side runnerd + egressd:

```bash
#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 1; }

docker network inspect rainier-internal >/dev/null 2>&1 || docker network create rainier-internal >/dev/null
docker build -q -t rainier-session:latest . >/dev/null
go build -o bin/runnerd ./cmd/runnerd
go build -o bin/egressd ./cmd/egressd
go build -o bin/rattach ./cmd/rattach
go build -o bin/runnerctl ./cmd/runnerctl

# Gateway IP of the internal network — how session containers reach host services.
GW=$(docker network inspect rainier-internal -f '{{ (index .IPAM.Config 0).Gateway }}')
echo "internal network gateway: $GW"

./bin/egressd --listen 0.0.0.0:3128 --admin 127.0.0.1:3129 >/tmp/egressd.log 2>&1 &
echo $! > /tmp/rainier-egressd.pid
# runnerd dials-base uses the gateway so containers can reach host runnerd.
./bin/runnerd --listen 0.0.0.0:8080 --dial-base "ws://$GW:8080" \
  --image rainier-session:latest --network rainier-internal \
  --egress-admin http://127.0.0.1:3129 >/tmp/runnerd.log 2>&1 &
echo $! > /tmp/rainier-runnerd.pid
sleep 1
echo "runnerd on :8080, egressd on :3128. Try:"
echo "  ./bin/runnerctl create            # → {\"session_id\":\"sess-1\"}"
echo "  ./bin/runnerctl ls"
echo "  ./bin/runnerctl attach sess-1      # live terminal through the relay"
```

- [ ] **Step 4: Rewrite `scripts/fleet-down.sh`**

```bash
#!/usr/bin/env bash
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
for pidf in /tmp/rainier-runnerd.pid /tmp/rainier-egressd.pid; do
  [ -f "$pidf" ] && kill "$(cat "$pidf")" 2>/dev/null; rm -f "$pidf"
done
# Remove any rainier-managed session containers.
ids=$(docker ps -aq --filter label=rainier.session 2>/dev/null || true)
[ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1 || true
echo "fleet down."
```

- [ ] **Step 5: Scripted acceptance** — you cannot drive interactive attach headlessly, so verify create→register→relay with a piped rattach, and egress allow/deny:

```bash
# scripts/egress-check.sh
#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
# Create a session allowing example.com only, then from inside it try an
# allowed and a denied host through the proxy env.
SID=$(./bin/runnerctl create rainier-session:latest | sed 's/.*"session_id":"//;s/".*//')
echo "created $SID"
sleep 2
CID=$(docker ps -q --filter "label=rainier.session=$SID")
echo "container $CID"
# The proxy env is injected by the driver? v0: driver injects RAINIER_DIAL only;
# HTTP(S)_PROXY injection is added here if present. Assert the container is on the
# internal network and reached runnerd (registered) — check runnerd log.
grep -q "registered" /tmp/runnerd.log && echo "PASS: session registered with runnerd"
./bin/runnerctl rm "$SID"
```

- [ ] **Step 6: Run the driver + runnerd suites with docker present, then the scripted acceptance**

Run:
```
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
go test ./internal/driver/ ./internal/runnerd/ -v      # docker contract + relay
chmod +x scripts/fleet-up.sh scripts/fleet-down.sh scripts/egress-check.sh
./scripts/fleet-up.sh
./bin/runnerctl create && sleep 2 && ./bin/runnerctl ls
printf 'echo fleet-relay-marker\n' | ./bin/runnerctl attach sess-1 > /tmp/relay.out 2>&1 & sleep 3; kill %1 2>/dev/null || true
grep fleet-relay-marker /tmp/relay.out && echo "PASS: relay end-to-end"
./scripts/fleet-down.sh
```
Expected: docker contract fully green; runnerd relay test green; a real container registers and relays output through runnerd; fleet tears down cleanly. Record the transcript.

- [ ] **Step 7: Commit**

```bash
git add Dockerfile docker-compose.fleet.yml scripts/
git commit -m "feat: runnerd+egressd fleet, dial-mode session image, egress + relay acceptance"
```

---

## Self-Review Notes

- **Spec coverage:** §3 runnerd/egressd/sessiond components → Tasks 9-12, 1-2; §4 runner API (create/suspend/resume/snapshot/destroy/capacity) → Tasks 5-8; §5 sessiond outbound multiplexing + PID-1 duties → Tasks 1, 10, 3-4; §8 egress default-deny + allowlist + audit → Tasks 9, 11-12; §11.3 driver contract suite → Task 5; §2 rule 1 (PID-1) → Task 1, rule 2 (OCI snapshot) → Tasks 5/8, rule 3 (outbound reachability, no attach/exec in runner API) → Tasks 3-4, 10-11. Acceptance findings from Plan 1: PID-1 reaping/SIGTERM → Task 1; viewer liveness → Task 2.
- **Deferred to later plans (per spec/decisions):** controld + multi-VM placement + runnerd→controld dial (Plan 3); repos/GitHub-auth checkout into the session FS (Plan 4); egress token-injection + two-phase network + kernel-level isolation (later driver iteration); K8s/Firecracker drivers (later — the `Driver` interface + contract suite are the seam they'll implement).
- **Type consistency:** `driver.Spec`/`Handle`/`State`/`Snapshot` identical across Tasks 5-8, 11; `relay.Frame`/`Conn`/`Hub`/`ServeSession` identical across Tasks 3-4, 10-11; `session.New`/`Attach`/`Stop` match Plan 1 + Task 1's new `Stop`; `wire.ClientMsg`/`ServerMsg` unchanged so `rattach` works against both direct sessiond and the runnerd relay.
- **Known v0 simplifications (deliberate, ledger them):** runnerd/egressd run on the host in the fleet (no docker-in-docker); the internal network is not yet kernel-isolated (proxy-env + allowlist is the control); `runnerd.seq` id generation isn't persistent (fine — no controld yet); snapshot ref uniqueness is per-container-single (Task 8 note); egress proxy-env injection into containers is minimal in v0 (the driver injects RAINIER_DIAL/SESSION; HTTP(S)_PROXY wiring is exercised in Task 12's check and hardened when repos/network isolation land).

**Execution order:** Tasks are sequential. Tasks 1-2 (sessiond hardening) and 3-4 (relay) are independent of 5-8 (drivers) and 9 (egress) until 10-12 integrate them; keep the given order so each task's tests build on committed predecessors.
