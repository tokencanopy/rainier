# sessiond Core Implementation Plan (Rainier v0, Plan 1 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `sessiond` — the in-sandbox session host that owns an agent's PTY, maintains terminal state, and gives clients instant, laptop-sleep-proof attach/detach/reattach — plus a dev attach client and a Docker demo.

**Architecture:** `sessiond` spawns a command on a PTY it owns forever; a headless VT emulator tracks the current screen; every output frame is appended to a sequence-numbered event log. Viewers connect over WebSocket: a fresh viewer gets a serialized screen snapshot then live frames; a resuming viewer sends its last sequence number and gets exactly the missed frames. The session never depends on any client connection.

**Tech Stack:** Go 1.23+, `github.com/creack/pty` (PTY), `github.com/charmbracelet/x/vt` (VT emulator; fallback `github.com/hinshun/vt10x`), `github.com/coder/websocket` + `wsjson` (WebSocket), `golang.org/x/term` (client raw mode). No cgo.

**Spec:** `docs/superpowers/specs/2026-08-27-rainier-design.md` (§3 components, §5 sessiond, §2 portability rules)

## Global Constraints

- Go module path: `rainier` (local module; rename at publish is mechanical).
- `CGO_ENABLED=0`; `sessiond` must build as a static Linux binary (spec portability rule 1).
- Spec rule 1: sessiond is PID-1-in-sandbox shaped — no host-side reach-in assumptions.
- Spec rule 3 note: production sessiond **dials out**; this plan's listener is the dev/debug path. Keep all per-connection logic in a `serve(conn)` function that is transport-direction-agnostic so Plan 2 adds outbound dialing without refactoring.
- Never replay the raw byte log to paint a screen for a fresh viewer — snapshot + deltas only (spec §5).
- On Linux, reading a PTY master after child exit returns `EIO`; treat as `io.EOF`, never as an error.
- License: Apache-2.0 (LICENSE already in repo). No AGPL/GPL dependencies.
- Commit messages: conventional (`feat:`, `test:`, `chore:`), each task ends in a commit.

## File Structure

```
go.mod, Makefile, .gitignore
internal/eventlog/eventlog.go        # append-only sequenced log, JSONL-backed
internal/eventlog/eventlog_test.go
internal/wire/wire.go                # client/server message types (JSON)
internal/wire/wire_test.go
internal/term/term.go                # Emulator interface, Cell/Color types, Serialize()
internal/term/serialize.go
internal/term/emulator.go            # wrapper over charmbracelet/x/vt (or vt10x)
internal/term/term_test.go           # fixture + snapshot round-trip tests
internal/session/proc.go             # PTY process runner
internal/session/proc_test.go
internal/session/resize.go           # smallest-active-viewer policy (pure)
internal/session/resize_test.go
internal/session/session.go          # core: proc → emulator + log + viewer fan-out
internal/session/session_test.go
internal/server/server.go            # WebSocket serve(conn) + /attach handler
internal/server/server_test.go
cmd/sessiond/main.go
cmd/rattach/main.go                  # dev attach client (raw mode terminal)
Dockerfile
scripts/demo.sh                      # docker demo: attach → detach → reattach
testdata/vt/*.input                  # emulator fixtures
```

---

### Task 1: Repo scaffolding + event log

**Files:**
- Create: `go.mod`, `Makefile`, `.gitignore`
- Create: `internal/eventlog/eventlog.go`
- Test: `internal/eventlog/eventlog_test.go`

**Interfaces:**
- Consumes: nothing (first task)
- Produces:
  ```go
  package eventlog
  type Entry struct { Seq uint64; Type string; Data []byte; TS int64 }
  func Open(path string) (*Log, error)          // creates or reloads; Seq resumes
  func (l *Log) Append(typ string, data []byte) (uint64, error) // returns seq (1-based)
  func (l *Log) Since(seq uint64) ([]Entry, error) // entries with Seq > seq, in order
  func (l *Log) LastSeq() uint64
  func (l *Log) Close() error
  ```

- [ ] **Step 1: Scaffold module**

```bash
cd ~/Desktop/rainier
go mod init rainier
printf 'bin/\n*.log\n' > .gitignore
cat > Makefile <<'EOF'
.PHONY: test build
test:
	go test ./...
build:
	CGO_ENABLED=0 go build -o bin/sessiond ./cmd/sessiond
	CGO_ENABLED=0 go build -o bin/rattach ./cmd/rattach
EOF
```

- [ ] **Step 2: Write the failing test**

```go
// internal/eventlog/eventlog_test.go
package eventlog

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestAppendSinceRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.log")
	l, err := Open(p)
	if err != nil { t.Fatal(err) }
	s1, _ := l.Append("output", []byte("hello"))
	s2, _ := l.Append("output", []byte("world"))
	if s1 != 1 || s2 != 2 { t.Fatalf("seqs = %d,%d, want 1,2", s1, s2) }
	got, err := l.Since(1)
	if err != nil { t.Fatal(err) }
	if len(got) != 1 || got[0].Seq != 2 || !bytes.Equal(got[0].Data, []byte("world")) {
		t.Fatalf("Since(1) = %+v", got)
	}
	if l.LastSeq() != 2 { t.Fatalf("LastSeq = %d", l.LastSeq()) }
}

func TestReloadResumesSeq(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.log")
	l, _ := Open(p)
	l.Append("output", []byte("a"))
	l.Close()
	l2, err := Open(p)
	if err != nil { t.Fatal(err) }
	defer l2.Close()
	s, _ := l2.Append("output", []byte("b"))
	if s != 2 { t.Fatalf("seq after reload = %d, want 2", s) }
	got, _ := l2.Since(0)
	if len(got) != 2 { t.Fatalf("len = %d, want 2", len(got)) }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/eventlog/ -v`
Expected: FAIL (package does not compile: `Open` undefined)

- [ ] **Step 4: Implement**

```go
// internal/eventlog/eventlog.go
// Package eventlog is an append-only, sequence-numbered event log backed by a
// JSONL file. It is the resume backbone: viewers reconnect with a sequence
// cursor and replay exactly what they missed.
package eventlog

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type Entry struct {
	Seq  uint64 `json:"seq"`
	Type string `json:"t"`
	Data []byte `json:"d"` // std json base64-encodes []byte
	TS   int64  `json:"ts"`
}

type Log struct {
	mu      sync.Mutex
	f       *os.File
	w       *bufio.Writer
	entries []Entry // in-memory copy; v0 keeps all entries (bounded later by rotation)
	last    uint64
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil { return nil, err }
	l := &Log{f: f}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		var e Entry
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			l.entries = append(l.entries, e)
			l.last = e.Seq
		}
	}
	if err := sc.Err(); err != nil { f.Close(); return nil, err }
	l.w = bufio.NewWriter(f)
	return l, nil
}

func (l *Log) Append(typ string, data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.last++
	e := Entry{Seq: l.last, Type: typ, Data: append([]byte(nil), data...), TS: time.Now().UnixMilli()}
	b, err := json.Marshal(e)
	if err != nil { return 0, err }
	if _, err := l.w.Write(append(b, '\n')); err != nil { return 0, err }
	if err := l.w.Flush(); err != nil { return 0, err }
	l.entries = append(l.entries, e)
	return e.Seq, nil
}

func (l *Log) Since(seq uint64) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Entry
	for _, e := range l.entries {
		if e.Seq > seq { out = append(out, e) }
	}
	return out, nil
}

func (l *Log) LastSeq() uint64 { l.mu.Lock(); defer l.mu.Unlock(); return l.last }
func (l *Log) Close() error    { l.mu.Lock(); defer l.mu.Unlock(); l.w.Flush(); return l.f.Close() }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/eventlog/ -v`
Expected: PASS (both tests)

- [ ] **Step 6: Commit**

```bash
git add go.mod Makefile .gitignore internal/eventlog/
git commit -m "feat: event log with sequence-numbered append and replay-since"
```

---

### Task 2: Wire protocol

**Files:**
- Create: `internal/wire/wire.go`
- Test: `internal/wire/wire_test.go`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```go
  package wire
  // Client → server
  type ClientMsg struct { Type string; Data []byte; Cols, Rows int }
  // Type: "stdin" (Data), "resize" (Cols/Rows)
  // Server → client
  type ServerMsg struct { Type string; Seq uint64; Data []byte; Cols, Rows int; ExitCode int }
  // Type: "snapshot" (Data=repaint bytes, Seq=high-water mark, Cols/Rows),
  //       "output" (Data, Seq), "exit" (ExitCode)
  ```

- [ ] **Step 1: Write the failing test**

```go
// internal/wire/wire_test.go
package wire

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := ServerMsg{Type: "output", Seq: 7, Data: []byte{0x1b, '[', 'H'}}
	b, err := json.Marshal(in)
	if err != nil { t.Fatal(err) }
	var out ServerMsg
	if err := json.Unmarshal(b, &out); err != nil { t.Fatal(err) }
	if out.Type != "output" || out.Seq != 7 || string(out.Data) != "\x1b[H" {
		t.Fatalf("round trip = %+v", out)
	}
}

func TestClientMsgOmitsEmpty(t *testing.T) {
	b, _ := json.Marshal(ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	s := string(b)
	if s != `{"type":"resize","cols":80,"rows":24}` {
		t.Fatalf("marshal = %s", s)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wire/ -v`
Expected: FAIL (types undefined)

- [ ] **Step 3: Implement**

```go
// internal/wire/wire.go
// Package wire defines the JSON messages exchanged between sessiond and
// viewers. One message type per direction keeps the protocol greppable.
package wire

type ClientMsg struct {
	Type string `json:"type"`           // "stdin" | "resize"
	Data []byte `json:"data,omitempty"` // stdin bytes
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
}

type ServerMsg struct {
	Type     string `json:"type"` // "snapshot" | "output" | "exit"
	Seq      uint64 `json:"seq,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/wire/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/wire/
git commit -m "feat: wire protocol message types"
```

---

### Task 3: Emulator interface, serializer, and fixture tests

**Files:**
- Create: `internal/term/term.go`, `internal/term/serialize.go`
- Create: `testdata/vt/` fixtures (inline in tests for v0)
- Test: `internal/term/term_test.go`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```go
  package term
  type Color struct { Mode uint8; Idx uint8; R, G, B uint8 } // Mode: 0=default 1=ansi256 2=rgb
  type Cell struct { R rune; FG, BG Color; Bold, Underline, Reverse bool }
  type Screen struct { Cells [][]Cell; CursorX, CursorY int; CursorHidden bool; Alt bool; Cols, Rows int }
  type Emulator interface {
      Feed(p []byte)
      Screen() Screen
      Resize(cols, rows int)
  }
  func NewEmulator(cols, rows int) Emulator     // implemented in Task 4
  func Serialize(s Screen) []byte               // repaint escape sequence
  ```

The **snapshot round-trip test** is the contract for both the serializer and the emulator: feed a fixture into emulator A, serialize A's screen, feed that into a fresh emulator B, require identical screens. No golden bytes needed; correctness is self-checking.

- [ ] **Step 1: Write the serializer (pure function, no emulator needed)**

```go
// internal/term/term.go
package term

type Color struct {
	Mode uint8 // 0=default, 1=ansi256, 2=rgb
	Idx  uint8
	R, G, B uint8
}

type Cell struct {
	R          rune
	FG, BG     Color
	Bold       bool
	Underline  bool
	Reverse    bool
}

type Screen struct {
	Cells        [][]Cell // [row][col]
	CursorX      int
	CursorY      int
	CursorHidden bool
	Alt          bool
	Cols, Rows   int
}

type Emulator interface {
	Feed(p []byte)
	Screen() Screen
	Resize(cols, rows int)
}
```

```go
// internal/term/serialize.go
package term

import (
	"fmt"
	"strings"
)

// Serialize renders a Screen as one repaint escape sequence: reset, clear,
// paint every row with minimal SGR changes, restore cursor. Feeding the
// result into a fresh emulator of the same size reproduces the screen.
func Serialize(s Screen) []byte {
	var b strings.Builder
	b.WriteString("\x1b[0m\x1b[2J\x1b[H")
	cur := styleKey{}
	for y := 0; y < s.Rows; y++ {
		b.WriteString(fmt.Sprintf("\x1b[%d;1H", y+1))
		for x := 0; x < s.Cols; x++ {
			c := cellAt(s, y, x)
			k := keyOf(c)
			if k != cur {
				b.WriteString("\x1b[0m")
				writeSGR(&b, c)
				cur = k
			}
			r := c.R
			if r == 0 { r = ' ' }
			b.WriteRune(r)
		}
	}
	b.WriteString("\x1b[0m")
	b.WriteString(fmt.Sprintf("\x1b[%d;%dH", s.CursorY+1, s.CursorX+1))
	if s.CursorHidden { b.WriteString("\x1b[?25l") } else { b.WriteString("\x1b[?25h") }
	return []byte(b.String())
}

func cellAt(s Screen, y, x int) Cell {
	if y < len(s.Cells) && x < len(s.Cells[y]) { return s.Cells[y][x] }
	return Cell{R: ' '}
}

type styleKey struct {
	fg, bg  Color
	b, u, r bool
}

func keyOf(c Cell) styleKey { return styleKey{c.FG, c.BG, c.Bold, c.Underline, c.Reverse} }

func writeSGR(b *strings.Builder, c Cell) {
	if c.Bold { b.WriteString("\x1b[1m") }
	if c.Underline { b.WriteString("\x1b[4m") }
	if c.Reverse { b.WriteString("\x1b[7m") }
	switch c.FG.Mode {
	case 1: fmt.Fprintf(b, "\x1b[38;5;%dm", c.FG.Idx)
	case 2: fmt.Fprintf(b, "\x1b[38;2;%d;%d;%dm", c.FG.R, c.FG.G, c.FG.B)
	}
	switch c.BG.Mode {
	case 1: fmt.Fprintf(b, "\x1b[48;5;%dm", c.BG.Idx)
	case 2: fmt.Fprintf(b, "\x1b[48;2;%d;%d;%dm", c.BG.R, c.BG.G, c.BG.B)
	}
}
```

- [ ] **Step 2: Write the fixture + round-trip tests (they fail: no NewEmulator yet)**

```go
// internal/term/term_test.go
package term

import "testing"

// fixtures: name → input bytes fed to a fresh 20x5 emulator
var fixtures = map[string][]byte{
	"plain":     []byte("hello"),
	"newline":   []byte("line1\r\nline2"),
	"color":     []byte("\x1b[31mred\x1b[0m plain \x1b[1;38;5;40mbold-green\x1b[0m"),
	"cursor":    []byte("abc\x1b[2;3Hxy\x1b[Hz"),
	"clear":     []byte("junk junk junk\x1b[2J\x1b[Hclean"),
	"altscreen": []byte("main\x1b[?1049halt-content"),
	"wrap":      []byte("aaaaaaaaaaaaaaaaaaaaaaaaaa"), // wider than 20 cols
}

func screensEqual(t *testing.T, a, b Screen) {
	t.Helper()
	if a.Cols != b.Cols || a.Rows != b.Rows { t.Fatalf("size %dx%d vs %dx%d", a.Cols, a.Rows, b.Cols, b.Rows) }
	if a.CursorX != b.CursorX || a.CursorY != b.CursorY {
		t.Fatalf("cursor (%d,%d) vs (%d,%d)", a.CursorX, a.CursorY, b.CursorX, b.CursorY)
	}
	for y := 0; y < a.Rows; y++ {
		for x := 0; x < a.Cols; x++ {
			ca, cb := cellAt(a, y, x), cellAt(b, y, x)
			if ca.R == 0 { ca.R = ' ' }
			if cb.R == 0 { cb.R = ' ' }
			if ca != cb { t.Fatalf("cell (%d,%d): %+v vs %+v", y, x, ca, cb) }
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	for name, input := range fixtures {
		t.Run(name, func(t *testing.T) {
			a := NewEmulator(20, 5)
			a.Feed(input)
			sa := a.Screen()
			b := NewEmulator(20, 5)
			b.Feed(Serialize(sa))
			screensEqual(t, sa, b.Screen())
		})
	}
}

func TestPlainTextLandsInCells(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed([]byte("hi"))
	s := e.Screen()
	if s.Cells[0][0].R != 'h' || s.Cells[0][1].R != 'i' {
		t.Fatalf("row0 = %q %q", s.Cells[0][0].R, s.Cells[0][1].R)
	}
	if s.CursorX != 2 || s.CursorY != 0 { t.Fatalf("cursor = (%d,%d)", s.CursorX, s.CursorY) }
}

func TestAltScreenFlag(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed([]byte("\x1b[?1049h"))
	if !e.Screen().Alt { t.Fatal("expected Alt=true after 1049h") }
	e.Feed([]byte("\x1b[?1049l"))
	if e.Screen().Alt { t.Fatal("expected Alt=false after 1049l") }
}

func TestResize(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed([]byte("hello"))
	e.Resize(40, 10)
	s := e.Screen()
	if s.Cols != 40 || s.Rows != 10 { t.Fatalf("size = %dx%d", s.Cols, s.Rows) }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/term/ -v`
Expected: FAIL to compile: `NewEmulator` undefined (serializer compiles)

- [ ] **Step 4: Commit the interface, serializer, and tests**

```bash
git add internal/term/
git commit -m "feat: terminal Screen model, snapshot serializer, fixture round-trip tests"
```

---

### Task 4: Emulator implementation (library wrapper)

**Files:**
- Create: `internal/term/emulator.go`
- Test: existing `internal/term/term_test.go` must pass

**Interfaces:**
- Consumes: `term.Emulator` interface, fixture tests (Task 3)
- Produces: `term.NewEmulator(cols, rows int) term.Emulator`

**Library note:** wrap `github.com/charmbracelet/x/vt` (pin the latest tagged version in `go.mod`). Its exact method names come from `pkg.go.dev/github.com/charmbracelet/x/vt` — the wrapper below shows the expected shape; adjust the library calls mechanically to the pinned API. **The Task 3 tests are the contract.** If the library cannot express alt-screen state or per-cell style access, switch to `github.com/hinshun/vt10x` and record the decision in the commit message. Do not modify the tests to fit the library (except: if the library legitimately normalizes something cosmetic — e.g. cursor-after-wrap position — prefer adjusting the fixture input to avoid the ambiguity, and note it).

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/charmbracelet/x/vt@latest
```

- [ ] **Step 2: Implement the wrapper (expected shape)**

```go
// internal/term/emulator.go
package term

import (
	"sync"

	vt "github.com/charmbracelet/x/vt"
)

type emu struct {
	mu sync.Mutex
	t  *vt.Terminal // consult pinned API: constructor is vt.NewTerminal(cols, rows) or similar
}

func NewEmulator(cols, rows int) Emulator {
	return &emu{t: vt.NewTerminal(cols, rows)}
}

func (e *emu) Feed(p []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.t.Write(p) // vt terminals implement io.Writer for the host-output stream
}

func (e *emu) Resize(cols, rows int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.t.Resize(cols, rows)
}

func (e *emu) Screen() Screen {
	e.mu.Lock()
	defer e.mu.Unlock()
	cols, rows := e.t.Width(), e.t.Height()
	s := Screen{Cols: cols, Rows: rows, Cells: make([][]Cell, rows)}
	for y := 0; y < rows; y++ {
		s.Cells[y] = make([]Cell, cols)
		for x := 0; x < cols; x++ {
			lc := e.t.Cell(x, y) // per-cell rune + style access
			s.Cells[y][x] = convertCell(lc)
		}
	}
	x, y := e.t.CursorPosition()
	s.CursorX, s.CursorY = x, y
	s.CursorHidden = e.t.CursorHidden()
	s.Alt = e.t.IsAltScreen()
	return s
}

// convertCell maps the library's cell/style type to term.Cell, translating its
// color representation into Color{Mode 0/1/2}. Write this against the pinned
// API's actual style type.
func convertCell(c vt.Cell) Cell { /* per pinned API */ return Cell{} }
```

- [ ] **Step 3: Run the Task 3 tests until they pass**

Run: `go test ./internal/term/ -v`
Expected: PASS on all fixtures including `altscreen` and the full round-trip set. Iterate on `convertCell`/method mapping until green. If blocked by missing library capability, execute the vt10x fallback and re-run.

- [ ] **Step 4: Commit**

```bash
git add internal/term/ go.mod go.sum
git commit -m "feat: VT emulator wrapper passing snapshot round-trip fixtures"
```

---

### Task 5: PTY process runner

**Files:**
- Create: `internal/session/proc.go`
- Test: `internal/session/proc_test.go`

**Interfaces:**
- Consumes: nothing (self-contained; used by Task 6)
- Produces:
  ```go
  package session
  type Proc interface {
      Write(p []byte) (int, error)         // stdin
      Resize(cols, rows int) error
      Wait() int                            // blocks; returns exit code
      Stop()                                // SIGTERM the child
  }
  // StartProc spawns argv on a new PTY. onOutput is called from a single
  // goroutine with each chunk read from the PTY until child exit.
  func StartProc(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error)
  ```

- [ ] **Step 1: Write the failing test**

```go
// internal/session/proc_test.go
package session

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProcEchoAndCleanExit(t *testing.T) {
	var mu sync.Mutex
	var out strings.Builder
	p, err := StartProc([]string{"sh", "-c", "echo hello-pty"}, 80, 24, func(b []byte) {
		mu.Lock(); out.Write(b); mu.Unlock()
	})
	if err != nil { t.Fatal(err) }
	code := p.Wait() // must return despite Linux EIO-on-exit behavior
	if code != 0 { t.Fatalf("exit = %d", code) }
	mu.Lock(); defer mu.Unlock()
	if !strings.Contains(out.String(), "hello-pty") { t.Fatalf("output = %q", out.String()) }
}

func TestProcStdinReachesChild(t *testing.T) {
	var mu sync.Mutex
	var out strings.Builder
	p, err := StartProc([]string{"cat"}, 80, 24, func(b []byte) {
		mu.Lock(); out.Write(b); mu.Unlock()
	})
	if err != nil { t.Fatal(err) }
	p.Write([]byte("ping\n"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock(); s := out.String(); mu.Unlock()
		if strings.Contains(s, "ping") { p.Stop(); p.Wait(); return }
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("stdin never echoed; output = %q", out.String())
}

func TestProcExitCode(t *testing.T) {
	p, err := StartProc([]string{"sh", "-c", "exit 3"}, 80, 24, func([]byte) {})
	if err != nil { t.Fatal(err) }
	if code := p.Wait(); code != 3 { t.Fatalf("exit = %d, want 3", code) }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/session/ -run TestProc -v`
Expected: FAIL (StartProc undefined)

- [ ] **Step 3: Implement**

```go
// internal/session/proc.go
package session

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/creack/pty"
)

type proc struct {
	cmd  *exec.Cmd
	ptmx *os.File
	mu   sync.Mutex
	done chan struct{}
	code int
}

type Proc interface {
	Write(p []byte) (int, error)
	Resize(cols, rows int) error
	Wait() int
	Stop()
}

func StartProc(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
	if err != nil { return nil, err }
	p := &proc{cmd: cmd, ptmx: ptmx, done: make(chan struct{})}
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 { onOutput(buf[:n]) }
			if err != nil {
				// Linux returns EIO from the PTY master when the child exits.
				if errors.Is(err, io.EOF) || isEIO(err) { break }
				break
			}
		}
		p.code = waitCode(cmd)
		ptmx.Close()
		close(p.done)
	}()
	return p, nil
}

func isEIO(err error) bool {
	var pe *fs.PathError
	return errors.As(err, &pe) && errors.Is(pe.Err, syscall.EIO)
}

func waitCode(cmd *exec.Cmd) int {
	if err := cmd.Wait(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) { return ee.ExitCode() }
		return -1
	}
	return 0
}

func (p *proc) Write(b []byte) (int, error) { return p.ptmx.Write(b) }
func (p *proc) Resize(cols, rows int) error {
	return pty.Setsize(p.ptmx, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
}
func (p *proc) Wait() int { <-p.done; return p.code }
func (p *proc) Stop()     { p.cmd.Process.Signal(syscall.SIGTERM) }
```

Run `go get github.com/creack/pty@latest` before building.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/session/ -run TestProc -v`
Expected: PASS (all three)

- [ ] **Step 5: Commit**

```bash
git add internal/session/ go.mod go.sum
git commit -m "feat: PTY process runner with EIO-as-EOF and exit-code capture"
```

---

### Task 6: Resize policy

**Files:**
- Create: `internal/session/resize.go`
- Test: `internal/session/resize_test.go`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```go
  package session
  type Size struct { Cols, Rows int }
  // EffectiveSize returns the smallest cols and smallest rows across viewers;
  // ok=false when there are no viewers (caller keeps the current size).
  func EffectiveSize(viewers []Size) (Size, bool)
  ```

- [ ] **Step 1: Write the failing test**

```go
// internal/session/resize_test.go
package session

import "testing"

func TestEffectiveSize(t *testing.T) {
	cases := []struct {
		name string
		in   []Size
		want Size
		ok   bool
	}{
		{"none", nil, Size{}, false},
		{"one", []Size{{120, 40}}, Size{120, 40}, true},
		{"smallest-wins-per-axis", []Size{{120, 30}, {80, 40}}, Size{80, 30}, true},
	}
	for _, c := range cases {
		got, ok := EffectiveSize(c.in)
		if ok != c.ok || got != c.want {
			t.Fatalf("%s: = %+v,%v want %+v,%v", c.name, got, ok, c.want, c.ok)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/session/ -run TestEffectiveSize -v`
Expected: FAIL (EffectiveSize undefined)

- [ ] **Step 3: Implement**

```go
// internal/session/resize.go
package session

type Size struct{ Cols, Rows int }

func EffectiveSize(viewers []Size) (Size, bool) {
	if len(viewers) == 0 { return Size{}, false }
	eff := viewers[0]
	for _, v := range viewers[1:] {
		if v.Cols < eff.Cols { eff.Cols = v.Cols }
		if v.Rows < eff.Rows { eff.Rows = v.Rows }
	}
	return eff, true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/session/ -run TestEffectiveSize -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/session/resize.go internal/session/resize_test.go
git commit -m "feat: smallest-active-viewer resize policy"
```

---

### Task 7: Session core

**Files:**
- Create: `internal/session/session.go`
- Test: `internal/session/session_test.go`

**Interfaces:**
- Consumes: `eventlog.Log` (Task 1), `term.Emulator`/`term.Serialize` (Tasks 3–4), `Proc`/`StartProc` (Task 5), `EffectiveSize` (Task 6), `wire.ServerMsg` (Task 2)
- Produces:
  ```go
  package session
  type Config struct { Argv []string; Cols, Rows int; LogPath string }
  func New(cfg Config, start func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error)) (*Session, error)
  type Attachment struct {
      ID     int
      Msgs   <-chan wire.ServerMsg // snapshot-or-replay first, then live; closed on detach/exit
  }
  func (s *Session) Attach(since uint64, size Size) (*Attachment, error)
  func (s *Session) Detach(id int)
  func (s *Session) Stdin(p []byte)
  func (s *Session) SetSize(id int, size Size)
  func (s *Session) Exited() <-chan struct{} // closed after child exit ("exit" msg sent to viewers)
  ```
  Fan-out rule: each attachment has a 256-buffered channel; if it overflows, the viewer is force-detached (slow-consumer policy).

- [ ] **Step 1: Write the failing tests (fake Proc via injected start func)**

```go
// internal/session/session_test.go
package session

import (
	"path/filepath"
	"testing"
	"time"

	"rainier/internal/wire"
)

// fakeProc lets tests drive output and observe stdin/resize without a real PTY.
type fakeProc struct {
	onOutput func([]byte)
	stdin    chan []byte
	resizes  chan Size
	exit     chan int
}

func (f *fakeProc) Write(p []byte) (int, error) { f.stdin <- append([]byte(nil), p...); return len(p), nil }
func (f *fakeProc) Resize(c, r int) error       { f.resizes <- Size{c, r}; return nil }
func (f *fakeProc) Wait() int                   { return <-f.exit }
func (f *fakeProc) Stop()                       { close(f.exit) }

func newFakeSession(t *testing.T) (*Session, *fakeProc) {
	t.Helper()
	fp := &fakeProc{stdin: make(chan []byte, 8), resizes: make(chan Size, 8), exit: make(chan int)}
	s, err := New(Config{Argv: []string{"fake"}, Cols: 20, Rows: 5, LogPath: filepath.Join(t.TempDir(), "s.log")},
		func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error) {
			fp.onOutput = onOutput
			return fp, nil
		})
	if err != nil { t.Fatal(err) }
	return s, fp
}

func recv(t *testing.T, ch <-chan wire.ServerMsg) wire.ServerMsg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for msg")
		return wire.ServerMsg{}
	}
}

func TestFreshAttachGetsSnapshotThenLive(t *testing.T) {
	s, fp := newFakeSession(t)
	fp.onOutput([]byte("hello"))
	a, err := s.Attach(0, Size{20, 5})
	if err != nil { t.Fatal(err) }
	m1 := recv(t, a.Msgs)
	if m1.Type != "snapshot" || m1.Seq == 0 { t.Fatalf("first = %+v", m1) }
	fp.onOutput([]byte(" world"))
	m2 := recv(t, a.Msgs)
	if m2.Type != "output" || string(m2.Data) != " world" || m2.Seq != m1.Seq+1 {
		t.Fatalf("live = %+v", m2)
	}
}

func TestResumeReplaysOnlyMissedFrames(t *testing.T) {
	s, fp := newFakeSession(t)
	fp.onOutput([]byte("one"))
	fp.onOutput([]byte("two"))
	a, _ := s.Attach(0, Size{20, 5})
	snap := recv(t, a.Msgs) // snapshot at seq 2
	s.Detach(a.ID)
	fp.onOutput([]byte("three"))
	b, _ := s.Attach(snap.Seq, Size{20, 5})
	m := recv(t, b.Msgs)
	if m.Type != "output" || string(m.Data) != "three" { t.Fatalf("resume = %+v", m) }
}

func TestStdinForwarded(t *testing.T) {
	s, fp := newFakeSession(t)
	s.Stdin([]byte("x"))
	select {
	case got := <-fp.stdin:
		if string(got) != "x" { t.Fatalf("stdin = %q", got) }
	case <-time.After(time.Second):
		t.Fatal("stdin not forwarded")
	}
}

func TestSmallestViewerResizesProc(t *testing.T) {
	s, fp := newFakeSession(t)
	a, _ := s.Attach(0, Size{120, 40})
	recv(t, a.Msgs)
	<-fp.resizes // 120x40 from first attach
	b, _ := s.Attach(0, Size{80, 50})
	recv(t, b.Msgs)
	got := <-fp.resizes
	if got != (Size{80, 40}) { t.Fatalf("resize = %+v, want {80 40}", got) }
	_ = b
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/session/ -run 'TestFresh|TestResume|TestStdin|TestSmallest' -v`
Expected: FAIL (Session/New undefined)

- [ ] **Step 3: Implement**

```go
// internal/session/session.go
// Session wires a PTY process to the emulator, the event log, and viewer
// fan-out. The session never depends on any viewer connection.
package session

import (
	"sync"

	"rainier/internal/eventlog"
	"rainier/internal/term"
	"rainier/internal/wire"
)

type Config struct {
	Argv       []string
	Cols, Rows int
	LogPath    string
}

type viewer struct {
	id   int
	ch   chan wire.ServerMsg
	size Size
}

type Session struct {
	mu      sync.Mutex
	emu     term.Emulator
	log     *eventlog.Log
	proc    Proc
	viewers map[int]*viewer
	nextID  int
	size    Size
	exited  chan struct{}
	exitC   int
}

func New(cfg Config, start func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error)) (*Session, error) {
	lg, err := eventlog.Open(cfg.LogPath)
	if err != nil { return nil, err }
	s := &Session{
		emu:     term.NewEmulator(cfg.Cols, cfg.Rows),
		log:     lg,
		viewers: map[int]*viewer{},
		size:    Size{cfg.Cols, cfg.Rows},
		exited:  make(chan struct{}),
	}
	p, err := start(cfg.Argv, cfg.Cols, cfg.Rows, s.onOutput)
	if err != nil { lg.Close(); return nil, err }
	s.proc = p
	go func() {
		code := p.Wait()
		s.mu.Lock()
		s.exitC = code
		for _, v := range s.viewers { s.trySend(v, wire.ServerMsg{Type: "exit", ExitCode: code}) }
		s.mu.Unlock()
		close(s.exited)
	}()
	return s, nil
}

func (s *Session) onOutput(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emu.Feed(b)
	seq, _ := s.log.Append("output", b)
	msg := wire.ServerMsg{Type: "output", Seq: seq, Data: append([]byte(nil), b...)}
	for _, v := range s.viewers { s.trySend(v, msg) }
}

// trySend enforces the slow-consumer policy: overflow force-detaches.
func (s *Session) trySend(v *viewer, m wire.ServerMsg) {
	select {
	case v.ch <- m:
	default:
		delete(s.viewers, v.id)
		close(v.ch)
	}
}

type Attachment struct {
	ID   int
	Msgs <-chan wire.ServerMsg
}

func (s *Session) Attach(since uint64, size Size) (*Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := &viewer{id: s.nextID, ch: make(chan wire.ServerMsg, 256), size: size}
	s.nextID++
	s.viewers[v.id] = v
	if since > 0 && since <= s.log.LastSeq() {
		entries, err := s.log.Since(since)
		if err == nil {
			for _, e := range entries {
				v.ch <- wire.ServerMsg{Type: "output", Seq: e.Seq, Data: e.Data}
			}
		}
	} else {
		scr := s.emu.Screen()
		v.ch <- wire.ServerMsg{
			Type: "snapshot", Seq: s.log.LastSeq(),
			Data: term.Serialize(scr), Cols: scr.Cols, Rows: scr.Rows,
		}
	}
	s.applySizeLocked()
	return &Attachment{ID: v.id, Msgs: v.ch}, nil
}

func (s *Session) Detach(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.viewers[id]; ok {
		delete(s.viewers, id)
		close(v.ch)
		s.applySizeLocked()
	}
}

func (s *Session) Stdin(p []byte) { s.proc.Write(p) }

func (s *Session) SetSize(id int, size Size) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.viewers[id]; ok {
		v.size = size
		s.applySizeLocked()
	}
}

func (s *Session) applySizeLocked() {
	var sizes []Size
	for _, v := range s.viewers { sizes = append(sizes, v.size) }
	eff, ok := EffectiveSize(sizes)
	if !ok || eff == s.size { return }
	s.size = eff
	s.emu.Resize(eff.Cols, eff.Rows)
	s.proc.Resize(eff.Cols, eff.Rows)
}

func (s *Session) Exited() <-chan struct{} { return s.exited }
func (s *Session) ExitCode() int           { return s.exitC }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/session/ -v`
Expected: PASS (all session, proc, resize tests)

- [ ] **Step 5: Commit**

```bash
git add internal/session/
git commit -m "feat: session core wiring PTY, emulator, event log, viewer fan-out"
```

---

### Task 8: WebSocket server + sessiond binary

**Files:**
- Create: `internal/server/server.go`
- Create: `cmd/sessiond/main.go`
- Test: `internal/server/server_test.go`

**Interfaces:**
- Consumes: `session.Session` API (Task 7), `wire` types (Task 2)
- Produces:
  - HTTP endpoint `GET /attach?since=<seq>` upgrading to WebSocket; server sends `wire.ServerMsg` JSON, reads `wire.ClientMsg` JSON. First client message MUST be `resize` (announces viewer size before attach completes).
  - `server.New(s *session.Session) http.Handler`
  - `sessiond` flags: `--listen 127.0.0.1:7070 --log /tmp/session.log -- <argv...>`

- [ ] **Step 1: Add dependency**

```bash
go get github.com/coder/websocket@latest
```

- [ ] **Step 2: Write the failing test**

```go
// internal/server/server_test.go
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

func startBash(t *testing.T) *session.Session {
	t.Helper()
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }
	return s
}

func dial(t *testing.T, url string, since string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, strings.Replace(url, "http", "ws", 1)+"/attach?since="+since, nil)
	if err != nil { t.Fatal(err) }
	wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	return c
}

func readUntil(t *testing.T, c *websocket.Conn, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var all strings.Builder
	for {
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, c, &m); err != nil {
			t.Fatalf("read: %v (so far: %q)", err, all.String())
		}
		all.Write(m.Data)
		if strings.Contains(all.String(), want) { return }
	}
}

func TestAttachTypeReattach(t *testing.T) {
	s := startBash(t)
	srv := httptest.NewServer(New(s))
	defer srv.Close()

	c1 := dial(t, srv.URL, "0")
	ctx := context.Background()
	wsjson.Write(ctx, c1, wire.ClientMsg{Type: "stdin", Data: []byte("echo marker-123\n")})
	readUntil(t, c1, "marker-123")
	c1.Close(websocket.StatusNormalClosure, "detach") // client vanishes; session lives

	c2 := dial(t, srv.URL, "0") // fresh attach → snapshot must contain prior output
	readUntil(t, c2, "marker-123")
	c2.Close(websocket.StatusNormalClosure, "")
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/server/ -v`
Expected: FAIL (`New` undefined)

- [ ] **Step 4: Implement server**

```go
// internal/server/server.go
// serve(conn) is deliberately transport-direction-agnostic: Plan 2 reuses it
// verbatim on an outbound-dialed connection (spec portability rule 3).
package server

import (
	"context"
	"net/http"
	"strconv"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/session"
	"rainier/internal/wire"
)

type handler struct{ s *session.Session }

func New(s *session.Session) http.Handler {
	mux := http.NewServeMux()
	h := &handler{s: s}
	mux.HandleFunc("/attach", h.attach)
	return mux
}

func (h *handler) attach(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	c, err := websocket.Accept(w, r, nil)
	if err != nil { return }
	defer c.CloseNow()
	serve(r.Context(), c, h.s, since)
}

func serve(ctx context.Context, c *websocket.Conn, s *session.Session, since uint64) {
	// First message must announce viewer size.
	var first wire.ClientMsg
	if err := wsjson.Read(ctx, c, &first); err != nil || first.Type != "resize" {
		return
	}
	att, err := s.Attach(since, session.Size{Cols: first.Cols, Rows: first.Rows})
	if err != nil { return }
	defer s.Detach(att.ID)

	// Writer: session → client.
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for m := range att.Msgs {
			if wsjson.Write(ctx, c, m) != nil { return }
		}
	}()

	// Reader: client → session.
	for {
		var m wire.ClientMsg
		if err := wsjson.Read(ctx, c, &m); err != nil { return }
		switch m.Type {
		case "stdin":
			s.Stdin(m.Data)
		case "resize":
			s.SetSize(att.ID, session.Size{Cols: m.Cols, Rows: m.Rows})
		}
	}
}
```

- [ ] **Step 5: Implement the binary**

```go
// cmd/sessiond/main.go
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"rainier/internal/server"
	"rainier/internal/session"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7070", "dev listener address")
	logPath := flag.String("log", "/tmp/session.log", "event log path")
	cols := flag.Int("cols", 120, "initial cols")
	rows := flag.Int("rows", 32, "initial rows")
	flag.Parse()
	argv := flag.Args()
	if len(argv) == 0 {
		log.Fatal("usage: sessiond [flags] -- <command> [args...]")
	}
	s, err := session.New(session.Config{Argv: argv, Cols: *cols, Rows: *rows, LogPath: *logPath}, session.StartProc)
	if err != nil { log.Fatal(err) }
	go func() {
		<-s.Exited()
		log.Printf("child exited with code %d; sessiond stays up for viewers", s.ExitCode())
	}()
	log.Printf("sessiond listening on %s", *listen)
	if err := http.ListenAndServe(*listen, server.New(s)); err != nil {
		log.Fatal(err)
	}
	_ = os.Stdout
}
```

- [ ] **Step 6: Run tests + build**

Run: `go test ./internal/server/ -v && make build`
Expected: PASS; both binaries build with `CGO_ENABLED=0`

- [ ] **Step 7: Commit**

```bash
git add internal/server/ cmd/sessiond/ go.mod go.sum
git commit -m "feat: WebSocket attach server and sessiond binary"
```

---

### Task 9: rattach dev client

**Files:**
- Create: `cmd/rattach/main.go`

**Interfaces:**
- Consumes: wire protocol over WebSocket (Task 8's `/attach` endpoint)
- Produces: `rattach --url ws://127.0.0.1:7070 --since 0` — raw-mode terminal client; `Ctrl-]` detaches (leaves session running).

- [ ] **Step 1: Add dependency and implement**

```bash
go get golang.org/x/term@latest
```

```go
// cmd/rattach/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/term"

	"rainier/internal/wire"
)

const detachKey = 0x1d // Ctrl-]

func main() {
	url := flag.String("url", "ws://127.0.0.1:7070", "sessiond base URL")
	since := flag.Uint64("since", 0, "resume from sequence number")
	flag.Parse()

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, fmt.Sprintf("%s/attach?since=%d", *url, *since), nil)
	if err != nil { log.Fatal(err) }
	defer c.CloseNow()

	fd := int(os.Stdin.Fd())
	oldState, err := term.MakeRaw(fd)
	if err != nil { log.Fatal(err) }
	defer term.Restore(fd, oldState)

	sendSize := func() {
		w, h, err := term.GetSize(fd)
		if err == nil {
			wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: w, Rows: h})
		}
	}
	sendSize() // required first message

	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	go func() { for range winch { sendSize() } }()

	var lastSeq uint64
	go func() {
		for {
			var m wire.ServerMsg
			if err := wsjson.Read(ctx, c, &m); err != nil {
				term.Restore(fd, oldState)
				fmt.Printf("\r\n[disconnected at seq %d; rattach --since %d to resume]\r\n", lastSeq, lastSeq)
				os.Exit(0)
			}
			switch m.Type {
			case "snapshot", "output":
				os.Stdout.Write(m.Data)
				if m.Seq > 0 { lastSeq = m.Seq }
			case "exit":
				term.Restore(fd, oldState)
				fmt.Printf("\r\n[session process exited: %d]\r\n", m.ExitCode)
				os.Exit(0)
			}
		}
	}()

	buf := make([]byte, 1024)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil { return }
		for i := 0; i < n; i++ {
			if buf[i] == detachKey {
				term.Restore(fd, oldState)
				fmt.Printf("\r\n[detached at seq %d; session still running]\r\n", lastSeq)
				return
			}
		}
		wsjson.Write(ctx, c, wire.ClientMsg{Type: "stdin", Data: append([]byte(nil), buf[:n]...)})
	}
}
```

- [ ] **Step 2: Build and manually verify**

```bash
make build
./bin/sessiond --listen 127.0.0.1:7070 --log /tmp/s.log -- bash &
./bin/rattach --url ws://127.0.0.1:7070
# type: echo hi   → see output
# press Ctrl-]    → detached, sessiond still running
./bin/rattach --url ws://127.0.0.1:7070
# screen repaints with prior state (snapshot)
kill %1
```

Expected: interactive shell works; detach leaves session alive; reattach repaints prior screen.

- [ ] **Step 3: Commit**

```bash
git add cmd/rattach/ go.mod go.sum
git commit -m "feat: rattach dev client with raw mode, resize, and Ctrl-] detach"
```

---

### Task 10: Docker image + sleep-proof demo

**Files:**
- Create: `Dockerfile`, `scripts/demo.sh`
- Modify: `Makefile` (add `demo` target)

**Interfaces:**
- Consumes: `sessiond` and `rattach` binaries (Tasks 8–9)
- Produces: `make demo` — the end-to-end proof that a session survives client death.

- [ ] **Step 1: Write Dockerfile**

```dockerfile
# Dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sessiond ./cmd/sessiond

FROM alpine:3.20
RUN apk add --no-cache bash
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1: the spec's rule-1 shape.
ENTRYPOINT ["sessiond", "--listen", "0.0.0.0:7070", "--log", "/tmp/session.log", "--"]
CMD ["bash", "-i"]
```

- [ ] **Step 2: Write demo script**

```bash
#!/usr/bin/env bash
# scripts/demo.sh — proves the session outlives its clients.
set -euo pipefail
docker build -q -t rainier-sessiond .
docker rm -f rainier-demo >/dev/null 2>&1 || true
docker run -d --name rainier-demo -p 7070:7070 rainier-sessiond
sleep 1
echo "── attach #1: writing a marker into the live shell"
printf 'MARKER=hello-from-attach-1\r' | timeout 3 ./bin/rattach --url ws://127.0.0.1:7070 || true
echo "── client #1 gone (simulated laptop sleep). Reattaching…"
echo "── attach #2: interactive — you should see the prior screen. Ctrl-] to exit."
./bin/rattach --url ws://127.0.0.1:7070
docker rm -f rainier-demo >/dev/null
```

```bash
chmod +x scripts/demo.sh
```

Add to `Makefile`:

```make
demo: build
	./scripts/demo.sh
```

- [ ] **Step 3: Run the demo and verify**

Run: `make demo`
Expected: second attach repaints a shell whose scrollback shows `MARKER=hello-from-attach-1` — session state survived the first client's death.

- [ ] **Step 4: Run the full suite**

Run: `make test`
Expected: all packages PASS.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile scripts/demo.sh Makefile
git commit -m "feat: docker image and sleep-proof attach/detach/reattach demo"
```

---

### Task 11: Real-TUI fixture capture tool

**Files:**
- Create: `cmd/vtcap/main.go`
- Modify: `internal/term/term_test.go` (file-based fixture loading)

**Interfaces:**
- Consumes: `session.StartProc` (Task 5)
- Produces: `vtcap --out testdata/vt/<name>.input -- <command...>` — records raw PTY output of a real program for the emulator fixture suite; `TestSnapshotRoundTripFiles` runs the round-trip over every `testdata/vt/*.input`.

- [ ] **Step 1: Implement capture tool**

```go
// cmd/vtcap/main.go
// vtcap records a program's raw PTY output to a fixture file. Run real TUIs
// (claude, htop, vim) and press keys yourself; Ctrl-C stops recording.
package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"rainier/internal/session"
)

func main() {
	out := flag.String("out", "", "output fixture path")
	flag.Parse()
	if *out == "" || len(flag.Args()) == 0 {
		log.Fatal("usage: vtcap --out testdata/vt/name.input -- <command...>")
	}
	f, err := os.Create(*out)
	if err != nil { log.Fatal(err) }
	defer f.Close()
	var mu sync.Mutex
	p, err := session.StartProc(flag.Args(), 120, 32, func(b []byte) {
		mu.Lock()
		f.Write(b)
		os.Stdout.Write(b) // mirror so the operator sees the TUI
		mu.Unlock()
	})
	if err != nil { log.Fatal(err) }
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := os.Stdin.Read(buf)
			if err != nil { return }
			p.Write(buf[:n])
		}
	}()
	code := p.Wait()
	log.Printf("recorded to %s (exit %d)", *out, code)
}
```

- [ ] **Step 2: Add file-based fixture test**

Append to `internal/term/term_test.go`:

```go
func TestSnapshotRoundTripFiles(t *testing.T) {
	files, _ := filepath.Glob("../../testdata/vt/*.input")
	if len(files) == 0 { t.Skip("no recorded fixtures yet") }
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			input, err := os.ReadFile(f)
			if err != nil { t.Fatal(err) }
			a := NewEmulator(120, 32)
			a.Feed(input)
			sa := a.Screen()
			b := NewEmulator(120, 32)
			b.Feed(Serialize(sa))
			screensEqual(t, sa, b.Screen())
		})
	}
}
```

(Add `"os"` and `"path/filepath"` to the test file's imports.)

- [ ] **Step 3: Record one real fixture and run**

```bash
make build && go build -o bin/vtcap ./cmd/vtcap
./bin/vtcap --out testdata/vt/htop-or-vim.input -- vi -u NONE -c 'set nocp' /etc/hosts
# interact briefly, then :q!
go test ./internal/term/ -run TestSnapshotRoundTripFiles -v
```

Expected: PASS on the recorded fixture. (Recording a real Claude Code capture is a follow-up once an agent is installed in a sandbox — the tool and test are ready for it.)

- [ ] **Step 4: Commit**

```bash
git add cmd/vtcap/ internal/term/term_test.go testdata/
git commit -m "feat: vtcap fixture recorder and file-based round-trip suite"
```

---

### Task 12: Local fleet acceptance (cmux)

**Files:**
- Create: `scripts/fleet-up.sh`, `scripts/fleet-down.sh`, `docker-compose.fleet.yml`

**Interfaces:**
- Consumes: Docker image (Task 10), `rattach` (Task 9)
- Produces: a 5-session local fleet for manual acceptance from cmux.

- [ ] **Step 1: Write compose file**

```yaml
# docker-compose.fleet.yml — five independent sessions on ports 7071-7075
services:
  s1: { image: rainier-sessiond, ports: ["7071:7070"] }
  s2: { image: rainier-sessiond, ports: ["7072:7070"] }
  s3: { image: rainier-sessiond, ports: ["7073:7070"] }
  s4: { image: rainier-sessiond, ports: ["7074:7070"] }
  s5: { image: rainier-sessiond, ports: ["7075:7070"] }
```

- [ ] **Step 2: Write helper scripts**

```bash
#!/usr/bin/env bash
# scripts/fleet-up.sh
set -euo pipefail
docker build -q -t rainier-sessiond .
docker compose -f docker-compose.fleet.yml up -d
echo "Fleet up. Attach from any terminal (one per cmux pane):"
for p in 7071 7072 7073 7074 7075; do echo "  ./bin/rattach --url ws://127.0.0.1:$p"; done
```

```bash
#!/usr/bin/env bash
# scripts/fleet-down.sh
docker compose -f docker-compose.fleet.yml down
```

```bash
chmod +x scripts/fleet-up.sh scripts/fleet-down.sh
```

- [ ] **Step 3: Manual acceptance from cmux (the user drives this)**

1. `./scripts/fleet-up.sh`
2. In cmux, open five panes/tabs; run one `rattach` command per pane.
3. In each: start distinct work (`top`, an editor, a long build).
4. Detach some (Ctrl-]), close a pane entirely, quit and reopen cmux.
5. Reattach each session — every screen must repaint with its prior state.
6. `./scripts/fleet-down.sh`

Pass criteria: all five sessions independent; no session lost to any client-side event; reattach latency feels instant.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.fleet.yml scripts/fleet-up.sh scripts/fleet-down.sh
git commit -m "feat: local docker fleet for cmux acceptance testing"
```

---

## Self-Review Notes

- **Spec coverage (this plan's slice):** §5.1 PTY ownership → Tasks 5, 7; §5.2 VT emulator + snapshot-on-attach → Tasks 3, 4, 7; §5.3 event log + `attach(since_seq)` → Tasks 1, 7, 8; smallest-viewer resize → Tasks 6, 7; §11.1 golden fixtures → Tasks 3, 11; §11.4 e2e attach/detach/reattach → Tasks 8, 10; portability rule 1 (PID 1 static binary) → Tasks 8, 10; rule 3 staging → `serve(conn)` note in Task 8. Deferred to later plans (per spec): adapters, egress, suspend/resume, snapshot(), controld relay, multiplexed outbound connection.
- **Known simplifications, deliberate for Plan 1:** single listener (dev path), event log unrotated, scrollback not yet exposed beyond what the emulator retains, no auth on the dev listener (localhost only; runnerd/controld own transport auth in Plans 2–3).
- **Type consistency check:** `wire.ServerMsg`/`ClientMsg` field names match across Tasks 2, 7, 8, 9; `session.Size` used consistently; `StartProc` signature identical in Tasks 5, 8, 11.

**Execution order:** Tasks are sequential; each ends green and committed.
