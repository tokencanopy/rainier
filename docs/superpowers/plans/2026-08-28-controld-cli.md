# controld + rainier CLI Implementation Plan (Rainier v0, Plan 3 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A control plane (controld: authenticated REST + Postgres + multi-runner placement + terminal relay) and a real `rainier` CLI, with the Plan 2 fleet inverted to dial controld outbound — preserving "sessions outlive everything else."

**Architecture:** runnerd dials controld over one control WebSocket (announce → reconcile → command dispatch); attach traffic uses separate outbound dial-back connections so terminal bytes never share a stream with control messages. Postgres is the only durable store; create is write-ahead durable (row commits before dispatch); there is no message queue — `queued` rows plus reconcile-on-reannounce with id-idempotent dispatch are the queue.

**Tech Stack:** Go 1.25, `github.com/coder/websocket` (already vendored), **new dependency: `github.com/jackc/pgx/v5`** (pgxpool). No ORM, no migration framework (tiny embedded runner), no Redis/MQ.

**Spec:** `docs/superpowers/specs/2026-08-28-plan3-controld-design.md` (this plan argues from it; read it first). Parent: `docs/superpowers/specs/2026-08-27-rainier-design.md`.

## Global Constraints

- Preserve Plan 1/2 public interfaces verbatim unless a task explicitly changes them: `driver.Driver/Spec/Handle/Snapshot`, `relay.Hub/ServeSession/Conn/Frame/WSConn`, `wire.ClientMsg/ServerMsg`, `session.*`, `server.serve` resize-first contract. `rattach` must still work against direct sessiond AND runnerd's relay after every task.
- All reachability outbound (spec rule 3): sessiond → runnerd → controld; clients talk only to controld. controld never dials a runner.
- Write-ahead create invariant: the sessions row commits (state `queued`) before the HTTP 202 is written and before any dispatch. Never violate this ordering.
- Error envelope everywhere on the client API: `{"error":{"code":"...","message":"..."}}`; codes are exactly: `invalid_request`, `unauthenticated`, `forbidden`, `not_found`, `conflict`, `no_capacity`, `session_not_ready`, `runner_unreachable`, `internal`. HTTP handlers never leak Go error strings from internals into `message` for 5xx (log them; message is generic).
- Session ids: `sess_` + 32 lowercase hex chars (16 random bytes). User ids: `usr_` + 32 hex. API tokens: `rnr_` + 64 hex (32 random bytes); only the SHA-256 hex of the WHOLE token string is stored.
- Session lifecycle states (exact strings, used in Postgres, rwire, and JSON): `queued`, `creating`, `running`, `suspended_warm`, `suspended_cold`, `canceled`, `failed`, `dead`, `destroyed`. Terminal = `canceled|failed|dead|destroyed`. Slot-occupying = `creating|running|suspended_warm`.
- `CGO_ENABLED=0` builds must keep working (`make build`).
- Fail closed: empty admin+member allowlist ⇒ every login rejected; missing `--runner-token` ⇒ runnerd dial mode and controld refuse to start; egress default-deny unchanged.
- Timestamps in API JSON: RFC 3339 UTC. All list responses are object-wrapped (`{"sessions":[...]}`), never bare arrays.
- Commits: conventional prefixes (`feat:`/`fix:`/`test:`/`docs:`) matching the repo's log.

## File Structure

```
internal/rwire/rwire.go                      controld↔runnerd message vocabulary (Task 1)
internal/controld/store.go                   domain types, Store interface, errors, id/token helpers (Task 2)
internal/controld/memstore.go                in-memory Store (Task 2)
internal/controld/storetest/contract.go      Store contract suite, run by memstore + pgstore (Task 2)
internal/controld/pgstore/pgstore.go         Postgres Store (Task 3)
internal/controld/pgstore/migrate.go         embedded migration runner (Task 3)
internal/controld/pgstore/migrations/0001_init.sql  (Task 3)
internal/controld/controld.go                Server, Config, New, Handler, Run (Task 7)
internal/controld/runners.go                 runner conns, /v0/runners/connect, reconcile, dispatch (Task 7)
internal/controld/sched.go                   scheduler loop, placement (Task 8)
internal/controld/auth.go                    GitHub exchange, bearer middleware, roles (Task 9)
internal/controld/api.go                     sessions/runners REST, error envelope, pagination (Task 10)
internal/controld/attach.go                  pairing table, client attach WS, attach-back, splice (Task 11)
cmd/controld/main.go                         flags/env wiring (Task 10)
internal/attachio/attachio.go                raw-mode attach loop extracted from rattach (Task 12)
internal/cli/config.go, client.go            ~/.config/rainier config + REST client (Task 12)
cmd/rainier/main.go                          the CLI (Task 12)
internal/driver/driver.go, docker.go, fake.go   List(), Spec.ProxyURL, Capacity fix (Task 4)
internal/runnerd/runnerd.go, registry.go     recovery, hub-death inspect, OnEvent hook, core-op extraction (Tasks 4–6)
internal/runnerd/agent.go                    dial mode: connect/announce/execute (Task 6)
cmd/sessiond/main.go                         redial loop (Task 5)
cmd/runnerd/main.go                          --controld/--runner-token/--runner-name flags (Task 6)
docker-compose.fleet.yml, scripts/fleet-up.sh   R4 + controld dev wiring (Tasks 13, 14)
scripts/egress-check.sh, scripts/e2e-fleet.sh, scripts/gce-up.sh  (Tasks 13, 14)
docs/deploy-gce.md, README.md                (Task 14)
```

Execution order is Task 1 → 14; Tasks 1–3 (types/store) and 4–5 (fleet hardening) are independent of each other and may be done in either order, but everything from Task 6 on assumes all of 1–5 are merged.

---

### Task 1: `internal/rwire` — runnerd↔controld message vocabulary

**Files:**
- Create: `internal/rwire/rwire.go`
- Test: `internal/rwire/rwire_test.go`

**Interfaces:**
- Consumes: nothing (pure types).
- Produces (used by Tasks 6, 7, 11): `rwire.Proto = 1`; `rwire.FromRunner{Type, Proto, Runner, Sessions, Used, Total, ReqID, OK, Detail, Session, State}`; `rwire.ToRunner{Type, ReqID, Session, Spec, Warm, Attach}`; `rwire.SessionInfo{ID, State}`; `rwire.Spec{Name, Image, Cmd, EgressAllow}`; `rwire.Attach{AttachID, Since, Cols, Rows, TargetURL}`.

- [ ] **Step 1: Write the failing test**

```go
// internal/rwire/rwire_test.go
package rwire

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := ToRunner{Type: "create", ReqID: 7, Session: "sess_ab12",
		Spec: &Spec{Image: "img", Cmd: []string{"bash"}, EgressAllow: []string{"example.com"}}}
	b, err := json.Marshal(in)
	if err != nil { t.Fatal(err) }
	var out ToRunner
	if err := json.Unmarshal(b, &out); err != nil { t.Fatal(err) }
	if out.ReqID != 7 || out.Spec == nil || out.Spec.Image != "img" {
		t.Fatalf("round trip mangled: %+v", out)
	}
}

func TestUnknownFieldsTolerated(t *testing.T) {
	// Forward compatibility: an older side must not choke on new fields.
	var m FromRunner
	if err := json.Unmarshal([]byte(`{"type":"event","session":"s","state":"running","future_field":1}`), &m); err != nil {
		t.Fatalf("unknown field should be ignored: %v", err)
	}
	if m.State != "running" { t.Fatalf("state lost: %+v", m) }
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./internal/rwire/` → FAIL (package missing).

- [ ] **Step 3: Implement**

```go
// Package rwire defines the JSON messages exchanged between runnerd and
// controld over runnerd's single outbound control WebSocket. One struct per
// direction, same idiom as internal/wire. Proto gates major changes: controld
// rejects an announce whose Proto it doesn't speak, with a close reason
// naming both versions (design §4.3).
package rwire

const Proto = 1

// FromRunner: runnerd → controld. Used/Total (capacity) piggyback on every
// message type so controld's runner view is always current without a separate
// capacity message.
type FromRunner struct {
	Type     string        `json:"type"` // "announce" | "result" | "event"
	Proto    int           `json:"proto,omitempty"`    // announce
	Runner   string        `json:"runner,omitempty"`   // announce
	Sessions []SessionInfo `json:"sessions,omitempty"` // announce
	Used     int           `json:"used"`
	Total    int           `json:"total"`
	ReqID    uint64        `json:"req_id,omitempty"` // result: correlates ToRunner.ReqID
	OK       bool          `json:"ok,omitempty"`     // result
	Detail   string        `json:"detail,omitempty"` // result: error text, or snapshot ref
	Session  string        `json:"session,omitempty"` // event
	State    string        `json:"state,omitempty"`   // event: "running" | "dead"
}

type SessionInfo struct {
	ID    string `json:"id"`
	State string `json:"state"` // "running"|"suspended_warm"|"suspended_cold"
}

// ToRunner: controld → runnerd.
type ToRunner struct {
	Type    string  `json:"type"` // "create"|"destroy"|"suspend"|"resume"|"snapshot"|"dial_attach"
	ReqID   uint64  `json:"req_id,omitempty"`
	Session string  `json:"session,omitempty"`
	Spec    *Spec   `json:"spec,omitempty"`   // create
	Warm    bool    `json:"warm,omitempty"`   // suspend
	Attach  *Attach `json:"attach,omitempty"` // dial_attach
}

type Spec struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
}

type Attach struct {
	AttachID  string `json:"attach_id"`
	Since     uint64 `json:"since"`
	Cols      int    `json:"cols"`
	Rows      int    `json:"rows"`
	TargetURL string `json:"target_url"` // ws(s) URL of THIS controld replica's attach-back endpoint
}
```

- [ ] **Step 4: Run to verify pass** — `go test ./internal/rwire/` → PASS.
- [ ] **Step 5: Commit** — `git add internal/rwire && git commit -m "feat: rwire message vocabulary for runnerd<->controld protocol"`

---

### Task 2: controld domain types, `Store` interface, contract suite, memstore

**Files:**
- Create: `internal/controld/store.go`, `internal/controld/memstore.go`, `internal/controld/storetest/contract.go`
- Test: `internal/controld/memstore_test.go`

**Interfaces:**
- Produces (used by every later controld task):

```go
package controld

type SessionState string
const (
	StateQueued        SessionState = "queued"
	StateCreating      SessionState = "creating"
	StateRunning       SessionState = "running"
	StateSuspendedWarm SessionState = "suspended_warm"
	StateSuspendedCold SessionState = "suspended_cold"
	StateCanceled      SessionState = "canceled"
	StateFailed        SessionState = "failed"
	StateDead          SessionState = "dead"
	StateDestroyed     SessionState = "destroyed"
)
func (s SessionState) Terminal() bool     // canceled|failed|dead|destroyed
func (s SessionState) OccupiesSlot() bool // creating|running|suspended_warm
var NonTerminal []SessionState            // every non-terminal state, for Transition from-lists

type User struct { ID string; GitHubID int64; Login, Role string; CreatedAt time.Time }
type Session struct {
	ID, OwnerID, Name, Image string
	Cmd, EgressAllow []string
	State SessionState
	Runner string          // runner name; "" until placed
	IdempotencyKey string  // "" = none
	Error string
	CreatedAt, UpdatedAt, LastEventAt time.Time
}
type Runner struct { Name string; CapacityUsed, CapacityTotal int; Connected bool; LastSeenAt time.Time }

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")   // guarded transition lost / name taken
var ErrIdemReplay = errors.New("idempotent replay") // CreateSession: key already used

type TransitionOpts struct { Runner *string; Error *string } // nil = leave column alone

type SessionQuery struct {
	States []SessionState; Runner string
	IncludeTerminal bool
	Limit int; Cursor string
}

type Store interface {
	UpsertUser(ctx context.Context, githubID int64, login, role string) (User, error)
	InsertToken(ctx context.Context, userID, tokenHash string) error
	UserByToken(ctx context.Context, tokenHash string) (User, error) // touches last_used_at; ErrNotFound

	CreateSession(ctx context.Context, s Session) (Session, error) // ErrConflict (name), ErrIdemReplay (idem key)
	GetSession(ctx context.Context, id string) (Session, error)
	SessionByIdem(ctx context.Context, ownerID, key string) (Session, error)
	SessionByName(ctx context.Context, ownerID, name string) (Session, error) // non-terminal only
	ListSessions(ctx context.Context, q SessionQuery) (rows []Session, next string, err error)
	SessionsOnRunner(ctx context.Context, runner string, states []SessionState) ([]Session, error)
	OldestQueued(ctx context.Context) ([]Session, error) // created_at asc
	Transition(ctx context.Context, id string, from []SessionState, to SessionState, opts TransitionOpts) error // ErrConflict if state ∉ from; also bumps updated_at + last_event_at

	UpsertRunner(ctx context.Context, r Runner) error       // by Name; sets connected/capacity/last_seen
	SetRunnerConnected(ctx context.Context, name string, connected bool) error
	ListRunners(ctx context.Context) ([]Runner, error)
}

func NewSessionID() string  // "sess_"+32hex
func NewUserID() string     // "usr_"+32hex
func NewToken() (token, hash string) // "rnr_"+64hex, sha256 hex of the token string
func HashToken(token string) string
func NewMemStore() Store
```

- `storetest.RunContract(t *testing.T, open func(t *testing.T) controld.Store)` — the suite both adapters must pass (mirrors `driver.RunContract`).

- [ ] **Step 1: Write the contract suite (this IS the failing test)**

`internal/controld/storetest/contract.go` — one exported func running named subtests. Write these subtests, each a complete test body (they define Store's semantics; pgstore in Task 3 must pass them unchanged):

```go
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"rainier/internal/controld"
)

func RunContract(t *testing.T, open func(t *testing.T) controld.Store) {
	ctx := context.Background()
	mkUser := func(t *testing.T, st controld.Store) controld.User {
		u, err := st.UpsertUser(ctx, 42, "alice", "admin")
		if err != nil { t.Fatal(err) }
		return u
	}
	mkSess := func(t *testing.T, st controld.Store, owner, name string) controld.Session {
		s, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: owner, Name: name,
			Image: "img", State: controld.StateQueued})
		if err != nil { t.Fatal(err) }
		return s
	}

	t.Run("user upsert is stable by github id", func(t *testing.T) {
		st := open(t)
		u1 := mkUser(t, st)
		u2, err := st.UpsertUser(ctx, 42, "alice-renamed", "admin")
		if err != nil { t.Fatal(err) }
		if u1.ID != u2.ID { t.Fatalf("same github id must keep user id: %s vs %s", u1.ID, u2.ID) }
		if u2.Login != "alice-renamed" { t.Fatalf("login should update") }
	})

	t.Run("token round trip and unknown token", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		tok, hash := controld.NewToken()
		if err := st.InsertToken(ctx, u.ID, hash); err != nil { t.Fatal(err) }
		got, err := st.UserByToken(ctx, controld.HashToken(tok))
		if err != nil || got.ID != u.ID { t.Fatalf("lookup: %v %+v", err, got) }
		if _, err := st.UserByToken(ctx, controld.HashToken("rnr_bogus")); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("guarded transition: wrong from-state loses with ErrConflict", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s := mkSess(t, st, u.ID, "")
		r := "vm1"
		if err := st.Transition(ctx, s.ID, []controld.SessionState{controld.StateQueued}, controld.StateCreating, controld.TransitionOpts{Runner: &r}); err != nil { t.Fatal(err) }
		err := st.Transition(ctx, s.ID, []controld.SessionState{controld.StateQueued}, controld.StateCanceled, controld.TransitionOpts{})
		if !errors.Is(err, controld.ErrConflict) { t.Fatalf("want ErrConflict, got %v", err) }
		got, _ := st.GetSession(ctx, s.ID)
		if got.State != controld.StateCreating || got.Runner != "vm1" { t.Fatalf("state clobbered: %+v", got) }
	})

	t.Run("active name unique per owner; freed by terminal state", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		mkSess(t, st, u.ID, "dev")
		_, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, Name: "dev", State: controld.StateQueued})
		if !errors.Is(err, controld.ErrConflict) { t.Fatalf("want ErrConflict, got %v", err) }
		// terminal frees the name
		first, _ := st.SessionByName(ctx, u.ID, "dev")
		if err := st.Transition(ctx, first.ID, controld.NonTerminal, controld.StateCanceled, controld.TransitionOpts{}); err != nil { t.Fatal(err) }
		if _, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, Name: "dev", State: controld.StateQueued}); err != nil {
			t.Fatalf("terminal session must free the name: %v", err)
		}
	})

	t.Run("idempotency key replays", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s1, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, IdempotencyKey: "k1", State: controld.StateQueued})
		if err != nil { t.Fatal(err) }
		_, err = st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, IdempotencyKey: "k1", State: controld.StateQueued})
		if !errors.Is(err, controld.ErrIdemReplay) { t.Fatalf("want ErrIdemReplay, got %v", err) }
		got, err := st.SessionByIdem(ctx, u.ID, "k1")
		if err != nil || got.ID != s1.ID { t.Fatalf("replay lookup: %v %+v", err, got) }
	})

	t.Run("list pagination is stable and cursor resumes", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		var ids []string
		for i := 0; i < 5; i++ {
			s := mkSess(t, st, u.ID, "")
			ids = append(ids, s.ID)
			time.Sleep(2 * time.Millisecond) // distinct created_at
		}
		page1, next, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 3})
		if err != nil || len(page1) != 3 || next == "" { t.Fatalf("page1: %v n=%d next=%q", err, len(page1), next) }
		page2, next2, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 3, Cursor: next})
		if err != nil || len(page2) != 2 || next2 != "" { t.Fatalf("page2: %v n=%d", err, len(page2)) }
		if page1[0].ID != ids[4] { t.Fatalf("newest first: got %s want %s", page1[0].ID, ids[4]) }
		seen := map[string]bool{}
		for _, s := range append(page1, page2...) { seen[s.ID] = true }
		if len(seen) != 5 { t.Fatalf("pages overlap or drop: %v", seen) }
	})

	t.Run("terminal sessions hidden unless IncludeTerminal", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s := mkSess(t, st, u.ID, "")
		st.Transition(ctx, s.ID, controld.NonTerminal, controld.StateDead, controld.TransitionOpts{})
		rows, _, _ := st.ListSessions(ctx, controld.SessionQuery{Limit: 10})
		if len(rows) != 0 { t.Fatalf("terminal leaked into default list") }
		rows, _, _ = st.ListSessions(ctx, controld.SessionQuery{Limit: 10, IncludeTerminal: true})
		if len(rows) != 1 { t.Fatalf("IncludeTerminal missing row") }
	})

	t.Run("runners upsert and sessions-on-runner filter", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		if err := st.UpsertRunner(ctx, controld.Runner{Name: "vm1", CapacityUsed: 1, CapacityTotal: 4, Connected: true, LastSeenAt: time.Now()}); err != nil { t.Fatal(err) }
		s := mkSess(t, st, u.ID, "")
		r := "vm1"
		st.Transition(ctx, s.ID, controld.NonTerminal, controld.StateCreating, controld.TransitionOpts{Runner: &r})
		on, err := st.SessionsOnRunner(ctx, "vm1", []controld.SessionState{controld.StateCreating})
		if err != nil || len(on) != 1 || on[0].ID != s.ID { t.Fatalf("on-runner: %v %+v", err, on) }
		runners, _ := st.ListRunners(ctx)
		if len(runners) != 1 || runners[0].CapacityTotal != 4 { t.Fatalf("runners: %+v", runners) }
	})

	t.Run("oldest queued ordering", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		a := mkSess(t, st, u.ID, "")
		time.Sleep(2 * time.Millisecond)
		mkSess(t, st, u.ID, "")
		q, err := st.OldestQueued(ctx)
		if err != nil || len(q) != 2 || q[0].ID != a.ID { t.Fatalf("fifo order: %v %+v", err, q) }
	})
}
```

`internal/controld/memstore_test.go`:

```go
package controld_test

import (
	"testing"
	"rainier/internal/controld"
	"rainier/internal/controld/storetest"
)

func TestMemStoreContract(t *testing.T) {
	storetest.RunContract(t, func(t *testing.T) controld.Store { return controld.NewMemStore() })
}
```

- [ ] **Step 2: Run to verify fail** — `go test ./internal/controld/...` → FAIL (types missing).

- [ ] **Step 3: Implement `store.go`** — types exactly as the Interfaces block above, plus:

```go
func (s SessionState) Terminal() bool {
	switch s {
	case StateCanceled, StateFailed, StateDead, StateDestroyed: return true
	}
	return false
}
func (s SessionState) OccupiesSlot() bool {
	switch s {
	case StateCreating, StateRunning, StateSuspendedWarm: return true
	}
	return false
}
var NonTerminal = []SessionState{StateQueued, StateCreating, StateRunning, StateSuspendedWarm, StateSuspendedCold}

func randHex(n int) string { b := make([]byte, n); rand.Read(b); return hex.EncodeToString(b) } // crypto/rand
func NewSessionID() string { return "sess_" + randHex(16) }
func NewUserID() string    { return "usr_" + randHex(16) }
func NewToken() (string, string) { tok := "rnr_" + randHex(32); return tok, HashToken(tok) }
func HashToken(tok string) string { h := sha256.Sum256([]byte(tok)); return hex.EncodeToString(h[:]) }
```

- [ ] **Step 4: Implement `memstore.go`** — one mutex, `map[string]*Session`, `map[string]*User` (+ by-github index), `map[string]string` token-hash→userID, `map[string]*Runner`. Semantics to get exactly right (the contract pins them):
  - `CreateSession`: reject with `ErrConflict` if another **non-terminal** session of the same owner has the same non-empty name; reject with `ErrIdemReplay` if the owner already used the key. Stamp `CreatedAt/UpdatedAt/LastEventAt = time.Now()` when zero.
  - `Transition`: `if !slices.Contains(from, cur.State) { return ErrConflict }`; apply `to`, `opts.Runner`/`opts.Error` when non-nil, bump `UpdatedAt`+`LastEventAt`. Missing id → `ErrNotFound`.
  - `ListSessions`: sort `created_at desc, id desc`; cursor is `fmt.Sprintf("%d|%s", CreatedAt.UnixNano(), ID)` base64-rawurl-encoded; a page starts strictly after the cursor row; `next` empty when the page wasn't full. Default-exclude terminal states unless `IncludeTerminal`.
  - Return **copies**, never internal pointers (same discipline as `registry.list()`).

- [ ] **Step 5: Run to verify pass** — `go test ./internal/controld/...` → PASS.
- [ ] **Step 6: `go vet ./... && make build`** → clean.
- [ ] **Step 7: Commit** — `git commit -m "feat: controld domain types, Store interface, contract suite, memstore"`

---

### Task 3: `pgstore` — Postgres Store with embedded migrations

**Files:**
- Create: `internal/controld/pgstore/pgstore.go`, `internal/controld/pgstore/migrate.go`, `internal/controld/pgstore/migrations/0001_init.sql`
- Test: `internal/controld/pgstore/pgstore_test.go`

**Interfaces:**
- Consumes: `controld.Store` + `storetest.RunContract` (Task 2).
- Produces: `pgstore.Open(ctx context.Context, dsn string) (*Store, error)` (runs migrations, returns a `controld.Store`); `pgstore.Migrate(ctx context.Context, pool *pgxpool.Pool) error`.

- [ ] **Step 1: Add the dependency** — `go get github.com/jackc/pgx/v5@latest && go mod tidy`. Commit separately: `git commit -m "feat: add pgx dependency" go.mod go.sum`.

- [ ] **Step 2: Write the failing test** — the contract suite against a throwaway Postgres container (same require-docker pattern as the docker driver tests):

```go
// internal/controld/pgstore/pgstore_test.go
package pgstore

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"rainier/internal/controld"
	"rainier/internal/controld/storetest"
)

// startPostgres runs a disposable postgres:16-alpine container on an
// ephemeral host port and returns a DSN. Skips when docker is unavailable
// (mirrors the docker driver's mustDockerPath discipline).
func startPostgres(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil { t.Skip("docker not available") }
	out, err := exec.Command("docker", "run", "-d", "--rm",
		"-e", "POSTGRES_PASSWORD=test", "-p", "127.0.0.1:0:5432", "postgres:16-alpine").Output()
	if err != nil { t.Fatalf("start postgres: %v", err) }
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", id).Run() })
	port, err := exec.Command("docker", "port", id, "5432/tcp").Output()
	if err != nil { t.Fatal(err) }
	// "127.0.0.1:49213\n" → port
	hp := strings.TrimSpace(strings.Split(string(port), "\n")[0])
	dsn := fmt.Sprintf("postgres://postgres:test@%s/postgres?sslmode=disable", hp)
	// wait for readiness: retry Open until it succeeds or 30s passes
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := Open(context.Background(), dsn); err == nil { break }
		if time.Now().After(deadline) { t.Fatal("postgres never became ready") }
		time.Sleep(300 * time.Millisecond)
	}
	return dsn
}

func TestPGStoreContract(t *testing.T) {
	dsn := startPostgres(t)
	n := 0
	storetest.RunContract(t, func(t *testing.T) controld.Store {
		// fresh schema per subtest: unique search_path-free approach — create
		// a numbered database so subtests don't see each other's rows.
		n++
		name := fmt.Sprintf("contract_%d", n)
		admin, err := Open(context.Background(), dsn)
		if err != nil { t.Fatal(err) }
		if err := admin.execAdmin(context.Background(), "CREATE DATABASE "+name); err != nil { t.Fatal(err) }
		st, err := Open(context.Background(), strings.Replace(dsn, "/postgres?", "/"+name+"?", 1))
		if err != nil { t.Fatal(err) }
		return st
	})
}
```

(`execAdmin` is a tiny unexported helper on `*Store` running raw SQL — test-only convenience, exported to the test via same-package access.)

- [ ] **Step 3: Run to verify fail** — `go test ./internal/controld/pgstore/` → FAIL (no `Open`).

- [ ] **Step 4: Write the migration** — `migrations/0001_init.sql`:

```sql
CREATE TABLE users (
  id text PRIMARY KEY,
  github_id bigint UNIQUE NOT NULL,
  login text NOT NULL,
  role text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE api_tokens (
  id text PRIMARY KEY,
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash text UNIQUE NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz
);
CREATE TABLE runners (
  name text PRIMARY KEY,
  capacity_used int NOT NULL DEFAULT 0,
  capacity_total int NOT NULL DEFAULT 0,
  connected boolean NOT NULL DEFAULT false,
  last_seen_at timestamptz
);
CREATE TABLE sessions (
  id text PRIMARY KEY,
  owner_id text NOT NULL REFERENCES users(id),
  name text NOT NULL DEFAULT '',
  image text NOT NULL DEFAULT '',
  cmd jsonb NOT NULL DEFAULT '[]',
  egress_allow jsonb NOT NULL DEFAULT '[]',
  state text NOT NULL,
  runner text,
  idempotency_key text,
  error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_event_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX sessions_idem ON sessions(owner_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX sessions_owner_name_active ON sessions(owner_id, name)
  WHERE name <> '' AND state NOT IN ('canceled','failed','dead','destroyed');
CREATE INDEX sessions_list ON sessions(created_at DESC, id DESC);
CREATE INDEX sessions_runner ON sessions(runner) WHERE runner IS NOT NULL;
```

- [ ] **Step 5: Write `migrate.go`** — `//go:embed migrations/*.sql`; ~40 lines: `CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`; list embedded files sorted by name; for each version > max(applied): run the file and insert the version **in one transaction**. Versions are the leading integer of the filename.

- [ ] **Step 6: Write `pgstore.go`** — `type Store struct { pool *pgxpool.Pool }`. Implementation notes that must be followed (each maps to a contract subtest):
  - `UpsertUser`: `INSERT ... ON CONFLICT (github_id) DO UPDATE SET login=EXCLUDED.login, role=EXCLUDED.role RETURNING ...`; generate `NewUserID()` for the insert path.
  - `CreateSession`: single INSERT; map unique-violation on `sessions_owner_name_active` → `ErrConflict`, on `sessions_idem` → `ErrIdemReplay` (pgx: `pgconn.PgError.Code == "23505"`, disambiguate by `ConstraintName`).
  - `Transition`: `UPDATE sessions SET state=$1, runner=COALESCE($2, runner), error=COALESCE($3, error), updated_at=now(), last_event_at=now() WHERE id=$4 AND state = ANY($5)`; 0 rows affected → `GetSession` to distinguish `ErrNotFound` from `ErrConflict`.
  - `ListSessions` cursor: decode `unixnano|id`; `WHERE (created_at, id) < (to_timestamp($c/1e9), $id)` with `ORDER BY created_at DESC, id DESC LIMIT $n`; same base64-rawurl cursor format as memstore (the CLI treats it as opaque; the two stores still must agree because tests share the contract).
  - `UserByToken`: `UPDATE api_tokens SET last_used_at=now() WHERE token_hash=$1 RETURNING user_id` then fetch user; 0 rows → `ErrNotFound`.
  - `cmd`/`egress_allow` marshal via `json.Marshal`/`Unmarshal` to jsonb.

- [ ] **Step 7: Run to verify pass** — `go test ./internal/controld/... -count=1` → PASS (memstore and pgstore run the same suite).
- [ ] **Step 8: Commit** — `git commit -m "feat: pgstore Postgres Store with embedded migrations"`

---

### Task 4: driver `List` + Capacity fix + `Spec.ProxyURL`; runnerd recovery from labels

**Files:**
- Modify: `internal/driver/driver.go`, `internal/driver/docker.go`, `internal/driver/fake.go`, `internal/driver/contract.go`
- Modify: `internal/runnerd/runnerd.go`, `internal/runnerd/registry.go`
- Test: `internal/driver/docker_test.go` (extend), `internal/runnerd/runnerd_test.go` (extend)

**Interfaces:**
- Produces:

```go
// driver.go additions
type Listed struct { SessionID string; Handle Handle }
// Driver interface gains:
List(ctx context.Context) ([]Listed, error) // every rainier-labeled container, any state
// Spec gains:
ProxyURL string // egress proxy URL; injected as HTTP_PROXY/HTTPS_PROXY (+NO_PROXY=localhost,127.0.0.1,host.docker.internal)
// runnerd
func (s *Server) Recover(ctx context.Context) error // rebuild registry from drv.List()
```

- **Capacity semantic change (design §4.7):** `Capacity` counts only slot-occupying containers — `running` + `paused` — NOT stopped (cold-parked) ones. Today's `docker ps -aq` counts everything; that would make cold sessions eat slots.

- [ ] **Step 1: Extend the driver contract suite** with two subtests in `contract.go` (they run against fake and docker drivers):
  - `list reflects create and destroy`: create → `List` contains `{SessionID, Handle.ID}` with `StateRunning`; destroy → gone from `List`.
  - `capacity ignores cold-parked`: create (used=1) → `Suspend(id, false)` (cold) → `Capacity` used drops back to 0, but `List` still returns the container with `StateSuspended`.
- [ ] **Step 2: Run to verify fail** — `go test ./internal/driver/ -run Contract` → FAIL (no `List`; capacity counts stopped).
- [ ] **Step 3: Implement.**
  - `docker.go` `List`: `docker ps -a --filter label=<label> --format {{.ID}}\t{{.Label "<label>"}}\t{{.State}}`; parse lines; map state `running→StateRunning`, everything else → `StateSuspended` (same mapping as `Inspect`).
  - `docker.go` `Capacity`: `docker ps -q --filter label=<label> --filter status=running --filter status=paused` (multiple status filters OR together); count lines.
  - `docker.go` `Create`: when `spec.ProxyURL != ""`, inject **both cases** of each var (tools disagree on which they read — BusyBox wget and curl read lowercase, many Go/Node tools read uppercase): `HTTP_PROXY`/`http_proxy`/`HTTPS_PROXY`/`https_proxy` = the URL, `NO_PROXY`/`no_proxy` = `localhost,127.0.0.1,host.docker.internal`.
  - `fake.go`: track per-session state; `List` returns everything, `Capacity` counts only running+warm entries. Honor cold vs warm in `Suspend(id, warm)`.
- [ ] **Step 4: Run to verify pass** — `go test ./internal/driver/` → PASS (docker subtests auto-skip without docker; run them locally where docker exists).
- [ ] **Step 5: Write the failing runnerd recovery test** in `runnerd_test.go`: build a fake driver pre-seeded with two sessions (one running, one cold-suspended); `s := runnerd.New(fake, dial, ""); s.Recover(ctx)`; then `GET /sessions` (httptest) lists both, states `running` and `suspended`; an attach to the running one waits for a hub (does not 404); a `resume`+re-register on the suspended one works.
- [ ] **Step 6: Implement `Recover`** in `runnerd.go`:

```go
// Recover rebuilds the in-memory registry from the driver's labeled
// containers, so a restarted runnerd is truthful about sessions that
// outlived it. Hubs stay nil until each sessiond redials /register
// (Task 5's redial loop is what makes that happen).
func (s *Server) Recover(ctx context.Context) error {
	listed, err := s.drv.List(ctx)
	if err != nil { return err }
	for _, l := range listed {
		state := "running"
		if l.Handle.State == driver.StateSuspended { state = "suspended" }
		e := &sessionEntry{id: l.SessionID, handle: l.Handle.ID, state: state}
		s.reg.put(l.SessionID, e)
	}
	return nil
}
```

Call it from `cmd/runnerd/main.go` before serving, logging the count. Note: recovered entries have `allow: nil` — egress rules were pushed at create and egressd (a separate process) still holds them; re-pushing is unnecessary in v0 and egressd restart wiping rules is a ledgered known gap (document in the code comment).
- [ ] **Step 7: Run to verify pass** — `go test ./internal/runnerd/` → PASS.
- [ ] **Step 8: Commit** — `git commit -m "feat: driver List + slot-accurate Capacity + proxy env; runnerd registry recovery"`

---

### Task 5: sessiond redial; runnerd inspect-before-destroy on hub death

**Files:**
- Modify: `cmd/sessiond/main.go`, `internal/runnerd/registry.go`, `internal/runnerd/runnerd.go`
- Test: `internal/runnerd/runnerd_test.go` (extend)

**Interfaces:**
- Produces (registry, replacing `onHubDeath`):

```go
// hubDied clears the entry's hub if (and only if) it is deadHub, returning
// the handle and state so the caller can decide the container's fate.
// Keeps the entry either way.
func (r *registry) hubDied(id string, deadHub *relay.Hub) (handle, state string, ok bool)
// removeIfHubless removes the entry only if no re-register has installed a
// fresh hub since hubDied — the guard that makes inspect-then-remove safe.
func (r *registry) removeIfHubless(id string) bool
```

- `runnerd.Server` gains `OnEvent func(sessionID, state string)` (nil-safe), fired with `"running"` after a successful `setHub` in `register()`, and `"dead"` when the crash path destroys a container. Task 6's agent wires it to the control conn; HTTP-only mode leaves it nil.

**Why:** today sessiond dials once and `log.Fatalf`s; a runnerd restart kills every session on the VM. And runnerd's `onHubDeath` treats every non-suspend hub death as a container crash and destroys it — wrong once sessiond survives connection loss. Design §4.8: **hub death no longer implies container death**; inspect first.

- [ ] **Step 1: Write the failing runnerd test** — `TestHubDeathAliveContainerKeepsSession`: fake driver whose `Inspect` reports `StateRunning`; register a fake sessiond conn for a created session; kill the conn (close it); assert: `Destroy` was NOT called (fake records calls), the registry entry still exists (GET /sessions shows it `running`), and a second `/register` dial-in for the same id succeeds and attach works. Companion test `TestHubDeathGoneContainerDestroys`: fake `Inspect` reports `StateGone` → entry removed, `Destroy` called (today's behavior preserved for real crashes).
- [ ] **Step 2: Run to verify fail** — the alive-container case fails (current code destroys unconditionally).
- [ ] **Step 3: Implement the registry change** (replace `onHubDeath` with the two methods above — `hubDied` keeps the suspend-state logic: on `suspending|suspended` it also normalizes state to `suspended`) **and the `register()` tail**:

```go
	<-hub.Done()
	handle, state, ok := s.reg.hubDied(id, hub)
	hub.Close()
	if !ok { return } // stale: a newer hub already replaced this one
	if state == "suspending" || state == "suspended" { return } // deliberate cold suspend: keep
	// The conn died but that no longer proves the container did: sessiond
	// now survives conn loss and redials (see cmd/sessiond). Ask the driver.
	h, err := s.drv.Inspect(context.Background(), handle)
	if err == nil && h.State == driver.StateRunning {
		log.Printf("session %s lost its conn but the container is alive; awaiting re-register", id)
		return
	}
	if s.reg.removeIfHubless(id) {
		s.drv.Destroy(context.Background(), handle)
		if s.OnEvent != nil { s.OnEvent(id, "dead") }
	}
```

And after the successful `setHub` in `register()`: `if s.OnEvent != nil { s.OnEvent(id, "running") }`.
- [ ] **Step 4: Run to verify pass** — `go test ./internal/runnerd/` → PASS, including the pre-existing suspend-keeps-entry tests (they pin the deliberate-suspend branch).
- [ ] **Step 5: sessiond redial.** Replace the one-shot dial block in `cmd/sessiond/main.go` with:

```go
	if *dial != "" {
		dialLoop(context.Background(), *dial, *sessionID, s)
		return
	}
```

```go
// dialLoop keeps sessiond registered with runnerd for the life of the
// process: dial failures at boot and conn deaths later both retry with
// jittered exponential backoff (1s..30s cap). The session — the PTY, the
// agent, the event log — is never coupled to any single connection's
// lifetime (spec §10: sessions outlive everything else). A destroyed
// session's container is removed by runnerd itself, which is what actually
// ends this loop (SIGTERM → main's handler).
func dialLoop(ctx context.Context, dial, sessionID string, s *session.Session) {
	backoff := time.Second
	for {
		c, _, err := websocket.Dial(ctx, dial+"?session="+sessionID, nil)
		if err == nil {
			c.SetReadLimit(16 << 20)
			log.Printf("sessiond registered with runnerd as %s", sessionID)
			backoff = time.Second
			if err := relay.ServeSession(ctx, relay.WSConn(c), s); err != nil {
				log.Printf("relay ended: %v; redialing", err)
			}
		} else {
			log.Printf("dial runnerd: %v; retrying in %s", err, backoff)
		}
		jitter := time.Duration(mrand.Int63n(int64(backoff / 2)))
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			return
		}
		if backoff < 30*time.Second { backoff *= 2 }
	}
}
```

(`mrand` = `math/rand`; timing jitter, not security.)
- [ ] **Step 6: Integration check** — `make build && make test`; then manually (docker present): `./scripts/fleet-up.sh`, `runnerctl create`, `kill` the runnerd process, restart runnerd **with Recover wired (Task 4)**, and verify `runnerctl ls` shows the session and `runnerctl attach` works after the sessiond redial lands (≤30 s). Record the result in the commit message.
- [ ] **Step 7: Commit** — `git commit -m "fix: sessiond redials with backoff; runnerd inspects before destroying on hub death"`

---

### Task 6: runnerd dial mode (the agent)

**Files:**
- Modify: `internal/runnerd/runnerd.go` (extract core ops), `cmd/runnerd/main.go`
- Create: `internal/runnerd/agent.go`
- Test: `internal/runnerd/agent_test.go`

**Interfaces:**
- Consumes: `rwire` (Task 1), `Recover` (Task 4), `OnEvent` (Task 5).
- Produces:

```go
// Extracted core ops (HTTP handlers now call these; the agent calls the same ones):
func (s *Server) CreateWithID(ctx context.Context, id string, spec driver.Spec, allow []string) error
func (s *Server) Op(ctx context.Context, id, op string, warm bool) (snapshotRef string, err error) // op ∈ suspend|resume|snapshot
func (s *Server) Delete(ctx context.Context, id string) error
func (s *Server) Announce() []rwire.SessionInfo // registry snapshot in rwire vocabulary
// The agent:
type AgentConfig struct {
	ControldURL string // e.g. ws://host:9090 — /v0/runners/connect appended
	Token       string
	RunnerName  string
	ProxyURL    string // forwarded into every driver.Spec (egress R4)
}
func (s *Server) RunAgent(ctx context.Context, cfg AgentConfig) error // reconnect loop; returns only on ctx cancel
```

- [ ] **Step 1: Extract core ops (refactor, no behavior change).** Move the bodies of `sessions` POST and `sessionOp` into `CreateWithID` / `Op` / `Delete`; the HTTP handlers keep id minting (`s.newID()` — dev surface only), status-code mapping, and JSON. `CreateWithID` performs exactly today's sequence: `pushEgress` → `reg.put(starting)` → `drv.Create` (with `SessionID: id, DialURL: s.dialBase+"/register", ProxyURL: s.proxyURL`) → `setHandle` → `setState("running")`. `Op("suspend", warm=false)` keeps the `suspending`-before-Suspend ordering and its rollback. `Delete` keeps hub-close-before-remove. Run `go test ./internal/runnerd/` after the refactor — every existing test must still pass unmodified. Commit: `refactor: extract runnerd core ops from HTTP handlers`.
- [ ] **Step 2: `Announce()`** — registry snapshot mapped to rwire states: entry `running` (hub set or awaiting re-register) → `"running"`; `suspended` with hub ≠ nil (warm pause keeps the conn) → `"suspended_warm"`; `suspended` with nil hub → `"suspended_cold"`; skip `starting` entries (mid-create; the create's result will speak for them).
- [ ] **Step 3: Write the failing agent test.** In `agent_test.go`, stand up a fake controld: `httptest.NewServer` whose `/v0/runners/connect` accepts the ws (checking `Authorization: Bearer testtoken`), reads the announce, then drives a script. Test cases:
  - `TestAgentAnnounces`: seed fake driver with one running session; `RunAgent`; fake controld asserts announce has `Proto: 1`, `Runner: "vm1"`, that session `running`, and `Used/Total` populated.
  - `TestAgentExecutesCreateAndReportsResult`: controld sends `{"type":"create","req_id":1,"session":"sess_x","spec":{"image":"img"}}`; assert a `result{req_id:1, ok:true}` comes back and the fake driver saw `Spec{SessionID:"sess_x", Image:"img"}`.
  - `TestAgentIdempotentCreate`: send the same create twice; assert the second returns `ok:true` without a second driver `Create` call.
  - `TestAgentForwardsEvents`: after create, simulate a sessiond registering (dial the runnerd `/register` endpoint as tests do today); assert an `event{session, state:"running"}` arrives at the fake controld.
  - `TestAgentReconnects`: fake controld closes the conn; assert the agent redials (a second announce arrives) within 5 s.
- [ ] **Step 4: Run to verify fail**, then **implement `agent.go`**:

```go
// jitter returns a random duration in [0, d/2) — timing spread, not security.
func jitter(d time.Duration) time.Duration { return time.Duration(mrand.Int63n(int64(d / 2))) }

func (s *Server) RunAgent(ctx context.Context, cfg AgentConfig) error {
	s.proxyURL = cfg.ProxyURL
	backoff := time.Second
	for {
		err := s.agentSession(ctx, cfg)
		if ctx.Err() != nil { return ctx.Err() }
		log.Printf("controld conn ended: %v; redialing in %s", err, backoff)
		select {
		case <-time.After(backoff + jitter(backoff)):
		case <-ctx.Done(): return ctx.Err()
		}
		if backoff < 30*time.Second { backoff *= 2 }
	}
}

func (s *Server) agentSession(ctx context.Context, cfg AgentConfig) error {
	hdr := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	c, _, err := websocket.Dial(ctx, cfg.ControldURL+"/v0/runners/connect", &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil { return err }
	defer c.CloseNow()
	c.SetReadLimit(16 << 20)

	out := make(chan rwire.FromRunner, 64)
	send := func(m rwire.FromRunner) {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m.Used, m.Total, _ = s.drv.Capacity(cctx) // best-effort; piggybacked on every message
		cancel()
		select { case out <- m: default: /* drop under absurd backlog; announce restores truth */ }
	}
	// OnEvent must be swapped atomically per connection and cleared on exit,
	// or a dead conn's closure keeps receiving events.
	s.OnEvent = func(id, state string) { send(rwire.FromRunner{Type: "event", Session: id, State: state}) }
	defer func() { s.OnEvent = nil }()

	used, total, _ := s.drv.Capacity(ctx)
	ann := rwire.FromRunner{Type: "announce", Proto: rwire.Proto, Runner: cfg.RunnerName,
		Sessions: s.Announce(), Used: used, Total: total}
	if err := wsjson.Write(ctx, c, ann); err != nil { return err }

	writeDone := make(chan error, 1)
	go func() { // single writer
		for m := range out {
			if err := wsjson.Write(ctx, c, m); err != nil { writeDone <- err; return }
		}
	}()

	for {
		var m rwire.ToRunner
		if err := wsjson.Read(ctx, c, &m); err != nil { return err }
		go s.execute(ctx, m, send, cfg) // ops are slow (docker); never block the reader
	}
}
```

`execute` switches on `m.Type`:
  - `create`: **idempotency first** — if `s.reg.get(m.Session)` exists, reply `result{ok:true}` immediately. Else `CreateWithID(ctx, m.Session, driver.Spec{Name: m.Spec.Name, Image: m.Spec.Image, Cmd: m.Spec.Cmd, EgressAllow: m.Spec.EgressAllow}, m.Spec.EgressAllow)` → `result{ok, detail: err}`.
  - `suspend`/`resume`/`snapshot`: `Op`; snapshot's ref rides in `result.Detail`.
  - `destroy`: `Delete` → `result{ok:true}` (missing session is still ok — desired state reached).
  - `dial_attach`: implemented in Task 11 (until then: log-and-ignore).
- [ ] **Step 5: Run to verify pass** — `go test ./internal/runnerd/ -count=1`.
- [ ] **Step 6: Wire `cmd/runnerd/main.go`** — new flags `--controld` (default ""), `--runner-token`, `--runner-name` (default `os.Hostname()`), `--proxy-url` (default ""). Behavior: always `Recover(ctx)` first; always serve the local HTTP surface (dev/debug, unchanged); when `--controld` is set, require `--runner-token` non-empty (else `log.Fatal` — fail closed) and run `RunAgent` in the foreground. `make build` clean.
- [ ] **Step 7: Commit** — `git commit -m "feat: runnerd dial mode — announce, command execution, event forwarding"`

---

### Task 7: controld skeleton + runner plane (connect, reconcile, dispatch)

**Files:**
- Create: `internal/controld/controld.go`, `internal/controld/runners.go`
- Test: `internal/controld/runners_test.go`

**Interfaces:**
- Consumes: `Store` (Task 2), `rwire` (Task 1).
- Produces (used by Tasks 8–11):

```go
type Config struct {
	RunnerToken   string
	Admins        []string // github logins
	Members       []string
	GitHubAPIBase string        // default https://api.github.com; tests override
	ExternalURL   string        // http(s)://host:port clients AND runners reach us at; ws URL derived
	OpTimeout     time.Duration // default 60s: dispatch round-trip budget
}
func New(st Store, cfg Config) (*Server, error) // validates config: empty RunnerToken/ExternalURL → error
func (s *Server) Handler() http.Handler   // everything: client API + runner endpoints
func (s *Server) Run(ctx context.Context) // scheduler loop (Task 8); blocks until ctx done
// runners.go internals produced for other tasks:
func (s *Server) dispatch(ctx context.Context, runner string, m rwire.ToRunner) (rwire.FromRunner, error)
	// assigns ReqID, sends, waits for the matching result or OpTimeout/conn-death
	// (error wraps errRunnerUnreachable for API mapping)
func (s *Server) sendToRunner(runner string, m rwire.ToRunner) error // fire-and-forget (destroy-orphan, dial_attach)
func (s *Server) runnerConnected(name string) bool
```

- [ ] **Step 1: Write the failing tests** (`runners_test.go`) with a **fake runner** helper — dials `/v0/runners/connect` on an `httptest.Server` running `Handler()`, sends a scripted announce, then records every `ToRunner` and answers per script:
  - `TestRunnerAuthRequired`: dial without/with wrong bearer → HTTP 401 before upgrade.
  - `TestProtoRejected`: announce `Proto: 99` → conn closed; close reason contains `"proto 99"` and `"proto 1"`.
  - `TestAnnounceUpsertsRunner`: after announce, `ListRunners` shows `{Name:"vm1", Connected:true, Used:1, Total:4}`; after conn close, `Connected:false`.
  - `TestReconcileTable` — the design §4.8 table, one subtest per row. Seed store rows + announce content; assert resulting states:
    | seed (PG) | announce | expect |
    |---|---|---|
    | running on vm1 | `running` | still running, `LastEventAt` bumped |
    | running on vm1 | `suspended_cold` | adopted: `suspended_cold` |
    | running on vm1 | absent | `dead`, error `lost at announce` |
    | creating on vm1 | absent | back to `queued`, runner cleared |
    | destroyed on vm1 | present | a `destroy` ToRunner is sent for it (terminal row ⇒ orphan) |
    | (no row) | present `sess_ghost` | a `destroy` ToRunner is sent |
  - `TestDispatchCorrelatesResults`: two concurrent `dispatch` calls with interleaved out-of-order results resolve to the right callers.
  - `TestDispatchUnreachable`: `dispatch` to a never-connected runner name errors immediately with the unreachable error.
  - `TestEventUpdatesStore`: event `{session, "running"}` moves a `creating` row to `running`; event `{session, "dead"}` moves `running` → `dead`.
- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement.** `controld.go`: `Server{st Store; cfg Config; mu sync.Mutex; runners map[string]*runnerConn; attach *attachTable (Task 11); schedWake chan struct{}}`; `New` validates config (empty `RunnerToken` → error — construct via `New(st, cfg) (*Server, error)`), defaults `OpTimeout` 60 s, `GitHubAPIBase`. `Handler()` builds the full mux (routes added by later tasks return 404 until then).

`runners.go` core:

```go
type runnerConn struct {
	name    string
	out     chan rwire.ToRunner        // single writer goroutine drains this
	mu      sync.Mutex
	pending map[uint64]chan rwire.FromRunner
	seq     atomic.Uint64
	done    chan struct{}
}
```

`handleRunnerConnect`: bearer check against `cfg.RunnerToken` (constant-time compare) → `websocket.Accept` → read announce (must be first message; `Type != "announce"` or bad proto ⇒ close with reason) → `st.UpsertRunner{Connected: true, ...}` → **register the conn under `s.mu`, closing any previous conn for the same name** (a reconnect replaces; the old goroutine's cleanup must not mark the new conn disconnected — guard by comparing pointers before `SetRunnerConnected(false)`) → `reconcile` → then two goroutines: writer drains `out`; reader loop dispatches by `Type` (`result` → deliver to `pending[req_id]`; `event` → `applyEvent`; every message also `UpsertRunner` used/total/last-seen). On read error: close, deregister (pointer-guarded), `SetRunnerConnected(false)`, wake the scheduler (a runner death frees nothing, but a reconnect follows and re-announce may).

`reconcile(ctx, name string, announced []rwire.SessionInfo)` — implement exactly the table (test-pinned). Adoption transition uses `from: NonTerminal`; requeue uses `from: [creating] to: queued` with `Runner: ptr("")`. Orphans (announced id with no row **or** a terminal row): `sendToRunner(name, ToRunner{Type: "destroy", Session: id})` — fire-and-forget; the next announce trues it up.

`applyEvent`: `"running"` → `Transition(from: [creating, running, suspended_warm, suspended_cold], to: running)`; `"dead"` → `Transition(from: NonTerminal, to: dead, Error: ptr("runner reported dead"))`. Ignore `ErrConflict`/`ErrNotFound` (stale events race reconciliation; the announce is truth).

`dispatch`: seq → register `pending` chan (buffer 1) → enqueue to `out` (conn gone ⇒ unreachable error) → select result / `OpTimeout` / conn `done` → always delete pending entry.
- [ ] **Step 4: Run to verify pass** — `go test ./internal/controld/ -count=1`.
- [ ] **Step 5: Commit** — `git commit -m "feat: controld runner plane — connect, announce reconcile, correlated dispatch"`

---

### Task 8: scheduler + placement

**Files:**
- Create: `internal/controld/sched.go`
- Test: `internal/controld/sched_test.go`

**Interfaces:**
- Consumes: Store, `dispatch`, `runnerConnected` (Task 7).
- Produces: `func pickRunner(rs []runnerView) (string, bool)`; `type runnerView struct { Name string; Free int }`; `func (s *Server) wakeScheduler()` (non-blocking send on `schedWake`); the loop inside `Run(ctx)`.

- [ ] **Step 1: Write failing placement tests** (pure function): max free wins; tie → lexicographically smaller name; zero free everywhere → `false`; empty slice → `false`.
- [ ] **Step 2: Write the failing scheduler flow test**: memstore + fake runner (Task 7 helper) with `Total: 2`, scripted to `ok` every create; insert 3 `queued` rows; start `Run(ctx)`; assert: exactly 2 creates dispatched (oldest two), rows `creating`; then fake runner sends `event running` for one and controld receives a capacity update showing a free slot **only after** a session terminates — so send `event dead` for one and assert the third row goes `creating` within 2 s. Also `TestCreateDispatchFailureRequeues`: script `result{ok:false, detail:"boom"}` → row `failed`, error `boom`; and script a dispatch timeout (never answer) → after `OpTimeout` the row returns to `queued` with runner cleared *(superseded by the final whole-branch review: a timeout on a live conn now leaves the row `creating` — the create was delivered; only conn-death requeues)*.
- [ ] **Step 3: Run to verify fail**, then implement:

```go
func (s *Server) schedulerLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second) // safety net; wakes are the fast path
	defer t.Stop()
	for {
		select {
		case <-ctx.Done(): return
		case <-s.schedWake:
		case <-t.C:
		}
		s.drainQueue(ctx)
	}
}
```

`drainQueue`: `OldestQueued` → for each, compute per-runner free = `CapacityTotal - CapacityUsed - len(SessionsOnRunner(name, [creating]))` over connected runners (creating rows aren't in docker's count yet — without this a burst overshoots every runner) → `pickRunner` → none: stop (leave the rest queued) → `Transition(queued→creating, Runner)` (ErrConflict ⇒ skip: canceled meanwhile) → **goroutine**: `dispatch(create)`; on `ok:false` → `failed` with detail; on transport error/timeout → `Transition(creating→queued, Runner: ptr(""))` and `wakeScheduler()`. Sequential placement, concurrent dispatch: placement math stays single-threaded (no double-booking), slow runners don't stall the queue.

`wakeScheduler()` calls added where capacity can appear: `applyEvent` on `dead`, announce/reconcile completion, session DELETE/destroy result (Task 10), runner connect.
- [ ] **Step 4: Run to verify pass**, race check: `go test ./internal/controld/ -race -count=1`.
- [ ] **Step 5: Commit** — `git commit -m "feat: controld scheduler — FIFO queue, least-loaded placement, requeue on dispatch failure"`

---

### Task 9: auth — GitHub exchange, bearer middleware, roles

**Files:**
- Create: `internal/controld/auth.go`
- Test: `internal/controld/auth_test.go`

**Interfaces:**
- Consumes: Store (Task 2), Config (Task 7).
- Produces: `POST /v0/auth/github` route; `func (s *Server) requireUser(next func(http.ResponseWriter, *http.Request, User)) http.HandlerFunc`; `GET /v0/me`; `func (s *Server) roleFor(login string) (string, bool)` (admin beats member; not listed ⇒ false).

- [ ] **Step 1: Write the failing tests** with a fake GitHub (`httptest.Server` serving `GET /user` → `{"id":42,"login":"alice"}` when `Authorization: Bearer gho_good`, 401 otherwise); `cfg.GitHubAPIBase` pointed at it:
  - exchange with valid token + allowlisted login → 200 `{"token":"rnr_...","user":{"login":"alice","role":"admin"}}`; the returned token then passes `GET /v0/me`.
  - valid GitHub token, login not allowlisted → 403 `forbidden`.
  - invalid GitHub token → 401 `unauthenticated` (GitHub's 401 mapped, body not leaked).
  - GitHub 500 → 502-style mapping: use 500 + code `internal` with generic message (upstream text logged, not returned).
  - `GET /v0/me` without/with bogus bearer → 401.
  - empty allowlists: exchange with any valid GitHub user → 403 (fail closed).
  - unknown fields in the request body → 400 `invalid_request` (decoder with `DisallowUnknownFields`).
- [ ] **Step 2: Run to verify fail**, then implement. Exchange handler: decode `{access_token string}` (reject unknown fields, cap body 4 KB via `http.MaxBytesReader`); call `{GitHubAPIBase}/user` with 10 s timeout; **validate the upstream shape** (`id > 0`, `login != ""` — third-party responses are untrusted input); `roleFor` → `UpsertUser` → `NewToken` → `InsertToken` → respond. `requireUser`: parse `Authorization: Bearer `, `HashToken`, `UserByToken`; 401 on any miss. Log request outcomes with the request id (Task 10's middleware); never log tokens.
- [ ] **Step 3: Run to verify pass.**
- [ ] **Step 4: Commit** — `git commit -m "feat: controld auth — github token exchange, allowlist roles, bearer middleware"`

---

### Task 10: sessions + runners REST, cmd/controld

**Files:**
- Create: `internal/controld/api.go`, `cmd/controld/main.go`
- Test: `internal/controld/api_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces: the `/v0` client surface (design §4.4) — exact routes and status codes below; `sessionJSON(s Session, reachable bool)` (adds `"reachable"`, RFC 3339 times); the middleware chain (request id, `X-Content-Type-Options: nosniff`, `Cache-Control: no-store` on GETs).

Route → behavior map (each line is a contract test):

| Route | Behavior |
|---|---|
| `POST /v0/sessions` | body `{name?, image?, cmd?, egress_allow?}` (unknown fields → 400; body cap 64 KB). Insert row `queued` (owner = caller) — **commit before reply** — `wakeScheduler()`, reply **202** + `Location: /v0/sessions/{id}` + `{"session":{...}}`. `Idempotency-Key` header → replay returns the existing row, still 202. Name taken → 409 `conflict`. |
| `GET /v0/sessions` | query `state`, `runner`, `all`, `limit` (default 50, cap 100), `cursor` (invalid → 400). 200 `{"sessions":[...],"next_cursor":""}`. Team-visible. |
| `GET /v0/sessions/{id}` | 200 `{"session":{...}}`; unknown id → 404. |
| `DELETE /v0/sessions/{id}` | owner or admin (else 403). `queued`→`canceled` (204, no dispatch). `creating` → 409 `conflict`. Placed + runner connected → dispatch destroy; `ok` → `destroyed`, 204; unreachable/timeout → 502 `runner_unreachable`. Placed + runner disconnected → mark `destroyed`, 204 (reconcile's terminal-row-orphan rule cleans the container if the runner returns — design §5). Terminal already → 204 idempotent. Always `wakeScheduler()` on success. |
| `POST /v0/sessions/{id}/suspend` | owner/admin. body `{warm?}` default true. From `running` only (else 409). Dispatch; `ok` → `suspended_warm|suspended_cold`. Unreachable → 502 `runner_unreachable`. |
| `POST /v0/sessions/{id}/resume` | from `suspended_*` only. Runner disconnected → 502 `runner_unreachable`. Cold resume onto a full runner (free ≤ 0 by the Task 8 formula) → 409 `no_capacity`, message names the runner. `ok` → `running`. |
| `POST /v0/sessions/{id}/snapshot` | from `running|suspended_*`; dispatch; 200 `{"ref": result.Detail}`. |
| `GET /v0/runners` | 200 `{"runners":[{name, connected, capacity_used, capacity_total, last_seen_at}]}` (auth'd, team-visible). |
| `GET /healthz` | 200 `ok`, unauthenticated, no internals. |

`reachable` in session JSON = `s.Runner != "" && runnerConnected(s.Runner) && !s.State.Terminal()`.

- [ ] **Step 1: Write the failing contract tests** — table-driven over an httptest server with memstore + the Task 7 fake runner; per endpoint at minimum: happy path, one validation rejection, one authZ denial (non-owner non-admin DELETE → 403), and a response-shape regression pin (marshal the response into a `map[string]any` and assert the exact key set — additive evolution starts from a pinned shape). Plus: create-then-kill-store? no — write-ahead ordering is pinned by `TestCreateDurableBeforeDispatch`: wrap the memstore with a store whose `CreateSession` records a timestamp and a fake runner that records dispatch time; assert row-commit strictly precedes dispatch, and that a fake runner that never answers still leaves a `queued`/`creating` row in the store (never lost).
- [ ] **Step 2: Run to verify fail**, then implement `api.go`. Shared plumbing: `writeErr(w, status, code, msg)`; `writeJSON(w, status, v)` (sets `Content-Type: application/json; charset=utf-8`); middleware wrapping the whole mux: request id (accept `X-Request-Id` ≤ 128 chars or generate 16-hex; echo header; attach to logs), `nosniff`, `no-store` on GET. Method routing via `http.ServeMux` patterns (`"POST /v0/sessions"`, Go 1.22+ pattern syntax).
- [ ] **Step 3: Run to verify pass** — `go test ./internal/controld/... -race -count=1`.
- [ ] **Step 4: `cmd/controld/main.go`** — flags (env-overridable, flag wins): `--listen :9090`, `--db` DSN (**required**), `--runner-token` (**required**, or env `RAINIER_RUNNER_TOKEN`), `--admins`, `--members` (comma-separated logins), `--external-url` (**required**; printed at startup), `--github-api` (default `https://api.github.com`). Wire `pgstore.Open` → `controld.New` → `go srv.Run(ctx)` → `http.ListenAndServe(listen, srv.Handler())`. Startup log states admin/member counts and warns loudly when both are empty (nobody can log in). `make build` clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: controld REST API for sessions and runners; cmd/controld"`

---

### Task 11: attach plane — pairing, dial-back, splice

**Files:**
- Create: `internal/controld/attach.go`
- Modify: `internal/runnerd/agent.go` (handle `dial_attach`)
- Test: `internal/controld/attach_test.go`

**Interfaces:**
- Consumes: `relay.Conn`/`relay.WSConn` (reused as the transport abstraction), `dispatch`/`sendToRunner`, `hub.AttachClient` (runnerd side).
- Produces: `WS GET /v0/sessions/{id}/attach?since=` (bearer auth); `WS GET /v0/runners/attach-back?attach_id=` (runner-token auth); `attachTable` with 15 s pairing TTL.

Flow (design §4.2): client WS arrives → auth + session must be `running` and reachable (wait up to 10 s for `running`, mirroring runnerd's hub wait; then 503 `session_not_ready` / 502 `runner_unreachable`) → park under `attach_id` (16-hex) → `sendToRunner(dial_attach{attach_id, since, cols, rows, target_url})` where `target_url` = `cfg.ExternalURL` with scheme http→ws + `/v0/runners/attach-back?attach_id=...` → runnerd dials back, feeds the conn into `hub.AttachClient(ctx, relay.WSConn(c), since, cols, rows)` → controld pairs the two sockets and splices raw text frames both ways until either dies.

The client speaks `wire.ClientMsg/ServerMsg` end-to-end. **Resize-first contract:** controld reads the client's first message (must be `resize`, else close) to fill `dial_attach`'s cols/rows and does NOT forward it — identical to runnerd's `readFirstResize` discipline (the FrameOpen conveys the size; forwarding would double-deliver).

- [ ] **Step 1: Write the failing tests:**
  - `TestAttachEndToEnd`: full in-process chain — controld (memstore) + real runnerd `Server` with fake driver running `RunAgent` against it + a scripted fake sessiond that dials runnerd's `/register` and, on FrameOpen, replies with a `snapshot` ServerMsg then echoes stdin back as `output` (the pattern Plan 2's relay tests use). Client dials `/v0/sessions/{id}/attach`, sends resize, asserts snapshot arrives, sends stdin `"hi"`, asserts echoed output. Then closes; assert the runnerd-side attach conn closes too (splice teardown cascades).
  - `TestAttachRequiresAuth` (401 before upgrade), `TestAttachWrongState` (queued session → 503 `session_not_ready` after the bounded wait — use a 100 ms wait override on Server for tests), `TestAttachBackBogusID` (unknown attach_id → 404, nothing crashes), `TestPairingTTL`: park a client, never dial back (fake runner drops dial_attach) → client conn closed within TTL; table entry gone.
- [ ] **Step 2: Run to verify fail**, then implement `attach.go`:

```go
type attachTable struct {
	mu sync.Mutex
	m  map[string]*pendingAttach
}
type pendingAttach struct {
	client relay.Conn
	done   chan struct{} // closed when claimed or expired
}
```

`handleClientAttach`: auth → state wait → `websocket.Accept` → `SetReadLimit(16<<20)` → read-first-resize → park → `sendToRunner(dial_attach)` (error ⇒ close, 502 mapping is moot post-upgrade: close with reason) → `time.AfterFunc(15s)` closes+removes if unclaimed → block until claimed conn's splice ends.
`handleAttachBack`: runner-token auth → `websocket.Accept` → claim entry (missing ⇒ close) → `splice`.

```go
// splice pumps text frames both directions until either side dies, then
// closes both. controld stays a dumb relay: payloads are opaque bytes.
func splice(ctx context.Context, a, b relay.Conn) {
	done := make(chan struct{}, 2)
	pump := func(src, dst relay.Conn) {
		for {
			m, err := src.Read(ctx)
			if err != nil { break }
			if dst.Write(ctx, m) != nil { break }
		}
		done <- struct{}{}
	}
	go pump(a, b); go pump(b, a)
	<-done
	a.Close(); b.Close()
	<-done // let the second pump exit before returning
}
```

runnerd `agent.go` `dial_attach` case:

```go
case "dial_attach":
	go func() {
		at := m.Attach
		c, _, err := websocket.Dial(ctx, at.TargetURL, &websocket.DialOptions{
			HTTPHeader: http.Header{"Authorization": {"Bearer " + cfg.Token}}})
		if err != nil { log.Printf("attach-back dial: %v", err); return }
		c.SetReadLimit(16 << 20)
		hub, ok := s.reg.hub(m.Session) // wait loop like the HTTP attach path, 10s
		if !ok { c.CloseNow(); return }
		hub.AttachClient(ctx, relay.WSConn(c), at.Since, at.Cols, at.Rows) // blocks; cleanup on either death is Hub's existing behavior
	}()
```

(reuse the existing 10-second hub wait by extracting runnerd's poll loop into `waitHub(id) (*relay.Hub, bool)` used by both the HTTP `/attach` handler and this case).
- [ ] **Step 3: Run to verify pass** — `go test ./internal/... -race -count=1` (the e2e attach test is the heart of Plan 3; make it rock-solid, not flaky: every wait bounded, no sleeps for synchronization — use channels from the scripted sessiond).
- [ ] **Step 4: Commit** — `git commit -m "feat: attach plane — controld pairing + splice, runnerd dial-back"`

---

### Task 12: `rainier` CLI, `internal/attachio`, `internal/cli`

**Files:**
- Create: `internal/attachio/attachio.go`, `internal/cli/config.go`, `internal/cli/client.go`, `cmd/rainier/main.go`
- Modify: `cmd/rattach/main.go` (delegate to attachio)
- Test: `internal/attachio/attachio_test.go`, `internal/cli/client_test.go`

**Interfaces:**
- Produces:

```go
// internal/attachio — the raw-mode terminal loop, extracted verbatim from
// cmd/rattach (raw mode, SIGWINCH resize, Ctrl-] detach, seq tracking).
func AttachURL(base string, since uint64, session string) string // moved from rattach, same contract
func Run(ctx context.Context, wsURL string, header http.Header, since uint64) error
// Run dials wsURL with header, performs the resize-first contract, and pipes
// the terminal until detach/exit/disconnect. Prints the same status lines
// rattach does today. Returns nil on clean detach/exit.
func ScanDetach(buf []byte) int // index of Ctrl-] or -1 (pure; unit-tested)

// internal/cli
type Config struct { ServerURL, Token string }
func Load() (Config, error)   // ~/.config/rainier/config.json ($RAINIER_CONFIG overrides path)
func Save(c Config) error     // 0600, mkdir -p 0700
type Client struct { Base, Token string; HTTP *http.Client }
func (c *Client) Do(method, path string, in, out any) error
// non-2xx: decodes the error envelope and returns fmt.Errorf("%s: %s", code, message);
// transport errors returned as-is. Sets Authorization, X-Request-Id, Idempotency-Key
// (callers pass it via path-independent option — POST /v0/sessions generates one per invocation).
```

CLI command surface (exact):

```
rainier login [--from-gh] [--token GH_TOKEN] [--client-id ID] [--server URL]
rainier new [--name N] [--image IMG] [--egress host,host] [--detach] [-- CMD ARGS...]
rainier ls [--all]
rainier attach <id|name> [--since N]
rainier suspend <id|name> [--cold]
rainier resume <id|name>
rainier snapshot <id|name>
rainier rm <id|name>
```

- [ ] **Step 1: Extract attachio (refactor).** Move rattach's loop into `attachio.Run`; `cmd/rattach/main.go` becomes flag-parsing + `attachio.Run(ctx, attachio.AttachURL(*baseURL, *since, *session), nil, *since)`. Unit-test `AttachURL` (move the existing cases) and `ScanDetach` (mid-buffer, absent, first byte). Behavior must be byte-identical: detach message text, exit-code line, non-tty fallback resize 80×24. Run `make build`; manually smoke `make demo` + rattach. Commit: `refactor: extract attach terminal loop into internal/attachio`.
- [ ] **Step 2: Write failing client tests** — httptest server asserting auth header present, error envelope decoded (`404 {"error":{"code":"not_found",...}}` → error string `not_found: ...`), unknown-server-junk (non-JSON 500) doesn't panic.
- [ ] **Step 3: Implement `internal/cli` + `cmd/rainier`.** Subcommand dispatch in the runnerctl style (stdlib `flag` per subcommand, no cobra). Details that matter:
  - `login`: resolve server URL (`--server` flag > config > error with guidance). Token acquisition: `--from-gh` → `exec.Command("gh","auth","token")`; `--token` → use directly; `--client-id` → GitHub device flow: POST `https://github.com/login/device/code` (`client_id`, `scope=read:user`), print `user_code` + `verification_uri`, poll `https://github.com/login/oauth/access_token` at `interval` until success/expiry; none of the three → print the three options and exit 2. Then `POST /v0/auth/github`, save `{ServerURL, Token}`, print `logged in as <login> (<role>)`.
  - `new`: build `{name, image, cmd, egress_allow}` from flags/args; generate a fresh `Idempotency-Key` (16-hex) per invocation; POST; print the id; unless `--detach`, immediately attach (below), retrying `session_not_ready` for up to 60 s with a `waiting for session…` spinner line — this is "attach immediately and stream everything" (design §4.10).
  - `attach`: resolve arg — `sess_` prefix ⇒ id; else `GET /v0/sessions?state=&…` client-side name match via `SessionByName` semantics: call `GET /v0/sessions` pages and match `name` field (server has no by-name endpoint in v0; note in code). Then `attachio.Run(ctx, wsURL, http.Header{"Authorization": {"Bearer "+tok}}, since)` where `wsURL` = config ServerURL http→ws + `/v0/sessions/<id>/attach`.
  - `ls`: text/tabwriter columns `ID  NAME  STATE  RUNNER  REACHABLE  AGE`; `--all` adds `all=true`; follows `next_cursor` to exhaustion.
  - `suspend --cold` maps to `{"warm":false}`; `rm`/`resume`/`snapshot` are thin `Do` calls printing outcomes (snapshot prints the ref).
- [ ] **Step 4: Run to verify pass** — `go test ./internal/attachio/ ./internal/cli/ -count=1`; `make build`.
- [ ] **Step 5: CLI-against-real-controld smoke test** (`internal/cli/client_test.go` addition): spin the Task 11 in-process stack (controld+memstore+fake runner+scripted sessiond), drive `Client` through login(fake GitHub)/create/ls/get and `attachio` headless attach (non-tty path) — asserts the CLI's HTTP usage matches the server contract, not a mock of it.
- [ ] **Step 6: Commit** — `git commit -m "feat: rainier CLI — login, new, ls, attach, lifecycle ops"`

---

### Task 13: egress R4 — internal network + proxy env, enforced and verified

**Files:**
- Modify: `docker-compose.fleet.yml`, `scripts/fleet-up.sh`, `internal/runnerd/runnerd.go` (proxy plumb-through), `cmd/runnerd/main.go`
- Create: `scripts/egress-check.sh`
- Test: `internal/driver/docker_test.go` (proxy env assertion), the check script itself

**Interfaces:**
- Consumes: `Spec.ProxyURL` (Task 4), `AgentConfig.ProxyURL` (Task 6).

- [ ] **Step 1: The spike first (design §3 assumption).** Before changing anything: `docker network create --internal rainier-internal-spike`, run an alpine container on it, verify (a) it CANNOT reach the internet (`wget -T3 https://example.com` fails), (b) it CAN reach the host: on Linux dockerd the bridge gateway IP; on Docker Desktop/colima `host.docker.internal` (fleet-up.sh already probes exactly this — reuse its logic). Record findings as comments in the compose file. If (b) fails on the primary dev platform, STOP and redesign the egressd placement (e.g. egressd as a container on both networks) before proceeding — do not build on an unverified assumption.
- [ ] **Step 2: Flip the network** — `docker-compose.fleet.yml`: `internal: true`, delete the R4 warning comment block, replace with a comment stating the enforcement now active and pointing at `scripts/egress-check.sh`.
- [ ] **Step 3: Plumb the proxy** — `runnerd.New` gains the proxy URL (store on Server; `CreateWithID` sets `Spec.ProxyURL`); `cmd/runnerd` `--proxy-url` default `http://<dial-base-host>:3128` derived in fleet-up.sh (it already computes `$GW`). Driver test (docker-gated): create with `ProxyURL` set → `docker inspect -f '{{.Config.Env}}'` shows all three vars.
- [ ] **Step 4: Write `scripts/egress-check.sh`** — end-to-end acceptance, exits non-zero on any failure:

```bash
#!/usr/bin/env bash
# Verifies R4: sessions have no direct egress; egressd allowlist is the only path.
set -euo pipefail
BASE=${RUNNERD:-http://127.0.0.1:8080}
sid=$(curl -sf -X POST "$BASE/sessions" -d '{"image":"rainier-session:latest","egress_allow":["example.com"],"cmd":["--","sleep","600"]}' | sed 's/.*"session_id":"\([^"]*\)".*/\1/')
cid=$(docker ps -q --filter "label=rainier.session=$sid")
[ -n "$cid" ] || { echo "no container"; exit 1; }
# 1. direct egress must fail (no default route on the internal network)
docker exec "$cid" wget -q -T 5 -O /dev/null https://example.com && { echo "FAIL: direct egress worked"; exit 1; }
# 2. allowlisted host through the proxy must succeed. BusyBox wget reads the
# lowercase proxy vars, which the driver injects alongside uppercase (Task 4).
docker exec "$cid" sh -c 'wget -q -T 10 -O /dev/null https://example.com' || { echo "FAIL: allowlisted egress blocked"; exit 1; }
# 3. non-allowlisted host through the proxy must fail
docker exec "$cid" sh -c 'wget -q -T 5 -O /dev/null https://anthropic.com' && { echo "FAIL: deny leaked"; exit 1; }
curl -sf -X DELETE "$BASE/sessions/$sid" >/dev/null
echo "egress R4 OK"
```

(Step 1 in the checklist for this task: confirm alpine 3.20's BusyBox wget performs HTTPS via its TLS support and honors `https_proxy` — it does on stock alpine, but verify once in the spike container before relying on it; if not, `apk add curl` in the session image and use curl here. Note `docker exec` is a *test harness* peeking at the sandbox, not a runtime dependency — portability rule 1 constrains the system, not its tests. Step 1 and 3's expected-failure lines rely on `set -e` being suspended by `&&`/`||` context — they are, as written.)
- [ ] **Step 5: Wire into fleet-up.sh** (start egressd before runnerd with the derived `--proxy-url`) and run the script end-to-end on this machine. Both eyeballs on the audit log: the deny line for step 3 must appear in `/tmp/egressd.log`.
- [ ] **Step 6: Commit** — `git commit -m "feat: enforce egress R4 — internal network, proxy env, acceptance check"`

---

### Task 14: e2e + chaos suite, dev/prod wiring, GCE dogfood, docs

**Files:**
- Create: `internal/e2e/e2e_test.go`, `scripts/e2e-fleet.sh`, `scripts/gce-up.sh`, `docs/deploy-gce.md`
- Modify: `scripts/fleet-up.sh` (controld + postgres + dial-mode runnerd), `README.md`, `Makefile` (`e2e` target)

**Interfaces:** consumes everything; produces the Plan 3 acceptance evidence (success criteria 1–7 of the design).

- [ ] **Step 1: Go e2e (fake driver, no docker needed) — `internal/e2e/e2e_test.go`,** in-process: pgstore-or-memstore (env `RAINIER_TEST_PG_DSN` optional; default memstore) + controld on a real listener + TWO runnerd `Server`s with fake drivers (`Total: 2` each) running `RunAgent` + scripted sessionds. Test scenes, each an isolated subtest with its own stack:
  - `TestBurstQueuesAndDrains` (criterion 5): create 10 via the REST API (real HTTP) → exactly 4 reach `creating/running`, 6 sit `queued`; destroy 2 → 2 more drain; assert placement spread across both runners (least-loaded).
  - `TestControldRestartMidSession` (criterion 2): create+attach (scripted sessiond echoes), stop the controld http.Server + throw away the Server, build a NEW controld on the same store and listener; runners reconnect (their RunAgent loops are still running against the same URL); assert re-announce reconciles rows unchanged and a fresh attach works. Attach downtime is expected; session state loss is failure.
  - `TestRunnerRestartReRegisters` (criterion 3): kill a runnerd Server (cancel its ctx), recreate it with the same fake driver contents (simulating surviving containers), `Recover` + `RunAgent`; assert announce trues everything up and no session was marked dead.
  - `TestDeleteDisconnectedRunner`: destroy a session whose runner is down → row `destroyed`; reconnect the runner announcing that session → assert controld sends it a destroy (terminal-orphan rule).
- [ ] **Step 2: Run** — `go test ./internal/e2e/ -race -count=1` until green and non-flaky (`-count=5`).
- [ ] **Step 3: `scripts/e2e-fleet.sh` (real docker path)** — extends fleet-up: starts postgres container (`rainier-pg`, port 127.0.0.1:5433), controld on the host (`--db postgres://... --runner-token dev-$(openssl rand -hex 8) --external-url http://$GW:9090 --admins "$GITHUB_USER"`), runnerd in dial mode. Then drives the REAL CLI: `rainier login --from-gh`, `new`, `ls`, `attach` (non-tty piped stdin echo check), `suspend/resume`, `rm`, plus `egress-check.sh`. This is the local dress rehearsal for the GCE milestone. `Makefile`: `e2e: build` runs it.
- [ ] **Step 4: `scripts/gce-up.sh` + `docs/deploy-gce.md`** (criterion 1; decisions: project `rainier`, e2-medium, Tailscale). The script is idempotent and prints what it does:

```bash
#!/usr/bin/env bash
# Provision the Rainier dogfood VM: one e2-medium in GCP project "rainier".
# Prereqs: gcloud authed; project rainier exists with billing; a Tailscale
# account. Run once; safe to re-run.
set -euo pipefail
PROJECT=rainier ZONE=${ZONE:-us-west1-b} VM=rainier-1
gcloud compute instances describe $VM --project $PROJECT --zone $ZONE >/dev/null 2>&1 || \
gcloud compute instances create $VM --project $PROJECT --zone $ZONE \
  --machine-type e2-medium --image-family debian-12 --image-project debian-cloud \
  --boot-disk-size 50GB
gcloud compute ssh $VM --project $PROJECT --zone $ZONE --command '
  set -e
  command -v docker >/dev/null || { curl -fsSL https://get.docker.com | sudo sh; sudo usermod -aG docker $USER; }
  command -v tailscale >/dev/null || { curl -fsSL https://tailscale.com/install.sh | sh; }
  echo "now run: sudo tailscale up   (authenticate in the browser)"
'
echo "Next: docs/deploy-gce.md — build binaries on the VM, start postgres+controld+runnerd+egressd, then locally: rainier login --from-gh --server http://rainier-1:9090"
```

`docs/deploy-gce.md` walks the rest with exact commands: clone repo on VM, `make build`, postgres via docker, systemd-free tmux/nohup start (v0 honesty; systemd units are a ledgered nicety), controld `--external-url http://rainier-1:9090` (the tailnet MagicDNS name — runners on the same host reach it locally; the laptop reaches it over the tailnet), runner token generation, and the acceptance checklist copied from the design's success criteria 1–7 with space to record results.
- [ ] **Step 5: Run the milestone** (with Josh's GCP project ready): execute deploy-gce.md end-to-end from this laptop; run criteria 1–4 and 6–7 for real (5 is covered by e2e); record results in the doc. **Overnight criterion 4 spans a day — start it, note the start time, verify next session.**
- [ ] **Step 6: Docs sweep** — README: status line → "v0 Plan 3: control plane + CLI (see docs/deploy-gce.md)"; note `runnerctl`/`rattach` are dev tools; CLI quickstart block (`login --from-gh`, `new`, `ls`, `attach`).
- [ ] **Step 7: Commit** — `git commit -m "feat: e2e suite, dev fleet wiring, GCE dogfood script and docs"`

---

## Coverage ledger (self-review against the design)

- **Design §4.1 module map** → Tasks 1–3, 7–12 create exactly those files; `storetest` mirrors the existing driver-contract pattern.
- **§4.2 control conn + dial-back** → Tasks 6, 7, 11; single-mux alternative rejected in the design, not revisited here.
- **§4.3 proto/versioning** → Task 1 (Proto const, unknown-field tolerance), Task 7 (reject with legible reason); `/v0` additive stance pinned by shape-regression tests (Task 10).
- **§4.4 REST surface** → Task 10 route table 1:1, envelope/codes in Global Constraints; pagination day-one; opaque ids; object-level authZ (owner-or-admin on mutations, team-visible reads).
- **§4.5 identity** → Task 9 (exchange, allowlist fail-closed, hash-only storage); Task 12 (`--from-gh`, `--token`, `--client-id` device flow; 0600 config). Runner token: Tasks 6/7.
- **§4.6 schema + state machine + write-ahead** → Tasks 2/3 (guarded `Transition` is the state machine's enforcement; partial unique indexes for idem/name); write-ahead pinned by `TestCreateDurableBeforeDispatch` (Task 10).
- **§4.7 placement/capacity/no-MQ** → Task 8 (FIFO, least-loaded, creating-aware free calc); Task 4 (Capacity excludes cold); resume `no_capacity` in Task 10; no broker anywhere.
- **§4.8 reconciliation + carried hardening** → Task 7 (table, test-per-row), Task 4 (Recover), Task 5 (redial + inspect-before-destroy — the one Plan 2 semantic change, both branches tested).
- **§4.9 egress R4** → Task 13 (spike-first, internal network, proxy env, acceptance script).
- **§4.10 CLI/deploy** → Tasks 12, 14 (auto-attach on `new`, Ctrl-] semantics preserved via attachio extraction; compose dev path; GCE + Tailscale).
- **§5 edge cases** → cancel-vs-dispatch race (Task 8 ErrConflict-skip + Task 10 DELETE), attach-not-ready (Tasks 11/12), pairing TTL (Task 11), flap-timeout→requeue (Task 8; final review narrowed this to conn-death-only — see the superseded note in Task 8), PG-down = 503s with sessions unaffected (by construction: store errors map to `internal`, runner conns don't touch PG to stay up — note: reconcile failures log and close the runner conn so it retries), GitHub-down (Task 9), fail-closed defaults (Tasks 6, 9, 10).
- **§7 verification** → contract tests on every endpoint (Task 10 Step 1 minimums), protocol kill-tests (Task 7), race runs, e2e scenes = design's chaos list, cloud acceptance = criteria 1–7 (Task 14).
- **Deferred, per design (do NOT build):** event-plane WS, queued resume, per-runner tokens, keychain, multi-replica controld, `config-ssh`, Terraform, systemd units.

**Type-consistency check (done):** `rwire.FromRunner/ToRunner` field names match between Tasks 1, 6, 7, 11. `Store` method set identical across Tasks 2, 3, and consumers. `SessionState` strings identical in constants, SQL partial indexes, and `rwire.SessionInfo.State` mapping (Task 6 `Announce`). `relay.Conn` reused for splice + dial-back. `AttachURL` contract unchanged for rattach.
