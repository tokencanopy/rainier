package control

import (
	"context"
	"time"
)

// Requirements is the portable capability-and-resource ask a session (or its
// environment) makes of the pool that will run it. It names portable
// capabilities and resource minima; it has no provider SKU, machine type, or
// user-selected size. Session sizing stays optional and automatic for
// clients — environment defaults and host policy derive Requirements.
type Requirements struct {
	Capabilities   []string // portable runtime capabilities, e.g. "gpu"
	MinCPU         int64    // whole vCPU units; 0 = unspecified
	MinMemoryBytes int64    // 0 = unspecified
	MinDiskBytes   int64    // 0 = unspecified
}

// Pool is an opaque eligible scheduling boundary with aggregate capacity and
// capabilities. The pool resolver may change providers without changing a
// session request; the application selects among runners inside one pool.
type Pool struct {
	ID            PoolID
	Capabilities  []string
	CapacityUsed  int
	CapacityTotal int
}

// Runner is a registered fleet member within a pool and its current
// capacity. Identity and generation are opaque Rainier identities; provider
// identity stays in adapters.
type Runner struct {
	ID            RunnerID
	PoolID        PoolID
	CapacityUsed  int
	CapacityTotal int
	Connected     bool
	// Generation is monotonic per runner; a stale holder is rejected without
	// replacing the event shapes (see RunnerEvent).
	Generation   uint64
	Capabilities []string
	LastSeenAt   time.Time
}

// RunnerQuery filters and paginates ListRunners. Limit is capped by the
// implementation, Cursor is opaque, and rows come back in stable (id) order.
type RunnerQuery struct {
	Limit  int
	Cursor string
}

// RunnerPage is one page of ListRunners. NextCursor is empty on the last
// page.
type RunnerPage struct {
	Runners    []Runner
	NextCursor string
}

// RunnerSession is one session a runner reports it holds, for registration
// and reconciliation.
type RunnerSession struct {
	SessionID SessionID
	State     SessionState
}

// RunnerRegistration is a runner's registration claim. Fleet
// service-principal calls carry their authoritative workspace, pool, runner,
// and session bindings here rather than accepting a user-created Scope. Every
// field is adapter-derived; none is decoded directly from client JSON.
type RunnerRegistration struct {
	WorkspaceID   WorkspaceID
	PoolID        PoolID
	RunnerID      RunnerID
	Generation    uint64
	CapacityUsed  int
	CapacityTotal int
	Capabilities  []string
	Sessions      []RunnerSession
}

// RunnerRegistrationResult is the application's answer to a registration.
type RunnerRegistrationResult struct {
	// Accepted reports whether the registration took effect. A stale
	// generation is refused rather than recorded.
	Accepted bool
	// Generation is the generation now authoritative for this runner.
	Generation uint64
}

// RunnerSnapshot is a runner's authoritative current-state report for
// reconciliation: its identity, generation, capacity, and the sessions it
// holds.
type RunnerSnapshot struct {
	WorkspaceID   WorkspaceID
	PoolID        PoolID
	RunnerID      RunnerID
	Generation    uint64
	CapacityUsed  int
	CapacityTotal int
	Sessions      []RunnerSession
}

// ReconcileResult is the application's answer to a snapshot.
type ReconcileResult struct {
	// Generation is the generation now authoritative for this runner.
	Generation uint64
	// Fenced reports whether the snapshot lost to a newer generation and the
	// runner must stop acting on older authority.
	Fenced bool
	// Destroy names sessions the store has no live record of on this runner;
	// the runner must tear them down as orphans.
	Destroy []SessionID
}

// RunnerEvent is one unsolicited lifecycle report a runner makes about a
// session it holds. It carries its own authoritative workspace, pool, runner,
// and generation bindings; an event that does not match the authoritative
// workspace, pool, runner, or generation is stale and ignored. State is the
// lifecycle state the event reports; Detail is the runner's own sentence
// (failure tail, snapshot ref); ChildExitCode is set for a child exit.
type RunnerEvent struct {
	WorkspaceID   WorkspaceID
	PoolID        PoolID
	RunnerID      RunnerID
	Generation    uint64
	SessionID     SessionID
	State         SessionState
	Detail        string
	ChildExitCode *int
	// PlacementGeneration is the session placement generation the runner
	// was given when the sandbox was created, echoed back so a report from a
	// stale sandbox is fenced by the session's own authority and not only by
	// the connection it arrived on. Zero means "not carried" (an old runner)
	// and fences nothing.
	PlacementGeneration uint64
}

// Fleet is the fleet half of the caller-facing application contract. The
// first three methods are service-principal calls: they carry authoritative
// bindings in their payload and never accept a user-created Scope. ListRunners
// is an ordinary scoped query.
type Fleet interface {
	RegisterRunner(context.Context, RunnerRegistration) (RunnerRegistrationResult, error)
	ReconcileRunner(context.Context, RunnerSnapshot) (ReconcileResult, error)
	ApplyRunnerEvent(context.Context, RunnerEvent) error
	ListRunners(context.Context, Scope, RunnerQuery) (RunnerPage, error)
}

// Usage is the provider-neutral resource/usage fact set an Event may carry:
// CPU time, memory byte-seconds, storage and network bytes, and agent token
// counts when available. It never includes a provider rate or a customer
// charge.
type Usage struct {
	CPUTimeSeconds    float64
	MemoryByteSeconds int64
	StorageBytes      int64
	NetworkBytes      int64
	AgentTokenCount   int64
}

// Event is a fixed-field, provider-neutral application fact: its own stable
// event ID, the workspace and actor it happened for, the resource it touched
// and the action taken, the timestamp, and optional usage facts. It has no
// terminal data, conversation content, secret, raw error, price, or provider
// resource ID.
type Event struct {
	ID          EventID
	WorkspaceID WorkspaceID
	ActorID     ActorID
	Action      Action
	Resource    Resource
	At          time.Time
	Usage       Usage
	// PlacementGeneration is the session placement generation an event about
	// a session happened under, so usage is attributed to exactly one
	// generation; zero for an event about any other resource.
	PlacementGeneration uint64
}
