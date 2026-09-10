package control

import (
	"context"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// The host-supplied ports. Each is capability-sized for exactly one
// dependency; there is no Host, Backend, or catch-all store interface, and
// none of these ports exposes SQL transactions, rows, JSON blobs,
// credentials, secrets, user records, GitHub identities, or provider
// resources. Every tenant-bearing repository method takes WorkspaceID; every
// fleet transport/state method takes PoolID.

// Action is the closed action vocabulary of the frozen application
// operations. The action and the resource kind together name an operation the
// Authorizer may allow or deny.
type Action string

const (
	ActionCreate   Action = "create"
	ActionGet      Action = "get"
	ActionList     Action = "list"
	ActionUpdate   Action = "update"
	ActionDelete   Action = "delete"
	ActionSuspend  Action = "suspend"
	ActionResume   Action = "resume"
	ActionSnapshot Action = "snapshot"
	ActionAttach   Action = "attach"
	ActionDiff     Action = "diff"
	ActionPush     Action = "push"
	ActionPull     Action = "pull"
)

// ResourceKind is the closed resource-kind vocabulary of the frozen
// application operations.
type ResourceKind string

const (
	ResourceSession     ResourceKind = "session"
	ResourceEnvironment ResourceKind = "environment"
	ResourceRunner      ResourceKind = "runner"
)

// Resource names the resource an authorization decision is about: its kind,
// its authoritative workspace, its opaque ID, and its creator when the
// resource has one. It carries no role — roles are the adapter's to
// interpret, never a field here.
type Resource struct {
	Kind        ResourceKind
	WorkspaceID WorkspaceID
	ID          string // the resource's opaque identifier
	CreatorID   ActorID
}

// Authorizer is the current authorization authority. The application invokes
// it before any state disclosure or side effect; self-hosted and Cloud
// supply different policy adapters behind the same signature.
type Authorizer interface {
	Authorize(context.Context, Scope, Action, Resource) error
}

// SessionRepository is the workspace-keyed session persistence port. Every
// method mirrors one semantic read or transition the existing application
// behavior performs, mapped from internal/controld.Store as follows:
//
//	CreateSession        → Store.CreateSession
//	GetSession           → Store.GetSession
//	SessionByIDem        → Store.SessionByIdem (idempotency replay)
//	ListSessions         → Store.ListSessions
//	Transition           → Store.Transition
//	SetSessionSetupHash  → Store.SetSessionSetupHash
//	SetChildExitCode     → Store.SetChildExitCode
//
// NextControllerGeneration has no Store predecessor: the controller lease was
// process memory and is now the repository's, so a restart or a second
// replica cannot hand out the same authority twice.
//
// User, token, secret, and credential operations are deliberately absent:
// they belong to identity and vault adapters, not to the application service.
type SessionRepository interface {
	// CreateSession stores s and returns the stored row. ErrConflict when the
	// name is already held by another non-terminal session of the same
	// creator; a replayed idempotency key returns the existing row.
	CreateSession(ctx context.Context, ws WorkspaceID, s Session) (Session, error)
	GetSession(ctx context.Context, ws WorkspaceID, id SessionID) (Session, error)
	// SessionByIDem returns the session a creator already created under key.
	SessionByIDem(ctx context.Context, ws WorkspaceID, creator ActorID, key string) (Session, error)
	// ListSessions returns one page and the opaque next cursor ("" at the
	// end).
	ListSessions(ctx context.Context, ws WorkspaceID, q SessionQuery) ([]Session, string, error)
	// Transition moves id from any state in from to to, optionally updating
	// the transition columns. ErrConflict when the current state is not in
	// from; ErrNotFound when id does not exist. When opts.RunnerID names a
	// runner the repository also advances PlacementGeneration (see Session).
	Transition(ctx context.Context, ws WorkspaceID, id SessionID, from []SessionState, to SessionState, opts TransitionOpts) error
	// SetSessionSetupHash records the setup a session was dispatched with.
	SetSessionSetupHash(ctx context.Context, ws WorkspaceID, id SessionID, hash string) error
	// SetChildExitCode records the exit status of a session's agent process.
	SetChildExitCode(ctx context.Context, ws WorkspaceID, id SessionID, code int) error
	// NextControllerGeneration advances id's controller generation by one,
	// VACATES THE LEASE, and returns the new value, atomically with respect
	// to every other caller. ErrNotFound when id does not exist in ws. It is
	// the unconditional grant, for a client that negotiates nothing and can
	// only be admitted as the controller; a negotiated attach uses the
	// conditional pair below.
	//
	// Vacating the lease is part of the contract, not an implementation's
	// choice, and it is the same step CompareAndAdvanceControllerGeneration
	// takes for the same reason: this is a take-over, and it displaces
	// whoever held control. An implementation that advances the counter alone
	// leaves the DISPLACED holder's identity and future expiry on the row, so
	// for the rest of the lease TTL the session reports a live lease held by
	// somebody with no authority — and the next negotiated controller attach
	// reads that lease and is admitted a viewer.
	NextControllerGeneration(ctx context.Context, ws WorkspaceID, id SessionID) (uint64, error)
	// CompareAndAdvanceControllerGeneration advances id's controller
	// generation from expected to expected+1 and vacates the lease, in ONE
	// statement, and returns the new generation. It is the whole of the
	// handoff guarantee: two attaches racing from the same expected value
	// produce exactly one advance, because exactly one predicated update can
	// match. An implementation that reads the row and then writes it back is
	// not this method, whatever its tests say on an idle machine.
	//
	// ErrInvalid on an empty workspace. ErrStale when nothing matched — the
	// generation has moved, or the row is gone. Those are one answer on
	// purpose: the caller's remedy is identical (re-read and decide again),
	// and telling them apart would cost a second statement that could only
	// report a third state that was also already true a moment ago. A stale
	// call changes nothing, in particular it increments nothing.
	CompareAndAdvanceControllerGeneration(ctx context.Context, ws WorkspaceID, id SessionID, expected uint64) (uint64, error)
	// RenewControllerLease installs or extends l on id, atomically and fenced
	// by l.Generation: the update applies only while the row still carries
	// that generation and the lease is either vacant or already l.Holder's.
	// The first renew of a generation is how a claim takes the lease; the
	// ones after it are the heartbeat.
	//
	// ErrInvalid on an empty workspace, a zero generation, or an empty
	// holder. ErrStale when the fence did not match, which is how a
	// controller displaced by an attach on another replica learns that it no
	// longer holds control.
	RenewControllerLease(ctx context.Context, ws WorkspaceID, id SessionID, l ControllerLease) error
}

// EnvironmentRepository is the workspace-keyed environment persistence port,
// mapped from internal/controld.Store:
//
//	CreateEnvironment        → Store.CreateEnvironment
//	GetEnvironment           → Store.GetEnvironment
//	ListEnvironments         → Store.ListEnvironments
//	UpdateEnvironment        → Store.UpdateEnvironment
//	DeleteEnvironment        → Store.DeleteEnvironment
//	CountSessionsByEnvironment → Store.CountSessionsByEnvironment
//	SetEnvironmentSnapshot   → Store.SetEnvironmentSnapshot
type EnvironmentRepository interface {
	CreateEnvironment(ctx context.Context, ws WorkspaceID, e Environment) (Environment, error)
	GetEnvironment(ctx context.Context, ws WorkspaceID, id EnvironmentID) (Environment, error)
	ListEnvironments(ctx context.Context, ws WorkspaceID, q EnvironmentQuery) ([]Environment, string, error)
	UpdateEnvironment(ctx context.Context, ws WorkspaceID, e Environment) (Environment, error)
	DeleteEnvironment(ctx context.Context, ws WorkspaceID, id EnvironmentID) error
	// CountSessionsByEnvironment counts sessions on envID whose state is in
	// states; an empty states counts every session on the environment.
	CountSessionsByEnvironment(ctx context.Context, ws WorkspaceID, envID EnvironmentID, states []SessionState) (int, error)
	// SetEnvironmentSnapshot records a built snapshot against the environment
	// only while its SetupHash still equals expectHash; otherwise it changes
	// nothing and reports a stale/conflict outcome. ErrStale when SetupHash no
	// longer equals expectHash, ErrNotFound when envID does not exist in ws.
	SetEnvironmentSnapshot(ctx context.Context, ws WorkspaceID, envID EnvironmentID, expectHash, ref string, runnerID RunnerID) error
}

// FleetRepository is the pool-keyed runner/capacity persistence port, mapped
// from internal/controld.Store:
//
//	UpsertRunner       → Store.UpsertRunner
//	SetRunnerConnected → Store.SetRunnerConnected
//	ListRunners        → Store.ListRunners
//	SessionsOnRunner   → Store.SessionsOnRunner (capacity math, reconciliation)
//	OldestQueued       → Store.OldestQueued (placement pass)
type FleetRepository interface {
	// UpsertRunner stores r as pool's view of that runner. ErrStale when
	// r.Generation is below the runner's stored generation; nothing changes.
	UpsertRunner(ctx context.Context, pool PoolID, r Runner) error
	SetRunnerConnected(ctx context.Context, pool PoolID, id RunnerID, connected bool) error
	ListRunners(ctx context.Context, pool PoolID) ([]Runner, error)
	// SessionsOnRunner returns the sessions in states placed on id within pool.
	SessionsOnRunner(ctx context.Context, pool PoolID, id RunnerID, states []SessionState) ([]Session, error)
	// OldestQueued returns the queued sessions of pool, oldest first.
	OldestQueued(ctx context.Context, pool PoolID) ([]Session, error)
}

// PoolResolver owns product/provider policy: it returns the eligible pools
// for a session's requirements. The application owns selection among the
// eligible runners inside the chosen pool.
type PoolResolver interface {
	EligiblePools(context.Context, Scope, Requirements) ([]Pool, error)
}

// RunnerTransport dispatches commands to a runner and reports connectivity.
// Dispatch carries the public runner protocol as-is; transport authentication
// and connection ownership stay in adapters.
type RunnerTransport interface {
	Dispatch(ctx context.Context, pool PoolID, id RunnerID, m runner.ToRunner) (runner.FromRunner, error)
	Connected(pool PoolID, id RunnerID) bool
}

// AttachmentBroker splices a terminal stream onto a session, given a fully
// resolved AttachTarget. The granted controller generation arrives out of
// band; the broker never changes terminal JSON.
type AttachmentBroker interface {
	Attach(ctx context.Context, target AttachTarget, stream TerminalStream) error
}

// EventRecorder records one application event. It is deliberately a semantic
// port: the later persistence/outbox plan makes mutation plus event atomic
// without changing the event vocabulary.
type EventRecorder interface {
	Record(context.Context, Event) error
}

// UnitOfWork is the host's atomicity: Run executes fn so that every
// repository write and event record fn makes through the context it is
// handed commits together or not at all. A nested Run joins the enclosing
// unit rather than starting another. A host without transactions (an
// in-memory store) runs fn directly. Run returns fn's error unchanged, and
// ErrUnavailable when the unit itself cannot be opened or committed; no
// transaction, connection, or driver type crosses this port.
type UnitOfWork interface {
	Run(ctx context.Context, fn func(ctx context.Context) error) error
}

// CheckpointLocation is where a checkpoint can be booted: on any runner of
// the pool (Portable), or only on the named ones. Both empty means nowhere
// — the checkpoint is not usable right now and the caller boots without it.
type CheckpointLocation struct {
	Portable bool
	Runners  []RunnerID
}

// CheckpointLocator is the host's knowledge of where checkpoint artifacts
// live. Self-hosted snapshots are container images on the runner that built
// them; hosted checkpoints are regional objects any capable runner restores.
// The application asks and never assumes.
type CheckpointLocator interface {
	LocateCheckpoint(ctx context.Context, ws WorkspaceID, cp Checkpoint) (CheckpointLocation, error)
}

// Clock supplies time to the application. Adapters may freeze or skew it in
// tests.
type Clock interface {
	Now() time.Time
}

// IDGenerator mints the three identities the application allocates. Adapters
// may prefix, namespace, or verify uniqueness; the application only requires
// that returned IDs are non-empty and distinct.
type IDGenerator interface {
	NewSessionID() SessionID
	NewEnvironmentID() EnvironmentID
	NewEventID() EventID
}
