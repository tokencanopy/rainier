package control

// The typed identity vocabulary. Every identity in the control contract is a
// distinct named string type so a workspace, an actor, a session, an
// environment, a pool, a runner, and an event can never be assigned to one
// another by accident. Provider-native identifiers are never represented: a
// pool and a runner are opaque Rainier identities, and an adapter maps them
// to whatever the provider uses.
type (
	WorkspaceID   string
	ActorID       string
	SessionID     string
	EnvironmentID string
	PoolID        string
	RunnerID      string
	EventID       string
)

// ActorKind names the two kinds of principal an operation may run as. An
// actor is authoritative adapter output: the host constructs it from
// authenticated state, never by decoding a client-supplied field from JSON.
type ActorKind string

const (
	// ActorUser is a human operator acting on their own behalf.
	ActorUser ActorKind = "user"
	// ActorService is a narrowly scoped principal acting for background work
	// or runner events, not a user.
	ActorService ActorKind = "service"
)

// Actor is an authenticated principal. It is deliberately identity-only: it
// carries no role, email, or membership record, and no cached allow/deny
// decision. Current authorization is always an Authorizer call against the
// surrounding Scope.
type Actor struct {
	ID   ActorID
	Kind ActorKind
}

// ExecutionMode names the three Rainier execution products. It is a Rainier
// concept, not a provider one: dedicated and serverless both hide their
// provider underneath, and self_hosted names an installation the customer
// runs from the OSS repository.
type ExecutionMode string

const (
	// ExecutionSelfHosted is a customer-operated installation.
	ExecutionSelfHosted ExecutionMode = "self_hosted"
	// ExecutionDedicated is a Rainier-managed workspace-exclusive execution
	// plane.
	ExecutionDedicated ExecutionMode = "dedicated"
	// ExecutionServerless is Rainier-managed shared capacity with a hardened
	// sandbox per session.
	ExecutionServerless ExecutionMode = "serverless"
)

// PlacementScope is the Rainier placement context of a call: which product
// region the workspace lives in, which cell is its home, and which execution
// mode it runs under. It is authoritative adapter output — the host derives
// it from current authoritative records, and the client cannot supply or
// override it.
//
// For a self-hosted installation the region and cell are the documented
// installation-local values ("self-hosted" and "default" are typical), not a
// real cloud region.
type PlacementScope struct {
	ProductRegion string
	HomeCell      string
	Mode          ExecutionMode
}

// Scope is the authoritative context every hosted application command and
// query receives. It carries the workspace and actor the call is authorized
// as, plus the placement context. It contains no role and no cached
// allow/deny decision: the Authorizer is the current authority, invoked
// against this scope before any state disclosure or side effect.
type Scope struct {
	WorkspaceID WorkspaceID
	Actor       Actor
	Placement   PlacementScope
}

// Validate reports whether s is a usable scope. A zero Scope is invalid; so
// is one with an empty workspace or actor ID, an unknown actor kind, an
// unknown execution mode, or a missing product region or home cell. It
// returns ErrInvalid for every rejection, and never a free-form message.
func (s Scope) Validate() error {
	if s.WorkspaceID == "" {
		return ErrInvalid
	}
	if s.Actor.ID == "" {
		return ErrInvalid
	}
	switch s.Actor.Kind {
	case ActorUser, ActorService:
	default:
		return ErrInvalid
	}
	switch s.Placement.Mode {
	case ExecutionSelfHosted, ExecutionDedicated, ExecutionServerless:
	default:
		return ErrInvalid
	}
	if s.Placement.ProductRegion == "" || s.Placement.HomeCell == "" {
		return ErrInvalid
	}
	return nil
}
