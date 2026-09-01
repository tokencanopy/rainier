package control

import (
	"context"
	"encoding/json"
	"time"
)

// Connector is one declared attachment of an environment, treated opaquely
// by the control model: Type is the connector kind, and Raw is the original
// object verbatim. Adapters bound and validate connector payloads (the
// control model does not know GitHub token storage, file paths, or tunnel
// targets).
type Connector struct {
	Type string
	Raw  json.RawMessage
}

// Environment is a named, reusable template a session starts from, as the
// application service sees it: image, setup and init scripts, egress hosts,
// secret references (names only, never values), connectors, portable
// requirements, checkpoint metadata, and timestamps. It names no provider,
// machine shape, or size.
type Environment struct {
	ID              EnvironmentID
	WorkspaceID     WorkspaceID
	Name            string
	Image           string
	Setup           string
	SetupHash       string // identity of the build inputs (image + setup)
	Init            string // per-boot hook, run after setup and clone
	InitTimeoutSec  int
	EgressAllow     []string
	SecretRefs      []string // secret names; the control model never carries a value
	Connectors      []Connector
	Requirements    Requirements
	SetupTimeoutSec int
	// Snapshot is the cached checkpoint built from this environment's setup,
	// if one exists. SnapshotHash records which SetupHash it was built from;
	// a snapshot whose hash no longer matches the environment's is stale.
	Snapshot     Checkpoint
	SnapshotHash string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CreateEnvironment is the command for CreateEnvironment.
type CreateEnvironment struct {
	Name            string
	Image           string
	Setup           string
	Init            string
	InitTimeoutSec  int
	EgressAllow     []string
	SecretRefs      []string
	Connectors      []Connector
	Requirements    Requirements
	SetupTimeoutSec int
}

// UpdateEnvironment is the command for UpdateEnvironment. Pointer fields
// distinguish "leave this column alone" (nil) from "set it to the zero
// value" (a non-nil pointer): clearing a list and keeping it are different
// requests. ID selects the environment.
type UpdateEnvironment struct {
	ID              EnvironmentID
	Name            *string
	Image           *string
	Setup           *string
	Init            *string
	InitTimeoutSec  *int
	EgressAllow     *[]string
	SecretRefs      *[]string
	Connectors      *[]Connector
	Requirements    *Requirements
	SetupTimeoutSec *int
}

// EnvironmentQuery filters and paginates ListEnvironments. Limit is capped
// by the implementation, Cursor is opaque, and rows come back in stable
// (name, id) order.
type EnvironmentQuery struct {
	Limit  int
	Cursor string
}

// EnvironmentPage is one page of ListEnvironments. NextCursor is empty on the
// last page.
type EnvironmentPage struct {
	Environments []Environment
	NextCursor   string
}

// DeleteEnvironment is the command for DeleteEnvironment.
type DeleteEnvironment struct {
	ID EnvironmentID
}

// Environments is the environment half of the caller-facing application
// contract.
type Environments interface {
	CreateEnvironment(context.Context, Scope, CreateEnvironment) (Environment, error)
	GetEnvironment(context.Context, Scope, EnvironmentID) (Environment, error)
	ListEnvironments(context.Context, Scope, EnvironmentQuery) (EnvironmentPage, error)
	UpdateEnvironment(context.Context, Scope, UpdateEnvironment) (Environment, error)
	DeleteEnvironment(context.Context, Scope, DeleteEnvironment) error
}
