// internal/controld/pgstore/environments.go
package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/controld"
)

// pgEnvironments is control.EnvironmentRepository over the environments
// table.
type pgEnvironments struct{ s *Store }

// environmentCols is the native projection. The three snapshot columns are
// read (the cache is part of the row a caller sees) but written by exactly
// one statement, SetEnvironmentSnapshot's.
const environmentCols = `id, workspace_id, name, image, setup, setup_hash, init, init_timeout_sec, ` +
	`egress_allow, secret_refs, connectors, requirements, setup_timeout_sec, ` +
	`snapshot_ref, snapshot_runner, snapshot_hash, created_at, updated_at`

// requirementsJSON is the stored spelling of control.Requirements: a small
// object whose zero fields are simply absent, so an environment that asks for
// nothing stores "{}" rather than four zeros pretending to be an ask.
type requirementsJSON struct {
	Capabilities   []string `json:"capabilities,omitempty"`
	MinCPU         int64    `json:"min_cpu,omitempty"`
	MinMemoryBytes int64    `json:"min_memory_bytes,omitempty"`
	MinDiskBytes   int64    `json:"min_disk_bytes,omitempty"`
}

func encodeRequirements(r control.Requirements) ([]byte, error) {
	return json.Marshal(requirementsJSON{
		Capabilities:   r.Capabilities,
		MinCPU:         r.MinCPU,
		MinMemoryBytes: r.MinMemoryBytes,
		MinDiskBytes:   r.MinDiskBytes,
	})
}

func decodeRequirements(b []byte) (control.Requirements, error) {
	if len(b) == 0 {
		return control.Requirements{}, nil
	}
	var raw requirementsJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return control.Requirements{}, err
	}
	return control.Requirements{
		Capabilities:   raw.Capabilities,
		MinCPU:         raw.MinCPU,
		MinMemoryBytes: raw.MinMemoryBytes,
		MinDiskBytes:   raw.MinDiskBytes,
	}, nil
}

// encodeControlConnectors renders cs as the JSON array the connectors column
// holds: each element is that connector's own object, passed through
// untouched, so members this build knows nothing about survive a round trip.
// (jsonb still normalizes whitespace and member order — it stores a value,
// not a byte string — but adds, drops, and rewrites nothing.)
func encodeControlConnectors(cs []control.Connector) ([]byte, error) {
	raws := make([]json.RawMessage, 0, len(cs))
	for _, c := range cs {
		if len(c.Raw) == 0 {
			b, err := json.Marshal(struct {
				Type string `json:"type"`
			}{c.Type})
			if err != nil {
				return nil, err
			}
			raws = append(raws, b)
			continue
		}
		raws = append(raws, c.Raw)
	}
	return json.Marshal(raws)
}

// decodeControlConnectors is its inverse: one Connector per element, that
// element's bytes kept in Raw and its "type" member lifted out.
func decodeControlConnectors(b []byte) ([]control.Connector, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(b, &raws); err != nil {
		return nil, err
	}
	if len(raws) == 0 {
		return nil, nil
	}
	out := make([]control.Connector, 0, len(raws))
	for _, raw := range raws {
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, err
		}
		out = append(out, control.Connector{Type: envelope.Type, Raw: raw})
	}
	return out, nil
}

// scanControlEnvironment reads one row as callers see it: the stored columns,
// plus the affinity capability the cached snapshot lends the environment
// while it is still current. A snapshot whose hash no longer matches the
// environment's is stale, and a stale snapshot must not hold a session to one
// runner.
func scanControlEnvironment(row rowScanner) (control.Environment, error) {
	var (
		e                                control.Environment
		id, ws                           string
		snapshotRef, snapshotRunner      string
		egressBytes, refsBytes           []byte
		connectorBytes, requirementBytes []byte
	)
	if err := row.Scan(&id, &ws, &e.Name, &e.Image, &e.Setup, &e.SetupHash, &e.Init, &e.InitTimeoutSec,
		&egressBytes, &refsBytes, &connectorBytes, &requirementBytes, &e.SetupTimeoutSec,
		&snapshotRef, &snapshotRunner, &e.SnapshotHash, &e.CreatedAt, &e.UpdatedAt); err != nil {
		return control.Environment{}, err
	}
	e.ID = control.EnvironmentID(id)
	e.WorkspaceID = control.WorkspaceID(ws)
	if len(egressBytes) > 0 {
		if err := json.Unmarshal(egressBytes, &e.EgressAllow); err != nil {
			return control.Environment{}, err
		}
	}
	if len(refsBytes) > 0 {
		if err := json.Unmarshal(refsBytes, &e.SecretRefs); err != nil {
			return control.Environment{}, err
		}
	}
	connectors, err := decodeControlConnectors(connectorBytes)
	if err != nil {
		return control.Environment{}, err
	}
	e.Connectors = connectors
	requirements, err := decodeRequirements(requirementBytes)
	if err != nil {
		return control.Environment{}, err
	}
	e.Requirements = requirements
	if snapshotRef != "" {
		e.Snapshot = controld.SnapshotCheckpoint(snapshotRef)
	}
	if snapshotRef != "" && snapshotRunner != "" && e.SnapshotHash == e.SetupHash {
		e.Requirements.Capabilities = append(e.Requirements.Capabilities, controld.SnapshotCapability(control.RunnerID(snapshotRunner)))
	}
	return e, nil
}

// environmentWriteColumns marshals the four jsonb columns an insert or update
// writes. The snapshot affinity is stripped on the way in: the store owns
// that capability the way it owns the cache it describes, so a caller's copy
// of it — read back out and written straight in again — is dropped rather
// than persisted.
func environmentWriteColumns(e control.Environment) (egress, refs, connectors, requirements []byte, err error) {
	if egress, err = json.Marshal(nonNilStrings(e.EgressAllow)); err != nil {
		return nil, nil, nil, nil, err
	}
	if refs, err = json.Marshal(nonNilStrings(e.SecretRefs)); err != nil {
		return nil, nil, nil, nil, err
	}
	if connectors, err = encodeControlConnectors(e.Connectors); err != nil {
		return nil, nil, nil, nil, err
	}
	stripped := e.Requirements
	stripped.Capabilities = controld.StripSnapshotCapabilities(stripped.Capabilities)
	if requirements, err = encodeRequirements(stripped); err != nil {
		return nil, nil, nil, nil, err
	}
	return egress, refs, connectors, requirements, nil
}

func (r pgEnvironments) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	if ws == "" {
		return control.Environment{}, control.ErrInvalid
	}
	now := time.Now()
	createdAt, updatedAt := e.CreatedAt, e.UpdatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if updatedAt.IsZero() {
		updatedAt = now
	}
	egress, refs, connectors, requirements, err := environmentWriteColumns(e)
	if err != nil {
		return control.Environment{}, fmt.Errorf("pgstore: create environment: encode: %w", control.ErrInvalid)
	}

	// setup_hash is the caller's: the identity of an environment's build
	// inputs is decided above the store, and a repository that recomputed it
	// would quietly disagree with whoever built the snapshot. The three
	// snapshot columns are left at their defaults — a new environment has no
	// cache, and only SetEnvironmentSnapshot ever writes one.
	row := r.s.q(ctx).QueryRow(ctx, `
		INSERT INTO environments (id, workspace_id, name, image, setup, setup_hash, init, init_timeout_sec,
			egress_allow, secret_refs, connectors, requirements, setup_timeout_sec, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		RETURNING `+environmentCols,
		string(e.ID), string(ws), e.Name, e.Image, e.Setup, e.SetupHash, e.Init, e.InitTimeoutSec,
		egress, refs, connectors, requirements, e.SetupTimeoutSec, createdAt, updatedAt)

	out, err := scanControlEnvironment(row)
	if err != nil {
		switch code, _ := constraintViolation(err); code {
		case sqlstateUniqueViolation:
			// Either the id or the name inside this workspace: both mean an
			// identity somebody already holds.
			return control.Environment{}, control.ErrConflict
		case sqlstateForeignKeyViolation:
			return control.Environment{}, control.ErrNotFound
		}
		return control.Environment{}, unavailable("create environment", err)
	}
	return out, nil
}

func (r pgEnvironments) GetEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	if ws == "" {
		return control.Environment{}, control.ErrInvalid
	}
	row := r.s.q(ctx).QueryRow(ctx,
		`SELECT `+environmentCols+` FROM environments WHERE workspace_id = $1 AND id = $2`, string(ws), string(id))
	out, err := scanControlEnvironment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return control.Environment{}, control.ErrNotFound
		}
		return control.Environment{}, unavailable("get environment", err)
	}
	return out, nil
}

func (r pgEnvironments) ListEnvironments(ctx context.Context, ws control.WorkspaceID, q control.EnvironmentQuery) ([]control.Environment, string, error) {
	if ws == "" {
		return nil, "", control.ErrInvalid
	}
	var sb strings.Builder
	sb.WriteString(`SELECT ` + environmentCols + ` FROM environments WHERE workspace_id = $1`)
	args := []any{string(ws)}
	if q.Cursor != "" {
		name, id, err := decodeEnvironmentCursor(q.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, name, string(id))
		fmt.Fprintf(&sb, " AND (name, id) > ($%d, $%d)", len(args)-1, len(args))
	}
	sb.WriteString(` ORDER BY name ASC, id ASC`)
	if q.Limit > 0 {
		args = append(args, q.Limit)
		fmt.Fprintf(&sb, " LIMIT $%d", len(args))
	}

	rows, err := r.s.q(ctx).Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", unavailable("list environments", err)
	}
	defer rows.Close()

	out := make([]control.Environment, 0)
	for rows.Next() {
		e, err := scanControlEnvironment(rows)
		if err != nil {
			return nil, "", unavailable("list environments", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", unavailable("list environments", err)
	}

	next := ""
	if q.Limit > 0 && len(out) == q.Limit {
		last := out[len(out)-1]
		next = encodeEnvironmentCursor(last.Name, last.ID)
	}
	return out, next, nil
}

// UpdateEnvironment replaces the caller-owned columns and carries the cache
// forward untouched. The snapshot is the store's: an update may move the
// setup hash — that is how a cache goes stale — but it may not write, clear,
// or hijack the cache itself, so the three snapshot columns are absent from
// the SET list rather than defended in Go.
func (r pgEnvironments) UpdateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	if ws == "" {
		return control.Environment{}, control.ErrInvalid
	}
	egress, refs, connectors, requirements, err := environmentWriteColumns(e)
	if err != nil {
		return control.Environment{}, fmt.Errorf("pgstore: update environment: encode: %w", control.ErrInvalid)
	}

	row := r.s.q(ctx).QueryRow(ctx, `
		UPDATE environments SET
			name = $3, image = $4, setup = $5, setup_hash = $6, init = $7, init_timeout_sec = $8,
			egress_allow = $9, secret_refs = $10, connectors = $11, requirements = $12,
			setup_timeout_sec = $13, updated_at = now()
		WHERE workspace_id = $1 AND id = $2
		RETURNING `+environmentCols,
		string(ws), string(e.ID), e.Name, e.Image, e.Setup, e.SetupHash, e.Init, e.InitTimeoutSec,
		egress, refs, connectors, requirements, e.SetupTimeoutSec)

	out, err := scanControlEnvironment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return control.Environment{}, control.ErrNotFound
		}
		if code, _ := constraintViolation(err); code == sqlstateUniqueViolation {
			return control.Environment{}, control.ErrConflict
		}
		return control.Environment{}, unavailable("update environment", err)
	}
	return out, nil
}

func (r pgEnvironments) DeleteEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) error {
	if ws == "" {
		return control.ErrInvalid
	}
	ct, err := r.s.q(ctx).Exec(ctx,
		`DELETE FROM environments WHERE workspace_id = $1 AND id = $2`, string(ws), string(id))
	if err != nil {
		return unavailable("delete environment", err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

func (r pgEnvironments) CountSessionsByEnvironment(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, states []control.SessionState) (int, error) {
	if ws == "" {
		return 0, control.ErrInvalid
	}
	sql := `SELECT count(*) FROM sessions WHERE workspace_id = $1 AND environment_id = $2`
	args := []any{string(ws), string(envID)}
	if len(states) > 0 {
		strs := make([]string, len(states))
		for i, st := range states {
			strs[i] = string(st)
		}
		args = append(args, strs)
		sql += ` AND state = ANY($3)`
	}
	var n int
	if err := r.s.q(ctx).QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		return 0, unavailable("count sessions by environment", err)
	}
	return n, nil
}

// SetEnvironmentSnapshot is a compare-and-set on the setup hash: a snapshot
// built from setup that has since been edited must not land, and neither must
// one for an environment that is gone.
func (r pgEnvironments) SetEnvironmentSnapshot(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, expectHash, ref string, runnerID control.RunnerID) error {
	if ws == "" {
		return control.ErrInvalid
	}
	ct, err := r.s.q(ctx).Exec(ctx, `
		UPDATE environments
		SET snapshot_ref = $4, snapshot_runner = $5, snapshot_hash = $3, updated_at = now()
		WHERE workspace_id = $1 AND id = $2 AND setup_hash = $3`,
		string(ws), string(envID), expectHash, ref, string(runnerID))
	if err != nil {
		return unavailable("set environment snapshot", err)
	}
	if ct.RowsAffected() > 0 {
		return nil
	}
	// 0 rows has two causes and they are different answers: the environment
	// is gone, or it is here and its setup has moved on.
	if _, err := r.GetEnvironment(ctx, ws, envID); err != nil {
		return err
	}
	return control.ErrStale
}
