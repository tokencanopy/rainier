// internal/controld/pgstore/fleet.go
package pgstore

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// pgFleet is control.FleetRepository over the runners table and the pool half
// of the sessions table. It is keyed by pool, not by workspace: a runner
// belongs to a pool, and the sessions a pool's runner holds may come from any
// workspace that pool serves.
type pgFleet struct{ s *Store }

const runnerCols = `name, pool_id, capacity_used, capacity_total, connected, generation, capabilities, last_seen_at`

func scanControlRunner(row rowScanner) (control.Runner, error) {
	var (
		r          control.Runner
		id, pool   string
		generation int64
		capBytes   []byte
		lastSeen   *time.Time
	)
	if err := row.Scan(&id, &pool, &r.CapacityUsed, &r.CapacityTotal, &r.Connected, &generation, &capBytes, &lastSeen); err != nil {
		return control.Runner{}, err
	}
	r.ID = control.RunnerID(id)
	r.PoolID = control.PoolID(pool)
	r.Generation = uint64(generation)
	if len(capBytes) > 0 {
		if err := json.Unmarshal(capBytes, &r.Capabilities); err != nil {
			return control.Runner{}, err
		}
	}
	if lastSeen != nil {
		r.LastSeenAt = *lastSeen
	}
	return r, nil
}

// UpsertRunner fences on the stored generation inside the statement itself:
// a write from a superseded connection matches no row and changes nothing,
// rather than half-overwriting the current connection's view of its own
// capacity.
func (r pgFleet) UpsertRunner(ctx context.Context, pool control.PoolID, run control.Runner) error {
	if pool == "" {
		return control.ErrInvalid
	}
	caps, err := json.Marshal(nonNilStrings(run.Capabilities))
	if err != nil {
		return control.ErrInvalid
	}
	var lastSeen *time.Time
	if !run.LastSeenAt.IsZero() {
		lastSeen = &run.LastSeenAt
	}
	ct, err := r.s.pool.Exec(ctx, `
		INSERT INTO runners (pool_id, name, capacity_used, capacity_total, connected, generation, capabilities, last_seen_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (pool_id, name) DO UPDATE SET
			capacity_used = EXCLUDED.capacity_used, capacity_total = EXCLUDED.capacity_total,
			connected = EXCLUDED.connected, generation = EXCLUDED.generation,
			capabilities = EXCLUDED.capabilities, last_seen_at = EXCLUDED.last_seen_at
		WHERE runners.generation <= EXCLUDED.generation`,
		string(pool), string(run.ID), run.CapacityUsed, run.CapacityTotal, run.Connected,
		int64(run.Generation), caps, lastSeen)
	if err != nil {
		return unavailable("upsert runner", err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrStale
	}
	return nil
}

func (r pgFleet) SetRunnerConnected(ctx context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	if pool == "" {
		return control.ErrInvalid
	}
	ct, err := r.s.pool.Exec(ctx,
		`UPDATE runners SET connected = $3, last_seen_at = now() WHERE pool_id = $1 AND name = $2`,
		string(pool), string(id), connected)
	if err != nil {
		return unavailable("set runner connected", err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

func (r pgFleet) ListRunners(ctx context.Context, pool control.PoolID) ([]control.Runner, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	rows, err := r.s.pool.Query(ctx,
		`SELECT `+runnerCols+` FROM runners WHERE pool_id = $1 ORDER BY name ASC`, string(pool))
	if err != nil {
		return nil, unavailable("list runners", err)
	}
	defer rows.Close()

	out := make([]control.Runner, 0)
	for rows.Next() {
		run, err := scanControlRunner(rows)
		if err != nil {
			return nil, unavailable("list runners", err)
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, unavailable("list runners", err)
	}
	return out, nil
}

func (r pgFleet) SessionsOnRunner(ctx context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	var sb strings.Builder
	sb.WriteString(`SELECT ` + sessionCols + ` FROM sessions WHERE pool_id = $1 AND runner = $2`)
	args := []any{string(pool), string(id)}
	if len(states) > 0 {
		strs := make([]string, len(states))
		for i, st := range states {
			strs[i] = string(st)
		}
		args = append(args, strs)
		sb.WriteString(` AND state = ANY($3)`)
	}
	return r.querySessions(ctx, "sessions on runner", sb.String(), args...)
}

func (r pgFleet) OldestQueued(ctx context.Context, pool control.PoolID) ([]control.Session, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	return r.querySessions(ctx, "oldest queued",
		`SELECT `+sessionCols+` FROM sessions WHERE pool_id = $1 AND state = $2 ORDER BY created_at ASC, id ASC`,
		string(pool), string(control.StateQueued))
}

// querySessions runs one session read for the fleet's two listings and turns
// any failure into the contract's sentinel, naming the operation and nothing
// of the statement.
func (r pgFleet) querySessions(ctx context.Context, op, sql string, args ...any) ([]control.Session, error) {
	rows, err := r.s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, unavailable(op, err)
	}
	defer rows.Close()

	out := make([]control.Session, 0)
	for rows.Next() {
		sess, err := scanControlSession(rows)
		if err != nil {
			return nil, unavailable(op, err)
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, unavailable(op, err)
	}
	return out, nil
}
