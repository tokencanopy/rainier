// Package pgstore implements controld.Store on Postgres, using pgx/v5's
// pgxpool. It embeds and runs its own migrations (see migrate.go), so Open
// alone is enough to bring a fresh database up to schema.
package pgstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"rainier/internal/controld"
)

// terminalStates lists the SessionState values SessionState.Terminal
// reports true for, spelled out for use inside SQL literals. Keep in sync
// with controld.SessionState.Terminal.
const terminalStatesSQL = `'canceled','failed','dead','destroyed'`

// Store is a Postgres-backed controld.Store.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to dsn, runs any pending migrations, and returns a ready
// Store. The caller should call Close when done with it.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: ping: %w", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: migrate: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// execAdmin runs raw SQL that cannot go through the extended query
// protocol (e.g. CREATE DATABASE). Test-only convenience.
func (s *Store) execAdmin(ctx context.Context, sql string) error {
	_, err := s.pool.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	return err
}

var _ controld.Store = (*Store)(nil)

// --- users & tokens ---------------------------------------------------

func (s *Store) UpsertUser(ctx context.Context, githubID int64, login, role string) (controld.User, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO users (id, github_id, login, role)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (github_id) DO UPDATE SET login = EXCLUDED.login, role = EXCLUDED.role
		RETURNING id, github_id, login, role, created_at`,
		controld.NewUserID(), githubID, login, role)

	var u controld.User
	if err := row.Scan(&u.ID, &u.GitHubID, &u.Login, &u.Role, &u.CreatedAt); err != nil {
		return controld.User{}, fmt.Errorf("pgstore: upsert user: %w", err)
	}
	return u, nil
}

func (s *Store) InsertToken(ctx context.Context, userID, tokenHash string) error {
	// token_hash is already globally unique, so it doubles as the row id.
	_, err := s.pool.Exec(ctx, `
		INSERT INTO api_tokens (id, user_id, token_hash) VALUES ($1, $2, $3)`,
		tokenHash, userID, tokenHash)
	if err != nil {
		return fmt.Errorf("pgstore: insert token: %w", err)
	}
	return nil
}

func (s *Store) UserByToken(ctx context.Context, tokenHash string) (controld.User, error) {
	var userID string
	err := s.pool.QueryRow(ctx, `
		UPDATE api_tokens SET last_used_at = now() WHERE token_hash = $1
		RETURNING user_id`, tokenHash).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.User{}, controld.ErrNotFound
		}
		return controld.User{}, fmt.Errorf("pgstore: user by token: %w", err)
	}

	var u controld.User
	err = s.pool.QueryRow(ctx, `
		SELECT id, github_id, login, role, created_at FROM users WHERE id = $1`, userID).
		Scan(&u.ID, &u.GitHubID, &u.Login, &u.Role, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.User{}, controld.ErrNotFound
		}
		return controld.User{}, fmt.Errorf("pgstore: user by token: %w", err)
	}
	return u, nil
}

// --- sessions -----------------------------------------------------------

const selectSessionCols = `id, owner_id, name, image, cmd, egress_allow, state, runner, idempotency_key, error, environment_id, resolved_image, setup_hash, created_at, updated_at, last_event_at`

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// nonNilStrings returns ss, or an empty (non-nil) slice if ss is nil, so
// json.Marshal always produces "[]" rather than the JSON scalar null.
func nonNilStrings(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	return ss
}

func scanSession(row rowScanner) (controld.Session, error) {
	var sess controld.Session
	var state string
	var idem *string
	var cmdBytes, egressBytes []byte

	if err := row.Scan(&sess.ID, &sess.OwnerID, &sess.Name, &sess.Image, &cmdBytes, &egressBytes,
		&state, &sess.Runner, &idem, &sess.Error, &sess.EnvironmentID, &sess.ResolvedImage, &sess.SetupHash,
		&sess.CreatedAt, &sess.UpdatedAt, &sess.LastEventAt); err != nil {
		return controld.Session{}, err
	}
	sess.State = controld.SessionState(state)
	if idem != nil {
		sess.IdempotencyKey = *idem
	}
	if len(cmdBytes) > 0 {
		if err := json.Unmarshal(cmdBytes, &sess.Cmd); err != nil {
			return controld.Session{}, fmt.Errorf("pgstore: decode cmd: %w", err)
		}
	}
	if len(egressBytes) > 0 {
		if err := json.Unmarshal(egressBytes, &sess.EgressAllow); err != nil {
			return controld.Session{}, fmt.Errorf("pgstore: decode egress_allow: %w", err)
		}
	}
	return sess, nil
}

func (s *Store) CreateSession(ctx context.Context, sess controld.Session) (controld.Session, error) {
	now := time.Now()
	createdAt, updatedAt, lastEventAt := sess.CreatedAt, sess.UpdatedAt, sess.LastEventAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if updatedAt.IsZero() {
		updatedAt = now
	}
	if lastEventAt.IsZero() {
		lastEventAt = now
	}

	// Marshal a nil slice as "[]", not JSON null: the column is declared
	// NOT NULL DEFAULT '[]' and meant to always hold a JSON array, so any
	// SQL that expects to index or measure it (jsonb_array_length, etc.)
	// shouldn't have to special-case a bare JSON null.
	cmd, err := json.Marshal(nonNilStrings(sess.Cmd))
	if err != nil {
		return controld.Session{}, fmt.Errorf("pgstore: encode cmd: %w", err)
	}
	egress, err := json.Marshal(nonNilStrings(sess.EgressAllow))
	if err != nil {
		return controld.Session{}, fmt.Errorf("pgstore: encode egress_allow: %w", err)
	}
	// idempotency_key is stored NULL for "no key" so the sessions_idem
	// partial unique index (WHERE idempotency_key IS NOT NULL) doesn't
	// treat every empty-key session for an owner as a collision.
	var idem *string
	if sess.IdempotencyKey != "" {
		idem = &sess.IdempotencyKey
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO sessions (id, owner_id, name, image, cmd, egress_allow, state, runner, idempotency_key, error, environment_id, resolved_image, setup_hash, created_at, updated_at, last_event_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		RETURNING `+selectSessionCols,
		sess.ID, sess.OwnerID, sess.Name, sess.Image, cmd, egress, string(sess.State), sess.Runner, idem, sess.Error,
		sess.EnvironmentID, sess.ResolvedImage, sess.SetupHash, createdAt, updatedAt, lastEventAt)

	out, err := scanSession(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case "sessions_idem":
				return controld.Session{}, controld.ErrIdemReplay
			case "sessions_owner_name_active":
				return controld.Session{}, controld.ErrConflict
			}
		}
		return controld.Session{}, fmt.Errorf("pgstore: create session: %w", err)
	}
	return out, nil
}

func (s *Store) GetSession(ctx context.Context, id string) (controld.Session, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+selectSessionCols+` FROM sessions WHERE id = $1`, id)
	sess, err := scanSession(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Session{}, controld.ErrNotFound
		}
		return controld.Session{}, fmt.Errorf("pgstore: get session: %w", err)
	}
	return sess, nil
}

func (s *Store) SessionByIdem(ctx context.Context, ownerID, key string) (controld.Session, error) {
	if key == "" {
		return controld.Session{}, controld.ErrNotFound
	}
	row := s.pool.QueryRow(ctx, `SELECT `+selectSessionCols+` FROM sessions WHERE owner_id = $1 AND idempotency_key = $2`, ownerID, key)
	sess, err := scanSession(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Session{}, controld.ErrNotFound
		}
		return controld.Session{}, fmt.Errorf("pgstore: session by idem: %w", err)
	}
	return sess, nil
}

func (s *Store) SessionByName(ctx context.Context, ownerID, name string) (controld.Session, error) {
	if name == "" {
		return controld.Session{}, controld.ErrNotFound
	}
	row := s.pool.QueryRow(ctx, `SELECT `+selectSessionCols+` FROM sessions
		WHERE owner_id = $1 AND name = $2 AND state NOT IN (`+terminalStatesSQL+`)`, ownerID, name)
	sess, err := scanSession(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Session{}, controld.ErrNotFound
		}
		return controld.Session{}, fmt.Errorf("pgstore: session by name: %w", err)
	}
	return sess, nil
}

func (s *Store) ListSessions(ctx context.Context, q controld.SessionQuery) ([]controld.Session, string, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT ` + selectSessionCols + ` FROM sessions WHERE true`)
	var args []any

	if !q.IncludeTerminal {
		sb.WriteString(` AND state NOT IN (` + terminalStatesSQL + `)`)
	}
	if len(q.States) > 0 {
		states := make([]string, len(q.States))
		for i, st := range q.States {
			states[i] = string(st)
		}
		args = append(args, states)
		fmt.Fprintf(&sb, " AND state = ANY($%d)", len(args))
	}
	if q.Runner != "" {
		args = append(args, q.Runner)
		fmt.Fprintf(&sb, " AND runner = $%d", len(args))
	}
	if q.Cursor != "" {
		nano, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, time.Unix(0, nano))
		cArg := len(args)
		args = append(args, id)
		idArg := len(args)
		fmt.Fprintf(&sb, " AND (created_at, id) < ($%d, $%d)", cArg, idArg)
	}
	sb.WriteString(` ORDER BY created_at DESC, id DESC`)
	if q.Limit > 0 {
		args = append(args, q.Limit)
		fmt.Fprintf(&sb, " LIMIT $%d", len(args))
	}

	rows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", fmt.Errorf("pgstore: list sessions: %w", err)
	}
	defer rows.Close()

	out := make([]controld.Session, 0)
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return nil, "", fmt.Errorf("pgstore: list sessions: %w", err)
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("pgstore: list sessions: %w", err)
	}

	// Exact-multiple pagination: a full page always gets a next cursor,
	// even if there happen to be no further rows — no look-ahead query.
	next := ""
	if q.Limit > 0 && len(out) == q.Limit {
		last := out[len(out)-1]
		next = encodeCursor(last.CreatedAt, last.ID)
	}
	return out, next, nil
}

func (s *Store) SessionsOnRunner(ctx context.Context, runner string, states []controld.SessionState) ([]controld.Session, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT ` + selectSessionCols + ` FROM sessions WHERE runner = $1`)
	args := []any{runner}
	if len(states) > 0 {
		strs := make([]string, len(states))
		for i, st := range states {
			strs[i] = string(st)
		}
		args = append(args, strs)
		sb.WriteString(" AND state = ANY($2)")
	}

	rows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("pgstore: sessions on runner: %w", err)
	}
	defer rows.Close()

	out := make([]controld.Session, 0)
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("pgstore: sessions on runner: %w", err)
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

func (s *Store) OldestQueued(ctx context.Context) ([]controld.Session, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+selectSessionCols+` FROM sessions
		WHERE state = $1 ORDER BY created_at ASC, id ASC`, string(controld.StateQueued))
	if err != nil {
		return nil, fmt.Errorf("pgstore: oldest queued: %w", err)
	}
	defer rows.Close()

	out := make([]controld.Session, 0)
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("pgstore: oldest queued: %w", err)
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

func (s *Store) Transition(ctx context.Context, id string, from []controld.SessionState, to controld.SessionState, opts controld.TransitionOpts) error {
	fromStrs := make([]string, len(from))
	for i, f := range from {
		fromStrs[i] = string(f)
	}

	ct, err := s.pool.Exec(ctx, `
		UPDATE sessions
		SET state = $1, runner = COALESCE($2, runner), error = COALESCE($3, error),
		    updated_at = now(), last_event_at = now()
		WHERE id = $4 AND state = ANY($5)`,
		string(to), opts.Runner, opts.Error, id, fromStrs)
	if err != nil {
		return fmt.Errorf("pgstore: transition: %w", err)
	}
	if ct.RowsAffected() > 0 {
		return nil
	}

	// 0 rows affected: id doesn't exist (ErrNotFound) or exists but its
	// current state isn't in from (ErrConflict).
	if _, err := s.GetSession(ctx, id); err != nil {
		if errors.Is(err, controld.ErrNotFound) {
			return controld.ErrNotFound
		}
		return err
	}
	return controld.ErrConflict
}

func (s *Store) SetSessionSetupHash(ctx context.Context, id, hash string) error {
	// Unguarded and state-agnostic: the create dispatch writes this once,
	// before the command goes out, and nothing else ever writes it. It is
	// provenance, not lifecycle, so it deliberately leaves last_event_at
	// alone — a session's liveness clock must not tick for a bookkeeping
	// write.
	ct, err := s.pool.Exec(ctx, `UPDATE sessions SET setup_hash = $1, updated_at = now() WHERE id = $2`, hash, id)
	if err != nil {
		return fmt.Errorf("pgstore: set session setup hash: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return controld.ErrNotFound
	}
	return nil
}

// --- runners --------------------------------------------------------------

func (s *Store) UpsertRunner(ctx context.Context, r controld.Runner) error {
	var lastSeen *time.Time
	if !r.LastSeenAt.IsZero() {
		lastSeen = &r.LastSeenAt
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO runners (name, capacity_used, capacity_total, connected, last_seen_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE SET
			capacity_used = EXCLUDED.capacity_used,
			capacity_total = EXCLUDED.capacity_total,
			connected = EXCLUDED.connected,
			last_seen_at = EXCLUDED.last_seen_at`,
		r.Name, r.CapacityUsed, r.CapacityTotal, r.Connected, lastSeen)
	if err != nil {
		return fmt.Errorf("pgstore: upsert runner: %w", err)
	}
	return nil
}

func (s *Store) SetRunnerConnected(ctx context.Context, name string, connected bool) error {
	ct, err := s.pool.Exec(ctx, `UPDATE runners SET connected = $1, last_seen_at = now() WHERE name = $2`, connected, name)
	if err != nil {
		return fmt.Errorf("pgstore: set runner connected: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return controld.ErrNotFound
	}
	return nil
}

func (s *Store) ListRunners(ctx context.Context) ([]controld.Runner, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, capacity_used, capacity_total, connected, last_seen_at FROM runners`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list runners: %w", err)
	}
	defer rows.Close()

	out := make([]controld.Runner, 0)
	for rows.Next() {
		var r controld.Runner
		var lastSeen *time.Time
		if err := rows.Scan(&r.Name, &r.CapacityUsed, &r.CapacityTotal, &r.Connected, &lastSeen); err != nil {
			return nil, fmt.Errorf("pgstore: list runners: %w", err)
		}
		if lastSeen != nil {
			r.LastSeenAt = *lastSeen
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// --- environments ---------------------------------------------------------

const selectEnvironmentCols = `id, name, image, setup, setup_hash, egress_allow, secret_refs, connectors, placement, setup_timeout_sec, snapshot_ref, snapshot_runner, snapshot_hash, created_at, updated_at`

// encodeConnectors renders cs as the JSON array the connectors column holds:
// each element is that connector's own object, passed through untouched, so
// members this build knows nothing about survive a round trip. (jsonb still
// normalizes whitespace and member order on the way in — it stores a value,
// not a byte string — but adds, drops, and rewrites nothing.) A connector
// carrying no Raw, built in code rather than decoded from a request, is
// stored as the bare envelope {"type": ...} so the column never holds a
// JSON null.
func encodeConnectors(cs []controld.Connector) ([]byte, error) {
	raws := make([]json.RawMessage, 0, len(cs))
	for _, c := range cs {
		if len(c.Raw) == 0 {
			b, err := json.Marshal(c)
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

// decodeConnectors is encodeConnectors's inverse: it splits the stored array
// into one Connector per element, keeping that element's bytes in Raw and
// lifting its "type" member into Type.
func decodeConnectors(b []byte) ([]controld.Connector, error) {
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
	out := make([]controld.Connector, 0, len(raws))
	for _, raw := range raws {
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, err
		}
		out = append(out, controld.Connector{Type: envelope.Type, Raw: raw})
	}
	return out, nil
}

func scanEnvironment(row rowScanner) (controld.Environment, error) {
	var e controld.Environment
	var egressBytes, refsBytes, connectorBytes []byte

	if err := row.Scan(&e.ID, &e.Name, &e.Image, &e.Setup, &e.SetupHash, &egressBytes, &refsBytes,
		&connectorBytes, &e.Placement, &e.SetupTimeoutSec, &e.SnapshotRef, &e.SnapshotRunner, &e.SnapshotHash,
		&e.CreatedAt, &e.UpdatedAt); err != nil {
		return controld.Environment{}, err
	}
	if len(egressBytes) > 0 {
		if err := json.Unmarshal(egressBytes, &e.EgressAllow); err != nil {
			return controld.Environment{}, fmt.Errorf("pgstore: decode egress_allow: %w", err)
		}
	}
	if len(refsBytes) > 0 {
		if err := json.Unmarshal(refsBytes, &e.SecretRefs); err != nil {
			return controld.Environment{}, fmt.Errorf("pgstore: decode secret_refs: %w", err)
		}
	}
	connectors, err := decodeConnectors(connectorBytes)
	if err != nil {
		return controld.Environment{}, fmt.Errorf("pgstore: decode connectors: %w", err)
	}
	e.Connectors = connectors
	return e, nil
}

// environmentColumns marshals the jsonb columns an insert or update writes.
func environmentColumns(e controld.Environment) (egress, refs, connectors []byte, err error) {
	if egress, err = json.Marshal(nonNilStrings(e.EgressAllow)); err != nil {
		return nil, nil, nil, fmt.Errorf("pgstore: encode egress_allow: %w", err)
	}
	if refs, err = json.Marshal(nonNilStrings(e.SecretRefs)); err != nil {
		return nil, nil, nil, fmt.Errorf("pgstore: encode secret_refs: %w", err)
	}
	if connectors, err = encodeConnectors(e.Connectors); err != nil {
		return nil, nil, nil, fmt.Errorf("pgstore: encode connectors: %w", err)
	}
	return egress, refs, connectors, nil
}

func (s *Store) CreateEnvironment(ctx context.Context, e controld.Environment) (controld.Environment, error) {
	now := time.Now()
	createdAt, updatedAt := e.CreatedAt, e.UpdatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if updatedAt.IsZero() {
		updatedAt = now
	}
	egress, refs, connectors, err := environmentColumns(e)
	if err != nil {
		return controld.Environment{}, err
	}

	// The snapshot columns are left at their '' defaults: a new environment
	// has no cache, and only SetEnvironmentSnapshot ever writes one.
	row := s.pool.QueryRow(ctx, `
		INSERT INTO environments (id, name, image, setup, setup_hash, egress_allow, secret_refs, connectors, placement, setup_timeout_sec, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING `+selectEnvironmentCols,
		e.ID, e.Name, e.Image, e.Setup, controld.SetupHash(e.Image, e.Setup),
		egress, refs, connectors, e.Placement, e.SetupTimeoutSec, createdAt, updatedAt)

	out, err := scanEnvironment(row)
	if err != nil {
		// environments has exactly two unique constraints — the primary key
		// and the name — and either one means the caller lost a race for an
		// identity that is already taken.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return controld.Environment{}, controld.ErrConflict
		}
		return controld.Environment{}, fmt.Errorf("pgstore: create environment: %w", err)
	}
	return out, nil
}

func (s *Store) GetEnvironment(ctx context.Context, id string) (controld.Environment, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+selectEnvironmentCols+` FROM environments WHERE id = $1`, id)
	e, err := scanEnvironment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Environment{}, controld.ErrNotFound
		}
		return controld.Environment{}, fmt.Errorf("pgstore: get environment: %w", err)
	}
	return e, nil
}

func (s *Store) GetEnvironmentByName(ctx context.Context, name string) (controld.Environment, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+selectEnvironmentCols+` FROM environments WHERE name = $1`, name)
	e, err := scanEnvironment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Environment{}, controld.ErrNotFound
		}
		return controld.Environment{}, fmt.Errorf("pgstore: get environment by name: %w", err)
	}
	return e, nil
}

func (s *Store) ListEnvironments(ctx context.Context) ([]controld.Environment, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+selectEnvironmentCols+` FROM environments ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list environments: %w", err)
	}
	defer rows.Close()

	out := make([]controld.Environment, 0)
	for rows.Next() {
		e, err := scanEnvironment(rows)
		if err != nil {
			return nil, fmt.Errorf("pgstore: list environments: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) UpdateEnvironment(ctx context.Context, e controld.Environment) (controld.Environment, error) {
	egress, refs, connectors, err := environmentColumns(e)
	if err != nil {
		return controld.Environment{}, err
	}

	// created_at and the snapshot columns are absent on purpose: an update
	// re-states what the operator asked for, and leaves a stale snapshot
	// standing for the caching path to rebuild.
	row := s.pool.QueryRow(ctx, `
		UPDATE environments SET
			name = $2, image = $3, setup = $4, setup_hash = $5, egress_allow = $6, secret_refs = $7,
			connectors = $8, placement = $9, setup_timeout_sec = $10, updated_at = now()
		WHERE id = $1
		RETURNING `+selectEnvironmentCols,
		e.ID, e.Name, e.Image, e.Setup, controld.SetupHash(e.Image, e.Setup),
		egress, refs, connectors, e.Placement, e.SetupTimeoutSec)

	out, err := scanEnvironment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Environment{}, controld.ErrNotFound
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return controld.Environment{}, controld.ErrConflict
		}
		return controld.Environment{}, fmt.Errorf("pgstore: update environment: %w", err)
	}
	return out, nil
}

func (s *Store) DeleteEnvironment(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM environments WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("pgstore: delete environment: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return controld.ErrNotFound
	}
	return nil
}

func (s *Store) CountSessionsByEnvironment(ctx context.Context, envID string, states []controld.SessionState) (int, error) {
	sql := `SELECT count(*) FROM sessions WHERE environment_id = $1`
	args := []any{envID}
	if len(states) > 0 {
		strs := make([]string, len(states))
		for i, st := range states {
			strs[i] = string(st)
		}
		args = append(args, strs)
		sql += ` AND state = ANY($2)`
	}

	var n int
	if err := s.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("pgstore: count sessions by environment: %w", err)
	}
	return n, nil
}

func (s *Store) SetEnvironmentSnapshot(ctx context.Context, envID, expectHash, ref, runner string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE environments SET snapshot_ref = $3, snapshot_runner = $4, snapshot_hash = $2, updated_at = now()
		WHERE id = $1 AND setup_hash = $2`, envID, expectHash, ref, runner)
	if err != nil {
		return fmt.Errorf("pgstore: set environment snapshot: %w", err)
	}
	// No row matched: the environment's setup moved on (or the environment
	// is gone), so this snapshot is for a build nobody wants any more.
	if ct.RowsAffected() == 0 {
		return controld.ErrConflict
	}
	return nil
}

// --- secrets --------------------------------------------------------------

func (s *Store) PutSecret(ctx context.Context, name string, ciphertext, nonce []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO secrets (name, ciphertext, nonce) VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE SET
			ciphertext = EXCLUDED.ciphertext, nonce = EXCLUDED.nonce, updated_at = now()`,
		name, ciphertext, nonce)
	if err != nil {
		return fmt.Errorf("pgstore: put secret: %w", err)
	}
	return nil
}

func (s *Store) ListSecrets(ctx context.Context) ([]controld.SecretMeta, error) {
	// The ciphertext and nonce columns are deliberately not selected: a
	// listing has no business carrying secret material.
	rows, err := s.pool.Query(ctx, `SELECT name, created_at, updated_at FROM secrets ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list secrets: %w", err)
	}
	defer rows.Close()

	out := make([]controld.SecretMeta, 0)
	for rows.Next() {
		var m controld.SecretMeta
		if err := rows.Scan(&m.Name, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, fmt.Errorf("pgstore: list secrets: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) GetSecret(ctx context.Context, name string) ([]byte, []byte, error) {
	var ciphertext, nonce []byte
	err := s.pool.QueryRow(ctx, `SELECT ciphertext, nonce FROM secrets WHERE name = $1`, name).Scan(&ciphertext, &nonce)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, controld.ErrNotFound
		}
		return nil, nil, fmt.Errorf("pgstore: get secret: %w", err)
	}
	return ciphertext, nonce, nil
}

func (s *Store) DeleteSecret(ctx context.Context, name string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM secrets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("pgstore: delete secret: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return controld.ErrNotFound
	}
	return nil
}

// --- cursor ---------------------------------------------------------------

// encodeCursor and decodeCursor implement ListSessions's opaque page
// cursor: base64 raw-URL encoding of "<created_at unixnano>|<id>". This
// must stay byte-compatible with controld.memStore's cursor of the same
// name, since storetest.RunContract runs unchanged against both stores.
//
// created_at round-trips exactly despite Postgres's microsecond (not
// nanosecond) timestamptz precision because the nanosecond value encoded
// here always comes from a time.Time pgx itself produced (scanned from a
// column, or about to be sent to one) — its trailing three digits are
// always zero, so converting it to/from a time.Time for the next query's
// bound parameter loses nothing. This also avoids the float64 precision
// loss a `to_timestamp(nanos/1e9)` SQL expression would introduce at
// today's Unix-nanosecond magnitudes.
func encodeCursor(createdAt time.Time, id string) string {
	raw := fmt.Sprintf("%d|%s", createdAt.UnixNano(), id)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (createdAtNano int64, id string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", fmt.Errorf("pgstore: invalid cursor: %w", err)
	}
	nanoStr, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return 0, "", fmt.Errorf("pgstore: invalid cursor: %q", cursor)
	}
	nano, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("pgstore: invalid cursor: %w", err)
	}
	return nano, id, nil
}
