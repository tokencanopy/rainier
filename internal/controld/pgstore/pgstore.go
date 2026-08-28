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

const selectSessionCols = `id, owner_id, name, image, cmd, egress_allow, state, runner, idempotency_key, error, created_at, updated_at, last_event_at`

// sessionScanner is satisfied by both pgx.Row and pgx.Rows.
type sessionScanner interface {
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

func scanSession(row sessionScanner) (controld.Session, error) {
	var sess controld.Session
	var state string
	var idem *string
	var cmdBytes, egressBytes []byte

	if err := row.Scan(&sess.ID, &sess.OwnerID, &sess.Name, &sess.Image, &cmdBytes, &egressBytes,
		&state, &sess.Runner, &idem, &sess.Error, &sess.CreatedAt, &sess.UpdatedAt, &sess.LastEventAt); err != nil {
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
		INSERT INTO sessions (id, owner_id, name, image, cmd, egress_allow, state, runner, idempotency_key, error, created_at, updated_at, last_event_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING `+selectSessionCols,
		sess.ID, sess.OwnerID, sess.Name, sess.Image, cmd, egress, string(sess.State), sess.Runner, idem, sess.Error,
		createdAt, updatedAt, lastEventAt)

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
