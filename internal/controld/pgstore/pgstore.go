// Package pgstore implements controld.Store on Postgres, using pgx/v5's
// pgxpool. It embeds and runs its own migrations (see migrate.go), so Open
// alone is enough to bring a fresh database up to schema.
package pgstore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/controld"
)

// terminalStates lists the SessionState values SessionState.Terminal
// reports true for, spelled out for use inside SQL literals. Keep in sync
// with control.SessionState.Terminal.
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
	_, err := s.q(ctx).Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	return err
}

// Sessions, Environments, and Fleet are the three control repository ports,
// each a view over this store's own rows rather than a copy of them: the
// sessions Sessions() creates are the sessions Fleet() places, which is what
// makes one Store satisfy the whole repository contract (controlapp/repotest).
func (s *Store) Sessions() control.SessionRepository { return pgSessions{s} }

func (s *Store) Environments() control.EnvironmentRepository { return pgEnvironments{s} }

func (s *Store) Fleet() control.FleetRepository { return pgFleet{s} }

var (
	// The host's own persistence and the three control repositories: one
	// store, and the union of the two is controld.Store.
	_ controld.Store        = (*Store)(nil)
	_ controld.HostStore    = (*Store)(nil)
	_ controld.Repositories = (*Store)(nil)

	_ control.SessionRepository     = pgSessions{}
	_ control.EnvironmentRepository = pgEnvironments{}
	_ control.FleetRepository       = pgFleet{}
)

// --- users & tokens ---------------------------------------------------

func (s *Store) UpsertUser(ctx context.Context, githubID int64, login, role string) (controld.User, error) {
	row := s.q(ctx).QueryRow(ctx, `
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
	_, err := s.q(ctx).Exec(ctx, `
		INSERT INTO api_tokens (id, user_id, token_hash) VALUES ($1, $2, $3)`,
		tokenHash, userID, tokenHash)
	if err != nil {
		return fmt.Errorf("pgstore: insert token: %w", err)
	}
	return nil
}

func (s *Store) UserByToken(ctx context.Context, tokenHash string) (controld.User, error) {
	var userID string
	err := s.q(ctx).QueryRow(ctx, `
		UPDATE api_tokens SET last_used_at = now() WHERE token_hash = $1
		RETURNING user_id`, tokenHash).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.User{}, control.ErrNotFound
		}
		return controld.User{}, fmt.Errorf("pgstore: user by token: %w", err)
	}

	var u controld.User
	err = s.q(ctx).QueryRow(ctx, `
		SELECT id, github_id, login, role, created_at FROM users WHERE id = $1`, userID).
		Scan(&u.ID, &u.GitHubID, &u.Login, &u.Role, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.User{}, control.ErrNotFound
		}
		return controld.User{}, fmt.Errorf("pgstore: user by token: %w", err)
	}
	return u, nil
}

func (s *Store) GetUser(ctx context.Context, id string) (controld.User, error) {
	var u controld.User
	err := s.q(ctx).QueryRow(ctx, `
		SELECT id, github_id, login, role, created_at FROM users WHERE id = $1`, id).
		Scan(&u.ID, &u.GitHubID, &u.Login, &u.Role, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.User{}, control.ErrNotFound
		}
		return controld.User{}, fmt.Errorf("pgstore: get user: %w", err)
	}
	return u, nil
}

// --- row scanning ---------------------------------------------------------

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

// --- secrets --------------------------------------------------------------

func (s *Store) PutSecret(ctx context.Context, name string, ciphertext, nonce []byte) error {
	_, err := s.q(ctx).Exec(ctx, `
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
	rows, err := s.q(ctx).Query(ctx, `SELECT name, created_at, updated_at FROM secrets ORDER BY name ASC`)
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
	err := s.q(ctx).QueryRow(ctx, `SELECT ciphertext, nonce FROM secrets WHERE name = $1`, name).Scan(&ciphertext, &nonce)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, control.ErrNotFound
		}
		return nil, nil, fmt.Errorf("pgstore: get secret: %w", err)
	}
	return ciphertext, nonce, nil
}

func (s *Store) DeleteSecret(ctx context.Context, name string) error {
	ct, err := s.q(ctx).Exec(ctx, `DELETE FROM secrets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("pgstore: delete secret: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

// --- credentials ----------------------------------------------------------

const selectCredentialCols = `user_id, provider, ciphertext, nonce, refresh_ciphertext, refresh_nonce, status, scopes, obtained_at, expires_at, last_verified_at, last_used_at, updated_at`

func scanCredential(row rowScanner) (controld.Credential, error) {
	var c controld.Credential
	if err := row.Scan(&c.UserID, &c.Provider, &c.Ciphertext, &c.Nonce,
		&c.RefreshCiphertext, &c.RefreshNonce, &c.Status, &c.Scopes,
		&c.ObtainedAt, &c.ExpiresAt, &c.LastVerifiedAt, &c.LastUsedAt, &c.UpdatedAt); err != nil {
		return controld.Credential{}, err
	}
	return c, nil
}

// nilIfZero renders a zero time as SQL NULL, so the statement's COALESCE can
// stamp the server's own now() in its place. Every credential clock is set by
// the database, never by this process: SetCredentialStatus and
// TouchCredentialUsed can only use now(), and a row whose obtained_at came
// from a client whose clock runs fast would then look edited before it was
// created.
func nilIfZero(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (s *Store) UpsertCredential(ctx context.Context, c controld.Credential) error {
	status := c.Status
	if status == "" {
		status = controld.CredentialValid
	}
	// A whole-row replace: an upsert is a fresh login, so every column moves,
	// including obtained_at — the row now describes a different token.
	_, err := s.q(ctx).Exec(ctx, `
		INSERT INTO credentials (user_id, provider, ciphertext, nonce, refresh_ciphertext, refresh_nonce,
			status, scopes, obtained_at, expires_at, last_verified_at, last_used_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
			COALESCE($9::timestamptz, now()), $10::timestamptz,
			COALESCE($11::timestamptz, now()), COALESCE($12::timestamptz, now()), COALESCE($13::timestamptz, now()))
		ON CONFLICT (user_id, provider) DO UPDATE SET
			ciphertext = EXCLUDED.ciphertext,
			nonce = EXCLUDED.nonce,
			refresh_ciphertext = EXCLUDED.refresh_ciphertext,
			refresh_nonce = EXCLUDED.refresh_nonce,
			status = EXCLUDED.status,
			scopes = EXCLUDED.scopes,
			obtained_at = EXCLUDED.obtained_at,
			expires_at = EXCLUDED.expires_at,
			last_verified_at = EXCLUDED.last_verified_at,
			last_used_at = EXCLUDED.last_used_at,
			updated_at = EXCLUDED.updated_at`,
		c.UserID, c.Provider, c.Ciphertext, c.Nonce, c.RefreshCiphertext, c.RefreshNonce,
		status, c.Scopes, nilIfZero(c.ObtainedAt), c.ExpiresAt,
		nilIfZero(c.LastVerifiedAt), nilIfZero(c.LastUsedAt), nilIfZero(c.UpdatedAt))
	if err != nil {
		// Deliberately no value in the message: this error is logged, and a
		// credential's bytes must never reach a log line. The identity is
		// enough to find the row.
		return fmt.Errorf("pgstore: upsert credential for provider %q: %w", c.Provider, err)
	}
	return nil
}

func (s *Store) GetCredential(ctx context.Context, userID, provider string) (controld.Credential, error) {
	row := s.q(ctx).QueryRow(ctx, `SELECT `+selectCredentialCols+` FROM credentials WHERE user_id = $1 AND provider = $2`, userID, provider)
	c, err := scanCredential(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.Credential{}, control.ErrNotFound
		}
		return controld.Credential{}, fmt.Errorf("pgstore: get credential for provider %q: %w", provider, err)
	}
	return c, nil
}

func (s *Store) SetCredentialStatus(ctx context.Context, userID, provider, status string) error {
	ct, err := s.q(ctx).Exec(ctx, `
		UPDATE credentials SET status = $3, updated_at = now()
		WHERE user_id = $1 AND provider = $2`, userID, provider, status)
	if err != nil {
		return fmt.Errorf("pgstore: set credential status for provider %q: %w", provider, err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

func (s *Store) TouchCredentialUsed(ctx context.Context, userID, provider string) error {
	// last_used_at only. updated_at is the row's edit clock, and a mint is a
	// read: a session using a credential must not make it look freshly
	// changed to whoever is watching the status.
	ct, err := s.q(ctx).Exec(ctx, `
		UPDATE credentials SET last_used_at = now()
		WHERE user_id = $1 AND provider = $2`, userID, provider)
	if err != nil {
		return fmt.Errorf("pgstore: touch credential for provider %q: %w", provider, err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

func (s *Store) ListCredentials(ctx context.Context, userID string) ([]controld.Credential, error) {
	// Scoped to one user by the query itself, not by a filter above it: there
	// is no code path here that could hand back another user's row.
	rows, err := s.q(ctx).Query(ctx, `SELECT `+selectCredentialCols+` FROM credentials WHERE user_id = $1 ORDER BY provider ASC`, userID)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list credentials: %w", err)
	}
	defer rows.Close()

	out := make([]controld.Credential, 0)
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			return nil, fmt.Errorf("pgstore: list credentials: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// --- agent credentials ------------------------------------------------------
//
// Custody of a person's coding-agent logins (migration 0010). The sealed
// bytes are opaque here exactly as a git credential's are, and one property
// beyond that is this table's own: version is part of what the bytes were
// sealed against (agentvault.go), so the row's version and its ciphertext can
// never be allowed to drift apart. PutAgentCredential is the single statement
// that keeps them together.

func (s *Store) GetAgentCredential(ctx context.Context, userID, provider string) (controld.AgentCredential, error) {
	c := controld.AgentCredential{UserID: userID, Provider: provider}
	// The version is scanned as int64 and converted, the way every other
	// bigint-backed generation in this package is: pgx maps the column to
	// Go's signed 64-bit type, and the conversion is where the package says
	// so once rather than relying on a driver's coercion.
	var version int64
	err := s.q(ctx).QueryRow(ctx,
		`SELECT ciphertext, nonce, version, updated_at FROM agent_credentials WHERE user_id = $1 AND provider = $2`,
		userID, provider).Scan(&c.Ciphertext, &c.Nonce, &version, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controld.AgentCredential{}, control.ErrNotFound
		}
		// The identity is enough to find the row, and the message carries no
		// value — the same discipline UpsertCredential is written under, and
		// for the same reason: this error is logged.
		return controld.AgentCredential{}, fmt.Errorf("pgstore: get agent credential for provider %q: %w", provider, err)
	}
	c.Version = uint64(version)
	return c, nil
}

// PutAgentCredential writes the row in ONE statement whose WHERE is the
// version guard: the update fires only when the stored version is exactly one
// behind the version the caller sealed for, so two concurrent puts cannot
// both win and neither can leave a ciphertext sealed against a version the
// row does not have. A guard that did not fire returns no row, which is
// control.ErrConflict — the caller re-reads, re-seals, and tries again.
func (s *Store) PutAgentCredential(ctx context.Context, c controld.AgentCredential) (uint64, error) {
	if c.UserID == "" || c.Provider == "" || c.Version == 0 {
		return 0, control.ErrInvalid
	}
	var version int64
	err := s.q(ctx).QueryRow(ctx, `
		INSERT INTO agent_credentials (user_id, provider, ciphertext, nonce, version, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (user_id, provider) DO UPDATE SET
			ciphertext = EXCLUDED.ciphertext,
			nonce      = EXCLUDED.nonce,
			version    = EXCLUDED.version,
			updated_at = now()
		WHERE agent_credentials.version = EXCLUDED.version - 1
		RETURNING version`,
		c.UserID, c.Provider, c.Ciphertext, c.Nonce, int64(c.Version)).Scan(&version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, control.ErrConflict
		}
		return 0, fmt.Errorf("pgstore: put agent credential for provider %q: %w", c.Provider, err)
	}
	return uint64(version), nil
}

func (s *Store) DeleteAgentCredential(ctx context.Context, userID, provider string) error {
	// No RowsAffected check: a revoke of what is not there has already
	// achieved what it asked for, and reporting ErrNotFound would make an
	// idempotent operation fail on its second call.
	if _, err := s.q(ctx).Exec(ctx,
		`DELETE FROM agent_credentials WHERE user_id = $1 AND provider = $2`, userID, provider); err != nil {
		return fmt.Errorf("pgstore: delete agent credential for provider %q: %w", provider, err)
	}
	return nil
}

// ListAgentCredentials does not select ciphertext or nonce at all. Clearing
// them after the scan would be a promise this code keeps; not reading them is
// a promise the QUERY keeps, which survives someone editing the loop below.
func (s *Store) ListAgentCredentials(ctx context.Context, userID string) ([]controld.AgentCredential, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT provider, version, updated_at FROM agent_credentials WHERE user_id = $1 ORDER BY provider ASC`, userID)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list agent credentials: %w", err)
	}
	defer rows.Close()

	out := make([]controld.AgentCredential, 0)
	for rows.Next() {
		c := controld.AgentCredential{UserID: userID}
		var version int64
		if err := rows.Scan(&c.Provider, &version, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("pgstore: list agent credentials: %w", err)
		}
		c.Version = uint64(version)
		out = append(out, c)
	}
	return out, rows.Err()
}

// --- cursor ---------------------------------------------------------------

// encodeCursor and decodeCursor implement ListSessions's opaque page
// cursor: base64 raw-URL encoding of "<created_at unixnano>|<id>". This
// must stay byte-compatible with controld.memStore's cursor of the same
// name, since controlapp/repotest runs unchanged against both stores.
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

// encodeEnvironmentCursor and decodeEnvironmentCursor implement
// ListEnvironments's opaque page cursor: base64 raw-URL encoding of
// "<id>|<name>". Rows are ordered (name, id) ascending; the id leads the
// encoding because an environment id never contains a "|" and a name may, so
// the split stays unambiguous. It is byte-compatible with memstore's cursor
// of the same shape, because the two stores answer the same contract suite
// and a cursor minted by one has to read as the same position in the other.
func encodeEnvironmentCursor(name string, id control.EnvironmentID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(string(id) + "|" + name))
}

func decodeEnvironmentCursor(cursor string) (name string, id control.EnvironmentID, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("pgstore: invalid cursor: %w", control.ErrInvalid)
	}
	rawID, rawName, ok := strings.Cut(string(raw), "|")
	if !ok {
		return "", "", fmt.Errorf("pgstore: invalid cursor: %w", control.ErrInvalid)
	}
	return rawName, control.EnvironmentID(rawID), nil
}
