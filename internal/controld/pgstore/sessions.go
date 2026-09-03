// internal/controld/pgstore/sessions.go
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
)

// pgSessions is control.SessionRepository over the sessions table. Every
// method is workspace-keyed in its WHERE clause rather than filtered after
// the fact: a row of another workspace is not something this code can decide
// to hide, because no statement here can reach it.
type pgSessions struct{ s *Store }

// sessionCols is the native projection. resolved_image is deliberately absent
// — 0007 folded it into image, which is control.PortableSpec.Image, and the
// contract model has no second image column.
const sessionCols = `id, workspace_id, owner_id, name, image, cmd, egress_allow, repos, state, pool_id, runner, ` +
	`placement_generation, controller_generation, idempotency_key, error, environment_id, setup_hash, ` +
	`child_exit_code, created_at, updated_at, last_event_at`

func scanControlSession(row rowScanner) (control.Session, error) {
	var (
		s                          control.Session
		id, ws, creator, state     string
		pool, envID                string
		runner, idem               *string
		placementGen, controlleGen int64
		cmdBytes, egressBytes      []byte
		reposBytes                 []byte
	)
	if err := row.Scan(&id, &ws, &creator, &s.Name, &s.Spec.Image, &cmdBytes, &egressBytes, &reposBytes,
		&state, &pool, &runner, &placementGen, &controlleGen, &idem, &s.Error, &envID, &s.SetupHash,
		&s.ChildExitCode, &s.CreatedAt, &s.UpdatedAt, &s.LastEventAt); err != nil {
		return control.Session{}, err
	}
	s.ID = control.SessionID(id)
	s.WorkspaceID = control.WorkspaceID(ws)
	s.CreatorID = control.ActorID(creator)
	s.State = control.SessionState(state)
	s.PoolID = control.PoolID(pool)
	s.EnvironmentID = control.EnvironmentID(envID)
	if runner != nil {
		s.RunnerID = control.RunnerID(*runner)
	}
	if idem != nil {
		s.IdempotencyKey = *idem
	}
	s.PlacementGeneration = uint64(placementGen)
	s.ControllerGeneration = uint64(controlleGen)

	if len(cmdBytes) > 0 {
		if err := json.Unmarshal(cmdBytes, &s.Spec.Cmd); err != nil {
			return control.Session{}, err
		}
	}
	if len(egressBytes) > 0 {
		if err := json.Unmarshal(egressBytes, &s.Spec.EgressAllow); err != nil {
			return control.Session{}, err
		}
	}
	// A SQL NULL scans as no bytes at all and stays a nil Repos — the session
	// named no override. An empty JSON array decodes to a non-nil empty slice,
	// which is the other instruction ("clone nothing") and must not collapse
	// into the first.
	if len(reposBytes) > 0 {
		refs := []controlRepoRefJSON{}
		if err := json.Unmarshal(reposBytes, &refs); err != nil {
			return control.Session{}, err
		}
		s.Spec.Repos = make([]control.RepoRef, len(refs))
		for i, ref := range refs {
			s.Spec.Repos[i] = control.RepoRef{Repo: ref.Repo, BaseBranch: ref.BaseBranch}
		}
	}
	return s, nil
}

// sessionSpecColumns marshals the three jsonb columns a session's spec owns,
// keeping repos's nil-versus-empty distinction (see scanControlSession).
func sessionSpecColumns(spec control.PortableSpec) (cmd, egress, repos []byte, err error) {
	if cmd, err = json.Marshal(nonNilStrings(spec.Cmd)); err != nil {
		return nil, nil, nil, err
	}
	if egress, err = json.Marshal(nonNilStrings(spec.EgressAllow)); err != nil {
		return nil, nil, nil, err
	}
	if spec.Repos != nil {
		refs := make([]controlRepoRefJSON, len(spec.Repos))
		for i, r := range spec.Repos {
			refs[i] = controlRepoRefJSON{Repo: r.Repo, BaseBranch: r.BaseBranch}
		}
		if repos, err = json.Marshal(refs); err != nil {
			return nil, nil, nil, err
		}
	}
	return cmd, egress, repos, nil
}

// controlRepoRefJSON is the stored spelling of a repository override.
// control.RepoRef carries no struct tags — the control model is not a wire
// format — so the column's member names live here, matching the ones the
// pre-O9 rows already hold.
type controlRepoRefJSON struct {
	Repo       string `json:"repo"`
	BaseBranch string `json:"base_branch,omitempty"`
}

func (r pgSessions) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	if ws == "" {
		return control.Session{}, control.ErrInvalid
	}
	now := time.Now()
	createdAt, updatedAt, lastEventAt := s.CreatedAt, s.UpdatedAt, s.LastEventAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if updatedAt.IsZero() {
		updatedAt = now
	}
	if lastEventAt.IsZero() {
		lastEventAt = now
	}
	cmd, egress, repos, err := sessionSpecColumns(s.Spec)
	if err != nil {
		return control.Session{}, fmt.Errorf("pgstore: create session: encode spec: %w", control.ErrInvalid)
	}
	// idempotency_key is stored NULL for "no key" so the sessions_idem partial
	// unique index does not treat every keyless session of a creator as a
	// collision with the last one.
	var idem *string
	if s.IdempotencyKey != "" {
		key := s.IdempotencyKey
		idem = &key
	}

	// Three fields are the row's own history and not the caller's: a create
	// opens the first placement generation (GREATEST, so a caller's zero
	// becomes one and a resumed import keeps its own), no controller has
	// attached yet, and nothing has exited — so controller_generation and
	// child_exit_code are simply not written and take their defaults.
	row := r.s.pool.QueryRow(ctx, `
		INSERT INTO sessions (id, workspace_id, owner_id, name, image, cmd, egress_allow, repos, state,
			pool_id, runner, placement_generation, idempotency_key, error, environment_id, setup_hash,
			created_at, updated_at, last_event_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, GREATEST(1, $12::bigint), $13, $14, $15, $16, $17, $18, $19)
		RETURNING `+sessionCols,
		string(s.ID), string(ws), string(s.CreatorID), s.Name, s.Spec.Image, cmd, egress, repos, string(s.State),
		string(s.PoolID), string(s.RunnerID), int64(s.PlacementGeneration), idem, s.Error, string(s.EnvironmentID),
		s.SetupHash, createdAt, updatedAt, lastEventAt)

	out, err := scanControlSession(row)
	if err != nil {
		code, _ := constraintViolation(err)
		switch {
		case code == sqlstateUniqueViolation:
			// Both unique indexes land here, and the idempotency key wins,
			// exactly as it does in memstore: a replay is the caller seeing
			// the answer its key already got, not a collision with itself —
			// even when the row that answer names is also the one holding the
			// name this create asked for.
			if s.IdempotencyKey != "" {
				if existing, err := r.SessionByIDem(ctx, ws, s.CreatorID, s.IdempotencyKey); err == nil {
					return existing, nil
				}
			}
			return control.Session{}, control.ErrConflict
		case code == sqlstateForeignKeyViolation:
			// The workspace is the only foreign key a session row has: a
			// write into a workspace that does not exist lands nowhere, and
			// says nothing more than that.
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, unavailable("create session", err)
	}
	return out, nil
}

func (r pgSessions) GetSession(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	if ws == "" {
		return control.Session{}, control.ErrInvalid
	}
	row := r.s.pool.QueryRow(ctx,
		`SELECT `+sessionCols+` FROM sessions WHERE workspace_id = $1 AND id = $2`, string(ws), string(id))
	out, err := scanControlSession(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, unavailable("get session", err)
	}
	return out, nil
}

func (r pgSessions) SessionByIDem(ctx context.Context, ws control.WorkspaceID, creator control.ActorID, key string) (control.Session, error) {
	if ws == "" {
		return control.Session{}, control.ErrInvalid
	}
	if key == "" {
		return control.Session{}, control.ErrNotFound
	}
	row := r.s.pool.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions
		WHERE workspace_id = $1 AND owner_id = $2 AND idempotency_key = $3`, string(ws), string(creator), key)
	out, err := scanControlSession(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return control.Session{}, control.ErrNotFound
		}
		return control.Session{}, unavailable("session by idempotency key", err)
	}
	return out, nil
}

func (r pgSessions) ListSessions(ctx context.Context, ws control.WorkspaceID, q control.SessionQuery) ([]control.Session, string, error) {
	if ws == "" {
		return nil, "", control.ErrInvalid
	}
	var sb strings.Builder
	sb.WriteString(`SELECT ` + sessionCols + ` FROM sessions WHERE workspace_id = $1`)
	args := []any{string(ws)}
	if !q.IncludeTerminal {
		sb.WriteString(` AND state NOT IN (` + terminalStatesSQL + `)`)
	}
	if q.Cursor != "" {
		nano, id, err := decodeCursor(q.Cursor)
		if err != nil {
			// A cursor is opaque, so a caller cannot have "almost" got one
			// right: anything that does not decode is malformed input, and
			// the reason it did not decode is not theirs to see.
			return nil, "", fmt.Errorf("pgstore: invalid cursor: %w", control.ErrInvalid)
		}
		args = append(args, time.Unix(0, nano), id)
		fmt.Fprintf(&sb, " AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	sb.WriteString(` ORDER BY created_at DESC, id DESC`)
	if q.Limit > 0 {
		args = append(args, q.Limit)
		fmt.Fprintf(&sb, " LIMIT $%d", len(args))
	}

	rows, err := r.s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", unavailable("list sessions", err)
	}
	defer rows.Close()

	out := make([]control.Session, 0)
	for rows.Next() {
		sess, err := scanControlSession(rows)
		if err != nil {
			return nil, "", unavailable("list sessions", err)
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, "", unavailable("list sessions", err)
	}

	// A full page always gets a cursor, even when it happens to have emptied
	// the table: knowing otherwise would cost a look-ahead row on every page,
	// and one empty last page is cheaper than that.
	next := ""
	if q.Limit > 0 && len(out) == q.Limit {
		last := out[len(out)-1]
		next = encodeCursor(last.CreatedAt, string(last.ID))
	}
	return out, next, nil
}

// Transition is one statement: the guard, the columns, and the placement
// generation move together or not at all. The generation advances exactly
// when the transition names a runner to run on — a placement, an adoption, a
// cold resume — and stands still when it names none or clears the one it has,
// because clearing a placement ends a generation rather than opening one.
func (r pgSessions) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	if ws == "" {
		return control.ErrInvalid
	}
	fromStrs := make([]string, len(from))
	for i, f := range from {
		fromStrs[i] = string(f)
	}
	var runner *string
	if opts.RunnerID != nil {
		next := string(*opts.RunnerID)
		runner = &next
	}

	ct, err := r.s.pool.Exec(ctx, `
		UPDATE sessions
		SET state = $1,
		    runner = COALESCE($2::text, runner),
		    placement_generation = placement_generation + CASE WHEN $2::text IS NOT NULL AND $2::text <> '' THEN 1 ELSE 0 END,
		    error = COALESCE($3::text, error),
		    updated_at = now(), last_event_at = now()
		WHERE workspace_id = $4 AND id = $5 AND state = ANY($6)`,
		string(to), runner, opts.Error, string(ws), string(id), fromStrs)
	if err != nil {
		return unavailable("transition", err)
	}
	if ct.RowsAffected() > 0 {
		return nil
	}
	// 0 rows: either the row is not there at all, or it is and its state was
	// not in the from-list. They are different answers and the caller acts on
	// the difference.
	if _, err := r.GetSession(ctx, ws, id); err != nil {
		return err
	}
	return control.ErrConflict
}

func (r pgSessions) SetSessionSetupHash(ctx context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	if ws == "" {
		return control.ErrInvalid
	}
	// last_event_at is deliberately left alone: provenance is not a lifecycle
	// event, and a session's liveness clock must not tick for a bookkeeping
	// write.
	ct, err := r.s.pool.Exec(ctx,
		`UPDATE sessions SET setup_hash = $1, updated_at = now() WHERE workspace_id = $2 AND id = $3`,
		hash, string(ws), string(id))
	if err != nil {
		return unavailable("set session setup hash", err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

func (r pgSessions) SetChildExitCode(ctx context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	if ws == "" {
		return control.ErrInvalid
	}
	// Like the setup hash: the child exiting is an observation about the
	// process inside the container, not a transition of the session, which
	// stays attachable afterwards.
	ct, err := r.s.pool.Exec(ctx,
		`UPDATE sessions SET child_exit_code = $1, updated_at = now() WHERE workspace_id = $2 AND id = $3`,
		code, string(ws), string(id))
	if err != nil {
		return unavailable("set child exit code", err)
	}
	if ct.RowsAffected() == 0 {
		return control.ErrNotFound
	}
	return nil
}

// NextControllerGeneration advances the row's own controller counter in one
// statement. The lease is durable and shared by every replica, so two
// controllers cannot be handed the same authority whatever process they
// attached through.
func (r pgSessions) NextControllerGeneration(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (uint64, error) {
	if ws == "" {
		return 0, control.ErrInvalid
	}
	var generation int64
	err := r.s.pool.QueryRow(ctx, `
		UPDATE sessions SET controller_generation = controller_generation + 1, updated_at = now()
		WHERE workspace_id = $1 AND id = $2
		RETURNING controller_generation`, string(ws), string(id)).Scan(&generation)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, control.ErrNotFound
		}
		return 0, unavailable("next controller generation", err)
	}
	return uint64(generation), nil
}
