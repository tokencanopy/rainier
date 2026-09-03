-- 0007_workspace_scope.sql — O9 expand step.
--
-- Every tenant table carries its workspace; sessions carry their pool and
-- their placement and controller generations; runners carry their pool,
-- generation, and capabilities. The defaults name the one self-hosted
-- workspace and pool so this migration alone scopes every existing row and
-- the pre-O9 code paths keep working until the contract step (0008) removes
-- the defaults and the columns they replaced. Uniqueness becomes
-- workspace-composite here, because a constraint that ignores the workspace
-- is a cross-tenant collision waiting to happen.
CREATE TABLE workspaces (
  id text PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO workspaces (id) VALUES ('ws_self_hosted');

ALTER TABLE sessions
  ADD COLUMN workspace_id text NOT NULL DEFAULT 'ws_self_hosted' REFERENCES workspaces(id),
  ADD COLUMN pool_id text NOT NULL DEFAULT 'pool_self_hosted',
  ADD COLUMN placement_generation bigint NOT NULL DEFAULT 1,
  ADD COLUMN controller_generation bigint NOT NULL DEFAULT 0;
-- The resolved image IS the session's image (control.PortableSpec.Image).
UPDATE sessions SET image = resolved_image WHERE resolved_image <> '';

-- The identity of a session is (workspace, id): an id from another workspace
-- is not a key here, so two tenants may mint the same one without either
-- being able to reach the other's row.
ALTER TABLE sessions DROP CONSTRAINT sessions_pkey;
ALTER TABLE sessions ADD PRIMARY KEY (workspace_id, id);
-- owner_id holds control.Session.CreatorID: an actor of the workspace, which
-- in a hosted cell is not a row of this installation's own users table. The
-- column keeps its name and its meaning for self-hosted rows; what goes is
-- the assumption that every creator is an operator with a GitHub login here.
ALTER TABLE sessions DROP CONSTRAINT sessions_owner_id_fkey;

DROP INDEX sessions_idem;
CREATE UNIQUE INDEX sessions_idem ON sessions(workspace_id, owner_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
DROP INDEX sessions_owner_name_active;
CREATE UNIQUE INDEX sessions_owner_name_active ON sessions(workspace_id, owner_id, name)
  WHERE name <> '' AND state NOT IN ('canceled','failed','dead','destroyed');
DROP INDEX sessions_list;
CREATE INDEX sessions_list ON sessions(workspace_id, created_at DESC, id DESC);
DROP INDEX sessions_runner;
CREATE INDEX sessions_runner ON sessions(pool_id, runner) WHERE runner IS NOT NULL;
CREATE INDEX sessions_pool_queue ON sessions(pool_id, state, created_at ASC, id ASC);

ALTER TABLE environments
  ADD COLUMN workspace_id text NOT NULL DEFAULT 'ws_self_hosted' REFERENCES workspaces(id),
  ADD COLUMN requirements jsonb NOT NULL DEFAULT '{}';
-- An operator's runner pin becomes the portable capability the scheduler
-- already matches on (adapt_scope.go, placementCapabilityPrefix).
UPDATE environments
  SET requirements = jsonb_build_object('capabilities', jsonb_build_array('placement:' || placement))
  WHERE placement <> '';
ALTER TABLE environments DROP CONSTRAINT environments_name_key;
CREATE UNIQUE INDEX environments_workspace_name ON environments(workspace_id, name);
-- An environment id is a key only inside its workspace, for the same reason a
-- session id is.
ALTER TABLE environments DROP CONSTRAINT environments_pkey;
ALTER TABLE environments ADD PRIMARY KEY (workspace_id, id);

ALTER TABLE runners
  ADD COLUMN pool_id text NOT NULL DEFAULT 'pool_self_hosted',
  ADD COLUMN generation bigint NOT NULL DEFAULT 0,
  ADD COLUMN capabilities jsonb NOT NULL DEFAULT '[]';
ALTER TABLE runners DROP CONSTRAINT runners_pkey;
ALTER TABLE runners ADD PRIMARY KEY (pool_id, name);
