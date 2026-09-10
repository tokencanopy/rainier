-- 0013_controller_lease.sql — the durable half of the terminal controller
-- lease, beside the generation 0007 already added.
--
-- holder is an opaque per-attach identity (16 hex characters from
-- crypto/rand), never a user, device, account or browser-session identifier.
-- Existing rows get a vacant lease: '' and NULL, which is exactly "nobody
-- holds control", so a session that predates this migration behaves like one
-- created after it — the next attach claims from its current generation.
--
-- Expiry is passive and there is no sweeper: a row whose expiry has passed is
-- simply not live. The partial index is what keeps "is anybody holding this?"
-- cheap without indexing the vacant majority.
ALTER TABLE sessions
  ADD COLUMN IF NOT EXISTS controller_holder text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS controller_lease_expires_at timestamptz;

CREATE INDEX IF NOT EXISTS sessions_controller_lease_idx
  ON sessions (workspace_id, controller_lease_expires_at)
  WHERE controller_holder <> '';
