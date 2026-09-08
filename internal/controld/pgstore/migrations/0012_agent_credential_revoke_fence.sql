-- 0012_agent_credential_revoke_fence.sql — retain logout ordering after relogin.
--
-- This is separate from 0011 because an earlier build containing 0011 may
-- already have run it. Existing rows conservatively fence every older writer:
-- a session at the current version can still put, while an older one refetches.
ALTER TABLE agent_credentials
  ADD COLUMN IF NOT EXISTS last_revoked_version bigint NOT NULL DEFAULT 0;

UPDATE agent_credentials
SET last_revoked_version = version
WHERE last_revoked_version < version;
