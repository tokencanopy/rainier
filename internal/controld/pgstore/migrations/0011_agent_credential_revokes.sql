-- 0011_agent_credential_revokes.sql — represent logout without credential bytes.
ALTER TABLE agent_credentials
  ADD COLUMN IF NOT EXISTS revoked boolean NOT NULL DEFAULT false;
