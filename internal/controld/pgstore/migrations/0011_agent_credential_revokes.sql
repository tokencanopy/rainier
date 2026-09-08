-- 0011_agent_credential_revokes.sql — keep logout as a monotonic fence.
--
-- A delete loses the version that an in-flight sandbox put last observed. A
-- tombstone keeps that version without keeping credential bytes, so a put
-- that began before logout cannot recreate the credential afterwards.
ALTER TABLE agent_credentials
  ADD COLUMN IF NOT EXISTS revoked boolean NOT NULL DEFAULT false;
