-- 0010_agent_credentials.sql — custody of one person's coding-agent logins.
--
-- One row per (person, agent): the sealed credential set that person's own
-- login to that coding agent produced, which every session they later start —
-- in any workspace they are a member of, on any runner — is handed at boot.
-- The provider column holds a name from controlapp's table and this schema
-- never spells one, which is the plan's rule for everything below controlapp.
-- There is deliberately NO workspace column either: the set belongs to the
-- person, and the projection into workspaces happens above the store, at
-- delivery, where membership is re-checked every time.
--
-- ciphertext/nonce are opaque bytes, exactly as in `credentials`: sealing is
-- controld's (internal/controld/agentvault.go), and the database holds no key
-- and no plaintext. version counts puts and is bound into the seal's
-- additional authenticated data, so a row copied to another user, another
-- provider, or another version does not open — the column is part of the
-- credential's identity, not a bookkeeping field, and an edit to it destroys
-- the row rather than renaming it.
--
-- ON DELETE CASCADE: removing an operator removes their agent logins with
-- them, the same way it already removes their tokens and their git credential.
CREATE TABLE agent_credentials (
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider text NOT NULL,
  ciphertext bytea NOT NULL,
  nonce bytea NOT NULL,
  version bigint NOT NULL DEFAULT 1,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, provider)
);
