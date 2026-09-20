-- 0015_session_bootstrap.sql — the single-use bootstrap token a microVM
-- session exchanges for its environment's decrypted secrets
-- (docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md §3).
--
-- One row per session and no history: a fresh mint REPLACES its predecessor,
-- because a cold resume's token must be the only one that works and the one
-- the previous boot was handed must stop being an answer. That is why the
-- primary key is the session rather than the token.
--
-- token_hash is the hex-encoded SHA-256 of the token; the plaintext left the
-- mint and is stored nowhere, so a reader of this table learns which sessions
-- have a live token and nothing that could redeem one.
--
-- placement_generation is the fence: a session re-placed onto another runner
-- has moved past the sandbox the token was minted for. expires_at is
-- absolute so every replica agrees without agreeing on a TTL. consumed_at is
-- the spend, and single-use is enforced by the predicated UPDATE that sets it
-- — never by a read followed by a write.
--
-- ON DELETE CASCADE because a token outliving the session it names is a row
-- nothing will ever consume and nothing will ever clean up.
CREATE TABLE IF NOT EXISTS session_bootstraps (
  session_id           text PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
  workspace_id         text NOT NULL,
  token_hash           text NOT NULL,
  placement_generation bigint NOT NULL,
  expires_at           timestamptz NOT NULL,
  consumed_at          timestamptz,
  created_at           timestamptz NOT NULL DEFAULT now()
);
