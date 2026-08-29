CREATE TABLE credentials (
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider text NOT NULL,
  ciphertext bytea NOT NULL,
  nonce bytea NOT NULL,
  refresh_ciphertext bytea,
  refresh_nonce bytea,
  status text NOT NULL DEFAULT 'valid',
  scopes text NOT NULL DEFAULT '',
  obtained_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  last_verified_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, provider)
);
ALTER TABLE environments ADD COLUMN init text NOT NULL DEFAULT '';
ALTER TABLE environments ADD COLUMN init_timeout_sec int NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN child_exit_code int;
