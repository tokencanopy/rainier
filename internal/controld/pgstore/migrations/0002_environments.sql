CREATE TABLE environments (
  id text PRIMARY KEY,
  name text UNIQUE NOT NULL,
  image text NOT NULL,
  setup text NOT NULL DEFAULT '',
  setup_hash text NOT NULL,
  egress_allow jsonb NOT NULL DEFAULT '[]',
  secret_refs jsonb NOT NULL DEFAULT '[]',
  connectors jsonb NOT NULL DEFAULT '[]',
  placement text NOT NULL DEFAULT '',
  setup_timeout_sec int NOT NULL DEFAULT 0,
  snapshot_ref text NOT NULL DEFAULT '',
  snapshot_runner text NOT NULL DEFAULT '',
  snapshot_hash text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE secrets (
  name text PRIMARY KEY,
  ciphertext bytea NOT NULL,
  nonce bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE sessions ADD COLUMN environment_id text NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN resolved_image text NOT NULL DEFAULT '';
