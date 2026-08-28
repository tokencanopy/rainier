CREATE TABLE users (
  id text PRIMARY KEY,
  github_id bigint UNIQUE NOT NULL,
  login text NOT NULL,
  role text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE api_tokens (
  id text PRIMARY KEY,
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash text UNIQUE NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz
);
CREATE TABLE runners (
  name text PRIMARY KEY,
  capacity_used int NOT NULL DEFAULT 0,
  capacity_total int NOT NULL DEFAULT 0,
  connected boolean NOT NULL DEFAULT false,
  last_seen_at timestamptz
);
CREATE TABLE sessions (
  id text PRIMARY KEY,
  owner_id text NOT NULL REFERENCES users(id),
  name text NOT NULL DEFAULT '',
  image text NOT NULL DEFAULT '',
  cmd jsonb NOT NULL DEFAULT '[]',
  egress_allow jsonb NOT NULL DEFAULT '[]',
  state text NOT NULL,
  runner text,
  idempotency_key text,
  error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_event_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX sessions_idem ON sessions(owner_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX sessions_owner_name_active ON sessions(owner_id, name)
  WHERE name <> '' AND state NOT IN ('canceled','failed','dead','destroyed');
CREATE INDEX sessions_list ON sessions(created_at DESC, id DESC);
CREATE INDEX sessions_runner ON sessions(runner) WHERE runner IS NOT NULL;
