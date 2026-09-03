-- 0009_events.sql — the durable application-event table.
--
-- One row per control.Event: fixed, provider-neutral fields and nothing
-- else — no terminal data, content, secret, raw error, price, or provider
-- resource. It is written inside the same transaction as the mutation it
-- describes (pgstore.Store.Run), which is what makes the event a fact rather
-- than a hope. Nothing in this release reads it back; the hosted cell's
-- outbox dispatcher is the consumer this shape is for.
CREATE TABLE events (
  id text PRIMARY KEY,
  workspace_id text NOT NULL REFERENCES workspaces(id),
  actor_id text NOT NULL,
  action text NOT NULL,
  resource_kind text NOT NULL,
  resource_id text NOT NULL,
  resource_workspace_id text NOT NULL,
  resource_creator_id text NOT NULL DEFAULT '',
  at timestamptz NOT NULL,
  placement_generation bigint NOT NULL DEFAULT 0,
  cpu_time_seconds double precision NOT NULL DEFAULT 0,
  memory_byte_seconds bigint NOT NULL DEFAULT 0,
  storage_bytes bigint NOT NULL DEFAULT 0,
  network_bytes bigint NOT NULL DEFAULT 0,
  agent_token_count bigint NOT NULL DEFAULT 0,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_workspace_at ON events(workspace_id, at DESC, id DESC);
