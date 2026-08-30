-- Exact-name resolution is a CLI lookup, not a reason to page through the
-- team's entire terminal history. State leads the second index so the default
-- `rainier ls` failed-row existence probe stays bounded when no failures exist.
-- These are full indexes: ListSessions uses parameterized predicates, for
-- which PostgreSQL cannot reliably prove a partial-index implication.
CREATE INDEX sessions_name_list
  ON sessions(name, created_at DESC, id DESC);

CREATE INDEX sessions_state_list
  ON sessions(state, created_at DESC, id DESC);
