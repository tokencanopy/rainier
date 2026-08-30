-- A session's own `repos` override, as the client sent it. NULL means the
-- caller named none (the environment's github connectors decide); a JSON
-- array — including the empty one, which means "clone nothing" — means the
-- caller answered the question themselves. The nullability IS the semantic,
-- so this column deliberately has no NOT NULL DEFAULT '[]' like cmd and
-- egress_allow: an empty array here is an instruction, not an absence.
ALTER TABLE sessions ADD COLUMN repos jsonb;
