-- Exact placements counted in the same runner usage observation.
ALTER TABLE runners ADD COLUMN capacity_placements jsonb NOT NULL DEFAULT '{}'::jsonb;
