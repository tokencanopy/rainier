-- 0008_workspace_scope_contract.sql — O9 contract step.
--
-- No code path relies on a default workspace or pool any more: every insert
-- names its scope. Dropping the defaults makes a missing scope a database
-- error instead of a silent write into the installation workspace. The
-- columns the expand step replaced go with them.
ALTER TABLE sessions
  ALTER COLUMN workspace_id DROP DEFAULT,
  ALTER COLUMN pool_id DROP DEFAULT,
  DROP COLUMN resolved_image;
DROP INDEX sessions_name_list;
DROP INDEX sessions_state_list;
ALTER TABLE environments
  ALTER COLUMN workspace_id DROP DEFAULT,
  DROP COLUMN placement;
ALTER TABLE runners
  ALTER COLUMN pool_id DROP DEFAULT;
