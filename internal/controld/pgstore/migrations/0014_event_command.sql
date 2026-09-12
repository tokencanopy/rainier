-- 0014_event_command.sql — the one thing an exec event records beyond the
-- fields every event already has: the NAME of the command that ran.
--
-- The name and nothing else. It is the base name of argv[0], capped at 64
-- bytes by the application and replaced with '?' when it is not printable
-- ASCII, so this column can hold "git" and can never hold an argument, an
-- environment variable, a working directory, or a byte of input or output.
-- That is what makes an audit log answer "somebody ran a command in this
-- session at this time" without becoming a transcript of what they ran.
--
-- Existing rows get '', which is correct rather than a placeholder: every
-- event that predates this column is an attach, a create or a transfer, and
-- none of them ran a command.
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS command text NOT NULL DEFAULT '';
