-- Optional live-guest authorization. No private keys or RAM are persisted.
-- State lives on the bootstrap row so cold boot invalidation and reconnect
-- token replacement cannot race across independent authorization records.
ALTER TABLE session_bootstraps
 ADD COLUMN guest_boot_epoch text,
 ADD COLUMN guest_public_key text,
 ADD COLUMN reconnect_attempt text,
 ADD COLUMN reconnect_challenge text,
 ADD COLUMN reconnect_expires_at timestamptz,
 ADD COLUMN reconnect_runner_generation bigint,
 ADD COLUMN guest_connection_epoch bigint NOT NULL DEFAULT 0 CHECK (guest_connection_epoch >= 0),
 ADD CONSTRAINT guest_identity_complete CHECK ((guest_boot_epoch IS NULL) = (guest_public_key IS NULL));
