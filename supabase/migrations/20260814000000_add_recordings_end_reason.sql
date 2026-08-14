-- Add end_reason to recordings so the archive can show/query WHY each
-- recording stopped (model went offline, stream session expired, max
-- duration/filesize rotation, paused/stopped, session boundary).
--
-- The Go code writes this column via PostgREST; it degrades gracefully when
-- the column is missing (PGRST204 fallback drops the field), so this migration
-- is purely additive and can be applied any time.
ALTER TABLE recordings ADD COLUMN IF NOT EXISTS end_reason TEXT;
