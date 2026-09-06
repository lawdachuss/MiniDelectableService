-- Refresh last_recorded_at for channels a node OWNS but is not actively
-- recording, so "how long since this channel last captured" is readable.
--
-- Why: mark_channel_recording only fires while status='recording' (the
-- assignment-sync mark phase marks only channels this node is ACTIVELY
-- capturing). A claimed (owned-but-idle) channel's last_recorded_at freezes
-- at the end of its last session, so every claimed row looks stale forever —
-- the fleet-wide "stale last_recorded_at >30m" wall that hides genuinely dead
-- owners behind noise.
--
-- Safety properties:
--   * SECURITY DEFINER: plain anon PATCH is RLS-blocked on
--     channel_assignments (only service_role writes via REST); nodes hold the
--     anon key, same as every other assignment RPC.
--   * Owner-filtered: the UPDATE matches assigned_node = p_node_id AND
--     status = 'claimed' server-side. A row released (user pause/removal) or
--     reassigned between the caller's read and this call matches zero rows —
--     the touch can never follow a channel to its new owner.
--   * Recording rows are NEVER touched: status='recording' rows are excluded,
--     so the recording pin (status + last_heartbeat maintained by
--     mark_channel_recording) is untouched and the protect_recording_assignment
--     / protect_live_owner_assignment / enforce_fair_share_claim triggers see
--     only a last_recorded_at/updated_at change (assigned_node and status are
--     IS NOT DISTINCT FROM their old values → free path).
--   * Idempotent, safe to re-run; grant matches the other assignment RPCs.
--
-- Apply in the Supabase SQL editor (or `psql "$SUPABASE_DB_URL" -f thisfile`).

CREATE OR REPLACE FUNCTION public.touch_channel_recordings(
    p_node_id text,
    p_channels jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $fn$
DECLARE
    v_pair  jsonb;
    v_rows  int;
    v_touched int := 0;
BEGIN
    IF p_node_id IS NULL OR p_node_id = '' OR p_channels IS NULL THEN
        RETURN 0;
    END IF;

    FOR v_pair IN SELECT * FROM jsonb_array_elements(p_channels)
    LOOP
        UPDATE channel_assignments ca
           SET last_recorded_at = now(),
               updated_at       = now()
         WHERE ca.username      = v_pair->>'username'
           AND ca.site          = COALESCE(NULLIF(v_pair->>'site', ''), 'chaturbate')
           AND ca.assigned_node = p_node_id
           AND ca.status        = 'claimed';
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_touched := v_touched + v_rows;
    END LOOP;

    RETURN v_touched;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.touch_channel_recordings(text, jsonb) TO anon, authenticated;

-- ── One-time backfill ────────────────────────────────────────────────────────
-- The fleet has been running without idle touches, so every claimed row's
-- last_recorded_at froze at its last session end. Seed each claimed row with
-- its row's updated_at (the last time the claim/assignment machinery touched
-- it) instead of now() — now() would fabricate a fake "just recorded" instant
-- for channels that have been idle for days. Recording rows already refresh
-- via mark_channel_recording and are left alone.
UPDATE channel_assignments
   SET last_recorded_at = updated_at
 WHERE status = 'claimed'
   AND (last_recorded_at IS NULL OR last_recorded_at < updated_at);

SELECT 'touch_channel_recordings ok' AS progress;
