-- ============================================================================
-- FIX: stale schema cache + missing SECURITY DEFINER on reassign_channel.
--
-- Root causes:
-- 1. claim_controller_lease returns NULL (stale PostgREST cache after recent
--    DROP/CREATE of other RPCs) → no leader elected → balanceSite never runs.
-- 2. reassign_channel lacks SECURITY DEFINER → RLS-blocked UPDATE → 204 no-op
--    → channels can't move between nodes.
--
-- This file recreates ALL RPCs idempotently (with SECURITY DEFINER) and
-- reloads the schema cache at the end. Run ONCE in the Supabase SQL editor.
-- ============================================================================

-- ── Drop old versions (idempotent) ─────────────────────────────────────────
DROP FUNCTION IF EXISTS claim_controller_lease(text, int);
DROP FUNCTION IF EXISTS release_controller_lease(text);
DROP FUNCTION IF EXISTS claim_specific_channel(text, text, text);
DROP FUNCTION IF EXISTS reassign_channel(text, text, text, text);
DROP FUNCTION IF EXISTS mark_channel_recording(text, text);
DROP FUNCTION IF EXISTS reset_channel_status(text, text, text);
DROP FUNCTION IF EXISTS reclaim_node_channels(text);

-- ── 1) Leader lease ────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION claim_controller_lease(p_node_id text, p_ttl int)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  updated int;
BEGIN
  UPDATE controller_lease
     SET node_id     = p_node_id,
         acquired_at = now(),
         expires_at  = now() + (p_ttl || ' seconds')::interval
   WHERE id = 1
     AND (expires_at < now() OR node_id = p_node_id);
  GET DIAGNOSTICS updated = ROW_COUNT;

  IF updated = 0 THEN
    INSERT INTO controller_lease (id, node_id, expires_at)
    VALUES (1, p_node_id, now() + (p_ttl || ' seconds')::interval)
    ON CONFLICT (id) DO NOTHING;
    SELECT count(*) INTO updated
      FROM controller_lease
     WHERE id = 1 AND node_id = p_node_id AND expires_at > now();
    RETURN updated > 0;
  END IF;
  RETURN true;
END;
$$;
GRANT EXECUTE ON FUNCTION claim_controller_lease(text, int) TO anon;
GRANT EXECUTE ON FUNCTION claim_controller_lease(text, int) TO authenticated;

-- ── 2) Release lease on graceful shutdown ───────────────────────────────────
CREATE OR REPLACE FUNCTION release_controller_lease(p_node_id text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE controller_lease
     SET expires_at = now() - interval '1 second'
   WHERE node_id = p_node_id;
END;
$$;
GRANT EXECUTE ON FUNCTION release_controller_lease(text) TO anon;
GRANT EXECUTE ON FUNCTION release_controller_lease(text) TO authenticated;

-- ── 3) Claim a specific unassigned channel ──────────────────────────────────
CREATE OR REPLACE FUNCTION claim_specific_channel(p_username text, p_site text, p_node_id text)
RETURNS SETOF channel_assignments
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  UPDATE channel_assignments
     SET assigned_node = p_node_id,
         status        = 'claimed'
   WHERE username      = p_username
     AND site          = p_site
     AND assigned_node IS NULL
   RETURNING *;
END;
$$;
GRANT EXECUTE ON FUNCTION claim_specific_channel(text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION claim_specific_channel(text, text, text) TO authenticated;

-- ── 4) Reassign an already-assigned channel (must have SECURITY DEFINER!) ───
CREATE OR REPLACE FUNCTION reassign_channel(p_username text, p_site text, p_from_node text, p_to_node text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE channel_assignments
     SET assigned_node = p_to_node,
         status        = 'claimed',
         assigned_at   = now(),
         updated_at    = now()
   WHERE username      = p_username
     AND site          = p_site
     AND assigned_node = p_from_node
     AND status        <> 'recording';
END;
$$;
GRANT EXECUTE ON FUNCTION reassign_channel(text, text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION reassign_channel(text, text, text, text) TO authenticated;

-- ── 5) Mark a channel recording ─────────────────────────────────────────────
CREATE OR REPLACE FUNCTION mark_channel_recording(p_username text, p_site text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE channel_assignments
     SET status         = 'recording',
         last_recorded_at = now(),
         last_heartbeat = now(),
         updated_at     = now()
   WHERE username = p_username
     AND site     = p_site;
END;
$$;
GRANT EXECUTE ON FUNCTION mark_channel_recording(text, text) TO anon;
GRANT EXECUTE ON FUNCTION mark_channel_recording(text, text) TO authenticated;

-- ── 6) Reset channel status ─────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION reset_channel_status(p_username text, p_site text, p_status text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE channel_assignments
     SET status     = p_status,
         updated_at = now()
   WHERE username = p_username
     AND site     = p_site;
END;
$$;
GRANT EXECUTE ON FUNCTION reset_channel_status(text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION reset_channel_status(text, text, text) TO authenticated;

-- ── 7) Reclaim all channels from a dead node ────────────────────────────────
CREATE OR REPLACE FUNCTION reclaim_node_channels(p_node_id text)
RETURNS TABLE(reclaimed int)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  n int;
BEGIN
  UPDATE channel_assignments
     SET assigned_node = NULL,
         status        = 'unassigned',
         updated_at    = now()
   WHERE assigned_node = p_node_id
     AND status       <> 'recording';
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN QUERY SELECT n;
END;
$$;
GRANT EXECUTE ON FUNCTION reclaim_node_channels(text) TO anon;
GRANT EXECUTE ON FUNCTION reclaim_node_channels(text) TO authenticated;

-- ── 8) Expire stale leases ──────────────────────────────────────────────────
UPDATE controller_lease SET expires_at = now() - interval '1 hour' WHERE id = 1;

-- ── 9) Reload PostgREST schema cache (CRITICAL) ─────────────────────────────
-- This makes PostgREST pick up the fresh function definitions above.
SELECT pg_notify('pgrst', 'reload schema cache');
