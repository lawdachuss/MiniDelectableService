-- ============================================================================
-- Assignment writes that MUST bypass RLS.
--
-- The channel_assignments table only grants the anon role SELECT (and
-- service_role ALL). Plain anon PATCH/POST writes (MarkChannelRecording,
-- SetAssignmentStatus, ReleaseNodeChannels) are therefore RLS-blocked and
-- silently affect 0 rows. The claim/reassign paths already use SECURITY
-- DEFINER RPCs for this reason; these three RPCs bring the remaining
-- assignment writes in line so that:
--   * recordings are actually marked status='recording' (so reassign_channel
--     refuses to move them and in-progress recordings are never interrupted),
--   * finished recordings are reset back to 'claimed' so they can be rebalanced,
--   * dead-node channels are reclaimed (freed) so they get redistributed.
--
-- Apply in the Supabase SQL editor (or `psql "$SUPABASE_DB_URL" -f thisfile`).
-- ============================================================================

-- 1) Mark a channel as actively recording on its node. The per-node
--    assignment-sync loop calls this so the controller's rebalancer (which
--    refuses to move status='recording' rows) does not yank an in-progress
--    recording. SECURITY DEFINER so the anon API role can write it.
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

-- 2) Reset a channel's status (e.g. recording -> claimed once the stream is
--    confirmed offline). General-purpose single-column status setter,
--    SECURITY DEFINER so the anon API role can write it.
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

-- 3) Reclaim (free) every channel owned by a dead node so the controller can
--    redistribute them. Refuses to touch status='recording' defensively (a dead
--    node cannot be recording, but never interrupt a row mid-write). Returns the
--    number of channels reclaimed. SECURITY DEFINER so the anon API role can
--    write it. Returns a TABLE so PostgREST emits predictable
--    [{ "reclaimed": N }] JSON.
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
