-- ============================================================================
-- FIX: channel assignment is stuck (hundreds of channels "unassigned") because
-- the claim_specific_channel RPC matches 0 rows. Verified from the API: calling
-- it for an unassigned channel (and even for a reassignment) returns an empty
-- set, so no channel can ever be claimed. Root cause is almost certainly
-- `assigned_node = NULL` in its WHERE clause (never true) instead of IS NULL.
--
-- Apply in the Supabase SQL editor (or `psql "$SUPABASE_DB_URL" -f thisfile`).
-- ============================================================================

-- 1) Repair claim_specific_channel so it claims rows whose assigned_node IS NULL.
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

-- 2) release_controller_lease: properly expire the leader lease on graceful
-- shutdown. The Go client calls this RPC because the anon API role is RLS-
-- blocked from PATCHing controller_lease directly (a raw PATCH silently affects
-- 0 rows, leaving the lease valid and blocking every other node forever).
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

-- 3) Clear the currently-stuck lease row (held by a dead leader with a future
-- expires_at) so a healthy node can acquire it and run the assignment sweep.
UPDATE controller_lease SET expires_at = now() - interval '1 hour' WHERE id = 1;

-- 4) Sanity: after applying, claim one unassigned channel to confirm it works:
--    select count(*) from channel_assignments where assigned_node is null and status='unassigned';
