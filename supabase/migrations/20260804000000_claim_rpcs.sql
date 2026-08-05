-- Migration: Atomic claim / reassign RPC functions (SKIP LOCKED)
-- Ported from the node-3 distribution of chaturbate-dvr. The distributed
-- coordinator calls these via PostgREST RPC to claim channels atomically —
-- SELECT ... FOR UPDATE SKIP LOCKED guarantees two nodes can never claim the
-- same channel concurrently (no GET-then-PATCH TOCTOU window).
-- Safe to re-run (CREATE OR REPLACE).

CREATE OR REPLACE FUNCTION claim_channels(p_node_id TEXT, p_limit INT)
RETURNS SETOF channel_assignments
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RETURN QUERY
    WITH picked AS (
        SELECT ca.username, ca.site
        FROM channel_assignments ca
        WHERE ca.assigned_node IS NULL
        ORDER BY RANDOM()
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE channel_assignments ca
    SET
        assigned_node = p_node_id,
        status = 'claimed',
        assigned_at = NOW(),
        updated_at = NOW()
    FROM picked
    WHERE ca.username = picked.username
      AND ca.site = picked.site
    RETURNING ca.*;
END;
$$;

CREATE OR REPLACE FUNCTION claim_specific_channel(p_username TEXT, p_site TEXT, p_node_id TEXT)
RETURNS SETOF channel_assignments
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    picked channel_assignments%ROWTYPE;
BEGIN
    SELECT ca.* INTO picked
    FROM channel_assignments ca
    WHERE ca.username = p_username
      AND ca.site = p_site
      AND ca.assigned_node IS NULL
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF FOUND THEN
        UPDATE channel_assignments ca
        SET
            assigned_node = p_node_id,
            status = 'claimed',
            assigned_at = NOW(),
            updated_at = NOW()
        WHERE ca.username = p_username
          AND ca.site = p_site
        RETURNING ca.* INTO picked;

        RETURN NEXT picked;
    END IF;

    RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION reassign_channel(p_username TEXT, p_site TEXT, p_from_node TEXT, p_to_node TEXT)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    UPDATE channel_assignments ca
    SET
        assigned_node = p_to_node,
        status = 'claimed',
        assigned_at = NOW(),
        updated_at = NOW()
    WHERE ca.username = p_username
      AND ca.site = p_site
      AND ca.assigned_node = p_from_node
      AND (
        ca.status = 'claimed'
        OR ca.status = 'offline'
        OR ca.status = 'unassigned'
      );
END;
$$;

GRANT EXECUTE ON FUNCTION claim_channels(TEXT, INT) TO anon;
GRANT EXECUTE ON FUNCTION claim_specific_channel(TEXT, TEXT, TEXT) TO anon;
GRANT EXECUTE ON FUNCTION reassign_channel(TEXT, TEXT, TEXT, TEXT) TO anon;
