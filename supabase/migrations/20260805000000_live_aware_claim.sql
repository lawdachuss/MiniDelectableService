-- Migration: live-aware channel claiming (offline / live split)
-- Adds claim_offline_channels and claim_live_channels, atomic (SKIP LOCKED)
-- RPCs mirroring claim_channels but filtered by is_live. The claim cycle uses
-- them so a node's offline budget can never sweep live channels wholesale:
-- offline channels fill capacity first, and live channels are claimed only up
-- to a per-node live fair share (ceil(total_live / alive_nodes)), which spreads
-- the channels that actually get recorded across all nodes.
-- Safe to re-run (CREATE OR REPLACE).

CREATE OR REPLACE FUNCTION claim_offline_channels(p_node_id TEXT, p_limit INT)
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
          AND ca.is_live = FALSE
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

CREATE OR REPLACE FUNCTION claim_live_channels(p_node_id TEXT, p_limit INT)
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
          AND ca.is_live = TRUE
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

GRANT EXECUTE ON FUNCTION claim_offline_channels(TEXT, INT) TO anon;
GRANT EXECUTE ON FUNCTION claim_live_channels(TEXT, INT) TO anon;
