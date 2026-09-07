-- ============================================================================
-- FAIR-SHARE CAP: never let one node claim all (or most) of the channels.
--
-- Problem: fairness was enforced only in the Go coordinator (balanceSite's
-- equalSplitCounts + the client-computed claim budget). Every claim RPC
-- trusted the caller's p_limit, and claim_specific_channel had NO cap at all,
-- so any buggy/old binary, autopilot, or raw UPDATE could pile the whole pool
-- onto one node. This failure class has bitten the fleet before: the
-- releaseBatchSize comment in database/supabase.go documents a node hoarding
-- ~900 channels while the rest sat idle, and node-9's current_load=992 was
-- the same failure shape.
--
-- Fix, defense in depth:
--   1. All three bulk claim RPCs compute the claim budget SERVER-SIDE as the
--      node's remaining fair share:
--          share  = ceil(total_pool / eligible_nodes)
--          budget = min(client_limit, share - my_load)
--      The client's p_limit stays an upper bound but can never sweep the pool.
--   2. claim_specific_channel redirects to the coldest eligible node when the
--      requested node is at share (and accepts onto the requester only when
--      the whole fleet is saturated), so a claim is never stranded unassigned.
--   3. A BEFORE UPDATE trigger on channel_assignments backstops EVERY writer
--      (RPC, service-role REST PATCH, external tools): a genuine pool claim
--      (assigned_node NULL -> non-NULL) into a node already at share is
--      re-pointed to the coldest eligible node.
--
-- Deliberate scoping (each is a correctness requirement, not a nicety):
--   - Moves/re-pins BETWEEN nodes (assigned_node non-NULL -> non-NULL) are
--     NEVER touched by the cap. That keeps the owner-side re-pin
--     (ReassertAssignmentNode: wrong-node -> owner while actively recording)
--     fully functional — capping it would re-fragment recordings.
--   - Releases (assigned_node -> NULL) are never touched.
--   - Rows whose status says 'recording' are pinned by the existing
--     protect_recording_assignment / protect_live_owner_assignment triggers;
--     those keep working unchanged (BEFORE ROW triggers are independent).
--
-- share semantics: pool = ALL channel_assignments rows (including rows still
-- held by a briefly-offline node), eligible = nodes with status='online' and a
-- heartbeat within 20 minutes (mirrors nodeReclaimGrace in the Go controller).
-- This makes the DB cap slightly LOOSER than the Go controller's exact split —
-- intentional: the DB layer is the anti-hoarding backstop, the leader-elected
-- controller remains the precise splitter.
--
-- Apply in the Supabase SQL editor. Idempotent: safe to re-run. Takes effect
-- immediately; no fleet restart needed (the cap lives entirely in the DB).
--
-- If Studio reports an error, the FIRST error in the output is the root cause
-- — later errors (e.g. 42883 "function ... does not exist") are just cascade
-- from the same-transaction rollback. Every block below is independent and
-- idempotent, so it can also be applied one block at a time.
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 1/5: index + share helper.
-- ═══════════════════════════════════════════════════════════════════════════

-- Per-node ownership counts are computed by every claim and by the trigger;
-- without this index each one sequential-scans the assignments table.
CREATE INDEX IF NOT EXISTS idx_channel_assignments_assigned_node
    ON public.channel_assignments (assigned_node);

-- The fair share for one node (NULL if the node is not eligible).
-- eligible = online + fresh heartbeat (<= 20 min, mirrors nodeReclaimGrace).
-- Degenerate fallback: if NO node is fresh (e.g. the whole fleet just
-- restarted), the caller's share is the whole pool so the fleet can still
-- make progress instead of deadlocking at zero.
CREATE OR REPLACE FUNCTION public.fair_share_target(p_node_id text)
RETURNS int
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $fn$
    WITH pool AS (
        SELECT count(*)::int AS total FROM public.channel_assignments
    ),
    eligible AS (
        SELECT node_id FROM public.nodes
         WHERE status = 'online'
           AND last_heartbeat > now() - interval '20 minutes'
    ),
    num AS (
        SELECT greatest(count(*), 1)::int AS n FROM eligible
    )
    SELECT CASE
             WHEN EXISTS (SELECT 1 FROM eligible WHERE node_id = p_node_id)
               THEN ceil((SELECT total FROM pool)::numeric / (SELECT n FROM num))::int
             WHEN NOT EXISTS (SELECT 1 FROM eligible)
               THEN (SELECT total FROM pool)
             ELSE NULL
           END;
$fn$;

SELECT 'block 1/5 ok: index + fair_share_target' AS progress;

-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 2/5: bulk claim RPCs with a server-side fair-share budget.
-- The claim itself uses the ctid queue idiom (subquery takes ORDER BY random()
-- LIMIT budget FOR UPDATE SKIP LOCKED, UPDATE matches those ctids) so two
-- nodes racing can never claim the same row.
-- ═══════════════════════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS public.claim_channels(text, int);
CREATE FUNCTION public.claim_channels(p_node_id text, p_limit int)
RETURNS SETOF public.channel_assignments
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $fn$
DECLARE
    v_share  int;
    v_load   int;
    v_budget int;
BEGIN
    v_share := fair_share_target(p_node_id);
    IF v_share IS NULL THEN
        -- Caller is not an eligible node (draining/offline): fall back to the
        -- caller's limit; the trigger backstop still applies to every row.
        v_share := coalesce(p_limit, 0);
    END IF;

    SELECT count(*) INTO v_load
      FROM channel_assignments
     WHERE assigned_node = p_node_id;

    v_budget := least(coalesce(p_limit, 0), v_share - v_load);
    IF v_budget <= 0 THEN
        RETURN; -- already at/over fair share — never sweep the pool
    END IF;

    RETURN QUERY
    UPDATE channel_assignments ca
       SET assigned_node = p_node_id,
           status        = 'claimed',
           updated_at    = now()
     WHERE ca.ctid IN (
             SELECT victim.ctid
               FROM channel_assignments victim
              WHERE victim.assigned_node IS NULL
                AND victim.status = 'unassigned'
              ORDER BY random()
              LIMIT v_budget
              FOR UPDATE SKIP LOCKED
           )
     RETURNING ca.*;
END;
$fn$;

DROP FUNCTION IF EXISTS public.claim_offline_channels(text, int);
CREATE FUNCTION public.claim_offline_channels(p_node_id text, p_limit int)
RETURNS SETOF public.channel_assignments
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $fn$
DECLARE
    v_share  int;
    v_load   int;
    v_budget int;
BEGIN
    v_share := fair_share_target(p_node_id);
    IF v_share IS NULL THEN
        v_share := coalesce(p_limit, 0);
    END IF;

    SELECT count(*) INTO v_load
      FROM channel_assignments
     WHERE assigned_node = p_node_id;

    v_budget := least(coalesce(p_limit, 0), v_share - v_load);
    IF v_budget <= 0 THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE channel_assignments ca
       SET assigned_node = p_node_id,
           status        = 'claimed',
           updated_at    = now()
     WHERE ca.ctid IN (
             SELECT victim.ctid
               FROM channel_assignments victim
              WHERE victim.assigned_node IS NULL
                AND victim.status = 'unassigned'
                AND victim.is_live = false
              ORDER BY random()
              LIMIT v_budget
              FOR UPDATE SKIP LOCKED
           )
     RETURNING ca.*;
END;
$fn$;

DROP FUNCTION IF EXISTS public.claim_live_channels(text, int);
CREATE FUNCTION public.claim_live_channels(p_node_id text, p_limit int)
RETURNS SETOF public.channel_assignments
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $fn$
DECLARE
    v_share  int;
    v_load   int;
    v_budget int;
BEGIN
    v_share := fair_share_target(p_node_id);
    IF v_share IS NULL THEN
        v_share := coalesce(p_limit, 0);
    END IF;

    SELECT count(*) INTO v_load
      FROM channel_assignments
     WHERE assigned_node = p_node_id;

    v_budget := least(coalesce(p_limit, 0), v_share - v_load);
    IF v_budget <= 0 THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE channel_assignments ca
       SET assigned_node = p_node_id,
           status        = 'claimed',
           updated_at    = now()
     WHERE ca.ctid IN (
             SELECT victim.ctid
               FROM channel_assignments victim
              WHERE victim.assigned_node IS NULL
                AND victim.status = 'unassigned'
                AND victim.is_live = true
              ORDER BY random()
              LIMIT v_budget
              FOR UPDATE SKIP LOCKED
           )
     RETURNING ca.*;
END;
$fn$;

SELECT 'block 2/5 ok: 3 bulk claim RPCs capped' AS progress;

-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 3/5: claim_specific_channel — cap + redirect. Used by balanceSite's
-- fill sweep and the web UI. When the requested node is at share, the claim
-- lands on the coldest eligible node instead; only a fully saturated fleet
-- accepts onto the requester — so a claim is never refused AND never left
-- unassigned. balanceSite only requests under-target nodes, so in the healthy
-- path the redirect can never fire.
-- ═══════════════════════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS public.claim_specific_channel(text, text, text);
CREATE FUNCTION public.claim_specific_channel(p_username text, p_site text, p_node_id text)
RETURNS SETOF public.channel_assignments
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $fn$
DECLARE
    v_share  int;
    v_load   int;
    v_target text;
BEGIN
    v_target := p_node_id;
    v_share  := fair_share_target(p_node_id);

    IF v_share IS NOT NULL THEN
        SELECT count(*) INTO v_load
          FROM channel_assignments
         WHERE assigned_node = p_node_id;

        IF v_load >= v_share THEN
            -- At share: re-point to the coldest eligible node under share.
            SELECT n.node_id INTO v_target
              FROM nodes n
             WHERE n.status = 'online'
               AND n.last_heartbeat > now() - interval '20 minutes'
               AND (SELECT count(*) FROM channel_assignments ca
                     WHERE ca.assigned_node = n.node_id) < v_share
             ORDER BY (SELECT count(*) FROM channel_assignments ca
                        WHERE ca.assigned_node = n.node_id) ASC,
                      n.node_id ASC
             LIMIT 1;

            IF v_target IS NULL THEN
                v_target := p_node_id; -- fleet saturated: accept onto the requester
            END IF;
        END IF;
    END IF;

    RETURN QUERY
    UPDATE channel_assignments
       SET assigned_node = v_target,
           status        = 'claimed',
           updated_at    = now()
     WHERE username      = p_username
       AND site          = p_site
       AND assigned_node IS NULL
     RETURNING *;
END;
$fn$;

SELECT 'block 3/5 ok: claim_specific_channel capped' AS progress;

-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 4/5: trigger backstop — applies to EVERY writer on
-- channel_assignments. Only genuine pool claims (assigned_node NULL ->
-- non-NULL) are capped. Node-to-node moves (rebalance shed, owner re-pin) and
-- releases pass untouched, so the recording pins and ReassertAssignmentNode
-- keep working exactly as before. A claim into an at-share node is re-pointed
-- to the coldest eligible node; a saturated fleet (coldest also at share)
-- accepts the claim as-is so nothing is ever stranded unassigned.
--
-- Concurrent RPC claims can each pass their own budget check before either
-- locks rows; this trigger closes that race — the second claimant's overage
-- is re-pointed here, under its row lock.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.enforce_fair_share_claim() RETURNS trigger
LANGUAGE plpgsql
SET search_path = public
AS $fn$
DECLARE
    v_share        int;
    v_load         int;
    v_coldest      text;
    v_coldest_load int;
BEGIN
    -- Node unchanged: heartbeat/status/liveness updates take the free path.
    IF NEW.assigned_node IS NOT DISTINCT FROM OLD.assigned_node THEN
        RETURN NEW;
    END IF;

    -- Node-to-node move or release: never capped. The recording-pin triggers
    -- and the Go-side checks govern those; capping them would break the owner
    -- re-pin of an actively recording channel.
    IF OLD.assigned_node IS NOT NULL THEN
        RETURN NEW;
    END IF;

    -- OLD IS NULL and NEW IS NOT NULL: a genuine pool claim.
    v_share := fair_share_target(NEW.assigned_node);
    IF v_share IS NULL THEN
        RETURN NEW; -- writer outside the eligible set; nothing sane to compare
    END IF;

    SELECT count(*) INTO v_load
      FROM channel_assignments
     WHERE assigned_node = NEW.assigned_node;
    IF v_load < v_share THEN
        RETURN NEW; -- under share: allowed
    END IF;

    SELECT n.node_id,
           (SELECT count(*) FROM channel_assignments ca
             WHERE ca.assigned_node = n.node_id)
      INTO v_coldest, v_coldest_load
      FROM nodes n
     WHERE n.status = 'online'
       AND n.last_heartbeat > now() - interval '20 minutes'
     ORDER BY 2 ASC, n.node_id ASC
     LIMIT 1;

    IF v_coldest IS NOT NULL AND v_coldest <> NEW.assigned_node AND v_coldest_load < v_share THEN
        NEW.assigned_node := v_coldest;
    END IF;
    -- Saturated fleet (coldest is also at/over share): allow the claim as-is.
    RETURN NEW;
END;
$fn$;

DROP TRIGGER IF EXISTS trg_enforce_fair_share_claim ON public.channel_assignments;
CREATE TRIGGER trg_enforce_fair_share_claim
    BEFORE UPDATE ON public.channel_assignments
    FOR EACH ROW EXECUTE FUNCTION public.enforce_fair_share_claim();

SELECT 'block 4/5 ok: backstop trigger attached' AS progress;

-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 5/5: grants + PostgREST schema-cache reload + verification.
-- The final result set is the fairness picture: if any node's owned exceeds
-- its share, the cap will refuse/redirect further claims onto it from now on.
-- ═══════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION public.fair_share_target(text) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.claim_channels(text, int) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.claim_offline_channels(text, int) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.claim_live_channels(text, int) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.claim_specific_channel(text, text, text) TO anon, authenticated;

SELECT pg_notify('pgrst', 'reload schema cache');

SELECT 'block 5/5 ok: functions granted=' || count(*)::text AS progress
  FROM pg_proc
 WHERE pronamespace = 'public'::regnamespace
   AND proname IN ('fair_share_target','claim_channels',
                   'claim_offline_channels','claim_live_channels',
                   'claim_specific_channel','enforce_fair_share_claim');

SELECT n.node_id,
       public.fair_share_target(n.node_id) AS share,
       (SELECT count(*) FROM public.channel_assignments ca
         WHERE ca.assigned_node = n.node_id) AS owned
  FROM public.nodes n
 WHERE n.status = 'online'
 ORDER BY owned DESC;
