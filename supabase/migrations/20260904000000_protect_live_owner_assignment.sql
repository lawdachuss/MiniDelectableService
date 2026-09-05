-- "A row that is being recorded by a node that is STILL ALIVE can never be
-- handed to another node", enforced at the data layer for EVERY writer
-- (controller RPCs, raw REST UPDATEs, external tools).
--
-- The existing protect_recording_assignment trigger only reverts assigned_node
-- changes while status='recording'. This trigger closes what that trigger
-- cannot see: the controller's recording-lease reset first flips
-- status recording→claimed (no assigned_node change, so the old trigger stays
-- silent) and a later UPDATE then moves the row — while the original owner is
-- still alive and, if its mark path merely lagged, still capturing the stream.
-- A raw/rogue UPDATE can do the same in two steps.
--
-- The pin is the OWNER NODE'S LIVENESS combined with the row's recording
-- STATUS, NOT the recording heartbeat. The heartbeat refreshes every ~30s while
-- the owner captures, but a stalled assignment-sync makes it go stale (> 2 min)
-- while the owner is still alive and the row still says status='recording' —
-- the exact failure that produced duplicate captures on the live fleet. The
-- status survives a sync stall; the heartbeat does not, which is why the status
-- is the pin.
--
-- Deliberately NO heartbeat clause: a claimed row whose last mark was < 2 min
-- ago is a channel that just FINISHED recording (the owner-side cleanup flips
-- status recording→claimed without touching last_heartbeat), and pinning those
-- would freeze rebalancing of exactly the channels the fleet needs to spread.
-- The case the heartbeat clause was meant to catch — a lease-reset row whose
-- owner is still capturing — has a STALE heartbeat by definition (> 2 min at
-- reset time) and is covered by the Go-side exclusion (the controller never
-- resets markers on protected owners) plus the re-pin, not by this trigger.
--
-- Claims (assigned_node IS NULL → non-NULL) and releases back to the pool
-- (assigned_node → NULL, e.g. user pause or removal) are never blocked:
-- neither can hand a live capture to a second node. Idle claimed rows on a
-- live owner remain movable so the fleet can still rebalance non-recording
-- work. Rows whose owner is gone/offline stay fully movable, so cleanup,
-- rebalancing and dead-runner reclaim are never blocked.
--
-- NOTE on the node-side re-pin: the sync loop re-pins a channel it is
-- actively recording when the row was moved away. The only way the
-- controller moves a recording off its owner is the lease reset
-- (recording→claimed), so by the time the re-pin runs the row is 'claimed'
-- with a stale heartbeat and passes this guard; the guard blocks moves that
-- happen while the row still says 'recording', which is exactly the
-- actively-captured case nothing should move.
--
-- Apply in the Supabase SQL editor (or `psql "$SUPABASE_DB_URL" -f thisfile`).
-- Idempotent: drops the trigger first and uses CREATE OR REPLACE.

CREATE OR REPLACE FUNCTION protect_live_owner_assignment() RETURNS trigger AS $$
DECLARE
    owner_alive boolean;
BEGIN
    -- Claims from 'unassigned' have no owner to protect; releases to NULL
    -- remove the channel from every node (pause/removal) and can never create
    -- a duplicate capture.
    IF OLD.assigned_node IS NULL OR NEW.assigned_node IS NULL THEN
        RETURN NEW;
    END IF;
    -- Nothing relevant is changing.
    IF OLD.assigned_node IS NOT DISTINCT FROM NEW.assigned_node THEN
        RETURN NEW;
    END IF;

    SELECT (status IN ('online', 'draining') AND last_heartbeat > now() - interval '20 minutes')
      INTO owner_alive
      FROM nodes
     WHERE node_id = OLD.assigned_node;

    IF owner_alive IS NOT TRUE THEN
        RETURN NEW; -- owner is gone/offline — its rows may be cleaned & redistributed
    END IF;

    -- Owner is alive. The row is pinned while it could still be captured: it
    -- is still marked 'recording' (refreshed every ~30s by the owner while it
    -- captures; a stalled sync ages the heartbeat but never this status). A
    -- claimed row on a live owner — finished recording, or lease-reset before
    -- a re-pin — stays movable so the fleet can rebalance.
    IF OLD.status = 'recording' THEN
        NEW.assigned_node := OLD.assigned_node;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_live_owner_assignment ON channel_assignments;
CREATE TRIGGER trg_protect_live_owner_assignment
    BEFORE UPDATE ON channel_assignments
    FOR EACH ROW EXECUTE FUNCTION protect_live_owner_assignment();