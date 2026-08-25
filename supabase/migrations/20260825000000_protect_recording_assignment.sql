-- Make "never reassign a channel that is actively recording" authoritative at
-- the data layer. The coordinator's reassign_channel RPC already refuses to move
-- rows with status='recording', but an external autopilot that issues a raw
-- UPDATE can bypass that guard and fragment an in-progress recording (the
-- "channel stopped (handoff)" 20-30 min fragments). This trigger reverts any
-- assigned_node change while status='recording' back to the original node.
--
-- Legitimate reassignments happen AFTER the recording ends, when status is
-- changed away from 'recording' (reassign_channel / release / claim all set
-- status='claimed'|'unassigned'), so they are never affected. The Go side also
-- defends this (RemoveChannelForReassignment refuses, plus ReassertAssignmentNode
-- re-pins), so this is defense-in-depth and is purely additive.
CREATE OR REPLACE FUNCTION protect_recording_assignment() RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'recording' AND OLD.assigned_node IS DISTINCT FROM NEW.assigned_node THEN
        NEW.assigned_node := OLD.assigned_node;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_recording_assignment ON channel_assignments;
CREATE TRIGGER trg_protect_recording_assignment
    BEFORE UPDATE ON channel_assignments
    FOR EACH ROW EXECUTE FUNCTION protect_recording_assignment();
