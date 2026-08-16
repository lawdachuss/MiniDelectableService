-- Add instance_id to channel_logs so each log row can be attributed to the
-- node that produced it. The Go log forwarder stamps it with the node ID
-- (server.NodeID()); recordings/upload/tunnel rows all store instance_id as
-- "default", which made synchronized bursts (node-level HLS 404s, CF blocks,
-- handoffs) impossible to attribute to a specific node.
--
-- The Go writer degrades gracefully when the column is missing (PGRST204
-- fallback drops the field), so this migration is purely additive and can be
-- applied any time.
ALTER TABLE channel_logs ADD COLUMN IF NOT EXISTS instance_id TEXT;
