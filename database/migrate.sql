-- ============================================================================
-- Chaturbate DVR - Complete Supabase Schema (single file, run once)
-- ============================================================================
-- WARNING: This script DELETES ALL EXISTING DATA before creating tables.
-- It drops all tables, views, and cached data (with CASCADE) to ensure a
-- clean, reproducible schema on every run.
--
-- Safe to re-run: drops existing objects first, then recreates all tables.
-- Uses IF NOT EXISTS / DO $$ checks for idempotent ALTER operations.
--
-- Tables verified against Go structs in database/supabase.go and server/db.go.
-- Removed unused tables: tunnel_sessions, recording_sessions (no Go code).
-- Removed unused column: preview_images.github_path (not in Go struct).
-- Added pipeline_states table (existed only as comment in Go code).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS public;
SET search_path TO public;

-- ============================================================================
-- DELETE ALL EXISTING DATA — drops tables, views, sequences, and caches
-- ============================================================================

-- Drop views first (they reference tables).
DROP VIEW IF EXISTS public.recordings_with_links CASCADE;
DROP VIEW IF EXISTS public.channel_statistics CASCADE;
DROP VIEW IF EXISTS public.recent_activity CASCADE;

-- Drop tables in reverse dependency order with CASCADE to clean up FKs,
-- indexes, triggers, and RLS policies.
DROP TABLE IF EXISTS public.channel_assignments CASCADE;
DROP TABLE IF EXISTS public.nodes CASCADE;
DROP TABLE IF EXISTS public.upload_links CASCADE;
DROP TABLE IF EXISTS public.preview_images CASCADE;
DROP TABLE IF EXISTS public.channel_logs CASCADE;
DROP TABLE IF EXISTS public.recordings CASCADE;
DROP TABLE IF EXISTS public.pipeline_states CASCADE;
DROP TABLE IF EXISTS public.upload_journal CASCADE;
DROP TABLE IF EXISTS public.disk_usage CASCADE;
DROP TABLE IF EXISTS public.tunnels CASCADE;
DROP TABLE IF EXISTS public.app_settings CASCADE;
DROP TABLE IF EXISTS public.channels CASCADE;

-- Discard any cached plan / materialized state (no-op if no prepared stmts).
DISCARD PLANS;

-- ============================================================================
-- 1. CHANNELS
-- ============================================================================
CREATE TABLE IF NOT EXISTS channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(255) UNIQUE NOT NULL,
    is_paused BOOLEAN DEFAULT FALSE,
    framerate INTEGER DEFAULT 30,
    resolution INTEGER DEFAULT 1080,
    pattern TEXT DEFAULT 'videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}',
    max_duration INTEGER DEFAULT 0,
    max_filesize INTEGER DEFAULT 0,
    compress BOOLEAN DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Channel profile columns (kept in sync with migrate-channel-profiles.sql
    -- and supabase/migrations/*channel_profiles.sql).  These are written by
    -- database.Client.SaveChannelProfile and read by the archive site.
    follower_count      INTEGER        NOT NULL DEFAULT 0,
    location            TEXT           NOT NULL DEFAULT '',
    real_name           TEXT           NOT NULL DEFAULT '',
    body_decorations    TEXT           NOT NULL DEFAULT '',
    smoke_drink         TEXT           NOT NULL DEFAULT '',
    body_type           TEXT           NOT NULL DEFAULT '',
    display_birthday    TEXT           NOT NULL DEFAULT '',
    display_age         INTEGER        NOT NULL DEFAULT 0,
    about_me            TEXT           NOT NULL DEFAULT '',
    wish_list           TEXT           NOT NULL DEFAULT '',
    fan_club_cost       INTEGER        NOT NULL DEFAULT 0,
    sex                 TEXT           NOT NULL DEFAULT '',
    subgender           TEXT           NOT NULL DEFAULT '',
    interested_in       JSONB          NOT NULL DEFAULT '[]',
    photo_sets          JSONB          NOT NULL DEFAULT '[]',
    social_medias       JSONB          NOT NULL DEFAULT '[]',
    last_broadcast      TEXT           NOT NULL DEFAULT '',
    room_status         TEXT           NOT NULL DEFAULT '',
    avatar_url          TEXT           NOT NULL DEFAULT '',
    profile_scraped_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_channels_username ON channels(username);
CREATE INDEX IF NOT EXISTS idx_channels_created_at ON channels(created_at);
CREATE INDEX IF NOT EXISTS idx_channels_profile_scraped_at ON channels(profile_scraped_at DESC);

-- ============================================================================
-- 2. RECORDINGS
-- ============================================================================
CREATE TABLE IF NOT EXISTS recordings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id UUID REFERENCES channels(id) ON DELETE CASCADE,
    username VARCHAR(255) NOT NULL,
    filename VARCHAR(500) UNIQUE NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    room_title TEXT,
    tags TEXT[],
    viewers INTEGER DEFAULT 0,
    resolution VARCHAR(50),
    framerate INTEGER,
    filesize BIGINT DEFAULT 0,
    duration DOUBLE PRECISION DEFAULT 0,
    gender VARCHAR(50),
    thumbnail_url TEXT,
    sprite_url TEXT,
    embed_url TEXT,
    preview_url TEXT,
    instance_id TEXT NOT NULL DEFAULT 'default',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recordings_username ON recordings(username);
CREATE INDEX IF NOT EXISTS idx_recordings_filename ON recordings(filename);
CREATE INDEX IF NOT EXISTS idx_recordings_timestamp ON recordings(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_recordings_channel_id ON recordings(channel_id);
CREATE INDEX IF NOT EXISTS idx_recordings_gender ON recordings(gender);
CREATE INDEX IF NOT EXISTS idx_recordings_instance ON recordings(instance_id);

-- ============================================================================
-- 3. UPLOAD_LINKS
-- ============================================================================
CREATE TABLE IF NOT EXISTS upload_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recording_id UUID REFERENCES recordings(id) ON DELETE CASCADE,
    host VARCHAR(100) NOT NULL,
    url TEXT NOT NULL,
    instance_id TEXT NOT NULL DEFAULT 'default',
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_upload_links_recording_id ON upload_links(recording_id);
CREATE INDEX IF NOT EXISTS idx_upload_links_host ON upload_links(host);
CREATE INDEX IF NOT EXISTS idx_upload_links_instance ON upload_links(instance_id);

-- Safe migration: remove duplicate rows (keep newest) then add unique constraint.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'upload_links_recording_host_unique'
    ) THEN
        DELETE FROM upload_links a
        USING upload_links b
        WHERE a.id < b.id
          AND a.recording_id IS NOT DISTINCT FROM b.recording_id
          AND a.host IS NOT DISTINCT FROM b.host;

        ALTER TABLE upload_links ADD CONSTRAINT upload_links_recording_host_unique UNIQUE (recording_id, host);
    END IF;
END $$;

-- ============================================================================
-- 4. APP_SETTINGS
-- ============================================================================
CREATE TABLE IF NOT EXISTS app_settings (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- 5. TUNNELS
-- ============================================================================
CREATE TABLE IF NOT EXISTS tunnels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url TEXT NOT NULL,
    run_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    instance_id TEXT NOT NULL DEFAULT 'default',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_tunnels_active ON tunnels(is_active, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tunnels_run_id ON tunnels(run_id);
CREATE INDEX IF NOT EXISTS idx_tunnels_instance ON tunnels(instance_id);

-- ============================================================================
-- 6. CHANNEL_LOGS
-- ============================================================================
CREATE TABLE IF NOT EXISTS channel_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id UUID REFERENCES channels(id) ON DELETE CASCADE,
    username VARCHAR(255) NOT NULL,
    log_level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_channel_logs_channel_id ON channel_logs(channel_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_channel_logs_username ON channel_logs(username, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_channel_logs_created_at ON channel_logs(created_at DESC);

-- ============================================================================
-- 7. PREVIEW_IMAGES
-- ============================================================================
CREATE TABLE IF NOT EXISTS preview_images (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recording_id UUID REFERENCES recordings(id) ON DELETE CASCADE,
    filename VARCHAR(500) NOT NULL,
    thumbnail_url TEXT,
    sprite_url TEXT,
    preview_url TEXT,
    instance_id TEXT NOT NULL DEFAULT 'default',
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(filename)
);

CREATE INDEX IF NOT EXISTS idx_preview_images_recording_id ON preview_images(recording_id);
CREATE INDEX IF NOT EXISTS idx_preview_images_filename ON preview_images(filename);
CREATE INDEX IF NOT EXISTS idx_preview_images_instance ON preview_images(instance_id);

-- ============================================================================
-- 8. UPLOAD_JOURNAL
-- ============================================================================
CREATE TABLE IF NOT EXISTS upload_journal (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    file_hash TEXT NOT NULL,
    filename TEXT NOT NULL,
    host TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    error_msg TEXT,
    file_size BIGINT,
    instance_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(file_hash, host)
);

CREATE INDEX IF NOT EXISTS idx_upload_journal_hash ON upload_journal(file_hash);
CREATE INDEX IF NOT EXISTS idx_upload_journal_status ON upload_journal(status);

-- ============================================================================
-- 9. PIPELINE_STATES
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_states (
    file_hash TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    username TEXT NOT NULL DEFAULT '',
    file_size BIGINT DEFAULT 0,
    current_stage TEXT NOT NULL DEFAULT 'thumbnail',
    failed BOOLEAN DEFAULT FALSE,
    last_error TEXT DEFAULT '',
    thumb_url TEXT DEFAULT '',
    sprite_url TEXT DEFAULT '',
    preview_url TEXT DEFAULT '',
    embed_url TEXT DEFAULT '',
    links TEXT DEFAULT '{}',
    retries INTEGER NOT NULL DEFAULT 0,
    node_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- 10. DISK_USAGE
-- ============================================================================
CREATE TABLE IF NOT EXISTS disk_usage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    total_bytes BIGINT NOT NULL,
    used_bytes BIGINT NOT NULL,
    free_bytes BIGINT NOT NULL,
    percent_used INTEGER NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_disk_usage_recorded_at ON disk_usage(recorded_at DESC);

-- ============================================================================
-- 11. NODES
-- ============================================================================
CREATE TABLE IF NOT EXISTS nodes (
    node_id          TEXT PRIMARY KEY,
    hostname         TEXT NOT NULL DEFAULT '',
    instance_label   TEXT NOT NULL DEFAULT '',
    software_version TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'offline'
                     CHECK (status IN ('online','offline','draining')),
    current_load     INT NOT NULL DEFAULT 0,
    last_heartbeat   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    session_deadline TIMESTAMPTZ,  -- written by Register() via computeSessionDeadline();
                                   -- the deadline-migration loop moves channels off
                                   -- a node before its GitHub 6h runner kill fires
    web_url          TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
CREATE INDEX IF NOT EXISTS idx_nodes_heartbeat ON nodes(last_heartbeat);

-- ============================================================================
-- 12. CHANNEL ASSIGNMENTS
-- ============================================================================
CREATE TABLE IF NOT EXISTS channel_assignments (
    username        TEXT NOT NULL,
    site            TEXT NOT NULL DEFAULT 'chaturbate'
                    CHECK (site IN ('chaturbate','stripchat')),
    assigned_node   TEXT REFERENCES nodes(node_id),
    status          TEXT NOT NULL DEFAULT 'unassigned'
                    CHECK (status IN ('unassigned','claimed','recording','paused','error')),
    is_live         BOOLEAN NOT NULL DEFAULT FALSE,
    live_checked_at TIMESTAMPTZ,
    assigned_at     TIMESTAMPTZ,
    last_heartbeat  TIMESTAMPTZ,
    last_recorded_at TIMESTAMPTZ,  -- bumped by MarkChannelRecording (liveness loop)
    framerate       INT NOT NULL DEFAULT 60,
    resolution      INT NOT NULL DEFAULT 2160,
    pattern         TEXT NOT NULL DEFAULT '',
    max_duration    INT NOT NULL DEFAULT 60,
    max_filesize    INT NOT NULL DEFAULT 0,
    compress        BOOLEAN NOT NULL DEFAULT FALSE,
    min_duration_before_upload INT NOT NULL DEFAULT 1200,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, site)
);

CREATE INDEX IF NOT EXISTS idx_ca_assigned_node ON channel_assignments(assigned_node);
CREATE INDEX IF NOT EXISTS idx_ca_status ON channel_assignments(status);
CREATE INDEX IF NOT EXISTS idx_ca_islive ON channel_assignments(is_live);
CREATE INDEX IF NOT EXISTS idx_ca_heartbeat ON channel_assignments(last_heartbeat);

-- ============================================================================
-- FUNCTIONS AND TRIGGERS
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_channels_updated_at BEFORE UPDATE ON channels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_recordings_updated_at BEFORE UPDATE ON recordings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_app_settings_updated_at BEFORE UPDATE ON app_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_nodes_updated_at BEFORE UPDATE ON nodes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_channel_assignments_updated_at BEFORE UPDATE ON channel_assignments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- ROW LEVEL SECURITY
-- ============================================================================
ALTER TABLE channels ENABLE ROW LEVEL SECURITY;
ALTER TABLE recordings ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE tunnels ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE preview_images ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE disk_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE nodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_assignments ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
    pol RECORD;
BEGIN
    FOR pol IN
        SELECT policyname, tablename
        FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename IN ('channels', 'recordings', 'upload_links', 'app_settings',
                            'tunnels', 'channel_logs', 'preview_images',
                            'upload_journal', 'pipeline_states', 'disk_usage',
                            'nodes', 'channel_assignments')
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', pol.policyname, pol.tablename);
    END LOOP;
END $$;

CREATE POLICY "Allow all operations on channels" ON channels
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on recordings" ON recordings
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on upload_links" ON upload_links
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on app_settings" ON app_settings
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on tunnels" ON tunnels
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on channel_logs" ON channel_logs
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on preview_images" ON preview_images
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on upload_journal" ON upload_journal
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on pipeline_states" ON pipeline_states
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on disk_usage" ON disk_usage
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on nodes" ON nodes
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all operations on channel_assignments" ON channel_assignments
    FOR ALL USING (true) WITH CHECK (true);

-- ============================================================================
-- VIEWS
-- ============================================================================
CREATE OR REPLACE VIEW recordings_with_links WITH (security_invoker = true) AS
SELECT
    r.*,
    COALESCE(
        json_object_agg(ul.host, ul.url) FILTER (WHERE ul.host IS NOT NULL),
        '{}'::json
    ) AS links
FROM recordings r
LEFT JOIN upload_links ul ON r.id = ul.recording_id
GROUP BY r.id;

CREATE OR REPLACE VIEW channel_statistics WITH (security_invoker = true) AS
SELECT
    c.username,
    c.is_paused,
    COUNT(r.id) AS total_recordings,
    SUM(r.filesize) AS total_filesize_bytes,
    MAX(r.timestamp) AS last_recording_at,
    AVG(r.viewers) AS avg_viewers,
    c.created_at,
    c.updated_at
FROM channels c
LEFT JOIN recordings r ON c.username = r.username
GROUP BY c.id, c.username, c.is_paused, c.created_at, c.updated_at;

CREATE OR REPLACE VIEW recent_activity WITH (security_invoker = true) AS
SELECT
    'recording' AS activity_type,
    r.username,
    r.filename AS description,
    r.timestamp AS activity_time
FROM recordings r
UNION ALL
SELECT
    'log' AS activity_type,
    cl.username,
    cl.message AS description,
    cl.created_at AS activity_time
FROM channel_logs cl
ORDER BY activity_time DESC
LIMIT 100;

-- ============================================================================
-- MULTI-INSTANCE BOOTSTRAP
-- ============================================================================
INSERT INTO app_settings (key, value)
SELECT 'channels_default', value FROM app_settings WHERE key = 'channels'
ON CONFLICT (key) DO NOTHING;

INSERT INTO app_settings (key, value)
VALUES ('__healthcheck__', '{"status": "ok"}'::jsonb)
ON CONFLICT (key) DO NOTHING;

-- ============================================================================
-- PERMISSIONS
-- ============================================================================
GRANT USAGE ON SCHEMA public TO anon;
GRANT CREATE ON SCHEMA public TO anon;

GRANT ALL ON ALL TABLES IN SCHEMA public TO anon;

ALTER TABLE public.channels OWNER TO anon;
ALTER TABLE public.recordings OWNER TO anon;
ALTER TABLE public.upload_links OWNER TO anon;
ALTER TABLE public.app_settings OWNER TO anon;
ALTER TABLE public.tunnels OWNER TO anon;
ALTER TABLE public.channel_logs OWNER TO anon;
ALTER TABLE public.preview_images OWNER TO anon;
ALTER TABLE public.upload_journal OWNER TO anon;
ALTER TABLE public.pipeline_states OWNER TO anon;
ALTER TABLE public.disk_usage OWNER TO anon;
ALTER TABLE public.nodes OWNER TO anon;
ALTER TABLE public.channel_assignments OWNER TO anon;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO anon;

GRANT SELECT ON public.recordings_with_links TO anon;
GRANT SELECT ON public.channel_statistics TO anon;
GRANT SELECT ON public.recent_activity TO anon;

ALTER VIEW public.recordings_with_links OWNER TO anon;
ALTER VIEW public.channel_statistics OWNER TO anon;
ALTER VIEW public.recent_activity OWNER TO anon;
