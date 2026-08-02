-- ============================================================================
-- Chaturbate DVR - Channel Profile Columns (idempotent migration)
-- ============================================================================
-- Extends the EXISTING channels table with full-profile fields scraped from
-- the site's biocontext API (follower_count, bio, age, location, languages,
-- last_broadcast, etc.). Safe to re-run: each ALTER uses IF NOT EXISTS.
--
-- Run this once in the Supabase SQL editor (or via supabase db push).
-- The recorder stores profile data here on-demand via
-- database.Client.SaveChannelProfile — your existing archive site can read
-- these columns straight from the channels table.
-- ============================================================================

ALTER TABLE public.channels
    ADD COLUMN IF NOT EXISTS follower_count      INTEGER        NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS location            TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS real_name           TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS body_decorations    TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS smoke_drink         TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS body_type           TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS display_birthday    TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS display_age         INTEGER        NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS about_me            TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS wish_list           TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS fan_club_cost       INTEGER        NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sex                 TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS subgender           TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS interested_in       JSONB          NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS photo_sets          JSONB          NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS social_medias       JSONB          NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS last_broadcast      TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS room_status         TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS avatar_url          TEXT           NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS profile_scraped_at  TIMESTAMPTZ;

-- Make profile lookups fast when the archive site queries by username.
CREATE INDEX IF NOT EXISTS idx_channels_profile_scraped_at
    ON public.channels (profile_scraped_at DESC);
