-- 2026-08-04 Fix permissive RLS policies flagged by Supabase Advisor
--
-- Replace FOR ALL USING (true) WITH CHECK (true) policies with
-- restrictive SELECT-only policies for anon/authenticated roles.
-- Write operations remain available via service_role (which bypasses RLS).

-- ── channel_assignments ──────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on channel_assignments" ON public.channel_assignments;
DROP POLICY IF EXISTS channel_assignments_all_auth ON public.channel_assignments;
CREATE POLICY "channel_assignments_select_anon" ON public.channel_assignments
    FOR SELECT TO anon USING (true);
CREATE POLICY "channel_assignments_select_auth" ON public.channel_assignments
    FOR SELECT TO authenticated USING (true);

-- ── channels ─────────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on channels" ON public.channels;
DROP POLICY IF EXISTS anon_all ON public.channels;
DROP POLICY IF EXISTS channels_all_auth ON public.channels;
CREATE POLICY "channels_select_anon" ON public.channels
    FOR SELECT TO anon USING (true);
CREATE POLICY "channels_select_auth" ON public.channels
    FOR SELECT TO authenticated USING (true);

-- ── recordings ───────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on recordings" ON public.recordings;
DROP POLICY IF EXISTS anon_all ON public.recordings;
DROP POLICY IF EXISTS recordings_all_auth ON public.recordings;
CREATE POLICY "recordings_select_anon" ON public.recordings
    FOR SELECT TO anon USING (true);
CREATE POLICY "recordings_select_auth" ON public.recordings
    FOR SELECT TO authenticated USING (true);

-- ── upload_links ─────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on upload_links" ON public.upload_links;
DROP POLICY IF EXISTS anon_all ON public.upload_links;
DROP POLICY IF EXISTS upload_links_all_auth ON public.upload_links;
CREATE POLICY "upload_links_select_anon" ON public.upload_links
    FOR SELECT TO anon USING (true);
CREATE POLICY "upload_links_select_auth" ON public.upload_links
    FOR SELECT TO authenticated USING (true);

-- ── app_settings ─────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on app_settings" ON public.app_settings;
DROP POLICY IF EXISTS anon_all ON public.app_settings;
DROP POLICY IF EXISTS app_settings_all_auth ON public.app_settings;
CREATE POLICY "app_settings_select_anon" ON public.app_settings
    FOR SELECT TO anon USING (true);
CREATE POLICY "app_settings_select_auth" ON public.app_settings
    FOR SELECT TO authenticated USING (true);

-- ── tunnels ──────────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on tunnels" ON public.tunnels;
CREATE POLICY "tunnels_select_anon" ON public.tunnels
    FOR SELECT TO anon USING (true);
CREATE POLICY "tunnels_select_auth" ON public.tunnels
    FOR SELECT TO authenticated USING (true);

-- ── channel_logs ─────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on channel_logs" ON public.channel_logs;
CREATE POLICY "channel_logs_select_anon" ON public.channel_logs
    FOR SELECT TO anon USING (true);
CREATE POLICY "channel_logs_select_auth" ON public.channel_logs
    FOR SELECT TO authenticated USING (true);

-- ── preview_images ───────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on preview_images" ON public.preview_images;
DROP POLICY IF EXISTS preview_images_all_auth ON public.preview_images;
CREATE POLICY "preview_images_select_anon" ON public.preview_images
    FOR SELECT TO anon USING (true);
CREATE POLICY "preview_images_select_auth" ON public.preview_images
    FOR SELECT TO authenticated USING (true);

-- ── upload_journal ───────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on upload_journal" ON public.upload_journal;
DROP POLICY IF EXISTS anon_all ON public.upload_journal;
DROP POLICY IF EXISTS upload_journal_all_auth ON public.upload_journal;
CREATE POLICY "upload_journal_select_anon" ON public.upload_journal
    FOR SELECT TO anon USING (true);
CREATE POLICY "upload_journal_select_auth" ON public.upload_journal
    FOR SELECT TO authenticated USING (true);

-- ── pipeline_states ──────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on pipeline_states" ON public.pipeline_states;
DROP POLICY IF EXISTS anon_all ON public.pipeline_states;
DROP POLICY IF EXISTS pipeline_states_all_auth ON public.pipeline_states;
CREATE POLICY "pipeline_states_select_anon" ON public.pipeline_states
    FOR SELECT TO anon USING (true);
CREATE POLICY "pipeline_states_select_auth" ON public.pipeline_states
    FOR SELECT TO authenticated USING (true);

-- ── disk_usage ───────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on disk_usage" ON public.disk_usage;
CREATE POLICY "disk_usage_select_anon" ON public.disk_usage
    FOR SELECT TO anon USING (true);
CREATE POLICY "disk_usage_select_auth" ON public.disk_usage
    FOR SELECT TO authenticated USING (true);

-- ── nodes ────────────────────────────────────────────────────────────
DROP POLICY IF EXISTS "Allow all operations on nodes" ON public.nodes;
DROP POLICY IF EXISTS nodes_all_auth ON public.nodes;
CREATE POLICY "nodes_select_anon" ON public.nodes
    FOR SELECT TO anon USING (true);
CREATE POLICY "nodes_select_auth" ON public.nodes
    FOR SELECT TO authenticated USING (true);

-- ── user-facing tables (service_role already has ALL access) ────────
-- These tables also had permissive anon/authenticated ALL policies.
-- Restrict to SELECT for anon/authenticated; writes go through service_role.

-- user_collections (auth_rls_initplan)
DROP POLICY IF EXISTS uc_select_own ON public.user_collections;
DROP POLICY IF EXISTS uc_insert_own ON public.user_collections;
DROP POLICY IF EXISTS uc_update_own ON public.user_collections;
DROP POLICY IF EXISTS uc_delete_own ON public.user_collections;
CREATE POLICY "uc_select_own" ON public.user_collections
    FOR SELECT TO authenticated USING (auth.uid() = user_id);
CREATE POLICY "uc_insert_own" ON public.user_collections
    FOR INSERT TO authenticated WITH CHECK (auth.uid() = user_id);
CREATE POLICY "uc_update_own" ON public.user_collections
    FOR UPDATE TO authenticated USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);
CREATE POLICY "uc_delete_own" ON public.user_collections
    FOR DELETE TO authenticated USING (auth.uid() = user_id);

-- user_collection_items (auth_rls_initplan)
DROP POLICY IF EXISTS uci_select_own ON public.user_collection_items;
DROP POLICY IF EXISTS uci_insert_own ON public.user_collection_items;
DROP POLICY IF EXISTS uci_update_own ON public.user_collection_items;
DROP POLICY IF EXISTS uci_delete_own ON public.user_collection_items;
CREATE POLICY "uci_select_own" ON public.user_collection_items
    FOR SELECT TO authenticated USING (auth.uid() = user_id);
CREATE POLICY "uci_insert_own" ON public.user_collection_items
    FOR INSERT TO authenticated WITH CHECK (auth.uid() = user_id);
CREATE POLICY "uci_update_own" ON public.user_collection_items
    FOR UPDATE TO authenticated USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);
CREATE POLICY "uci_delete_own" ON public.user_collection_items
    FOR DELETE TO authenticated USING (auth.uid() = user_id);