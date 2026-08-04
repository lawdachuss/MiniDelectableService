-- ============================================================
-- 2026-08-04 auth.uid()-based RLS policies for the 7 user-site tables
-- so the site's frontend (Supabase-auth authenticated users) can read and
-- manage their own rows.
--
-- Design (verified against the live database):
--  * The tables were previously locked to service_role only (advisor fix).
--  * auth.uid() returns uuid.  Six tables store user_id as text; the policy
--    compares auth.uid()::text (the canonical uuid string — exactly what
--    supabase-js sends) so no uuid<->text cast errors can occur.
--  * user_notification_preferences.user_id is uuid with an FK to auth.users,
--    so it compares directly.
--  * WITH CHECK mirrors USING so authenticated users can INSERT/UPDATE their
--    own rows (the frontend must set user_id = auth.uid() on insert).
--  * anon (uid() = NULL) matches nothing on these policies.
--  * The service_role_all policies remain untouched (service_role bypasses
--    RLS anyway).
-- ============================================================

-- ── Text user_id tables ───────────────────────────────────────
CREATE POLICY "authenticated_own_user_profiles" ON public.user_profiles
  FOR ALL TO authenticated
  USING (auth.uid()::text = user_id)
  WITH CHECK (auth.uid()::text = user_id);

CREATE POLICY "authenticated_own_user_roles" ON public.user_roles
  FOR ALL TO authenticated
  USING (auth.uid()::text = user_id)
  WITH CHECK (auth.uid()::text = user_id);

CREATE POLICY "authenticated_own_user_notifications" ON public.user_notifications
  FOR ALL TO authenticated
  USING (auth.uid()::text = user_id)
  WITH CHECK (auth.uid()::text = user_id);

CREATE POLICY "authenticated_own_watch_history" ON public.watch_history
  FOR ALL TO authenticated
  USING (auth.uid()::text = user_id)
  WITH CHECK (auth.uid()::text = user_id);

CREATE POLICY "authenticated_own_watch_later_items" ON public.watch_later_items
  FOR ALL TO authenticated
  USING (auth.uid()::text = user_id)
  WITH CHECK (auth.uid()::text = user_id);

CREATE POLICY "authenticated_own_saved_videos" ON public.saved_videos
  FOR ALL TO authenticated
  USING (auth.uid()::text = user_id)
  WITH CHECK (auth.uid()::text = user_id);

-- ── uuid user_id table (FK to auth.users) ─────────────────────
CREATE POLICY "authenticated_own_user_notification_preferences" ON public.user_notification_preferences
  FOR ALL TO authenticated
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);
