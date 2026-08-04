-- 2026-08-04: Supabase Advisor fixes (4 findings on supabase.chuglii.in)
--
-- 1) + 4) channel_assignments / nodes had RLS DISABLED while permissive
--    policies already existed.  Enabling RLS activates them: the anon role
--    (used by the DVR app's PostgREST client) keeps identical access via the
--    existing "Allow all operations on ..." PUBLIC policies and the
--    "_all_auth" authenticated policies.  Behavior is unchanged; the Advisor
--    findings are resolved.
ALTER TABLE public.channel_assignments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.nodes ENABLE ROW LEVEL SECURITY;

-- 3) The 4 _supabase access-token functions are SECURITY DEFINER but had no
--    explicit search_path (mutable).  Every reference inside them is
--    schema-qualified (_supabase.access_tokens) or a pg_catalog built-in, so
--    pinning the search path is safe.
ALTER FUNCTION _supabase.register_access_token(text, text) SET search_path = pg_catalog, _supabase;
ALTER FUNCTION _supabase.revoke_token_by_id(uuid) SET search_path = pg_catalog, _supabase;
ALTER FUNCTION _supabase.check_token_jti(uuid) SET search_path = pg_catalog, _supabase;
ALTER FUNCTION _supabase.list_access_tokens(boolean) SET search_path = pg_catalog, _supabase;

-- 2) The 7 user-facing site tables had RLS ENABLED with zero policies.
--    Only the backend (service_role, which bypasses RLS anyway) should manage
--    them, so we grant service_role explicit access.  This keeps current
--    behavior identical (anon/authenticated still see nothing) while giving
--    the Advisor an explicit access definition.
CREATE POLICY service_role_all ON public.saved_videos FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.user_notification_preferences FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.user_notifications FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.user_profiles FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.user_roles FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.watch_history FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.watch_later_items FOR ALL TO service_role USING (true) WITH CHECK (true);
