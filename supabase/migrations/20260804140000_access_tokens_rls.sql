-- ============================================================
-- 2026-08-04 Enable RLS on the platform-owned _supabase.access_tokens table
-- so the Supabase Advisor shows a clean zero-issue state.
--
-- Safety analysis (verified against the live database):
--  * The 4 access-token RPCs (register_access_token, revoke_token_by_id,
--    check_token_jti, list_access_tokens) are SECURITY DEFINER owned by
--    postgres with owner-only EXECUTE — they bypass RLS and keep working
--    unchanged.
--  * postgres is the table owner and bypasses RLS for direct platform
--    access (FORCE ROW LEVEL SECURITY is deliberately NOT used).
--  * The single policy targets service_role only, which bypasses RLS
--    anyway — runtime behavior is identical; anon/authenticated are locked
--    out of direct table access and must go through the RPCs.
-- ============================================================

ALTER TABLE _supabase.access_tokens ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all_access_tokens" ON _supabase.access_tokens
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
