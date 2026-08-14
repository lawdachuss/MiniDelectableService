-- The Supabase CLI creates public._schema_migrations to track applied
-- migrations. The Security Advisor flags it ("RLS Disabled in Public")
-- because it lives in the PostgREST-exposed public schema. It only holds
-- migration bookkeeping, but enabling RLS is safe: the CLI and the DVR
-- connect with privileged roles (postgres / service_role) that bypass RLS,
-- so nothing breaks, and the anon key can no longer read migration history.
ALTER TABLE public._schema_migrations ENABLE ROW LEVEL SECURITY;
