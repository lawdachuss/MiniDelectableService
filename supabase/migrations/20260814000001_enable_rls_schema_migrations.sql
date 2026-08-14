-- The Supabase CLI tracks applied migrations in a bookkeeping table. Older
-- CLI versions use public._schema_migrations; newer ones use
-- supabase_migrations.schema_migrations. The Security Advisor flags whichever
-- one exists ("RLS Disabled in Public") because it lives in a
-- PostgREST-visible schema. Enabling RLS is safe: the CLI and the DVR connect
-- with privileged roles (postgres / service_role) that bypass RLS, so nothing
-- breaks, and the anon key can no longer read migration history.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = '_schema_migrations') THEN
        EXECUTE 'ALTER TABLE public._schema_migrations ENABLE ROW LEVEL SECURITY';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'supabase_migrations' AND c.relname = 'schema_migrations') THEN
        EXECUTE 'ALTER TABLE supabase_migrations.schema_migrations ENABLE ROW LEVEL SECURITY';
    END IF;
END $$;
