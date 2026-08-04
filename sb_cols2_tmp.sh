#!/usr/bin/env bash
set -a
source .env
set +a

U="$SUPABASE_URL"
K="$SUPABASE_SERVICE_ROLE_KEY"

echo '=== full columns of the 5 site-user tables ==='
cat > sb_c2_tmp.sql <<'SQLEOF'
SELECT table_name, ordinal_position, column_name, data_type
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name IN ('comments','reactions','comment_likes','performer_follows','requests')
ORDER BY table_name, ordinal_position;
SQLEOF
python -c "import json,sys; print(json.dumps({'query': sys.stdin.read()}))" < sb_c2_tmp.sql > sb_c2_tmp.json
curl -s -m 60 -X POST -H "apikey: $K" -H "Authorization: Bearer $K" -H 'Content-Type: application/json' --data-binary @sb_c2_tmp.json "$U/pg/query" | python -m json.tool 2>/dev/null | head -200

echo
echo '=== table-level GRANTs to anon / authenticated / PUBLIC ==='
cat > sb_c2_tmp.sql <<'SQLEOF'
SELECT c.relname AS table,
       COALESCE(STRING_AGG(DISTINCT CASE WHEN grantee='anon' THEN privilege_type END, ','),'') AS anon_grants,
       COALESCE(STRING_AGG(DISTINCT CASE WHEN grantee='authenticated' THEN privilege_type END, ','),'') AS auth_grants
FROM pg_class c
JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN information_schema.role_table_grants g ON g.table_name=c.relname AND g.table_schema='public'
WHERE n.nspname='public'
  AND c.relname IN ('comments','reactions','comment_likes','performer_follows','requests','channels','recordings','upload_links','upload_journal','preview_images','app_settings','nodes','channel_assignments','disk_usage','pipeline_states','tunnels','pool_autopilot')
GROUP BY c.relname
ORDER BY c.relname;
SQLEOF
python -c "import json,sys; print(json.dumps({'query': sys.stdin.read()}))" < sb_c2_tmp.sql > sb_c2_tmp.json
curl -s -m 60 -X POST -H "apikey: $K" -H "Authorization: Bearer $K" -H 'Content-Type: application/json' --data-binary @sb_c2_tmp.json "$U/pg/query" | python -m json.tool 2>/dev/null | head -120

rm -f sb_c2_tmp.sql sb_c2_tmp.json
