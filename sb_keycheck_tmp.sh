#!/usr/bin/env bash
# Redacted key check — prints match flags only, never key material.
set -a
source .env
set +a

U="$SUPABASE_URL"
K="$SUPABASE_SERVICE_ROLE_KEY"

anon_suffix="qPNIgWCMhnMMYySS-KFFQ-xi6f1YV88M2axjr-96EHU"
srv_suffix="UTDwoY0L6W6nllK7FvssoFLp3qvAx60PijJyL9XHyXQ"

echo '=== which key does SUPABASE_API_KEY hold? ==='
if grep -q "$anon_suffix" .env; then echo "anon-key-suffix present in .env"; else echo "anon-key-suffix NOT in .env"; fi
if grep -q "$srv_suffix" .env; then echo "service-key-suffix present in .env"; else echo "service-key-suffix NOT in .env"; fi

# Precise: compare the API_KEY env value against both known JWTs
api_key_len=${#SUPABASE_API_KEY}
case "$SUPABASE_API_KEY" in
  *"$anon_suffix") echo "API_KEY_ENV = ANON key (len=$api_key_len)" ;;
  *"$srv_suffix")  echo "API_KEY_ENV = SERVICE key (len=$api_key_len)" ;;
  *)               echo "API_KEY_ENV = UNKNOWN key (len=$api_key_len)" ;;
esac

echo
echo '=== site tables: user_id column present? ==='
cat > sb_cols_tmp.sql <<'SQLEOF'
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name IN ('comments','reactions','comment_likes','performer_follows','requests','app_settings','disk_usage','pool_autopilot','tunnels','pipeline_states','channels','recordings','upload_journal','upload_links','preview_images','nodes','channel_assignments')
  AND (column_name LIKE '%user%' OR column_name LIKE '%creator%' OR column_name LIKE '%owner%' OR column_name='id')
ORDER BY table_name, ordinal_position;
SQLEOF
python -c "import json,sys; print(json.dumps({'query': sys.stdin.read()}))" < sb_cols_tmp.sql > sb_cols_tmp.json
curl -s -m 60 -X POST -H "apikey: $K" -H "Authorization: Bearer $K" -H 'Content-Type: application/json' --data-binary @sb_cols_tmp.json "$U/pg/query" | python -m json.tool 2>/dev/null | head -120
rm -f sb_cols_tmp.sql sb_cols_tmp.json
