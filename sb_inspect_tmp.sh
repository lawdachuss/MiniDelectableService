#!/bin/bash
# Inspect the live Supabase database without printing secrets.
set -a
# shellcheck disable=SC1091
source .env
set +a

U="$SUPABASE_URL"
K="$SUPABASE_API_KEY"

if [ -z "$U" ] || [ -z "$K" ]; then
  echo "ERROR: SUPABASE_URL/SUPABASE_API_KEY missing from .env"
  exit 1
fi

echo "=== OPENAPI: top-level path keys (tables/views exposed via PostgREST) ==="
curl -s -m 30 -H "apikey: $K" -H "Authorization: Bearer $K" "$U/rest/v1/" -o /tmp/sb_openapi.json
python - <<'PY'
import json
try:
    data=json.load(open('/tmp/sb_openapi.json'))
    paths=sorted(data.get('paths',{}).keys())
    for p in paths: print(p)
    print("TOTAL:",len(paths))
except Exception as e:
    print("parse failed:",e)
    print(open('/tmp/sb_openapi.json').read()[:500])
PY

echo
echo "=== ROW COUNTS (exact) per table ==="
for t in channels recordings upload_links app_settings tunnels channel_logs preview_images upload_journal pipeline_states disk_usage nodes channel_assignments; do
  cnt=$(curl -s -m 20 -X HEAD -H "apikey: $K" -H "Authorization: Bearer $K" -H "Prefer: count=exact" "$U/rest/v1/$t?select=*&limit=0" -D - -o /dev/null | grep -i 'content-range' | sed 's/.*\///' | tr -d '\r')
  echo "table=$t rows=${cnt:-ERR}"
done
