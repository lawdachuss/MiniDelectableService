#!/bin/bash
set -a
source .env
set +a
U="$SUPABASE_URL"; K="$SUPABASE_API_KEY"
echo "== healthcheck app_settings =="
curl -s -m 10 -H "apikey: $K" -H "Authorization: Bearer $K" "$U/rest/v1/app_settings?key=eq.__healthcheck__&select=key&limit=1"
echo
echo "== openapi (first 3000 chars) =="
curl -s -m 15 -H "apikey: $K" -H "Authorization: Bearer $K" "$U/rest/v1/" | head -c 3000
echo
echo "== channels count =="
curl -s -m 10 -X HEAD -H "apikey: $K" -H "Authorization: Bearer $K" -H "Prefer: count=exact" "$U/rest/v1/channels?select=id&limit=1" -D - -o /dev/null | grep -i -E 'HTTP|content-range'
