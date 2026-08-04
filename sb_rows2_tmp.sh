#!/bin/bash
set -a
source .env
set +a
U="$SUPABASE_URL"; K="$SUPABASE_API_KEY"

count() {
  local t="$1"
  local out
  echo "counting $t ..."
  out=$(curl -s -m 8 -X HEAD -H "apikey: $K" -H "Authorization: Bearer $K" -H "Prefer: count=exact" "$U/rest/v1/$t?select=id&limit=0" -D - -o /dev/null 2>&1 | grep -i 'content-range' | tr -d '\r')
  echo "  table=$t -> ${out:-NO-ACCESS/ERR}"
}

count channels
count recordings
count upload_links
count app_settings
count tunnels
count channel_logs
count preview_images
count upload_journal
count pipeline_states
count disk_usage
count nodes
count channel_assignments
echo DONE
