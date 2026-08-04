#!/bin/bash
set -a
source .env
set +a
U="$SUPABASE_URL"; K="$SUPABASE_API_KEY"
hdr=(-H "apikey: $K" -H "Authorization: Bearer $K")

count() {
  local t="$1"
  local out
  out=$(curl -s -m 12 -X HEAD "${hdr[@]}" -H "Prefer: count=exact" "$U/rest/v1/$t?select=id&limit=0" -D - -o /dev/null | grep -i 'content-range' | tr -d '\r')
  echo "table=$t -> ${out:-NO-ACCESS}"
}

for t in channels recordings upload_links app_settings tunnels channel_logs preview_images upload_journal pipeline_states disk_usage nodes channel_assignments; do
  count "$t"
done

echo
echo "=== channels sample (keys) ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/channels?select=username,is_paused,framerate,resolution,compress,created_at&limit=12" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== recordings sample (keys, limit 5) ==="
curl -s -m 15 "${hdr[@]}" "$U/rest/v1/recordings?select=username,filename,timestamp,filesize,duration,instance_id&order=timestamp.desc&limit=5" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== upload_journal sample ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/upload_journal?select=file_hash,host,status&limit=5" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== channel_logs sample ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/channel_logs?select=username,log_level,created_at&limit=5" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== app_settings keys ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/app_settings?select=key&limit=100" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== disk_usage latest ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/disk_usage?order=recorded_at.desc&limit=1" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== nodes ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/nodes?select=node_id,status,last_heartbeat&limit=20" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"

echo
echo "=== channel_assignments summary ==="
curl -s -m 12 "${hdr[@]}" "$U/rest/v1/channel_assignments?select=username,site,status,is_live,assigned_node&limit=50" | python -c "import json,sys; d=json.load(sys.stdin); [print(r) for r in d]"
