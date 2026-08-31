#!/bin/bash
# Backfill .th.webp freeimage URLs to full animated .webp URLs in preview_mirrors.
# The .th.webp thumbnails are static single frames; the full .webp preserves animation.
#
# Usage: bash scripts/backfill_freeimage_webp.sh [--dry-run]
set -euo pipefail

U="${SUPABASE_URL:?SUPABASE_URL not set}"
K="${SUPABASE_API_KEY:?SUPABASE_API_KEY not set}"
DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

updated=0
errors=0
skipped=0
offset=0
batch_size=100

echo "=== Backfilling .th.webp → .webp in preview_images.preview_mirrors ==="
echo "Mode: $($DRY_RUN && echo 'DRY RUN' || echo 'LIVE')"
echo ""

while true; do
  # Fetch a batch of rows where preview_mirrors is not null
  # Use select to get only what we need
  batch=$(curl -s -m 30 \
    -H "apikey: $K" \
    -H "Authorization: Bearer $K" \
    "$U/rest/v1/preview_images?select=id,preview_mirrors&preview_mirrors=not.is.null&limit=$batch_size&offset=$offset" 2>&1)

  # Check for errors
  if echo "$batch" | grep -q '"code"'; then
    echo "Error fetching batch at offset $offset: $(echo "$batch" | head -c 200)"
    break
  fi

  # Count rows in batch
  count=$(echo "$batch" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
  if [ "$count" = "0" ]; then
    echo "No more rows at offset $offset"
    break
  fi

  echo "Batch offset=$offset: $count rows"

  # Process each row using python for reliable JSON handling
  echo "$batch" | python3 -c "
import json, sys

data = json.load(sys.stdin)
for row in data:
    rid = row['id']
    mirrors = row.get('preview_mirrors', {})
    if not mirrors:
        continue

    freeimage_url = mirrors.get('freeimage.host', '')
    if '.th.webp' not in freeimage_url:
        continue

    # Replace .th.webp with .webp
    new_url = freeimage_url.replace('.th.webp', '.webp')
    mirrors['freeimage.host'] = new_url
    print(json.dumps({'id': rid, 'mirrors': mirrors}))
" 2>/dev/null | while read -r line; do
    id=$(echo "$line" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null)
    mirrors=$(echo "$line" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['mirrors']))" 2>/dev/null)

    if [ "$DRY_RUN" = "true" ]; then
        skipped=$((skipped + 1))
        continue
    fi

    # PATCH the row
    patch_body="{\"preview_mirrors\": $mirrors}"
    http_code=$(curl -s -m 15 -X PATCH \
      -H "apikey: $K" \
      -H "Authorization: Bearer $K" \
      -H "Content-Type: application/json" \
      -H "Prefer: return=minimal" \
      "$U/rest/v1/preview_images?id=eq.$id" \
      -d "$patch_body" -o /dev/null -w "%{http_code}" 2>&1)

    if [ "$http_code" = "200" ] || [ "$http_code" = "204" ]; then
      updated=$((updated + 1))
    else
      echo "  ERROR updating $id: HTTP $http_code"
      errors=$((errors + 1))
    fi
  done

  offset=$((offset + batch_size))

  # Safety limit
  if [ "$offset" -ge 5000 ]; then
    echo "Safety limit reached. Run again to continue from offset $offset."
    break
  fi

  # Brief pause between batches
  sleep 0.2
done

echo ""
echo "=== Backfill complete ==="
echo "Updated: $updated rows"
echo "Errors: $errors"
[ "$DRY_RUN" = "true" ] && echo "(Dry run — no changes made)"
