#!/usr/bin/env node
// Backfill .th.webp freeimage URLs to full animated .webp URLs in preview_mirrors.
// The .th.webp thumbnails are static single frames; the full .webp preserves animation.
//
// Usage: node scripts/backfill_freeimage_webp.js [--dry-run]

const U = process.env.SUPABASE_URL;
const K = process.env.SUPABASE_API_KEY;
if (!U || !K) { console.error("SUPABASE_URL and SUPABASE_API_KEY must be set"); process.exit(1); }

const DRY_RUN = process.argv.includes("--dry-run");
const BATCH = 100;
const headers = { apikey: K, Authorization: `Bearer ${K}` };

let updated = 0, errors = 0, skipped = 0, offset = 0;

console.log("=== Backfilling .th.webp → .webp in preview_images.preview_mirrors ===");
console.log(`Mode: ${DRY_RUN ? "DRY RUN" : "LIVE"}`);

async function fetchBatch(offset) {
  const url = `${U}/rest/v1/preview_images?select=id,preview_mirrors&preview_mirrors=not.is.null&limit=${BATCH}&offset=${offset}`;
  const resp = await fetch(url, { headers });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

async function patchRow(id, mirrors) {
  const url = `${U}/rest/v1/preview_images?id=eq.${id}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { ...headers, "Content-Type": "application/json", Prefer: "return=minimal" },
    body: JSON.stringify({ preview_mirrors: mirrors }),
  });
  return resp.ok;
}

while (true) {
  let batch;
  try {
    batch = await fetchBatch(offset);
  } catch (e) {
    console.error(`Error fetching batch at offset ${offset}: ${e.message}`);
    break;
  }

  if (!batch || batch.length === 0) {
    console.log(`No more rows at offset ${offset}`);
    break;
  }

  console.log(`Batch offset=${offset}: ${batch.length} rows`);

  for (const row of batch) {
    const mirrors = row.preview_mirrors;
    if (!mirrors || !mirrors["freeimage.host"]) continue;

    const url = mirrors["freeimage.host"];
    if (!url.includes(".th.webp")) {
      skipped++;
      continue;
    }

    // Replace .th.webp with .webp
    mirrors["freeimage.host"] = url.replace(".th.webp", ".webp");

    if (DRY_RUN) {
      updated++;
      continue;
    }

    try {
      const ok = await patchRow(row.id, mirrors);
      if (ok) {
        updated++;
      } else {
        console.error(`  ERROR updating ${row.id}`);
        errors++;
      }
    } catch (e) {
      console.error(`  ERROR updating ${row.id}: ${e.message}`);
      errors++;
    }

    // Brief pause to avoid rate limiting
    await new Promise(r => setTimeout(r, 50));
  }

  offset += BATCH;
  if (offset >= 5000) {
    console.log("Safety limit reached. Run again to continue.");
    break;
  }
}

console.log("\n=== Backfill complete ===");
console.log(`Updated: ${updated} rows`);
console.log(`Skipped (no .th.webp): ${skipped} rows`);
console.log(`Errors: ${errors}`);
if (DRY_RUN) console.log("(Dry run — no changes made)");
