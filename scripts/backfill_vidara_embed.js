#!/usr/bin/env node
// Fast backfill: Vidara upload_links vidara.so/v/CODE → vidara.so/e/CODE
// Uses higher parallelism for speed

const https = require('https');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_API_KEY;

function supabasePatch(id, newUrl) {
  return new Promise((resolve) => {
    const url = new URL(SUPABASE_URL + '/rest/v1/upload_links?id=eq.' + id);
    const req = https.request({
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'apikey': SUPABASE_KEY,
        'Prefer': 'return=minimal'
      }
    }, (res) => { res.resume(); resolve(res.statusCode); });
    req.on('error', () => resolve(0));
    req.write(JSON.stringify({ url: newUrl }));
    req.end();
  });
}

function supabaseGet(offset, limit) {
  return new Promise((resolve, reject) => {
    const url = new URL(SUPABASE_URL + `/rest/v1/upload_links?select=id,url&host=eq.Vidara&offset=${offset}&limit=${limit}&order=id`);
    const req = https.request({
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'GET',
      headers: {
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'apikey': SUPABASE_KEY
      }
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(JSON.parse(d)));
    });
    req.on('error', reject);
    req.end();
  });
}

async function main() {
  let offset = 0;
  const limit = 500;
  let totalUpdated = 0;
  let totalErrors = 0;

  while (true) {
    const rows = await supabaseGet(offset, limit);
    if (!rows.length) break;

    const toUpdate = [];
    for (const row of rows) {
      if (!row.url.includes('vidara.so/v/')) continue;

      let code;
      const embedMatch = row.url.match(/\/e\/([A-Za-z0-9]+)/);
      if (embedMatch && row.url.includes('vidara.so/v/https://')) {
        code = embedMatch[1];
      } else {
        const c = row.url.split('/v/')[1];
        if (c && /^[A-Za-z0-9]+$/.test(c)) code = c;
      }
      if (code) toUpdate.push({ id: row.id, url: 'https://vidara.so/e/' + code });
    }

    // Update 20 at a time in parallel
    for (let i = 0; i < toUpdate.length; i += 20) {
      const batch = toUpdate.slice(i, i + 20);
      const results = await Promise.all(batch.map(r => supabasePatch(r.id, r.url)));
      for (const s of results) {
        if (s >= 200 && s < 300) totalUpdated++;
        else totalErrors++;
      }
    }

    process.stdout.write(`\r  offset=${offset} updated=${totalUpdated} errors=${totalErrors}`);
    offset += limit;
    if (rows.length < limit) break;
  }

  console.log(`\nDone: updated=${totalUpdated}, errors=${totalErrors}`);
}

main().catch(e => { console.error(e); process.exit(1); });
