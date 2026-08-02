# Proxy and Cookie Configuration Guide

## Cookie Storage: Supabase (Not .env)

**IMPORTANT:** This application now loads cookies from **Supabase**, not from the `.env` file!

### How It Works

1. **On startup**, the app loads cookies from Supabase `app_settings` table (key: `dvr_settings`)
2. **Falls back to .env** only if Supabase is not configured or empty
3. **Updates via Web UI** automatically save to Supabase

### Cookie Priority (Highest to Lowest)
```
1. Supabase (app_settings.dvr_settings)
2. .env file (COOKIES environment variable)
```

## Problem: "Stream URL unavailable (check cookies)"

This error occurs when:
1. **Cookies expired** - Cloudflare cookies expire quickly (minutes to hours)
2. **Geo-restrictions** - Some models restrict certain regions
3. **IP/Cookie mismatch** - Cookies collected from one IP/region, requests made from another

## Why Some Channels Work and Others Don't

- ✅ **Working channels**: No geo-restrictions or accept your region
- ❌ **Failing channels**: Have geo-restrictions or stricter validation
- ⚠️ **Cookie expiration**: `cf_clearance` and `__cf_bm` expire in minutes/hours

## Solutions

### Option 1: Update Cookies via Web UI (Recommended)

1. **Visit** `http://localhost:8080` (or your tunnel URL)
2. **Click "Settings"** in the navigation
3. **Paste fresh cookies** from your browser
4. **Click "Save"**
5. Cookies are **automatically saved to Supabase**

### Option 2: Update Cookies in Supabase Directly

1. **Open Supabase Dashboard** → SQL Editor
2. **Run this query** to view current cookies:
```sql
SELECT value FROM app_settings WHERE key = 'dvr_settings';
```

3. **Update cookies** with this query:
```sql
UPDATE app_settings 
SET value = jsonb_set(value, '{cookies}', '"YOUR_COOKIE_STRING"')
WHERE key = 'dvr_settings';
```

4. **Restart the app** to reload

## How to Get Fresh Cookies

### Chrome/Edge Method:
1. Visit https://chaturbate.com and log in
2. Press **F12** to open DevTools
3. Go to **Application** tab → **Cookies** → `https://chaturbate.com`
4. Copy all cookie values
5. Format as: `name1=value1; name2=value2; ...`

### Required Cookies:
- `csrftoken` - Required for API requests
- `cf_clearance` - Cloudflare clearance token ⚠️ **Expires in hours**
- `__cf_bm` - Cloudflare bot management ⚠️ **Expires in ~30 minutes**
- Session cookies (`sbr`, `_iidt`, `_vid_t`, etc.)

### Cookie String Example:
```
csrftoken=abc123; cf_clearance=xyz789; __cf_bm=def456; sessionid=ghi789
```

## Verifying Cookie Storage

### Check Startup Logs:
```
📦 Loading cookies from Supabase...
✅ Cookies loaded from Supabase
```

If you see:
```
⚠️  Failed to load cookies from Supabase: ...
   Falling back to .env cookies
```

Then check your Supabase configuration in .env:
```env
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_API_KEY="your-api-key"
```

## Troubleshooting

### All channels show "stream URL unavailable":
→ **Cookies expired or don't match your IP region**
→ Update cookies via Web UI with fresh ones

### Some channels work, others don't:
→ **Geo-restrictions vary per model**
→ Check the model's region restrictions or contact them

### Cookies expire frequently:
→ **Cloudflare's `__cf_bm` expires every 30 minutes**
→ Update via Web UI as needed

### "Failed to load cookies from Supabase":
→ **Check Supabase connection**:
```bash
# Check if Supabase is configured
echo $SUPABASE_URL
echo $SUPABASE_API_KEY
```
→ **Run the migration** if not done already:
```sql
-- See database/migrate.sql
```

## Cookie Lifetime

| Cookie | Lifetime | Notes |
|--------|----------|-------|
| `cf_clearance` | 1-24 hours | Cloudflare clearance |
| `__cf_bm` | ~30 minutes | Cloudflare bot management |
| `csrftoken` | Days to weeks | Django CSRF token |
| Session cookies | Until logout | Session management |

**Best practice**: Update cookies via Web UI when you notice recording failures.

## Advanced: Automatic Cookie Rotation

For production setups:
1. Browser automation (Puppeteer/Playwright) to refresh cookies
2. Multiple cookie sets for different regions
3. Webhook notifications when cookies expire
4. Scheduled cookie refresh job

## Debug Logs

The app shows helpful debug messages:
```
[DEBUG] username POST API response: status=public url=https://...
[WARN] username: POST API returned empty URL, trying GET API fallback (check cookies if this persists)
[DEBUG] POST API 403 response for username: {"error": "..."}
```

Watch these logs to diagnose cookie issues in real-time.

## Migration from .env to Supabase

If you previously used .env cookies:

1. **Your .env cookies will load on first startup**
2. **Update cookies once via Web UI**
3. **From then on, cookies load from Supabase**
4. **You can remove COOKIES from .env** (optional)

The app automatically migrates to Supabase when you use the Web UI.

