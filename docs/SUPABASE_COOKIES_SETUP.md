# Supabase Cookie Setup - Quick Start

## Overview

The app now loads cookies from **Supabase** instead of `.env` file. This allows:
- ✅ Centralized cookie management across instances
- ✅ Web UI updates automatically sync to database
- ✅ Easy cookie rotation without restarting servers
- ✅ Fallback to .env for initial setup

## Quick Setup

### 1. Ensure Supabase is Configured

Check your `.env` file:
```env
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_API_KEY="your-anon-key"
```

### 2. Run the Migration (If Not Done)

In Supabase SQL Editor, run:
```sql
-- See database/migrate.sql for full schema
```

The `app_settings` table stores cookies under key `dvr_settings`.

### 3. Add Initial Cookies

**Option A: Via Web UI (Easiest)**
1. Start the app: `.\chaturbate-dvr.exe`
2. Visit `http://localhost:8080`
3. Click "Settings" → Paste cookies → Save
4. Cookies automatically saved to Supabase ✅

**Option B: Via .env (Fallback)**
1. Add to `.env`:
```env
COOKIES="your_cookie_string"
USER_AGENT="your_user_agent"
```
2. Start the app (loads from .env)
3. Update once via Web UI to migrate to Supabase

**Option C: Direct SQL**
```sql
INSERT INTO app_settings (key, value) 
VALUES ('dvr_settings', jsonb_build_object(
  'cookies', 'your_cookie_string',
  'user_agent', 'Mozilla/5.0 ...'
))
ON CONFLICT (key) 
DO UPDATE SET value = EXCLUDED.value;
```

## How It Works

### Startup Flow
```
1. App starts
2. Checks for Supabase config
3. Loads cookies from Supabase app_settings
4. If empty/error → Falls back to .env
5. Continues with loaded cookies
```

### Cookie Priority
```
Supabase (dvr_settings) > .env (COOKIES)
```

### Update Flow
```
1. User updates via Web UI
2. POST /update-config
3. Saves to server.Config
4. Calls server.SaveSettings()
5. Writes to Supabase app_settings
6. All instances reload on next restart
```

## Verifying Setup

### Check Startup Logs
```bash
.\chaturbate-dvr.exe
```

**Success:**
```
📦 Loading cookies from Supabase...
✅ Cookies loaded from Supabase
```

**Fallback:**
```
⚠️  Failed to load cookies from Supabase: ...
   Falling back to .env cookies
```

### Check Supabase Data
```sql
SELECT 
  key, 
  value->>'cookies' as cookies,
  value->>'user_agent' as user_agent,
  updated_at
FROM app_settings 
WHERE key = 'dvr_settings';
```

## Updating Cookies

### Method 1: Web UI (Recommended)
1. Visit `http://localhost:8080`
2. Click "Settings"
3. Paste new cookies
4. Click "Save"
5. Done! ✅

### Method 2: SQL
```sql
UPDATE app_settings 
SET value = jsonb_set(
  value, 
  '{cookies}', 
  '"csrftoken=abc; cf_clearance=xyz; __cf_bm=def"'
)
WHERE key = 'dvr_settings';
```

### Method 3: API
```bash
curl -X POST http://localhost:8080/update-config \
  -H "Content-Type: application/json" \
  -d '{
    "cookies": "your_cookie_string",
    "user_agent": "your_user_agent"
  }'
```

## Multi-Instance Setup

If running multiple instances:

1. **All instances share the same cookies** from Supabase
2. **Update once** via any instance's Web UI
3. **Restart other instances** to reload

Alternatively, use different Supabase projects per instance.

## Troubleshooting

### "Failed to load cookies from Supabase"

**Cause:** Supabase not configured or connection failed

**Fix:**
1. Check `SUPABASE_URL` and `SUPABASE_API_KEY` in .env
2. Verify Supabase is accessible
3. Check migration was run
4. App will fall back to .env cookies

### "Stream URL unavailable (check cookies)"

**Cause:** Cookies expired or region mismatch

**Fix:**
1. Get fresh cookies from browser
2. Update via Web UI
3. See `docs/PROXY_AND_COOKIES.md` for details

### Cookies not updating

**Cause:** Update didn't save to Supabase

**Fix:**
1. Check logs for save errors
2. Verify Supabase RLS policies allow writes
3. Check SQL migration created policies correctly

## Data Structure

### Supabase Schema
```sql
-- app_settings table
CREATE TABLE app_settings (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- dvr_settings value structure
{
  "cookies": "csrftoken=...; cf_clearance=...; __cf_bm=...",
  "user_agent": "Mozilla/5.0 ...",
  "sessionid": "...",
  "csrftoken": "...",
  "cf_clearance": "...",
  "streamtape_login": "...",
  "streamtape_key": "...",
  "mixdrop_email": "...",
  "mixdrop_token": "...",
}
```

## Migration from .env

### Current State: .env Only
```env
COOKIES="your_cookies"
```

### Migration Steps:
1. Keep .env file as-is
2. Start app (loads from .env)
3. Update once via Web UI
4. App now uses Supabase
5. (Optional) Remove COOKIES from .env

### After Migration:
- Cookies load from Supabase
- .env serves as fallback only
- All updates go to Supabase

## Best Practices

1. **Update cookies via Web UI** - Easiest and safest
2. **Keep .env as fallback** - In case Supabase is down
3. **Rotate cookies regularly** - Especially `cf_clearance` and `__cf_bm`
4. **Monitor logs** - Watch for cookie expiration warnings
5. **Use fresh cookies** - Get them from a browser using the same IP/VPN the DVR runs on

## Security Notes

- ✅ Cookies stored in Supabase with RLS
- ✅ API key in .env (not committed)
- ⚠️ Don't commit .env file
- ⚠️ Use Supabase anon key (not service_role)
- ✅ RLS policies allow read/write for authenticated users

## References

- `docs/PROXY_AND_COOKIES.md` - Detailed cookie troubleshooting
- `database/migrate.sql` - Full database schema
- `server/config.go` - Cookie load/save logic
- `server/db.go` - Supabase integration
