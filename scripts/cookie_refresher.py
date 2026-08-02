#!/usr/bin/env python3
"""Cookie Refresher for Chaturbate DVR.

Reads current cookies from Supabase, tries to refresh cf_clearance using
curl_cffi (browser TLS impersonation — no full browser needed), merges
with existing sessionid/csrftoken, and writes back to Supabase.

If refresh fails, existing cookies are kept (they usually remain valid).

Usage: python scripts/cookie_refresher.py
Requires .env with SUPABASE_URL, SUPABASE_API_KEY.
"""

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


def load_dotenv(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("\"'")
        if key and not os.environ.get(key):
            os.environ[key] = val


def supabase_request(method, url, api_key, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("apikey", api_key)
    req.add_header("Authorization", f"Bearer {api_key}")
    if body:
        req.add_header("Content-Type", "application/json")
    if method == "PATCH":
        req.add_header("Prefer", "return=representation")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"  [WARN] Supabase {method} HTTP {e.code}: {error_body[:300]}")
        return None
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"  [WARN] Supabase {method} failed: {e}")
        return None


def parse_cookies(cookie_str):
    result = {}
    if not cookie_str:
        return result
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            result[k.strip()] = v.strip()
    return result


def join_cookies(cookie_dict):
    return "; ".join(f"{k}={v}" for k, v in cookie_dict.items())


def extract_single_cookie(cookie_str, name):
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            if k.strip() == name:
                return v.strip()
    return None


def try_refresh_with_curl_cffi(user_agent):
    """Try to get fresh cookies using curl_cffi (browser TLS impersonation).

    Returns dict of new cookies, or empty dict on failure.
    curl_cffi is lighter than a full browser and doesn't trigger Turnstile.
    """
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        print("  [INFO] curl_cffi not available")
        return {}

    print("  Trying curl_cffi (browser TLS impersonation)...")

    impersonate = "chrome124"
    session_cookies = {}


    # First: visit chaturbate.com to get initial cookies
    try:
        resp = cffi_requests.get(
            "https://chaturbate.com",
            impersonate=impersonate,
            timeout=60,
            headers={"User-Agent": user_agent} if user_agent else None,
            allow_redirects=True,
        )
        print(f"  curl_cffi status: {resp.status_code}, url: {resp.url}")

        # Extract cookies from response
        if hasattr(resp, "cookies"):
            for name, value in resp.cookies.items():
                session_cookies[name] = value
                print(f"    Cookie: {name}={value[:30]}...")

        # Also check Set-Cookie headers directly
        if hasattr(resp, "headers"):
            for header_val in resp.headers.get_list("set-cookie"):
                if "=" in header_val:
                    name = header_val.split("=")[0].strip()
                    value = header_val.split("=", 1)[1].split(";")[0].strip()
                    if name and value and name not in session_cookies:
                        session_cookies[name] = value
                        print(f"    Cookie (header): {name}={value[:30]}...")

    except Exception as e:
        print(f"  [WARN] curl_cffi request failed: {e}")
        return {}

    if not session_cookies:
        print("  [INFO] No cookies from curl_cffi")
        return {}

    return session_cookies


def save_to_supabase(rest, api_key, value, is_seed=False):
    patch_url = f"{rest}/app_settings?key=eq.dvr_settings"
    result = supabase_request("PATCH", patch_url, api_key, {"value": value})

    if result is not None and result != []:
        label = "seeded" if is_seed else "saved"
        print(f"  [OK] Cookies {label} to Supabase")
    else:
        label = "seed" if is_seed else "save"
        print(f"  Row may not exist, trying INSERT for {label}...")
        result = supabase_request(
            "POST",
            f"{rest}/app_settings",
            api_key,
            {"key": "dvr_settings", "value": value},
        )
        if result is not None:
            print(f"  [OK] Cookies {label}d into Supabase")
        else:
            print(f"  [ERROR] Failed to {label} cookies to Supabase")
            if not is_seed:
                sys.exit(1)

    if is_seed and result is not None:
        print("  Now proceeding to refresh cookies...")


def main():
    print("=" * 50)
    print("  Cookie Refresher")
    print("=" * 50)

    load_dotenv(".env")

    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    supabase_key = os.environ.get("SUPABASE_API_KEY", "")

    if not supabase_url or not supabase_key:
        print("  [SKIP] SUPABASE_URL or SUPABASE_API_KEY not set")
        return

    rest = f"{supabase_url}/rest/v1"
    get_url = f"{rest}/app_settings?key=eq.dvr_settings&select=value"

    # --- Load current cookies from Supabase ---
    print("\n[1/3] Loading current cookies from Supabase...")
    settings = supabase_request("GET", get_url, supabase_key)

    cookie_str = ""
    user_agent = os.environ.get("USER_AGENT", "")

    if settings and len(settings) > 0:
        val = settings[0].get("value", {})
        cookie_str = val.get("cookies", "")
        if not user_agent:
            user_agent = val.get("user_agent", "")

    # --- If no cookies in Supabase, seed from .env ---
    if not cookie_str:
        env_cookies = os.environ.get("COOKIES", "")
        if env_cookies:
            print("  No cookies in Supabase — seeding from .env...")
            cookie_str = env_cookies
            if not user_agent:
                user_agent = os.environ.get(
                    "USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/146.0.0.0 Safari/537.36",
                )
            seed_value = {
                "cookies": cookie_str,
                "user_agent": user_agent,
            }
            for key in ("sessionid", "csrftoken", "cf_clearance"):
                val = extract_single_cookie(cookie_str, key)
                if val:
                    seed_value[key] = val
            save_to_supabase(rest, supabase_key, seed_value, is_seed=True)
        else:
            print("  [SKIP] No cookies found in Supabase or .env")
            return

    if not user_agent:
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        )
    print(f"  Resolved User-Agent: {user_agent}")
    print(f"UA_EXTRACTED={user_agent}")

    old = parse_cookies(cookie_str)
    print(f"  Loaded {len(old)} cookies")
    print(f"  sessionid: {'[OK]' if 'sessionid' in old else '[NO]'}")
    print(f"  csrftoken: {'[OK]' if 'csrftoken' in old else '[NO]'}")
    print(f"  cf_clearance: {'[OK]' if 'cf_clearance' in old else '[NO]'}")
    print("  Proxy: [NO] (direct connection)")

    # --- Try to refresh cookies ---
    print("\n[2/3] Refreshing cookies...")

    new_cookies = try_refresh_with_curl_cffi(user_agent)

    # --- Merge and save ---
    print("\n[3/3] Merging cookies...")

    merged = dict(old)
    refreshed = False

    if new_cookies:
        for key in ("cf_clearance", "__cf_bm", "__cfruid", "sessionid", "csrftoken"):
            if key in new_cookies and new_cookies[key]:
                old_val = merged.get(key, "")
                new_val = new_cookies[key]
                if new_val != old_val:
                    merged[key] = new_val
                    refreshed = True

        old_cf = old.get("cf_clearance", "")
        new_cf = merged.get("cf_clearance", "")
        if new_cf and new_cf != old_cf:
            print(f"  cf_clearance refreshed: ...{new_cf[-20:]}")
        elif new_cf:
            print(f"  cf_clearance unchanged (still valid)")
        else:
            print(f"  [INFO] No new cf_clearance from curl_cffi")
    else:
        print("  [INFO] Keeping existing cookies (no new cookies obtained)")

    print(f"  Total cookies: {len(merged)}")
    print(f"  sessionid: {'[OK]' if 'sessionid' in merged else '[NO]'}")
    print(f"  csrftoken: {'[OK]' if 'csrftoken' in merged else '[NO]'}")
    print(f"  cf_clearance: {'[OK]' if 'cf_clearance' in merged else '[NO]'}")

    new_cookie_str = join_cookies(merged)

    # --- Write back to Supabase ---
    print("\nSaving to Supabase...")

    settings_value = {
        "cookies": new_cookie_str,
        "user_agent": user_agent,
    }
    for key in ("sessionid", "csrftoken", "cf_clearance"):
        if key in merged:
            settings_value[key] = merged[key]

    save_to_supabase(rest, supabase_key, settings_value)

    if refreshed:
        print("\n[OK] Cookies refreshed successfully")
    else:
        print("\n[OK] Cookies preserved (existing values)")


if __name__ == "__main__":
    main()
