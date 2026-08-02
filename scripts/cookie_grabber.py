#!/usr/bin/env python3
"""Browser-based cookie grabber for the Chaturbate DVR.

The anonymous curl_cffi refresher (cookie_refresher.py) can grab csrftoken and
__cf_bm, but cb.xxx serves a Cloudflare Turnstile challenge to datacenter IPs
(e.g. GitHub Actions runners) that requires real JavaScript execution. Only a
real browser can mint a valid, IP-bound cf_clearance for that runner.

This script launches Playwright with the system-installed Edge or Chrome,
loads the configured site, lets any challenge auto-clear (or waits), collects
the full cookie set (incl. cf_clearance), merges it with the cookies already
stored in Supabase, and writes the result back to app_settings.dvr_settings.

Best-effort: on any failure it exits non-zero and the DVR continues with the
cookies it already has.

Requires: pip install playwright  (uses system Edge/Chrome - no browser download)

Usage: python scripts/cookie_grabber.py
"""

import io
import json
import os
import sys
import time
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Reuse helpers from the refresher (load_dotenv, Supabase REST via curl_cffi,
# cookie parse/join). Import is guarded so the grabber still runs if the
# refresher module structure changes.
try:
    from cookie_refresher import (  # type: ignore
        load_dotenv,
        parse_cookies,
        join_cookies,
        supabase_request,
        extract_single_cookie,
    )
except Exception as e:  # pragma: no cover
    sys.stderr.write(f"  [ERROR] could not import cookie_refresher helpers: {e}\n")
    sys.exit(1)


CHROME146_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

# Buttons commonly used to dismiss the site's age-verification modal.
AGE_BUTTONS = [
    "Enter",
    "I Agree",
    "AGREE",
    "Yes, I am 18+",
    "I am 18 or older",
    "Continue",
]


def save_to_supabase(rest, api_key, value):
    """PATCH the dvr_settings row (INSERT fallback) with the new settings blob."""
    patch_url = f"{rest}/app_settings?key=eq.dvr_settings"
    result = supabase_request("PATCH", patch_url, api_key, {"value": value})
    if result is not None and result != []:
        print("  [OK] Cookies saved to Supabase")
        return True
    print("  Row may not exist, trying INSERT...")
    result = supabase_request(
        "POST", f"{rest}/app_settings", api_key, {"key": "dvr_settings", "value": value}
    )
    if result is not None:
        print("  [OK] Cookies inserted into Supabase")
        return True
    print("  [ERROR] Failed to save cookies to Supabase")
    return False


def launch_browser(p):
    """Launch Edge, Chrome, or bundled Chromium - first one that works."""
    channels = []
    for ch in ("msedge", "chrome"):
        try:
            b = p.chromium.launch(
                channel=ch,
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            print(f"  [OK] Launched browser channel: {ch}")
            return b
        except Exception as e:
            channels.append((ch, str(e)[:120]))
    try:
        b = p.chromium.launch(headless=True)
        print("  [OK] Launched bundled Chromium")
        return b
    except Exception as e:
        for ch, err in channels:
            print(f"  [WARN] launch '{ch}' failed: {err}")
        print(f"  [ERROR] bundled chromium launch failed: {e}")
        return None


def main():
    print("=" * 50)
    print("  Cookie Grabber (Playwright)")
    print("=" * 50)

    load_dotenv(".env")

    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    api_key = os.environ.get("SUPABASE_API_KEY", "")
    site_domain = os.environ.get("DOMAIN", "https://www.cb.xxx/").rstrip("/") + "/"

    if not supabase_url or not api_key:
        print("  [SKIP] SUPABASE_URL or SUPABASE_API_KEY not set")
        return 0

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  [WARN] playwright not installed (pip install playwright) - skipping browser grab")
        return 1

    rest = f"{supabase_url}/rest/v1"
    get_url = f"{rest}/app_settings?key=eq.dvr_settings&select=value"

    # --- Load existing cookies from Supabase ---
    print("\n[1/4] Loading current cookies from Supabase...")
    settings = supabase_request("GET", get_url, api_key)
    old_str = ""
    user_agent = os.environ.get("USER_AGENT", "")
    if settings and len(settings) > 0:
        val = settings[0].get("value", {})
        if isinstance(val, dict):
            old_str = val.get("cookies", "") or ""
            if not user_agent:
                user_agent = val.get("user_agent", "") or ""
    old = parse_cookies(old_str)
    print(f"  Existing cookies: {len(old)}")

    # --- Launch browser and visit the site ---
    print("\n[2/4] Launching browser...")
    with sync_playwright() as p:
        browser = launch_browser(p)
        if browser is None:
            print("  [ERROR] No usable browser - keeping existing cookies")
            return 1
        try:
            ctx = browser.new_context(
                user_agent=user_agent or CHROME146_UA,
                locale="en-US",
                viewport={"width": 1366, "height": 900},
            )
            page = ctx.new_page()
            print(f"  Visiting {site_domain} ...")
            page.goto(site_domain, timeout=60000, wait_until="domcontentloaded")

            # --- Wait for any Cloudflare challenge to clear ---
            print("\n[3/4] Waiting for Cloudflare challenge to clear...")
            cleared = False
            for i in range(45):
                title = ""
                try:
                    title = page.title() or ""
                except Exception:
                    pass
                has_cf = any(c.get("name") == "cf_clearance" for c in ctx.cookies())
                if has_cf and "just a moment" not in title.lower():
                    cleared = True
                    print(f"  Challenge cleared after ~{i}s (cf_clearance present)")
                    break
                time.sleep(1)
            if not cleared:
                print("  [WARN] Challenge did not auto-clear within 45s - grabbing whatever we have")

            # --- Dismiss the age-verification modal if present ---
            try:
                for label in AGE_BUTTONS:
                    btn = page.get_by_role("button", name=label)
                    if btn.count() > 0 and btn.first.is_visible(timeout=1500):
                        btn.first.click(timeout=3000)
                        print(f"  [OK] Dismissed age modal via '{label}'")
                        time.sleep(2)
                        break
            except Exception:
                pass

            time.sleep(2)

            # --- Collect cookies ---
            browser_cookies = {}
            for c in ctx.cookies():
                name = c.get("name")
                if name:
                    browser_cookies[name] = c.get("value", "")
            print(f"  Browser cookies: {len(browser_cookies)}")

            cf = browser_cookies.get("cf_clearance", "")
            print(f"  cf_clearance: {'[OK] len ' + str(len(cf)) if cf else '[NO]'}")
            print(f"  csrftoken: {'[OK]' if browser_cookies.get('csrftoken') else '[NO]'}")
            print(f"  __cf_bm: {'[OK]' if browser_cookies.get('__cf_bm') else '[NO]'}")
            print(f"  sessionid: {'[OK]' if browser_cookies.get('sessionid') else '[NO]'}")

            # --- Merge: browser cookies win for the CF-critical keys ---
            merged = dict(old)
            merged.update(browser_cookies)
            print(f"\n[4/4] Merged cookies: {len(merged)}")

            new_cookie_str = join_cookies(merged)
            settings_value = {
                "cookies": new_cookie_str,
                "user_agent": user_agent or CHROME146_UA,
            }
            for key in ("sessionid", "csrftoken", "cf_clearance", "__cf_bm"):
                if key in merged and merged[key]:
                    settings_value[key] = merged[key]

            ok = save_to_supabase(rest, api_key, settings_value)
            if ok:
                print("\n[OK] Cookie grab succeeded - fresh cookies saved to Supabase")
                return 0
            return 1
        finally:
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
