#!/usr/bin/env python3
"""Browser-based cookie grabber for the Chaturbate DVR.

The anonymous curl_cffi refresher (cookie_refresher.py) can grab csrftoken and
__cf_bm, but cb.xxx serves a Cloudflare Turnstile challenge to datacenter IPs
(e.g. GitHub Actions runners) that requires real JavaScript execution. Only a
real browser can mint a valid, IP-bound cf_clearance for that runner.

Strategy (defeats the managed Turnstile challenge):
  1. Launch a *headed* browser (Edge/Chrome) - headless browsers are finger-
     printed by Cloudflare and get stuck on the challenge. The GitHub runner
     has an interactive RDP desktop, so headed works there. Falls back to
     headless only if headed cannot open a window.
  2. Seed the browser context with the cookies already stored in Supabase so
     the session looks established (csrftoken, __cf_bm, etc.).
  3. Navigate to the site, then actively click the Turnstile checkbox if it
     appears (iframe[src*=challenges.cloudflare.com]), reload, and wait up to
     GRAB_TIMEOUT for cf_clearance to land.
  4. Verify the clearance actually works by reloading the page and checking
     the HTTP status + absence of the "Just a moment" challenge.
  5. Merge the browser cookie set with the stored one and save to Supabase.

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

DEFAULT_TIMEOUT = int(os.environ.get("GRAB_TIMEOUT", "120"))  # seconds


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
    """Launch Edge/Chrome *headed first* (needed to pass the managed Turnstile
    challenge on datacenter IPs), falling back to headless/bundled Chromium."""
    attempts = []
    for ch in ("msedge", "chrome"):
        for headless in (False, True):
            attempts.append((ch, headless))
            try:
                b = p.chromium.launch(
                    channel=ch,
                    headless=headless,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--window-size=1366,900",
                    ],
                )
                print(f"  [OK] Launched browser: channel={ch} headless={headless}")
                return b
            except Exception as e:
                print(f"  [WARN] launch channel={ch} headless={headless} failed: {str(e)[:140]}")
    try:
        b = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        print("  [OK] Launched bundled Chromium (headless)")
        return b
    except Exception as e:
        print(f"  [ERROR] bundled chromium launch failed: {e}")
        return None


def click_turnstile_checkbox(page):
    """Try to click the Turnstile 'Verify you are human' checkbox. Returns True
    if a click was performed."""
    for fr in page.frames:
        try:
            if "challenges.cloudflare.com" in (fr.url or ""):
                # The checkbox is a label/input inside the widget iframe.
                for sel in (
                    "input[type='checkbox']",
                    "label[for='cf-chl-widget-input']",
                    "#cf-chl-widget-input",
                    "input[type='checkbox']",
                ):
                    el = fr.locator(sel).first
                    if el.count() > 0:
                        try:
                            el.click(timeout=2500)
                            print("  [OK] Clicked Turnstile checkbox")
                            return True
                        except Exception:
                            continue
                # Fallback: click the widget iframe center
                try:
                    box = fr.locator("body").first.bounding_box(timeout=2000)
                    if box:
                        page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                        print("  [OK] Clicked Turnstile widget center")
                        return True
                except Exception:
                    pass
        except Exception:
            continue
    # Some layouts put the checkbox in a top-level iframe with a title
    for fr in page.frames:
        try:
            if "challenge" in (fr.title() or "").lower() or "verify" in (fr.title() or "").lower():
                for sel in ("input[type='checkbox']", "#cf-chl-widget-input"):
                    el = fr.locator(sel).first
                    if el.count() > 0:
                        try:
                            el.click(timeout=2500)
                            print("  [OK] Clicked checkbox in challenge frame")
                            return True
                        except Exception:
                            continue
        except Exception:
            continue
    return False


def wait_for_clearance(page, ctx, timeout):
    """Wait up to `timeout` seconds for cf_clearance to appear, actively
    clicking the Turnstile checkbox and reloading to nudge the challenge."""
    deadline = time.time() + timeout
    reloaded = False
    while time.time() < deadline:
        try:
            title = page.title() or ""
        except Exception:
            title = ""
        has_cf = any(c.get("name") == "cf_clearance" for c in ctx.cookies())
        just_moment = "just a moment" in title.lower() or "enable javascript" in title.lower()
        if has_cf and not just_moment:
            return True, title
        clicked = click_turnstile_checkbox(page)
        if clicked and not reloaded:
            time.sleep(3)
            try:
                page.reload(wait_until="domcontentloaded", timeout=30000)
                reloaded = True
                print("  Reloaded after checkbox click...")
            except Exception:
                pass
        time.sleep(1)
    return False, title


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
                # Seed the session with known-good cookies (csrftoken, __cf_bm)
                # so the browser looks like an established visitor.
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            # Seed the browser with the stored cookies so the session looks
            # established (csrftoken, __cf_bm, etc.). CF cookies are
            # domain-scoped; add_cookies handles the domain.
            try:
                ctx.add_cookies(
                    [
                        {
                            "name": k,
                            "value": v,
                            "domain": ".cb.xxx",
                            "path": "/",
                            "secure": True,
                        }
                        for k, v in old.items()
                        if v
                    ]
                )
                print(f"  [OK] Seeded {len(old)} stored cookies into the browser")
            except Exception as e:
                print(f"  [WARN] cookie seeding failed ({str(e)[:80]})")
            page = ctx.new_page()
            print(f"  Visiting {site_domain} ...")
            resp = page.goto(site_domain, timeout=60000, wait_until="domcontentloaded")
            print(f"  Initial response: HTTP {resp.status if resp else '?'}")

            # --- Wait for any Cloudflare challenge to clear ---
            print(f"\n[3/4] Waiting for Cloudflare challenge to clear (up to {DEFAULT_TIMEOUT}s)...")
            cleared, title = wait_for_clearance(page, ctx, DEFAULT_TIMEOUT)
            if cleared:
                print(f"  Challenge cleared (cf_clearance present, title='{title[:60]}')")
            else:
                print(f"  [WARN] cf_clearance did not appear within {DEFAULT_TIMEOUT}s - grabbing whatever we have")

            # --- Verify: reload and confirm we're NOT on a challenge page ---
            verify_ok = False
            resp2 = None
            try:
                time.sleep(1)
                resp2 = page.goto(site_domain, timeout=45000, wait_until="domcontentloaded")
                title2 = (page.title() or "").lower()
                if resp2 and resp2.status == 200 and "just a moment" not in title2:
                    verify_ok = True
            except Exception:
                pass
            status2 = resp2.status if resp2 is not None else "?"
            print(f"  Verification reload: HTTP {status2} - {'PASS' if verify_ok else 'STILL CHALLENGED'}")

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
