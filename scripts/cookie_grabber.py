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
  2. Start with a FULLY CLEAN browser (no stored cookies seeded - the refresher
     clears them first). A clean slate every start avoids stale, IP-bound
     cf_clearance cookies that raise the challenge difficulty.
  3. Navigate to the site. When the Turnstile checkbox appears, click it ONCE
     and then wait *quietly* (no spam clicks, no reloads) for Cloudflare to run
     its proof-of-work and auto-redirect to the real site with a fresh
     cf_clearance.
  4. Verify the clearance actually works: fetch a real API endpoint from
     *inside* the browser context (it carries the fresh cookies). HTTP 200
     means the Go client with the same cookies will pass too.
  5. Save the fresh browser cookie set to Supabase (no stale merge).

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

DEFAULT_TIMEOUT = int(os.environ.get("GRAB_TIMEOUT", "150"))  # seconds

STEALTH_JS = """
// Reduce automation fingerprints that Cloudflare's bot score uses.
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
window.chrome = window.chrome || { runtime: {} };
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
  window.navigator.permissions.query = (p) => (
    p.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : originalQuery(p)
  );
}
"""


def save_to_supabase(rest, api_key, value):
    """PATCH the dvr_settings row (INSERT fallback) with the new settings blob.

    Merges cookie fields on top of the existing value so other settings
    (voesx_api_key, streamtape_*, mixdrop_*, etc.) stored in the same row
    are preserved.  Only the keys in `value` are updated.
    """
    import sys
    patch_url = f"{rest}/app_settings?key=eq.dvr_settings"

    # --- Load existing value first so we don't wipe non-cookie settings ---
    get_url = f"{rest}/app_settings?key=eq.dvr_settings&select=value"
    existing = supabase_request("GET", get_url, api_key)
    merged_value = {}
    if existing and len(existing) > 0:
        raw = existing[0].get("value", {})
        if isinstance(raw, dict):
            merged_value = dict(raw)
            print(f"  [OK] Loaded existing settings ({len(merged_value)} keys) — will merge cookie fields on top")
        else:
            print("  [WARN] Existing value is not a dict — will overwrite")
    else:
        print("  [INFO] No existing dvr_settings row found — will INSERT")
    sys.stdout.flush()

    # Merge: cookie fields from `value` overwrite existing, other keys preserved
    merged_value.update(value)

    result = supabase_request("PATCH", patch_url, api_key, {"value": merged_value})
    if result is not None and result != []:
        print("  [OK] Cookies saved to Supabase (merged)")
        sys.stdout.flush()
        return True
    # PATCH returns [] when row exists but Prefer:return=representation has no rows
    # (shouldn't happen, but guard anyway).  Also handles missing-row case.
    print("  Row may not exist or PATCH returned empty, trying INSERT...")
    result = supabase_request(
        "POST", f"{rest}/app_settings", api_key, {"key": "dvr_settings", "value": merged_value}
    )
    if result is not None:
        print("  [OK] Cookies inserted into Supabase")
        sys.stdout.flush()
        return True
    print("  [ERROR] Failed to save cookies to Supabase")
    sys.stdout.flush()
    return False


def launch_browser(p):
    """Launch Chrome *first*, headed if possible.

    CRITICAL: the cf_clearance cookie is bound to the TLS fingerprint of the
    browser that minted it. The Go DVR presents that cookie over httpcloak's
    'chrome-146-windows' TLS fingerprint, so the clearance MUST be minted by a
    real Chrome 146 (whose fingerprint matches) - Edge and newer Chrome builds
    have different TLS fingerprints and Cloudflare rejects the mismatch (403),
    even with a fresh clearance. The workflow installs a pinned Chrome for
    Testing 146 (CHROME146_PATH) so the minting fingerprint matches exactly;
    system Chrome is only a last resort.
    """
    pinned = os.environ.get("CHROME146_PATH", "")
    if pinned and os.path.isfile(pinned):
        for headless in (False, True):
            try:
                b = p.chromium.launch(
                    executable_path=pinned,
                    headless=headless,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--window-size=1366,900",
                    ],
                )
                print(f"  [OK] Launched pinned Chrome 146 (headless={headless})")
                return b
            except Exception as e:
                print(f"  [WARN] pinned Chrome headless={headless} failed: {str(e)[:140]}")
    attempts = []
    for ch in ("chrome", "msedge"):
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
    """Find and click the Turnstile 'Verify you are human' checkbox exactly
    once. Returns True if a click was performed."""
    for fr in page.frames:
        try:
            if "challenges.cloudflare.com" in (fr.url or ""):
                # The widget renders a checkbox (input/label) inside the iframe.
                for sel in (
                    "input[type='checkbox']",
                    "label[for='cf-chl-widget-input']",
                    "#cf-chl-widget-input",
                ):
                    try:
                        el = fr.locator(sel).first
                        if el.count() > 0:
                            el.click(timeout=2500)
                            print("  [OK] Clicked Turnstile checkbox")
                            return True
                    except Exception:
                        continue
                # Fallback: click the widget area center once.
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
    return False


def on_real_site(page):
    """True when the current page is the actual site, not a challenge."""
    try:
        title = (page.title() or "").lower()
        url = page.url or ""
    except Exception:
        return False
    if "just a moment" in title or "enable javascript" in title:
        return False
    if "challenges.cloudflare.com" in url:
        return False
    return "cb.xxx" in url or "chaturbate" in url


def verify_api_via_browser(page):
    """Fetch a real API endpoint from inside the browser context (which carries
    the fresh cookies incl. cf_clearance). Returns True if it gets HTTP 200,
    which proves the clearance is valid for this IP."""
    try:
        result = page.evaluate(
            """async () => {
                try {
                    const r = await fetch('/api/biocontext/kittengirlxo/', {
                        headers: {'Accept': 'application/json'}
                    });
                    const t = await r.text();
                    return {status: r.status, bytes: t.length};
                } catch (e) {
                    return {status: 0, error: String(e)};
                }
            }"""
        )
        print(f"  API verify (biocontext): {result}")
        return isinstance(result, dict) and result.get("status") == 200
    except Exception as e:
        print(f"  [WARN] API verify failed: {e}")
        return False


def solve_challenge(page, ctx, timeout):
    """Click the Turnstile checkbox once, then wait quietly for Cloudflare to
    auto-redirect to the real site with a valid cf_clearance. Returns True on
    success."""
    deadline = time.time() + timeout
    clicked = False
    last_state = ""
    while time.time() < deadline:
        state = "real" if on_real_site(page) else "challenge"
        if state != last_state:
            print(f"  page state: {state} (title='{(page.title() or '')[:50]}')")
            last_state = state
        has_cf = any(c.get("name") == "cf_clearance" for c in ctx.cookies())
        if has_cf and state == "real":
            # Give the proof-of-work a moment to settle, then verify for real.
            time.sleep(3)
            if verify_api_via_browser(page):
                print("  [OK] cf_clearance verified - API returned HTTP 200")
                return True
            print("  [WARN] cf_clearance present but API still challenged - waiting...")
        if not clicked:
            if click_turnstile_checkbox(page):
                clicked = True
                print("  Checkbox clicked - waiting for Cloudflare to verify (no more clicks)...")
        time.sleep(2)
    return False


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
    stored_val = {}
    if settings and len(settings) > 0:
        stored_val = settings[0].get("value", {}) or {}
        if isinstance(stored_val, dict):
            old_str = stored_val.get("cookies", "") or ""
    old = parse_cookies(old_str)
    print(f"  Existing cookies: {len(old)}")

    # Fast path: skip browser solve ONLY if this specific runner already minted a
    # fresh cf_clearance during this same workflow run.  cf_clearance is IP-bound —
    # a token from a previous run (different runner IP) will cause 403.
    import sys
    sys.stdout.flush()
    current_run_id = os.environ.get("GITHUB_RUN_ID", "")
    stored_run_id = stored_val.get("github_run_id", "") if isinstance(stored_val, dict) else ""
    cf_val = old.get("cf_clearance", "")
    print(f"  github_run_id: current={current_run_id!r}  stored={stored_run_id!r}")
    print(f"  cf_clearance: {'len ' + str(len(cf_val)) if cf_val else '[NO]'}")
    sys.stdout.flush()

    if cf_val and len(cf_val) > 20 and current_run_id and current_run_id == stored_run_id:
        print(f"  [OK] cf_clearance from THIS run (run_id={current_run_id}) — skipping browser solve")
        print(f"  sessionid: {'[OK]' if 'sessionid' in old else '[NO]'}")
        print(f"  csrftoken: {'[OK]' if 'csrftoken' in old else '[NO]'}")
        sys.stdout.flush()
        return 0
    if cf_val and len(cf_val) > 20 and stored_run_id != current_run_id:
        print(f"  [INFO] cf_clearance is from a DIFFERENT run (stored={stored_run_id!r}) — must mint fresh one for this runner's IP")
    elif not cf_val or len(cf_val) <= 20:
        print(f"  [INFO] No valid cf_clearance found — launching browser")
    sys.stdout.flush()

    # CRITICAL: the DVR presents these cookies over httpcloak's
    # 'chrome-146-windows' TLS fingerprint, and cf_clearance is bound to the
    # TLS fingerprint + UA of the minting browser. We MUST use a Chrome 146 UA
    # (never Edge's UA - that mismatches the fingerprint and gets 403).
    user_agent = CHROME146_UA

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
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            ctx.add_init_script(STEALTH_JS)

            # FULLY CLEAN start: the refresher already cleared all stored
            # cookies in Supabase, so we seed nothing at all. Every start the
            # browser mints a completely fresh set (csrftoken, __cf_bm, and an
            # IP-bound cf_clearance) - stale cookies only raise the challenge
            # difficulty.

            page = ctx.new_page()
            print(f"  Visiting {site_domain} ...")
            resp = page.goto(site_domain, timeout=60000, wait_until="domcontentloaded")
            print(f"  Initial response: HTTP {resp.status if resp else '?'}")

            # --- Solve the Cloudflare challenge ---
            print(f"\n[3/4] Solving Cloudflare challenge (up to {DEFAULT_TIMEOUT}s)...")
            solved = solve_challenge(page, ctx, DEFAULT_TIMEOUT)
            if solved:
                print("  Challenge fully cleared (cf_clearance valid for this IP)")
            else:
                print(f"  [ERROR] could not obtain a VALID cf_clearance within {DEFAULT_TIMEOUT}s")
                print("  [INFO] Keeping the stored cookie set in Supabase - NOT overwriting with unverified cookies")
                sys.stdout.flush()
                return 1

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

            # --- Merge: browser cookies ARE the fresh set. We deliberately do
            # NOT merge stale IP-bound cookies back in. The ONLY exception is
            # __cf_bm / csrftoken from the refresher's *just-fetched* set (same
            # IP, seconds old) - the clean browser may not have re-issued them.
            merged = dict(browser_cookies)
            for keep in ("__cf_bm", "csrftoken"):
                if keep not in merged and keep in old and old[keep]:
                    merged[keep] = old[keep]
                    print(f"  Kept freshly-refreshed {keep} (browser had none)")
            print(f"\n[4/4] Fresh cookie set: {len(merged)}")

            new_cookie_str = join_cookies(merged)
            settings_value = {
                "cookies": new_cookie_str,
                "user_agent": user_agent or CHROME146_UA,
            }
            for key in ("sessionid", "csrftoken", "cf_clearance", "__cf_bm"):
                if key in merged and merged[key]:
                    settings_value[key] = merged[key]
            # Tag with run_id so the fast-path can confirm this cf_clearance was
            # minted during THIS workflow run (i.e. by THIS runner's IP).
            if current_run_id:
                settings_value["github_run_id"] = current_run_id

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
