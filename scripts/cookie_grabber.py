#!/usr/bin/env python3
"""Browser-based cookie grabber for the Chaturbate DVR.

The anonymous curl_cffi refresher (cookie_refresher.py) can grab csrftoken and
__cf_bm, but cb.xxx serves a Cloudflare Turnstile challenge to datacenter IPs
(e.g. GitHub Actions runners) that requires real JavaScript execution. Only a
real browser can mint a valid, IP-bound cf_clearance for that runner.

Strategy (defeats the managed Turnstile challenge):
  PRIMARY: Scrapling's StealthySession with solve_cloudflare=True - a purpose-
     built automatic solver that handles managed/interactive/invisible Turnstile
     challenges (same approach as the working node-3 deployment, but targeted at
     cb.xxx instead of the old chaturbate.com). It is pointed at the pinned
     Chrome for Testing 146 via executable_path so the TLS fingerprint + UA that
     mint cf_clearance match httpcloak's 'chrome-146-windows' preset exactly
     (cf_clearance is bound to the minting fingerprint). The clearance is
     verified with an API fetch from the SAME browser session (HTTP 200).
  FALLBACK: Playwright manual Turnstile checkbox clicking (headed browser), kept
     for environments where Scrapling is not installed.
  Both paths:
  1. Start with a FULLY CLEAN browser (no stored cookies seeded - the refresher
     clears them first). A clean slate every start avoids stale, IP-bound
     cf_clearance cookies that raise the challenge difficulty.
  2. Verify the clearance actually works: fetch a real API endpoint from
     *inside* the browser context (it carries the fresh cookies). HTTP 200
     means the Go client with the same cookies will pass too.
  3. Save the fresh browser cookie set to Supabase (no stale merge).

Best-effort: on any failure it exits non-zero and the DVR continues with the
cookies it already has.

Requires: pip install "scrapling[fetchers]" playwright
  (uses pinned Chrome 146 via CHROME146_PATH when present, else system
   Edge/Chrome - no extra browser download)

Usage: python scripts/cookie_grabber.py
"""

import io
import json
import os
import sys
import threading
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

# Hard wall-clock budget for the ENTIRE grab. Scrapling's solve_cloudflare
# solver does NOT honor the per-call timeout as a bound for managed Turnstile
# challenges - it loops internally for many minutes (observed: 25+ min per
# attempt) before its bounding_box locator times out, and retries multiply
# that. This watchdog force-exits the process so a CI step (or the DVR's
# startup cookie refresh in main.go) can never hang for tens of minutes.
# Override with GRAB_TOTAL_TIMEOUT (seconds).
GRAB_TOTAL_TIMEOUT = int(os.environ.get("GRAB_TOTAL_TIMEOUT", "480"))

# Budget for a single scrapling solve attempt (whole solve, incl. verify).
SCRAPLING_BUDGET = int(os.environ.get("SCRAPLING_BUDGET", "150"))


def install_watchdog():
    """Force-exit the whole process after GRAB_TOTAL_TIMEOUT seconds.

    The only reliable bound: scrapling/playwright will happily ignore per-call
    timeouts while a managed Turnstile challenge loops. On Windows, first
    taskkill the whole process tree (this python + any child browsers the
    solver spawned) so orphaned Camoufox/Chrome can't linger during a long
    DVR session, then os._exit as the guarantee.
    """

    def _boom():
        sys.stderr.write(
            f"\n  [FATAL] cookie_grabber exceeded {GRAB_TOTAL_TIMEOUT}s total budget "
            "- force exiting (DVR will keep existing cookies)\n"
        )
        sys.stderr.flush()
        if os.name == "nt":
            # Kill self + child browsers. Ignore failure; os._exit is the fallback.
            os.system(f"taskkill /PID {os.getpid()} /T /F >NUL 2>&1")
        os._exit(1)

    t = threading.Timer(GRAB_TOTAL_TIMEOUT, _boom)
    t.daemon = True  # must not block a fast-path exit
    t.start()


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


def _is_managed_challenge(page):
    """Detect a Cloudflare 'managed' Turnstile challenge.

    cb.xxx serves this mode to datacenter IPs (GitHub Actions runners), and a
    managed challenge cannot be cleared by manually clicking the checkbox —
    Cloudflare never auto-verifies it. The DVR logs the same discovery
    ("The turnstile version discovered is 'managed'") when it probes the
    challenge config. Positive-only detection: reads window._cf_chl_opt.cType
    (a.k.a. the turnstile mode); if the key is absent or the value differs,
    returns False and the normal solve path runs unchanged.
    """
    try:
        raw = page.evaluate(
            """() => {
                try {
                    const opt = window._cf_chl_opt || {};
                    return JSON.stringify({cType: opt.cType || '', turbo: opt.turbo || ''});
                } catch (e) { return ''; }
            }"""
        )
    except Exception:
        return False
    if not raw:
        return False
    try:
        cfg = json.loads(raw)
    except Exception:
        return False
    needle = (cfg.get("cType") or "").lower()
    return "managed" in needle or "managed" in (cfg.get("turbo") or "").lower()


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
        if state == "challenge" and _is_managed_challenge(page):
            print("  [FAIL] Managed Turnstile challenge detected — manual checkbox clicking can never clear it; abandoning early")
            print("  [INFO] Keeping the stored cookie set in Supabase - NOT overwriting with unverified cookies")
            sys.stdout.flush()
            return False
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


def _browser_pids():
    """Set of PIDs for known browser images (Windows only). Used to kill only
    browsers that appeared DURING a solve, never a user's existing browser."""
    import subprocess

    pids = set()
    try:
        out = subprocess.run(
            ["tasklist", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=20,
        ).stdout
        for line in out.splitlines():
            parts = line.strip('"').split('"' + ',' + '"')
            if len(parts) >= 2 and parts[0].lower() in (
                "chrome.exe", "msedge.exe", "camoufox.exe", "firefox.exe",
            ):
                try:
                    pids.add(int(parts[1].strip('"')))
                except ValueError:
                    pass
    except Exception:
        pass
    return pids


def _kill_new_browsers(pre_pids):
    """taskkill /T /F every browser PID that appeared after the snapshot."""
    import subprocess

    for pid in sorted(_browser_pids() - pre_pids):
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True, timeout=15,
            )
        except Exception:
            pass


def _scrapling_attempt(site_domain, user_agent, timeout, executable):
    """Run one scrapling solve. Executed in a worker thread so the caller can
    bound it with a hard budget (the internal solver ignores per-call timeouts
    on managed Turnstiles and would otherwise loop for many minutes).

    Returns a dict of cookies with a verified cf_clearance, or None on failure.
    """
    from scrapling.fetchers import StealthySession

    # Headed by default on the runner (it has an RDP desktop and Cloudflare
    # finger-prints headless browsers harder). Set SCRAPLING_HEADLESS=1 to
    # force headless on desktop-less hosts.
    headless = os.environ.get("SCRAPLING_HEADLESS", "0") == "1"
    session_kwargs = dict(
        headless=headless,
        solve_cloudflare=True,
        block_webrtc=True,
        hide_canvas=True,
        useragent=user_agent or CHROME146_UA,
        timeout=timeout * 1000,  # >=60s recommended for the CF solver
        retries=1,
        retry_delay=2,
    )
    if executable:
        # Pinned Chrome 146 -> use it via executable_path so the TLS
        # fingerprint that mints cf_clearance matches the DVR exactly.
        # NOTE: do NOT also set real_chrome=True here - that forces
        # channel="chrome" which conflicts with executable_path.
        session_kwargs["executable_path"] = executable
    with StealthySession(**session_kwargs) as session:
        resp = session.fetch(
            site_domain,
            solve_cloudflare=True,
            timeout=timeout * 1000,
            retries=1,
        )
        status = getattr(resp, "status", None)
        print(f"  [SCRAPLING] Page loaded (HTTP {status}) - collecting cookies...")

        cookies = {}
        if isinstance(resp.cookies, tuple):
            for c in resp.cookies:
                if isinstance(c, dict) and "name" in c:
                    cookies[c["name"]] = c["value"]
                elif isinstance(c, dict):
                    cookies.update(c)
        elif isinstance(resp.cookies, dict):
            cookies = dict(resp.cookies)
        print(f"  [SCRAPLING] Got {len(cookies)} cookies")

        if "cf_clearance" not in cookies or not cookies["cf_clearance"]:
            print("  [SCRAPLING] No cf_clearance minted - challenge not solved")
            return None
        print(f"  [SCRAPLING] cf_clearance found! length={len(cookies['cf_clearance'])}")

        # Verify the clearance actually works from inside the same session
        # (it carries the fresh cookies). HTTP 200 = the Go client with the
        # same cookies will pass too.
        verify_url = site_domain.rstrip("/") + "/api/biocontext/kittengirlxo/"
        try:
            vresp = session.fetch(verify_url, timeout=30000)
            vstatus = getattr(vresp, "status", None)
            print(f"  [SCRAPLING] API verify: HTTP {vstatus}")
            if vstatus != 200:
                print("  [SCRAPLING] API verify failed - cf_clearance not valid for this IP")
                return None
        except Exception as e:
            print(f"  [SCRAPLING] API verify threw: {e}")
            return None
        print("  [OK] Scrapling solved challenge + verified cf_clearance via API")
        return cookies


def solve_with_scrapling(site_domain, user_agent, timeout):
    """Solve the Cloudflare challenge using Scrapling's StealthySession.

    Scrapling's solve_cloudflare=True is an automatic solver for
    interactive/invisible Turnstile challenges (the same technique used by the
    working node-3 deployment, which fetched chaturbate.com - here we target
    cb.xxx, the domain the DVR actually uses). WARNING: cb.xxx serves a
    "managed" Turnstile to datacenter IPs that this solver cannot clear - the
    whole attempt is hard-bounded by SCRAPLING_BUDGET + GRAB_TOTAL_TIMEOUT so
    it fast-fails instead of hanging. It is pointed at the pinned Chrome for
    Testing 146 (CHROME146_PATH) so the TLS fingerprint that mints
    cf_clearance matches httpcloak's 'chrome-146-windows' preset exactly.

    Returns a dict of cookies with a verified cf_clearance, or None on failure.
    """
    try:
        from scrapling.fetchers import StealthySession  # noqa: F401  (presence check)
    except ImportError:
        print("  [WARN] scrapling not installed (pip install 'scrapling[fetchers]') - using Playwright fallback")
        return None

    pinned = os.environ.get("CHROME146_PATH", "")
    executable = pinned if pinned and os.path.isfile(pinned) else None
    print(f"  [SCRAPLING] Solving challenge (target={site_domain}, "
          f"executable={executable or 'bundled/stealth browser'}, "
          f"budget={SCRAPLING_BUDGET}s)...")

    # The internal solver ignores per-call timeouts on managed Turnstiles and
    # can loop for many minutes, so bound the WHOLE attempt from here. NOTE:
    # do NOT use `with ThreadPoolExecutor(...)` - its __exit__ calls
    # shutdown(wait=True) and would block forever if the solver thread is
    # stuck. shutdown(wait=False) + daemon workers (Py3.9+) lets the process
    # exit cleanly; the watchdog's taskkill /T cleans up any stray browser.
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FutureTimeoutError

    # Snapshot existing browser PIDs so we can kill only NEW ones (the solver's
    # Camoufox/Chrome) if the budget expires - never a user's own browser.
    pre_browsers = _browser_pids() if os.name == "nt" else set()
    ex = ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(_scrapling_attempt, site_domain, user_agent, timeout, executable)
        try:
            return fut.result(timeout=SCRAPLING_BUDGET)
        except FutureTimeoutError:
            print(f"  [SCRAPLING] solver exceeded {SCRAPLING_BUDGET}s budget - abandoning attempt")
            # The abandoned worker thread still holds a StealthySession browser.
            # Kill browsers that appeared during the solve so nothing leaks into
            # a long DVR session (covers the main.go startup path too, where no
            # workflow cleanup runs).
            if os.name == "nt":
                _kill_new_browsers(pre_browsers)
            return None
    except Exception as e:
        print(f"  [SCRAPLING] solve failed: {str(e)[:200]}")
        return None
    finally:
        ex.shutdown(wait=False)


def main():
    print("=" * 50)
    print("  Cookie Grabber (Playwright)")
    print("=" * 50)

    # Guarantee the whole run is bounded (Scrapling's managed-Turnstile solver
    # ignores per-call timeouts and can loop for 25+ min per attempt).
    install_watchdog()

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

    # --- PRIMARY: Scrapling's automatic Turnstile solver ---
    # Purpose-built solve_cloudflare=True (same as the working node-3
    # deployment, but targeting cb.xxx). Falls back to Playwright below.
    print("\n[2/4] Solving Cloudflare challenge with Scrapling (auto-solver)...")
    scrapling_cookies = solve_with_scrapling(site_domain, user_agent, DEFAULT_TIMEOUT)
    if scrapling_cookies and scrapling_cookies.get("cf_clearance"):
        merged = dict(scrapling_cookies)
        for keep in ("__cf_bm", "csrftoken"):
            if keep not in merged and keep in old and old[keep]:
                merged[keep] = old[keep]
                print(f"  Kept freshly-refreshed {keep} (Scrapling had none)")
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

    # --- FALLBACK: Playwright manual clicking ---
    # --- Launch browser and visit the site ---
    print("\n[2b] Scrapling unavailable/failed - falling back to Playwright...")
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
