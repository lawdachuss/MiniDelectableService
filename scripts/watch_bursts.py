#!/usr/bin/env python3
"""Burst detector for channel_logs — flags synchronized per-node / fleet-wide
failure bursts so they're diagnosable in real time.

A *burst* is the signature of a node-wide session event (Chaturbate session
invalidation, CF block wave, or a fleet egress cut). Two levels are detected:

  - NODE BURST:  >= threshold (default 3) DISTINCT channels on one node hitting
                 the same failure signature within the detection window.
  - FLEET BURST: >= min-nodes (default 2) nodes bursting on the same signature
                 in the same minute — a fleet-wide event worth paging on.

Healthy noise is excluded by design:

  - reconnect / "session expired ... reconnecting" lines are the NORMAL HLS
    session-rotation path (single channels reconnecting, files split but the
    channel never stops). They never count toward a burst unless
    --include-reconnects is passed.
  - offline / private-show / stopped lines are idle state, not failures.

Usage:
  python scripts/watch_bursts.py [--since ISO] [--minutes N] [--threshold N]
      [--window-min M] [--min-nodes N] [--include-reconnects] [--json]
  python scripts/watch_bursts.py --watch SECONDS [--output FILE] [--interval N]

Requires .env with SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (or
SUPABASE_API_KEY). Talks to the postgres-meta SQL endpoint behind Cloudflare
via curl_cffi (Chrome TLS impersonation), same as cookie_refresher.py.
"""

import argparse
import datetime
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Supabase access (curl_cffi chrome impersonation — Cloudflare rejects the
# default urllib TLS fingerprint with error 1010, same as cookie scripts).
# ---------------------------------------------------------------------------


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


def get_client():
    load_dotenv()
    from curl_cffi import requests as cffi_requests

    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_API_KEY", "")
    if not base or not key:
        print("[ERROR] SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_API_KEY) required in .env", file=sys.stderr)
        sys.exit(2)
    # postgres-meta SQL endpoint (executes as postgres, survives Cloudflare WAF
    # with a browser-like client).
    url = base + "/pg/query"

    def sql(query):
        resp = cffi_requests.post(
            url,
            json={"query": query},
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            impersonate="chrome124",
            timeout=60,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"SQL endpoint HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"SQL error: {data['error']}")
        return data

    return sql


# ---------------------------------------------------------------------------
# Classification — which lines are failures vs healthy noise.
# ---------------------------------------------------------------------------

# Failure signatures that count toward a burst (distinct kinds so mixed events
# like "CF block + ended" don't merge into one).
KIND_CF_BLOCK = "cf_block"      # Cloudflare challenge — node session/cookies dead
KIND_CDN_ERROR = "cdn_error"    # 403/404/forbidden on HLS fetch or retry
KIND_ENDED = "ended"            # recording finalized + cleanup (end of a segment)


def classify(message):
    """Return (kind, is_burst_signature). Reconnects are healthy noise by
    default (single-channel session rotation), everything failure-like is a
    signature."""
    m = message or ""
    low = m.lower()
    if "reconnect" in low or "session expired" in low:
        return "reconnect", False
    if "blocked by cloudflare" in low:
        return KIND_CF_BLOCK, True
    if "on retry:" in low or "forbidden" in low or "404" in low or "403" in low or "media endpoint" in low:
        return KIND_CDN_ERROR, True
    if "ended:" in low or "ended (" in low or "cleanup:" in low:
        return KIND_ENDED, True
    if "stopped" in low:
        return "stopped", False
    if "private show" in low:
        return "private", False
    if "offline" in low:
        return "offline", False
    return "other", False


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def detect(sql, since_iso, threshold, window_min, min_nodes, include_reconnects):
    """Scan channel_logs rows since `since_iso`. Returns
    (node_bursts, fleet_bursts, row_count, latest_iso).

    A NODE burst is >= `threshold` DISTINCT channels on one node hitting the
    same signature kind within a sliding `window_min` window (repeated lines
    from one channel — e.g. 3 retries of the same stream — count once).
    A FLEET burst is >= `min_nodes` nodes bursting the same kind in the same
    minute."""
    rows = sql(
        f"""
        select instance_id, username, message, created_at from channel_logs
        where created_at > '{since_iso}'
        order by created_at
        """
    )
    if not rows:
        return [], [], 0, since_iso

    latest = max(r["created_at"] for r in rows)
    window_delta = datetime.timedelta(minutes=window_min)

    # Group per (node, kind) with timestamps for sliding-window counting.
    by_node_kind = defaultdict(list)  # (node, kind) -> [(ts, username)]
    for r in rows:
        kind, is_sig = classify(r["message"])
        if not is_sig and not include_reconnects:
            continue
        if kind == "reconnect" and not include_reconnects:
            continue
        node = r["instance_id"] or "?"
        ts = datetime.datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
        by_node_kind[(node, kind)].append((ts, r["username"]))

    node_bursts = []
    from collections import deque

    for (node, kind), events in sorted(by_node_kind.items()):
        events.sort()
        # Sliding window over (ts, user) events: keep a deque of everything
        # within window_min of the right edge; distinct channels = len(counter).
        # Repeated lines from one channel stay in the window but count once.
        window = deque()
        counts = Counter()
        best_start, best_channels = None, 0
        for ts, user in events:
            window.append((ts, user))
            counts[user] += 1
            while window and window[0][0] < ts - window_delta:
                _, old_user = window.popleft()
                counts[old_user] -= 1
                if counts[old_user] <= 0:
                    del counts[old_user]
            if len(counts) >= threshold and len(counts) > best_channels:
                best_start, best_channels = ts, len(counts)
        if best_start is not None:
            node_bursts.append({
                "node": node,
                "kind": kind,
                "window": best_start.replace(second=0, microsecond=0).isoformat(),
                "channels": best_channels,
            })

    # Fleet burst: >= min_nodes nodes bursting the same kind in the same minute.
    by_minute_kind = defaultdict(set)
    for b in node_bursts:
        by_minute_kind[(b["window"], b["kind"])].add(b["node"])
    fleet_bursts = [
        {"window": w, "kind": k, "nodes": sorted(nodes)}
        for (w, k), nodes in sorted(by_minute_kind.items())
        if len(nodes) >= min_nodes
    ]
    return node_bursts, fleet_bursts, len(rows), latest


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

KIND_LABEL = {
    KIND_CF_BLOCK: "CF block wave",
    KIND_CDN_ERROR: "CDN 403/404 wave",
    KIND_ENDED: "end-of-segment wave",
}


def format_burst(b, fleet=False):
    label = KIND_LABEL.get(b["kind"], b["kind"])
    if fleet:
        return (
            f"FLEET BURST {b['window'][11:16]}Z: {label} on "
            f"{len(b['nodes'])} node(s): {', '.join(b['nodes'])}"
        )
    return (
        f"  node burst {b['node']} {b['window'][11:16]}Z: {label} "
        f"({b['channels']} distinct channels)"
    )


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def one_shot(sql, args):
    since = args.since
    if not since:
        since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=args.minutes)).isoformat()
    since = since if since.endswith("Z") or "+" in since else since + "Z"
    node_bursts, fleet_bursts, nrows, latest = detect(
        sql, since, args.threshold, args.window_min, args.min_nodes, args.include_reconnects
    )
    if args.json:
        print(json.dumps({
            "since": since,
            "rows": nrows,
            "latest": latest,
            "node_bursts": node_bursts,
            "fleet_bursts": fleet_bursts,
        }, indent=2, default=str))
        return
    print(f"scanned {nrows} row(s) since {since}")
    if not fleet_bursts and not node_bursts:
        print("no bursts detected")
        return
    for fb in fleet_bursts:
        print(format_burst(fb, fleet=True))
    for nb in node_bursts:
        print(format_burst(nb))


def watch(sql, args):
    """Live loop: sample every `interval` seconds, append findings to FILE."""
    interval = args.interval
    deadline = time.time() + args.watch
    cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)).isoformat() + "Z"
    seen = set()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a", encoding="utf-8") as f:
        f.write(f"\n[{datetime.datetime.now(datetime.timezone.utc).isoformat()}Z] "
                f"=== burst watch started ({args.watch}s) ===\n")
    print(f"watching channel_logs for {args.watch}s (interval {interval}s); "
          f"finding log: {out}")
    while time.time() < deadline:
        try:
            node_bursts, fleet_bursts, nrows, latest = detect(
                sql, cutoff, args.threshold, args.window_min, args.min_nodes, args.include_reconnects
            )
            if latest > cutoff:
                cutoff = latest
            with out.open("a", encoding="utf-8") as f:
                for fb in fleet_bursts:
                    sig = ("fleet", fb["window"], fb["kind"], tuple(fb["nodes"]))
                    if sig not in seen:
                        seen.add(sig)
                        f.write(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}Z] "
                                f"{format_burst(fb, fleet=True)}\n")
                        print(format_burst(fb, fleet=True), flush=True)
                for nb in node_bursts:
                    sig = ("node", nb["node"], nb["window"], nb["kind"])
                    if sig not in seen:
                        seen.add(sig)
                        f.write(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}Z] "
                                f"{format_burst(nb)}\n")
                        print(format_burst(nb), flush=True)
        except Exception as ex:
            print(f"[error] {ex}", file=sys.stderr)
        time.sleep(interval)
    with out.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat()}Z] "
                f"=== burst watch finished ===\n")
    print("watch finished")


def main():
    ap = argparse.ArgumentParser(
        description="Detect synchronized failure bursts in channel_logs "
                    "(node-wide session cuts / CF block waves / fleet egress cuts)."
    )
    ap.add_argument("--since", help="ISO timestamp to scan from (default: now - --minutes)")
    ap.add_argument("--minutes", type=int, default=60, help="lookback minutes for one-shot mode (default 60)")
    ap.add_argument("--threshold", type=int, default=3, help="distinct channels per node per window to count as a burst (default 3)")
    ap.add_argument("--window-min", type=float, default=2, help="burst window in minutes (default 2)")
    ap.add_argument("--min-nodes", type=int, default=2, help="nodes bursting the same signature in a minute for a FLEET alert (default 2)")
    ap.add_argument("--include-reconnects", action="store_true", help="count reconnect/session-expired lines as burst signatures (default: excluded as healthy HLS rotation)")
    ap.add_argument("--watch", type=int, default=0, help="live-watch for N seconds instead of one-shot")
    ap.add_argument("--interval", type=int, default=60, help="sampling interval for --watch (default 60)")
    ap.add_argument("--output", default="burst_findings.txt", help="findings file for --watch mode")
    ap.add_argument("--json", action="store_true", help="one-shot output as JSON")
    args = ap.parse_args()

    sql = get_client()
    if args.watch > 0:
        watch(sql, args)
    else:
        one_shot(sql, args)


if __name__ == "__main__":
    main()
