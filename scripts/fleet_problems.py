#!/usr/bin/env python3
"""Fleet-wide problem scanner for chaturbate-dvr.

Queries Supabase (postgres-meta /pg/query behind Cloudflare via curl_cffi)
and surfaces diverse problem patterns across the 18-node fleet:

  1. Top error-message clusters (normalized) in channel_logs over a window
  2. Per-node anomaly: nodes with disproportionately many errors
  3. Pipeline stages stuck / failed / high retry (pipeline_states)
  4. Upload-journal failures (upload_journal)
  5. Nodes offline / stale heartbeats / draining
  6. Recordings stuck without links (no embed / no upload_links)
  7. ffprobe / corruption signatures
  8. Disk usage anomalies

Usage:
  python scripts/fleet_problems.py [--minutes 360] [--json]
"""

import argparse
import datetime
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

SERVICE_ROLE = "SUPABASE_SERVICE_ROLE_KEY"


def load_dotenv(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip("\"'")
        if k and not os.environ.get(k):
            os.environ[k] = v


def get_client():
    load_dotenv()
    from curl_cffi import requests as cffi_requests
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get(SERVICE_ROLE, "") or os.environ.get("SUPABASE_API_KEY", "")
    if not base or not key:
        print(f"[ERROR] SUPABASE_URL + {SERVICE_ROLE} (or SUPABASE_API_KEY) required in .env", file=sys.stderr)
        sys.exit(2)
    url = base + "/pg/query"

    def sql(query):
        r = cffi_requests.post(
            url, json={"query": query},
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            impersonate="chrome124", timeout=90,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"SQL HTTP {r.status_code}: {r.text[:300]}")
        d = r.json()
        if isinstance(d, dict) and "error" in d:
            raise RuntimeError(f"SQL error: {d['error']}")
        return d
    return sql


# ---------------------------------------------------------------- normalizers

# Normalization: collapse volatile tokens (numbers, uuid, hosts, paths) so
# distinct-but-identical errors cluster into one bucket.
TOKEN = re.compile(
    r"\b\d{4,}\b"          # big numbers (bytes, timestamps, viewer counts)
    r"|\b\d{1,3}(\.\d{1,3}){3}\b"   # IPs
    r"|uuid|url|/tmp/[\w.\-/]+"       # leftovers
    r"|\b[a-f0-9]{8,}\b"    # hashes
)
def normalize(msg):
    m = (msg or "").strip()
    m = TOKEN.sub("#", m)
    m = re.sub(r"\s+", " ", m)
    return m[:180]


def classify_error(msg):
    """Return a short category label for an error line, or None if not an error."""
    m = (msg or "").lower()
    if "deadline exceeded" in m:
        return "timeout"
    if "context canceled" in m:
        return "context_canceled"
    if "failed to" in m or "could not" in m or "error" in m:
        return "generic_error"
    if "forbidden" in m or "403" in m:
        return "forbidden_403"
    if "404" in m:
        return "not_found_404"
    if "cloudflare" in m or "cf_" in m or "challenge" in m:
        return "cloudflare"
    if "stopped" in m or "ended" in m or "cleanup" in m:
        return "stop_end"
    return None


def parse_rfc(ts):
    try:
        return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def iso_utc(dt):
    # Emit a plain 'YYYY-MM-DDTHH:MM:SS+00:00' (no trailing Z to avoid the
    # double-suffix error: "...+00:00:00Z" is not a valid timestamptz literal).
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


# ---------------------------------------------------------------- sections

def top_errors(sql, since_iso):
    rows = sql(
        "select instance_id, message from channel_logs "
        f"where created_at > '{since_iso}' and (message ilike '%error%' or message ilike '%fail%' "
        "or message ilike '%deadline%' or message ilike '%forbidden%' or message ilike '% cloudflare%')"
    )
    buckets = defaultdict(lambda: {"count": 0, "nodes": set()})
    for r in rows:
        msg = r["message"] or ""
        cat = classify_error(msg)
        norm = normalize(msg)
        b = buckets[(cat, norm)]
        b["count"] += 1
        b["nodes"].add(r["instance_id"] or "?")
    ranked = sorted(buckets.items(), key=lambda kv: -kv[1]["count"])
    out = []
    for (cat, norm), b in ranked[:40]:
        out.append({
            "category": cat,
            "normalized": norm,
            "count": b["count"],
            "nodes": sorted(b["nodes"]),
        })
    return out


def per_node_error_rate(sql, since_iso):
    rows = sql(
        "select instance_id, "
        "count(*) as total, "
        "sum(case when message ilike '%error%' or message ilike '%fail%' or message ilike '%deadline%' or message ilike '%forbidden%' or message ilike '%cloudflare%' then 1 else 0 end) as errs "
        f"from channel_logs where created_at > '{since_iso}' group by instance_id order by errs desc"
    )
    return rows


def pipeline_problems(sql):
    return sql(
        "select current_stage, failed, count(*) as n, "
        "max(retries) as max_retries, "
        "sum(case when node_id is null or node_id='' then 1 else 0 end) as null_node "
        "from pipeline_states group by current_stage, failed order by n desc"
    )


def upload_failures(sql, since_iso):
    rows = sql(
        "select host, status, count(*) as n, "
        "count(distinct instance_id) as nodes "
        "from upload_journal "
        f"where created_at > '{since_iso}' group by host, status order by n desc"
    )
    return rows


def upload_failure_examples(sql, since_iso, host=None):
    where = f"status='failed' and created_at > '{since_iso}'"
    if host:
        where += f" and host='{host}'"
    return sql(
        "select host, instance_id, filename, error_msg, created_at "
        f"from upload_journal where {where} order by created_at desc limit 25"
    )


def node_health(sql):
    return sql(
        "select node_id, status, current_load, last_heartbeat, web_url "
        "from nodes order by node_id"
    )


def recordings_stuck(sql, cutoff):
    # Recordings older than cutoff with no embed_url and no upload links.
    return sql(
        "select username, filename, created_at, end_reason, instance_id "
        "from recordings r where r.created_at < '" + cutoff + "' and "
        "(r.embed_url is null or r.embed_url='') "
        "and not exists (select 1 from upload_links ul where ul.recording_id = r.id) "
        "order by r.created_at desc limit 50"
    )


def recordings_no_thumb(sql, cutoff):
    # Recordings older than cutoff with no thumbnail (UI shows blank).
    return sql(
        "select username, filename, created_at, instance_id "
        "from recordings r where r.created_at < '" + cutoff + "' and "
        "(r.thumbnail_url is null or r.thumbnail_url='') "
        "order by r.created_at desc limit 50"
    )


def disk_anomaly(sql):
    return sql(
        "select node_id, total_bytes, free_bytes, percent_used, recorded_at from disk_usage "
        "order by recorded_at desc limit 30"
    )


def stale_claimed_assignments(sql, cutoff):
    """Claimed (owned-but-idle) channel rows whose last_recorded_at is older
    than the cutoff, grouped per owning node.

    A claimed row's last_recorded_at is refreshed every ~30s by the node's
    assignment-sync touch loop (touch_channel_recordings RPC) once the new
    binary is rolled out — so a STALE timestamp means either:
      * owner node heartbeats are fresh  -> touch loop not deployed/broken on
        that node (visibility gap, not an outage), or
      * owner node heartbeat is ALSO stale -> the node is genuinely dead and
        its channels need reclaiming (use node_health / GetDeadNodes).
    Rows with status='recording' are excluded: those refresh via
    mark_channel_recording while actively capturing.
    """
    return sql(
        "select ca.assigned_node as node_id, "
        "count(*) as stale_rows, "
        "max(now() - ca.last_recorded_at) as oldest_age, "
        "n.status as node_status, "
        "n.last_heartbeat as node_hb, "
        "(now() - n.last_heartbeat) as node_hb_age "
        "from channel_assignments ca "
        "left join nodes n on n.node_id = ca.assigned_node "
        "where ca.status = 'claimed' "
        "and (ca.last_recorded_at is null or ca.last_recorded_at < '" + cutoff + "') "
        "group by ca.assigned_node, n.status, n.last_heartbeat "
        "order by stale_rows desc"
    )


# ---------------------------------------------------------------- helpers

def _interval_seconds(val):
    """Best-effort conversion of a Postgres interval (arriving as a string like
    '00:00:15.9' or '1 day, 02:03:04' from /pg/query) into total seconds.
    Returns None when it cannot be parsed."""
    if val is None:
        return None
    if hasattr(val, "total_seconds"):
        return val.total_seconds()
    text = str(val).replace(" ", "")
    days = 0
    if "," in text:
        d, text = text.split(",", 1)
        try:
            days = int(d.rstrip("day"))
        except ValueError:
            return None
    parts = text.split(":")
    if len(parts) != 3:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
        s = float(parts[2])
    except ValueError:
        return None
    return days * 86400 + h * 3600 + m * 60 + s


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=360)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    since = iso_utc(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=args.minutes))
    cutoff = iso_utc(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=30))
    sql = get_client()

    out = {}

    # 2. Node health
    nh = node_health(sql)
    out["node_health"] = nh

    # 3. Pipeline
    try:
        out["pipeline"] = pipeline_problems(sql)
    except Exception as e:
        out["pipeline"] = f"error: {e}"

    # 4. Upload journal
    try:
        out["upload_failures"] = upload_failures(sql, since)
    except Exception as e:
        out["upload_failures"] = f"error: {e}"

    # 5. Per-node error rate
    try:
        out["per_node_error_rate"] = per_node_error_rate(sql, since)
    except Exception as e:
        out["per_node_error_rate"] = f"error: {e}"

    # 1. Top error clusters
    try:
        out["top_error_clusters"] = top_errors(sql, since)
    except Exception as e:
        out["top_error_clusters"] = f"error: {e}"

    # 6. Recordings stuck without links
    try:
        out["recordings_stuck"] = recordings_stuck(sql, cutoff)
    except Exception as e:
        out["recordings_stuck"] = f"error: {e}"

    # 6b. Recordings without thumbnails
    try:
        out["recordings_no_thumb"] = recordings_no_thumb(sql, cutoff)
    except Exception as e:
        out["recordings_no_thumb"] = f"error: {e}"

    # 7. Disk
    try:
        out["disk"] = disk_anomaly(sql)
    except Exception as e:
        out["disk"] = f"error: {e}"

    # 9. Claimed rows with stale last_recorded_at (>30 min)
    try:
        out["stale_claimed"] = stale_claimed_assignments(sql, cutoff)
    except Exception as e:
        out["stale_claimed"] = f"error: {e}"

    # 4b. Sample upload failure messages (top failing hosts)
    try:
        uf = out["upload_failures"]
        if isinstance(uf, list) and uf:
            failing = [u for u in uf if u["status"] == "failed"]
            examples = {}
            for u in sorted(failing, key=lambda x: -x["n"])[:3]:
                examples[u["host"]] = upload_failure_examples(sql, since, u["host"])
            out["upload_failure_examples"] = examples
    except Exception as e:
        out["upload_failure_examples"] = f"error: {e}"

    if args.json:
        print(json.dumps(out, indent=2, default=str))
        return

    print(f"\n=== Node health ({args.minutes} min look) ===")
    for n in nh:
        print(f"  {n['node_id']:<9} {n['status']:<9} load={n['current_load']:<4} hb={n['last_heartbeat']} web={n['web_url']}")

    print(f"\n=== Pipeline states ===")
    if isinstance(out["pipeline"], list):
        for p in out["pipeline"]:
            print(f"  stage={p['current_stage']:<18} failed={str(p['failed']):<5} n={p['n']:<5} max_retry={p['max_retries']} null_node={p['null_node']}")
    else:
        print(" ", out["pipeline"])

    print(f"\n=== Upload journal failures ({args.minutes} min) ===")
    if isinstance(out["upload_failures"], list):
        for u in out["upload_failures"]:
            print(f"  {u['host']:<16} {u['status']:<8} n={u['n']:<6} nodes={u['nodes']}")
    else:
        print(" ", out["upload_failures"])

    if isinstance(out.get("upload_failure_examples"), dict):
        print(f"\n=== Sample failing upload messages ===")
        for host, exs in out["upload_failure_examples"].items():
            print(f"  -- {host} --")
            for e in exs[:6]:
                print(f"    {e['instance_id']} {e['filename'][:34]:<34} {str(e['error_msg'])[:90]}")
            if len(exs) > 6:
                print(f"    ... and {len(exs)-6} more")

    print(f"\n=== Top error clusters ({args.minutes} min) ===")
    if isinstance(out["top_error_clusters"], list):
        for t in out["top_error_clusters"][:30]:
            print(f"  [{t['category']}] x{t['count']:<6} nodes={','.join(t['nodes'])[:40]:<40} {t['normalized']}")
    else:
        print(" ", out["top_error_clusters"])

    print(f"\n=== Per-node error rate ({args.minutes} min) ===")
    if isinstance(out["per_node_error_rate"], list):
        for n in out["per_node_error_rate"]:
            print(f"  {n['instance_id']:<9} total={n['total']:<6} errs={n['errs']}")
    else:
        print(" ", out["per_node_error_rate"])

    print(f"\n=== Recordings stuck (no embed/links, >30min old) ===")
    if isinstance(out["recordings_stuck"], list):
        for r in out["recordings_stuck"]:
            print(f"  {r['username']:<22} {r['filename'][:40]:<40} {r['created_at']} end={r['end_reason']} node={r['instance_id']}")
    else:
        print(" ", out["recordings_stuck"])

    print(f"\n=== Recordings with NO thumbnail (>30min old) ===")
    if isinstance(out["recordings_no_thumb"], list):
        for r in out["recordings_no_thumb"]:
            print(f"  {r['username']:<22} {r['filename'][:40]:<40} {r['created_at']} node={r['instance_id']}")
    else:
        print(" ", out["recordings_no_thumb"])

    print(f"\n=== Disk usage ===")
    if isinstance(out["disk"], list):
        for d in out["disk"]:
            print(f"  {d['node_id']:<9} use%={d['percent_used']} free={d['free_bytes']} total={d['total_bytes']} at={d['recorded_at']}")
    else:
        print(" ", out["disk"])

    print(f"\n=== Claimed rows with stale last_recorded_at (>30 min) ===")
    sc = out.get("stale_claimed")
    if isinstance(sc, list):
        if not sc:
            print("  none — every claimed row was touched within the last 30 min")
        for r in sc:
            node = r["node_id"] or "NULL(orphaned)"
            hb_age = str(r["node_hb_age"])[:9] if r["node_hb_age"] is not None else "?"
            status = (r["node_status"] or "?")[:8]
            hb_secs = _interval_seconds(r["node_hb_age"])
            if hb_secs is not None and hb_secs < 180 and status.startswith("online"):
                verdict = "touch loop NOT deployed/broken on this node (node looks alive)"
            else:
                verdict = "node likely DEAD — reclaim its channels"
            print(f"  {node:<9} stale_rows={r['stale_rows']:<5} oldest={str(r['oldest_age'])[:9]:<10} "
                  f"node_status={status:<9} node_hb_age={hb_age:<10} -> {verdict}")
    else:
        print(" ", sc)


if __name__ == "__main__":
    main()
