import subprocess

# Get the failed-step logs, then filter for cookie-grab lines
r = subprocess.run(
    ['gh', 'run', 'view', '30803208117', '--repo', 'lawdachuss/node-16', '--log-failed'],
    capture_output=True, text=True, errors='replace'
)
print('=== log-failed (exit', r.returncode, ') ===')
if r.returncode != 0:
    print(r.stderr[:500])
    # The step SUCCEEDED, so --log-failed is empty - fall back to full log
    r = subprocess.run(
        ['gh', 'run', 'view', '30803208117', '--repo', 'lawdachuss/node-16', '--log'],
        capture_output=True, text=True, errors='replace'
    )
    print('=== full log tail (exit', r.returncode, ') ===')
    lines = r.stdout.splitlines()
    # Filter lines that look like cookie-grab step output
    wanted = [l for l in lines if any(k in l for k in ('[CG]', '[CGe]', 'SCRAPLING', 'cf_clearance', 'COOKIE', 'finished in', 'Grab fresh'))]
    for l in wanted[-40:]:
        # Strip the gh log prefix (timestamps / step refs): keep last tab-separated field
        parts = l.split('\t')
        print(parts[-1] if parts else l)