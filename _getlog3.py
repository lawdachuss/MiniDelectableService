import subprocess, sys

REPO = 'lawdachuss/node-16'
JOB = '91652458608'

# Raw log fetch (the /logs endpoint returns text/plain)
r = subprocess.run(
    ['gh', 'api', f'repos/{REPO}/actions/jobs/{JOB}/logs', '-H', 'Accept: application/vnd.github+json'],
    capture_output=True, text=True, errors='replace'
)
print('returncode:', r.returncode)
if r.returncode != 0:
    print('stderr:', r.stderr[:400])
    sys.exit(1)

lines = r.stdout.splitlines()
print('log lines:', len(lines))

# Find grab step boundaries
start = None
end = None
for i, line in enumerate(lines):
    if 'Grab fresh cookies' in line and start is None:
        start = i
    elif start is not None and 'Keep Runner Alive' in line:
        end = i
        break

if start is None:
    print('grab step not found. Last 20 lines:')
    for l in lines[-20:]:
        print(' ', l[:200])
    sys.exit(0)
if end is None:
    end = start + 400

print(f'\n=== Grab step section (lines {start}..{end}) ===')
for line in lines[start:end]:
    # gh log format: <timestamp>\t<step>\t<message>
    parts = line.split('\t', 2)
    msg = parts[-1] if len(parts) >= 3 else line
    print(msg)