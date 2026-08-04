import json, subprocess, sys

RUN = '30803208117'
REPO = 'lawdachuss/node-16'

# Get job IDs
r = subprocess.run(['gh', 'run', 'view', RUN, '--repo', REPO, '--json', 'jobs'],
                   capture_output=True, text=True)
jobs = json.loads(r.stdout)['jobs']
print(f'jobs: {len(jobs)}')
for j in jobs:
    print(f'  job {j["databaseId"]}: {j["name"]} ({j["status"]})')

# Fetch full log for the rdp job
job_id = jobs[0]['databaseId']
r2 = subprocess.run(
    ['gh', 'api', f'repos/{REPO}/actions/jobs/{job_id}/logs', '--jq', '.'],
    capture_output=True, text=True, errors='replace'
)
if r2.returncode != 0:
    print('api failed:', r2.stderr[:300])
    sys.exit(1)

lines = r2.stdout.splitlines()
print(f'\nlog lines: {len(lines)}')

# Find the Grab step section boundaries
start = None
for i, line in enumerate(lines):
    if 'Grab fresh cookies (browser)' in line and start is None:
        start = i
    if start is not None and i > start and 'Keep Runner Alive' in line:
        end = i
        break

if start is None:
    print('grab step not found in log!')
    # print last 30 lines to debug
    for l in lines[-30:]:
        print(l[:200])
    sys.exit(0)

# Print the grab step section, stripped of the gh-log prefix (timestamp \t stepname \t msg)
print('\n=== Grab fresh cookies step output ===')
for line in lines[start:end]:
    parts = line.split('\t', 2)
    msg = parts[-1] if len(parts) == 3 else line
    print(msg)