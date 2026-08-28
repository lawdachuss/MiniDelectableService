# prepare-cookies.ps1
#
# Runs every pre-DVR network task in PARALLEL background jobs, then mints a
# fresh cf_clearance cookie once everything is ready:
#   1. Fix Supabase DNS (public resolvers + hosts pin)      [background job]
#   2. Install cookie deps (pip + playwright chromium)      [background job]
#   3. Install pinned Chrome 146 (cf_clearance fingerprint) [background job]
#   4. Pre-warm the scrapling browser, then run cookie_grabber.py
#
# Every step is best-effort: failures are logged but never fatal, so the DVR
# can still start with whatever cookies are already stored in Supabase/.env.

$ErrorActionPreference = 'Continue'
$repoDir = $env:REPO_DIR
if (-not $repoDir) { $repoDir = "$env:GITHUB_WORKSPACE" }

# ========== JOB 1: Fix Supabase DNS ==========
$dnsJob = Start-Job -Name dns -ArgumentList ((($env:SUPABASE_URL -replace '^https?://', '') -split '[/:]')[0]) -ScriptBlock {
  param($sbHost)
  $ErrorActionPreference = 'Continue'
  if ([string]::IsNullOrWhiteSpace($sbHost) -or $sbHost -eq '-' -or $sbHost.Length -le 2) { $sbHost = 'supabase.chuglii.in' }
  Write-Host "(DNS) Supabase host: $sbHost"
  try {
    Get-NetAdapter -Physical -ErrorAction SilentlyContinue |
      Where-Object { $_.Status -eq 'Up' -and $_.Name } |
      ForEach-Object {
        $idx = $_.ifIndex
        try {
          $dns = Get-DnsClientServerAddress -InterfaceIndex $idx -ErrorAction SilentlyContinue
          if (-not $dns) {
            Write-Host "(DNS) Skipping $($_.Name) (idx $idx) — no DNS config support"
            return
          }
          Set-DnsClientServerAddress -InterfaceIndex $idx -ServerAddresses ('1.1.1.1', '8.8.8.8') -ErrorAction Stop | Out-Null
          Write-Host "(DNS) Set $($_.Name) DNS -> 1.1.1.1, 8.8.8.8"
        } catch { Write-Warning "(DNS) DNS set skipped on $($_.Name): $($_.Exception.Message)" }
      }
  } catch { Write-Warning "(DNS) DNS enumeration failed: $($_.Exception.Message)" }
  ipconfig /flushdns | Out-Null
  $hostsPath = "$env:SystemRoot\System32\drivers\etc\hosts"
  try {
    $hosts = Get-Content $hostsPath -Raw -ErrorAction SilentlyContinue
    $e1 = "104.21.25.61 $sbHost"; $e2 = "172.67.223.51 $sbHost"
    $need = @()
    if (-not $hosts -or $hosts -notmatch [regex]::Escape($e1)) { $need += $e1 }
    if (-not $hosts -or $hosts -notmatch [regex]::Escape($e2)) { $need += $e2 }
    if ($need.Count -gt 0) {
      Add-Content -Path $hostsPath -Value ("`n" + ($need -join "`n") + "`n") -ErrorAction Stop
      Write-Host "(DNS) Pinned: $($need -join ' | ')"
    } else { Write-Host "(DNS) Hosts entry already present" }
  } catch { Write-Warning "(DNS) hosts write failed: $($_.Exception.Message)" }
  Start-Sleep -Seconds 2
  try {
    $code = & curl.exe -s -o NUL -w "%{http_code}" "https://$sbHost/rest/v1/" -m 20 --noproxy "*" --tlsv1.2 2>$null
    Write-Host "(DNS) Supabase reachability check: HTTP $code"
  } catch { Write-Host "(DNS) Supabase reachability check threw: $($_.Exception.Message)" }
}

# ========== JOB 2: Cookie deps (pip + playwright browsers) ==========
$depsJob = Start-Job -Name deps -ArgumentList $repoDir -ScriptBlock {
  param($d)
  $ErrorActionPreference = 'Continue'
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $pipOk = $false
  for ($i = 1; $i -le 3 -and -not $pipOk; $i++) {
    Write-Host "[DEPS] pip install attempt $i"
    try { python -m pip install --quiet --disable-pip-version-check -r "$d\requirements.txt" } catch {}
    if ($LASTEXITCODE -eq 0) { $pipOk = $true }
    if (-not $pipOk) {
      try { py -3 -m pip install --quiet --disable-pip-version-check -r "$d\requirements.txt" } catch {}
      if ($LASTEXITCODE -eq 0) { $pipOk = $true }
    }
    if (-not $pipOk) { Write-Warning "[DEPS] pip install attempt $i failed - retrying in 5s"; Start-Sleep -Seconds 5 }
  }
  if (-not $pipOk) { Write-Warning "[DEPS] pip install failed after 3 attempts" }
  try {
    python -c "import curl_cffi; print('curl_cffi OK', curl_cffi.__version__); import playwright; print('playwright OK'); from scrapling.fetchers import StealthySession; print('scrapling OK')"
    if ($LASTEXITCODE -ne 0) { Write-Warning "[DEPS] import check failed - scrapling/playwright may be missing" }
  } catch { Write-Warning "[DEPS] import check error: $($_.Exception.Message)" }
  $chromiumOk = $false
  for ($i = 1; $i -le 3 -and -not $chromiumOk; $i++) {
    Write-Host "[PLAYWRIGHT] Chromium install attempt $i"
    try { python -m playwright install chromium 2>&1 | Out-Null } catch {}
    if ($LASTEXITCODE -eq 0) { $chromiumOk = $true }
    if (-not $chromiumOk) { Write-Warning "[PLAYWRIGHT] Chromium install attempt $i failed - retrying in 5s"; Start-Sleep -Seconds 5 }
  }
}
# ========== JOB 3: Pinned Chrome 146 (cf_clearance TLS fingerprint) ==========
$chromeJob = Start-Job -Name chrome -ScriptBlock {
  $ErrorActionPreference = 'Continue'
  $dir = "C:\chrome146"
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  $exe = "$dir\chrome-win64\chrome.exe"
  if (-not (Test-Path $exe)) {
    $zip = "$dir\chrome-win64.zip"
    if (-not (Test-Path $zip) -or ((Get-Item $zip).Length -lt 50MB)) {
      Write-Host "[CHROME146] Downloading Chrome for Testing 146.0.7680.165 (~190MB)..."
      & curl.exe -L --fail --retry 3 --retry-delay 5 -o $zip "https://storage.googleapis.com/chrome-for-testing-public/146.0.7680.165/win64/chrome-win64.zip"
      if ($LASTEXITCODE -ne 0 -or -not (Test-Path $zip) -or (Get-Item $zip).Length -lt 50MB) {
        Write-Warning "[CHROME146] curl failed - falling back to Invoke-WebRequest"
        for ($r = 1; $r -le 2; $r++) {
          try {
            Invoke-WebRequest -Uri "https://storage.googleapis.com/chrome-for-testing-public/146.0.7680.165/win64/chrome-win64.zip" -OutFile $zip -UseBasicParsing -TimeoutSec 600
            if ((Get-Item $zip).Length -gt 50MB) { break }
          } catch { Write-Warning "[CHROME146] download attempt $r failed: $($_.Exception.Message)" }
        }
      }
    } else { Write-Host "[CHROME146] Using cached zip" }
    if ((Test-Path $zip) -and ((Get-Item $zip).Length -gt 50MB)) {
      Write-Host "[CHROME146] Extracting..."
      Expand-Archive -Path $zip -DestinationPath $dir -Force
    }
  }
  if (Test-Path $exe) {
    Write-Host "[CHROME146] OK: $exe ($([math]::Round((Get-Item $exe).Length / 1MB, 1)) MB)"
    return $exe
  }
  Write-Warning "[CHROME146] Chrome 146 not available - cookie grab will fall back to system Chrome"
  return $null
}

# ========== Wait for all three jobs in parallel ==========
# These tasks are best-effort.  Waiting for them one-by-one accidentally made
# a slow Chrome download hold the whole DVR startup for DNS + deps + Chrome
# timeouts combined.  Use one shared deadline, then continue with the stored
# cookies rather than leave a fleet node offline.
Write-Host "[PREP] Running DNS + deps + Chrome install in parallel (shared 360s cap)..."; $null = [System.Console]::Out.Flush()
$prepDeadline = (Get-Date).AddSeconds(360)
$prepJobs = @($dnsJob, $depsJob, $chromeJob)
while ((@($prepJobs | Where-Object { $_.State -eq 'Running' }).Count -gt 0) -and (Get-Date) -lt $prepDeadline) {
  Start-Sleep -Seconds 5
}
$stillRunning = @($prepJobs | Where-Object { $_.State -eq 'Running' })
if ($stillRunning.Count -gt 0) {
  Write-Warning "[PREP] Shared setup cap reached; stopping $($stillRunning.Count) slow task(s) and continuing"
  $stillRunning | Stop-Job -ErrorAction SilentlyContinue
}
$chromePath = Receive-Job $chromeJob -ErrorAction SilentlyContinue
Receive-Job $dnsJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  [DNS] $_" }
Receive-Job $depsJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  [DEPS] $_" }
Receive-Job $chromeJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  [CHROME] $_" }
Remove-Job $dnsJob, $depsJob, $chromeJob -Force -ErrorAction SilentlyContinue

$env:CHROME146_PATH = $null
if ($chromePath -and (Test-Path $chromePath)) {
  $env:CHROME146_PATH = $chromePath
  Write-Host "[PREP] CHROME146_PATH=$chromePath"
  try { if ($env:GITHUB_ENV) { echo "CHROME146_PATH=$chromePath" >> $env:GITHUB_ENV } } catch { }
}

# ========== Pre-warm scrapling browser (cap 240s) ==========
Write-Host "[SCRAPLING] Pre-warming scrapling browser (cap 240s)..."
try {
  $warmScript = "$env:TEMP\_scrap_warmup.py"
  Set-Content $warmScript -Value "import os, sys; from scrapling.fetchers import StealthySession; kw={}; exe=os.environ.get('CHROME146_PATH',''); kw['executable_path']=exe if exe and os.path.isfile(exe) else None; kw={k:v for k,v in kw.items() if v}; s=StealthySession(headless=True, **kw); s.start(); print('warmup: browser launched OK'); s.close()" -Encoding utf8
  $warmJob = Start-Job -ScriptBlock { param($f) python -u $f 2>&1 } -ArgumentList $warmScript
  if (-not (Wait-Job $warmJob -Timeout 240)) {
    Write-Warning "[SCRAPLING] warm-up timed out - first grab will pay the cost"
    Stop-Job $warmJob -ErrorAction SilentlyContinue
  } else {
    Receive-Job $warmJob | ForEach-Object { Write-Host "  [SCRAP] $_" }
  }
  Remove-Job $warmJob -Force -ErrorAction SilentlyContinue
  Remove-Item $warmScript -Force -ErrorAction SilentlyContinue
} catch { Write-Warning "[SCRAPLING] warm-up failed: $($_.Exception.Message)" }

# ========== Grab fresh cookies (retry up to 3x; hard cap per attempt) ==========
Set-Location $repoDir
$env:PYTHONUNBUFFERED = '1'
$env:GRAB_TOTAL_TIMEOUT = '240'
$capSec = 240
$maxAttempts = 3
$grabSucceeded = $false
$grabExit = 1
for ($attempt = 1; $attempt -le $maxAttempts -and -not $grabSucceeded; $attempt++) {
  Write-Host "[COOKIE] Running cookie_grabber.py (attempt $attempt of $maxAttempts, cap ${capSec}s)"
  $preGrab = @(Get-Process chrome,msedge,camoufox,firefox -ErrorAction SilentlyContinue | ForEach-Object { $_.Id })
  $sw = [Diagnostics.Stopwatch]::StartNew()
  $grabJob = Start-Job -ScriptBlock {
    param($dir)
    Set-Location $dir
    $env:PYTHONUNBUFFERED = '1'
    & python -u scripts/cookie_grabber.py 2>&1
    Write-Output "__EXITCODE__$LASTEXITCODE"
  } -ArgumentList $repoDir
  $lines = @()
  while ($grabJob.State -eq 'Running' -and $sw.Elapsed.TotalSeconds -lt $capSec) {
    $out = @(Receive-Job $grabJob -ErrorAction SilentlyContinue)
    if ($out.Count -gt $lines.Count) {
      $out[$lines.Count..($out.Count - 1)] | ForEach-Object { if ($_ -notmatch '^__EXITCODE__') { Write-Host "  [CG] $_" } }
      $lines = $out
    }
    Start-Sleep -Seconds 2
  }
  $rem = @(Receive-Job $grabJob -ErrorAction SilentlyContinue)
  foreach ($_ in $rem) { if ($_ -notmatch '^__EXITCODE__') { Write-Host "  [CG] $_" } }
  $capped = $sw.Elapsed.TotalSeconds -ge $capSec
  if ($capped) {
    Write-Warning "[COOKIE] attempt $attempt exceeded ${capSec}s hard cap - stopping job"
    Stop-Job $grabJob -ErrorAction SilentlyContinue
  }
  $grabExit = 1
  foreach ($_ in ($lines + $rem)) { if ($_ -match '__EXITCODE__(\d+)') { $grabExit = [int]$matches[1] } }
  Remove-Job $grabJob -Force -ErrorAction SilentlyContinue
  Get-Process chrome,msedge,camoufox,firefox -ErrorAction SilentlyContinue |
    Where-Object { $preGrab -notcontains $_.Id } |
    Stop-Process -Force -ErrorAction SilentlyContinue
  Write-Host "[COOKIE] attempt $attempt finished (exit $grabExit, capped=$capped)"
  if ($grabExit -eq 0) { $grabSucceeded = $true }
  if (-not $grabSucceeded -and $attempt -lt $maxAttempts) { Write-Host "[COOKIE] attempt $attempt failed - retrying in 5s"; Start-Sleep -Seconds 5 }
}
if (-not $grabSucceeded) {
  Write-Warning "[COOKIE] cookie_grabber.py failed after $maxAttempts attempts - DVR will use existing cookies from Supabase"
} else {
  Write-Host "[COOKIE] Fresh cf_clearance saved to Supabase - DVR will load it on startup"
}
Write-Host "[PREP] Done"
