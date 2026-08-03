$repoDir = "$env:REPO_DIR"
$rdpPassFile = "$env:TEMP\_rdppass"
$tailscaleOk = "$env:TEMP\_ts_ok"
$tailscaleIpFile = "$env:TEMP\_ts_ip"

# Capture env vars for $using: in jobs (Start-Job can't see $env: directly)
$mTsAuthKey = $env:TAILSCALE_AUTH_KEY
$mRunId = $env:GITHUB_RUN_ID

# ========== JOB: RDP Config + User ==========
$rdpJob = Start-Job -Name rdp -ScriptBlock {
  param($pf)
  Set-ItemProperty 'HKLM:\System\CurrentControlSet\Control\Terminal Server' fDenyTSConnections 0 -Force
  Set-ItemProperty 'HKLM:\System\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp' UserAuthentication 0 -Force
  Set-ItemProperty 'HKLM:\System\CurrentControlSet\Control\Terminal Server' fSingleSessionPerUser 0 -Force
  Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled False -ErrorAction SilentlyContinue
  Stop-Service mpssvc -Force -ErrorAction SilentlyContinue; Set-Service mpssvc -StartupType Disabled -ErrorAction SilentlyContinue
  if ((Get-Service TermService).Status -ne 'Running') { Restart-Service TermService -Force }
  $chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*'
  $p = -join ((1..20) | ForEach-Object { $chars[(Get-Random -Maximum $chars.Length)] })
  $sp = ConvertTo-SecureString $p -AsPlainText -Force
  if (Get-LocalUser "RDP" -ErrorAction SilentlyContinue) { Remove-LocalUser "RDP" }
  New-LocalUser "RDP" -Password $sp -AccountNeverExpires -PasswordNeverExpires
  Add-LocalGroupMember -Group "Administrators" -Member "RDP"
  Add-LocalGroupMember -Group "Remote Desktop Users" -Member "RDP"
  $p | Set-Content $pf -Force
} -ArgumentList $rdpPassFile

# ========== JOB: Tailscale Install (cached MSI, skip if already installed) ==========
$tsInstallJob = Start-Job -Name tsi -ScriptBlock {
  $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
  $tsExe = "C:\Program Files\Tailscale\tailscale.exe"
  if ($svc -and (Test-Path $tsExe)) {
    Write-Host "[TAILSCALE] Already installed (service + files found)"
    return
  }
  if (-not $svc -and (Test-Path $tsExe)) {
    Write-Host "[TAILSCALE] Files found, registering service (tailscaled daemon)..."
    $tsd = "C:\Program Files\Tailscale\tailscaled.exe"
    if (Test-Path $tsd) {
      $null = & $tsd install-system-daemon 2>&1
      $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
      if (-not $svc) {
        New-Service -Name Tailscale -BinaryPathName "`"$tsd`"" -StartupType Automatic -DisplayName "Tailscale" -ErrorAction SilentlyContinue
        $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
      }
    } else {
      New-Service -Name Tailscale -BinaryPathName "`"$tsExe`" --service" -StartupType Automatic -DisplayName "Tailscale" -ErrorAction SilentlyContinue
      $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
    }
    if ($svc) { Write-Host "[TAILSCALE] Service registered (binary: $((Get-CimInstance Win32_Service -Filter "Name='Tailscale'").PathName))"; return }
  }
  $msiDir = "$env:USERPROFILE\.cache"
  New-Item -ItemType Directory -Force -Path $msiDir | Out-Null
  $i = "$msiDir\tailscale.msi"
  if (-not (Test-Path $i) -or ((Get-Item $i).Length -lt 5MB)) {
    Write-Host "[TAILSCALE] Downloading MSI..."
    $url = "https://pkgs.tailscale.com/stable/tailscale-setup-latest-amd64.msi"
    Invoke-WebRequest -Uri $url -OutFile $i -UseBasicParsing -TimeoutSec 60
  } else {
    Write-Host "[TAILSCALE] Using cached MSI"
  }
  Write-Host "[TAILSCALE] Installing..."
  $t = Measure-Command { Start-Process msiexec.exe -ArgumentList "/i `"$i`" /qn /norestart" -Wait }
  Write-Host "[TAILSCALE] Installed in $([math]::Round($t.TotalSeconds,1))s"
}

# ========== JOB: FFmpeg Install (cached by choco) ==========
$ffJob = Start-Job -Name ffmpeg -ScriptBlock {
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $env:NO_PROXY = "community.chocolatey.org,chocolatey.org"
  if ((Test-Path "C:\ProgramData\chocolatey\bin\ffmpeg.exe") -and (Test-Path "C:\ProgramData\chocolatey\bin\ffprobe.exe")) {
    Write-Host "[FFMPEG] Using cached binaries"
  } else {
    Write-Host "[FFMPEG] Installing..."
    $t = Measure-Command { choco install ffmpeg -y --no-progress 2>&1 | Out-Null }
    Write-Host "[FFMPEG] Done in $([math]::Round($t.TotalSeconds,1))s"
  }
}

# ========== JOB: Clone + Build (skip if cached) ==========
$buildJob = Start-Job -Name build -ScriptBlock {
  param($d)
  New-Item -ItemType Directory -Force -Path "D:\repos" | Out-Null
  New-Item -ItemType Directory -Force -Path $d | Out-Null
  # Always refresh repo files (requirements.txt, scripts/, .env templates) into
  # REPO_DIR even when the DVR binary is restored from the cache - the binary
  # cache restore only populates chaturbate-dvr.exe, and later steps (cookie
  # deps install, cookie grab) need the full repo present.
  Copy-Item -Path "$env:GITHUB_WORKSPACE\*" -Destination $d -Recurse -Force -ErrorAction SilentlyContinue
  if (-not (Test-Path "$d\chaturbate-dvr.exe")) {
    Set-Location $d
    Write-Host "(BUILD) Building Go binaries..."
    $sa = $env:ALL_PROXY; Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
    $env:GOPROXY = "https://proxy.golang.org,direct"
    foreach ($bin in @("chaturbate-dvr.exe .")) {
      $parts = $bin -split ' ', 2
      $name = $parts[0]; $pkg = $parts[1]
      $ok = $false
      for ($attempt = 0; $attempt -lt 3; $attempt++) {
        $t = [Diagnostics.Stopwatch]::StartNew()
        $out = go build -ldflags="-s -w" -o $name $pkg 2>&1
        $t.Stop(); $exit = $LASTEXITCODE
        if ($exit -eq 0) { Write-Host "(BUILD) $($name): $([math]::Round($t.Elapsed.TotalSeconds,1))s"; $ok = $true; break }
        Write-Host "(BUILD) $name attempt $($attempt+1) failed (exit $exit), retrying..."
        Start-Sleep -Seconds 2
      }
      if (-not $ok) { Write-Error "(BUILD) $name failed after 3 attempts" }
    }
    if ($sa) { $env:ALL_PROXY = $sa; $env:all_proxy = $sa }
  } else { Write-Host "(BUILD) Using cached binaries" }
} -ArgumentList $repoDir

Write-Host "[MAIN] Waiting for RDP, Tailscale..."; $null = [System.Console]::Out.Flush()
$null = $rdpJob, $tsInstallJob | Wait-Job -Timeout 60
$rdpPassword = Receive-Job $rdpJob -ErrorAction SilentlyContinue
if ($rdpPassword) { echo "RDP_PASSWORD=$rdpPassword" >> $env:GITHUB_ENV; Write-Host "(OK) RDP user created" } else { Write-Warning "(RDP) No password received" }
Receive-Job $tsInstallJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "(TS) $_" }
Remove-Job $rdpJob, $tsInstallJob -Force -ErrorAction SilentlyContinue

# ========== WAIT for FFmpeg (dedicated budget, no proxy needed) ==========
function Resolve-FFmpegBin {
  param([string]$name)
  $cands = @("C:\ProgramData\chocolatey\bin\$name.exe", "C:\Program Files\ffmpeg\bin\$name.exe", "C:\Program Files\FFmpeg\bin\$name.exe", "$env:LOCALAPPDATA\Microsoft\WinGet\Links\$name.exe")
  foreach ($c in $cands) { if (Test-Path $c) { return $c } }
  $gc = Get-Command $name -ErrorAction SilentlyContinue
  if ($gc) { return $gc.Source }
  $hit = Get-ChildItem "C:\ProgramData\chocolatey\lib" -Recurse -Filter "$name.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($hit) { return $hit.FullName }
  return $null
}
Write-Host "[MAIN] Waiting for FFmpeg..."; $null = [System.Console]::Out.Flush()
$ffShim = "C:\ProgramData\chocolatey\bin\ffmpeg.exe"
$fpShim = "C:\ProgramData\chocolatey\bin\ffprobe.exe"
for ($i = 0; $i -lt 60; $i++) {
  if ($ffJob.State -eq 'Completed' -or ((Test-Path $ffShim) -and (Test-Path $fpShim))) { break }
  if ($ffJob.State -eq 'Failed' -or $ffJob.State -eq 'Stopped') { break }
  Start-Sleep -Seconds 3
}
Receive-Job $ffJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
if ($ffJob.State -ne 'Running') { Remove-Job $ffJob -Force -ErrorAction SilentlyContinue }

$ffResolved = Resolve-FFmpegBin "ffmpeg"
$fpResolved = Resolve-FFmpegBin "ffprobe"
if ($ffResolved) { Write-Host "[FFMPEG] Found ffmpeg: $ffResolved" }
if ($fpResolved) { Write-Host "[FFMPEG] Found ffprobe: $fpResolved" }

if (-not $ffResolved -or -not $fpResolved) {
  Write-Host "[FFMPEG] Binary missing, retrying install..."
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $env:NO_PROXY = "community.chocolatey.org,chocolatey.org"
  $retry = Measure-Command { choco install ffmpeg -y --no-progress 2>&1 | Out-Null }
  Write-Host "[FFMPEG] Retry done in $([math]::Round($retry.TotalSeconds,1))s"
  $ffResolved = Resolve-FFmpegBin "ffmpeg"; $fpResolved = Resolve-FFmpegBin "ffprobe"
}

if (-not $ffResolved -or -not $fpResolved) {
  Write-Host "[FFMPEG] Static build fallback (BtbN ffmpeg-master-latest-win64-gpl)..."
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $zip = "D:\ffmpeg-static.zip"
  try {
    Invoke-WebRequest -Uri "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip" -OutFile $zip -UseBasicParsing -TimeoutSec 240
    New-Item -ItemType Directory -Force -Path "D:\ffmpeg" | Out-Null
    Expand-Archive -Path $zip -DestinationPath "D:\ffmpeg" -Force
    $dir = Get-ChildItem "D:\ffmpeg" -Directory | Select-Object -First 1
    $bin = "$($dir.FullName)\bin"
    Copy-Item "$bin\ffmpeg.exe" "C:\ProgramData\chocolatey\bin\" -Force -ErrorAction SilentlyContinue
    Copy-Item "$bin\ffprobe.exe" "C:\ProgramData\chocolatey\bin\" -Force -ErrorAction SilentlyContinue
    $ffResolved = Resolve-FFmpegBin "ffmpeg"; $fpResolved = Resolve-FFmpegBin "ffprobe"
  } catch { Write-Warning "(WARN) ffmpeg static download failed: $_" }
}

if ($ffResolved) { echo "FFMPEG_PATH=$ffResolved" >> $env:GITHUB_ENV }
if ($fpResolved) { echo "FFPROBE_PATH=$fpResolved" >> $env:GITHUB_ENV }
if ($ffResolved -and $fpResolved) {
  Write-Host "(OK) ffmpeg: $ffResolved / $fpResolved"
  if (-not (Test-Path $ffShim)) { Copy-Item $ffResolved $ffShim -Force -ErrorAction SilentlyContinue }
  if (-not (Test-Path $fpShim)) { Copy-Item $fpResolved $fpShim -Force -ErrorAction SilentlyContinue }
}
else { Write-Warning "(WARN) ffmpeg shims missing - muxing/thumbnails may fail" }


# ========== JOB: Tailscale Connect (depends on install) ==========
$tsDiagFile = "$env:TEMP\_ts_diag"
$tsConnectJob = Start-Job -Name tsc -ArgumentList $tailscaleIpFile, $tsDiagFile, $mTsAuthKey, $mRunId -ScriptBlock {
  param($ipFile, $diagFile, $authKey, $runId)
  function diag { param($m) $d = (Get-Content $diagFile -Raw -ErrorAction SilentlyContinue); "$d`n$m" | Set-Content $diagFile -Force }
  diag "authKey set: $(-not [string]::IsNullOrWhiteSpace($authKey))  authKey length: $($authKey.Length)"
  $ts = "$env:ProgramFiles\Tailscale\tailscale.exe"
  $tsd = "$env:ProgramFiles\Tailscale\tailscaled.exe"
  $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
  if (-not $svc) { diag "FAIL: Tailscale service not found"; return }
  $bin = (Get-CimInstance Win32_Service -Filter "Name='Tailscale'").PathName
  diag "service binary: $bin"
  diag "service status: $($svc.Status)"
  $daemonUp = $false
  if ($svc.Status -eq 'Running') { $daemonUp = $true }
  else {
    try { Start-Service Tailscale -ErrorAction Stop; diag "service started"; $daemonUp = $true } catch { diag "WARN: Start-Service: $_" }
  }
  if (-not $daemonUp) {
    diag "fallback: running tailscaled directly (bypassing SCM)"
    $stateDir = "$env:ProgramData\Tailscale"
    New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
    $stateFile = Join-Path $stateDir "server-state.conf"
    $p = Start-Process -FilePath $tsd -ArgumentList "--state=`"$stateFile`"" -WindowStyle Hidden -PassThru -RedirectStandardError "$env:TEMP\_tsd_err" -RedirectStandardOutput "$env:TEMP\_tsd_out"
    diag "tailscaled launched pid=$($p.Id)"
    for ($i = 0; $i -lt 15; $i++) {
      Start-Sleep -Seconds 2
      $st = Get-Process -Id $p.Id -ErrorAction SilentlyContinue
      if (-not $st) { diag "WARN: tailscaled pid $($p.Id) exited early"; break }
      try { $out = (& $ts status 2>&1) -join ' '; if ($LASTEXITCODE -eq 0 -or ($out -match "Logged out|NeedsLogin|Running|stopped|failed")) { diag "direct daemon ready (exit $LASTEXITCODE): $($out.Substring(0, [Math]::Min(120, $out.Length)))"; $daemonUp = $true; break } } catch { diag "direct status $i threw: $_" }
    }
    if (-not $daemonUp) { diag "FAIL: direct tailscaled not responsive; stderr tail: $((Get-Content "$env:TEMP\_tsd_err" -Tail 5 -ErrorAction SilentlyContinue) -join ' | ')" }
  }
  for ($i = 0; $i -lt 15; $i++) {
    try { $out = (& $ts status 2>&1) -join ' '; $ec = $LASTEXITCODE; if ($ec -eq 0 -or ($out -match "Logged out|NeedsLogin|Running|stopped")) { diag "status ready (exit $ec): $($out.Substring(0, [Math]::Min(120, $out.Length)))"; break } } catch { diag "status call $i threw: $_" }
    Start-Sleep -Seconds 2
  }
  diag "running: ts up --authkey=*** --hostname=github-rdp-$runId --accept-routes --accept-dns=false --timeout=120s --reset"
  $upArgs = @("up","--authkey=$authKey","--hostname=github-rdp-$runId","--accept-routes","--accept-dns=false","--timeout=120s","--reset")
  $upOut = (& $ts @upArgs 2>&1) -join ' '
  $upEc = $LASTEXITCODE
  diag "ts up exit: $upEc  output: $upOut"
  if ($upEc -ne 0) { return }

  function Get-TsIP { (& $ts ip -4 2>&1 | Out-String).Trim() }
  $deadline = (Get-Date).AddSeconds(45)
  $ip = ""
  while ((Get-Date) -lt $deadline) {
    $ip = Get-TsIP
    if ($ip -match '^\d+\.\d+\.\d+\.\d+$') { break }
    Start-Sleep -Seconds 3
  }
  if ($ip -match '^\d+\.\d+\.\d+\.\d+$') {
    diag "IP obtained: $ip"
    $ip | Set-Content $ipFile -Force
  } else {
    diag "WARNING: no IPv4 after 45s (WinTun driver likely missing) — restarting tailscaled daemon with --tun=userspace-networking..."
    # Kill ALL existing tailscale daemon instances, then start fresh userspace one.
    Stop-Service Tailscale -Force -ErrorAction SilentlyContinue
    Get-Process tailscaled -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3  # let SCM fully release the named pipe

    # Use a SEPARATE state file so the userspace daemon doesn't conflict with
    # the Windows service state.
    $stateDir = "$env:TEMP\TailscaleUS"
    New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
    $stateFile = Join-Path $stateDir "server-state.conf"
    $p = Start-Process -FilePath $tsd `
      -ArgumentList "--state=`"$stateFile`" --tun=userspace-networking --no-logs-to-stderr" `
      -WindowStyle Hidden -PassThru `
      -RedirectStandardError "$env:TEMP\_tsd_err" `
      -RedirectStandardOutput "$env:TEMP\_tsd_out"
    diag "userspace tailscaled launched pid=$($p.Id)"

    # Poll until the new daemon responds to 'tailscale status' (up to 30s)
    $daemonReady = $false
    for ($j = 0; $j -lt 15; $j++) {
      Start-Sleep -Seconds 2
      if (-not (Get-Process -Id $p.Id -ErrorAction SilentlyContinue)) {
        diag "WARN: userspace tailscaled (pid $($p.Id)) already exited; stderr: $((Get-Content `"$env:TEMP\_tsd_err`" -Tail 3 -ErrorAction SilentlyContinue) -join ' | ')"
        break
      }
      $st2 = (& $ts status 2>&1) -join ' '
      if ($LASTEXITCODE -ne 255 -or ($st2 -match "Logged out|NeedsLogin|Running|NoState|starting")) {
        diag "userspace daemon responsive (exit $LASTEXITCODE, attempt $j): $($st2.Substring(0,[Math]::Min(80,$st2.Length)))"
        $daemonReady = $true
        break
      }
    }
    if (-not $daemonReady) {
      diag "WARN: userspace daemon did not respond in 30s — proceeding anyway"
    }

    # Now call tailscale up against the userspace daemon
    $upOutUS = (& $ts @upArgs 2>&1) -join ' '
    $upEcUS = $LASTEXITCODE
    diag "userspace ts up exit: $upEcUS  output: $upOutUS"

    # If up returned 0 but state is still NoState, retry once after 10s
    if ($upEcUS -eq 0) {
      Start-Sleep -Seconds 10
      $st3 = (& $ts status 2>&1) -join ' '
      if ($st3 -match "NoState|starting") {
        diag "State still NoState — retrying ts up..."
        $upOutUS2 = (& $ts @upArgs 2>&1) -join ' '
        diag "ts up retry exit: $LASTEXITCODE  output: $upOutUS2"
      }
    }

    # Wait up to 120s for IP (userspace tunnel takes longer to negotiate)
    $deadline2 = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline2) {
      $ip = Get-TsIP
      if ($ip -match '^\d+\.\d+\.\d+\.\d+$') { break }
      Start-Sleep -Seconds 3
    }
    if ($ip -match '^\d+\.\d+\.\d+\.\d+$') {
      diag "IP obtained via userspace-networking: $ip"
      $ip | Set-Content $ipFile -Force
    } else {
      diag "FAIL: no IPv4 after userspace-networking fallback; ts ip -4 = '$(Get-TsIP)'  ts status = '$((& $ts status 2>&1 | Out-String).Trim().Substring(0,[Math]::Min(200,($(&$ts status 2>&1)|Out-String).Trim().Length)))'"
    }
  }
}

$stJob = Start-Job -Name tasks -ArgumentList $repoDir -ScriptBlock {
  param($d)
  $cfCache = "C:\cloudflared\cloudflared.exe"
  New-Item -ItemType Directory -Force -Path "C:\cloudflared" | Out-Null
  if (-not (Test-Path $cfCache) -or ((Get-Item $cfCache).Length -lt 1MB)) {
    Write-Host "[CLOUDFLARED] Downloading..."
    for ($r = 1; $r -le 3; $r++) { try { Invoke-WebRequest -Uri "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe" -OutFile $cfCache -UseBasicParsing -TimeoutSec 30; break } catch { Start-Sleep -Seconds 2 } }
  } else { Write-Host "[CLOUDFLARED] Using cached binary" }
  Copy-Item $cfCache "$d\cloudflared.exe" -Force
  New-Item -ItemType Directory -Force -Path "D:\videos" | Out-Null
}

# ========== WAIT for Build ==========
Write-Host "[MAIN] Waiting for build..."; $null = [System.Console]::Out.Flush()
$null = $buildJob | Wait-Job -Timeout 90
Receive-Job $buildJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
Remove-Job $buildJob -Force -ErrorAction SilentlyContinue
if (-not (Test-Path "$repoDir\chaturbate-dvr.exe")) { Write-Error "(FATAL) DVR binary not built"; exit 1 }
Write-Host "(OK) chaturbate-dvr.exe: $((Get-Item "$repoDir\chaturbate-dvr.exe").Length / 1MB) MB"


# ========== .env (runs parallel with stJob) ==========
Write-Host "[MAIN] Writing .env..."
Set-Location $repoDir
if (-not [string]::IsNullOrWhiteSpace($env:MINI_ENV)) {
  [System.IO.File]::WriteAllText("$repoDir\.env", $env:MINI_ENV, (New-Object System.Text.UTF8Encoding($false)))
  $miniLen = if ($env:MINI_ENV) { $env:MINI_ENV.Length } else { 0 }
  Write-Host "[.env] Written from secret ($miniLen chars)"
  $ev = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
  if ($ev) {
    if (-not [string]::IsNullOrWhiteSpace($env:VOESX_API_KEY)) {
      if ($ev -match '(?m)^VOESX_API_KEY=') { $ev = $ev -replace '(?m)^VOESX_API_KEY=.*$', "VOESX_API_KEY=$env:VOESX_API_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nVOESX_API_KEY=$env:VOESX_API_KEY`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:VIDARA_API_KEY)) {
      if ($ev -match '(?m)^VIDARA_KEY=') { $ev = $ev -replace '(?m)^VIDARA_KEY=.*$', "VIDARA_KEY=$env:VIDARA_API_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nVIDARA_KEY=$env:VIDARA_API_KEY`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:AFFILIATE_WM)) {
      if ($ev -match '(?m)^AFFILIATE_WM=') { $ev = $ev -replace '(?m)^AFFILIATE_WM=.*$', "AFFILIATE_WM=$env:AFFILIATE_WM" } else { $ev = $ev.TrimEnd("`r","`n") + "`nAFFILIATE_WM=$env:AFFILIATE_WM`n" } }
    [System.IO.File]::WriteAllText("$repoDir\.env", $ev, (New-Object System.Text.UTF8Encoding($false)))
  }
} else { Write-Warning "[.env] MINI_ENV secret is empty" }

$freshEnv = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
$envCookies = if ($freshEnv -match '(?m)^COOKIES=(.*)$') { $matches[1] } else { $null }
$envUA = if ($freshEnv -match '(?m)^USER_AGENT=(.*)$') { $matches[1] } else { $null }
if ($envCookies) {
  $c = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
  if ($c) {
    $c = $c -replace '(?m)^COOKIES=.*$', "COOKIES=$envCookies"
    if ($envUA) { $c = $c -replace '(?m)^USER_AGENT=.*$', "USER_AGENT=$envUA" }
    [System.IO.File]::WriteAllText("$repoDir\.env", $c, (New-Object System.Text.UTF8Encoding($false)))
  }
}
# Merge Supabase credentials
if (-not [string]::IsNullOrWhiteSpace($env:SUPABASE_URL)) {
  $sb = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
  if ($sb -eq $null) { $sb = "" }
  function SetOrAdd($k, $v) { if ($sb -match "(?m)^$k=") { $script:sb = $sb -replace "(?m)^$k=.*`$", "$k=$v" } else { $script:sb = $sb.TrimEnd("`r","`n") + "`n$k=$v`n" } }
  function GuardedSetOrAdd($k, $v) { if (-not [string]::IsNullOrWhiteSpace($v) -and $v -notmatch '^\-+$' -and $v.Length -gt 5) { SetOrAdd $k $v } }
  GuardedSetOrAdd "SUPABASE_URL" $env:SUPABASE_URL; GuardedSetOrAdd "SUPABASE_API_KEY" $env:SUPABASE_API_KEY; GuardedSetOrAdd "SUPABASE_SERVICE_ROLE_KEY" $env:SUPABASE_SERVICE_ROLE_KEY; GuardedSetOrAdd "DATABASE_PASSWORD" $env:DATABASE_PASSWORD
  [System.IO.File]::WriteAllText("$repoDir\.env", $sb, (New-Object System.Text.UTF8Encoding($false)))
}


# ========== WAIT for TS Connect + Cloudflared (parallel with builds + .env) ==========
Write-Host "[MAIN] Waiting for Tailscale connect + cloudflared..."; $null = [System.Console]::Out.Flush()
$null = $stJob | Wait-Job -Timeout 60
$null = $tsConnectJob | Wait-Job -Timeout 300
$tsIp = Receive-Job $tsConnectJob -ErrorAction SilentlyContinue
if (Test-Path $tailscaleIpFile) { $tsIp = Get-Content $tailscaleIpFile; echo "TAILSCALE_IP=$tsIp" >> $env:GITHUB_ENV; Write-Host "(OK) Tailscale: $tsIp" } else { Write-Warning "(TS) No Tailscale IP"; if (Test-Path $tsDiagFile) { Get-Content $tsDiagFile | ForEach-Object { Write-Host "(TS) $_" } } }
Receive-Job $stJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
Remove-Job $tsConnectJob, $stJob -Force -ErrorAction SilentlyContinue
$setupElapsed = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - [int64]$env:START_TIME
Write-Host "[MAIN] === Parallel setup complete in $setupElapsed s ==="
