# clean-c.ps1 - Safe C-drive cleaner (Windows / GitHub Actions RDP runners)
# Removes only disposable TEMP / JUNK locations on C:.
#
# DEFAULT (and background -LoopMinutes) mode removes ONLY:
#   - STALE temp files (not modified in > 1 day) in the user & system temp folders
#   - Recycle Bin
# Nothing your software uses is ever touched:
#   - age filter (1 day) keeps any recent/active file,
#   - locked/in-use files are skipped by the removal calls,
#   - Go/pip/playwright/chocolatey/Tailscale/cloudflared/proxy caches are never targeted,
#   - D:\temp, D:\repos, D:\videos are never touched.
#
# Opt-in extras (NOT run in background):
#   -CleanMore          thumbnails, icon cache, INetCache, DirectX shader cache,
#                       crash dumps, WER reports, Prefetch, browser caches
#   -Deep               Windows Update download cache, Delivery Optimization,
#                       Windows.old
#   -IncludeDevCaches   pip/npm/go/nuget/uv/yarn build caches
#
# NEVER touches (would damage Windows or your apps):
#   WinSxS (manual deletion can break Windows), ProgramData\Package Cache
#   (installer/uninstaller data), ProgramData\chocolatey (workflow uses it),
#   Program Files, user Documents/Desktop/Downloads/OneDrive.
#
# Usage:
#   .\scripts\clean-c.ps1 -DryRun            # preview what would be removed
#   .\scripts\clean-c.ps1                     # clean stale temp + Recycle Bin
#   .\scripts\clean-c.ps1 -CleanMore          # + system/browser caches
#   .\scripts\clean-c.ps1 -Deep               # + Windows Update/Delivery/Windows.old
#   .\scripts\clean-c.ps1 -IncludeDevCaches   # + pip/npm/go/nuget build caches
#   .\scripts\clean-c.ps1 -LoopMinutes 60     # background loop (used by keep-alive)
#
# Re-launches itself as Administrator automatically when needed.

param(
  [switch]$DryRun,
  [switch]$CleanMore,
  [switch]$Deep,
  [switch]$IncludeDevCaches,
  [int]$LoopMinutes = 0
)

$ErrorActionPreference = 'SilentlyContinue'
$script:Freed = [long]0
$script:RemovedFiles = 0
$script:RemovedDirs = 0
$script:Blocked = 0
$script:RunCount = 0

# Hard block-list: anything under these roots is never removed.
$script:ForbiddenRoots = @(
  'C:\Windows\WinSxS',
  'C:\ProgramData\Package Cache',
  'C:\ProgramData\chocolatey',
  'C:\Program Files',
  'C:\Program Files (x86)'
)

function Test-SafePath {
  param([string]$Path)
  if ([string]::IsNullOrWhiteSpace($Path)) { return $false }
  $full = [System.IO.Path]::GetFullPath($Path)
  foreach ($root in $script:ForbiddenRoots) {
    $r = [System.IO.Path]::GetFullPath($root)
    if ($full.StartsWith($r, [System.StringComparison]::OrdinalIgnoreCase)) { return $false }
  }
  return $true
}

function Get-FolderSize {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return 0 }
  $sum = [long]0
  Get-ChildItem -LiteralPath $Path -Recurse -File -Force -ErrorAction SilentlyContinue |
    ForEach-Object { $sum += $_.Length }
  return $sum
}

function Remove-Target {
  param([string]$Path, [string]$Description, [int]$MaxAgeHours = 0)
  if (-not (Test-Path -LiteralPath $Path)) { return }
  if (-not (Test-SafePath $Path)) {
    Write-Host "[BLOCK] unsafe path skipped: $Path"
    $script:Blocked++
    return
  }
  $cutoff = if ($MaxAgeHours -gt 0) { (Get-Date).AddHours(-$MaxAgeHours) } else { $null }
  $items = @(Get-ChildItem -LiteralPath $Path -Force -ErrorAction SilentlyContinue)
  if ($cutoff) { $items = @($items | Where-Object { $_.LastWriteTime -le $cutoff }) }
  $before = Get-FolderSize $Path
  if ($DryRun) {
    Write-Host ("[DRY-RUN] would clean {0} item(s): {1,-50} {2,9:N1} MB" -f $items.Count, $Description, ($before / 1MB))
    return
  }
  foreach ($it in $items) {
    if ($it.PSIsContainer) {
      Remove-Item -LiteralPath $it.FullName -Recurse -Force -ErrorAction SilentlyContinue
      if (-not (Test-Path -LiteralPath $it.FullName)) { $script:RemovedDirs++ }
    } else {
      Remove-Item -LiteralPath $it.FullName -Force -ErrorAction SilentlyContinue
      if (-not (Test-Path -LiteralPath $it.FullName)) { $script:RemovedFiles++ }
    }
  }
  $freed = $before - (Get-FolderSize $Path)
  $script:Freed += $freed
  Write-Host ("[OK]    cleaned: {0,-50} freed {1,9:N1} MB" -f $Description, ($freed / 1MB))
}

function Remove-MatchingFiles {
  param([string]$Path, [string]$Filter, [string]$Description)
  if (-not (Test-Path -LiteralPath $Path)) { return }
  if (-not (Test-SafePath $Path)) {
    Write-Host "[BLOCK] unsafe path skipped: $Path"
    $script:Blocked++
    return
  }
  $before = Get-FolderSize $Path
  if ($DryRun) {
    Write-Host ("[DRY-RUN] would clean: {0,-50} {1,9:N1} MB" -f $Description, ($before / 1MB))
    return
  }
  Get-ChildItem -LiteralPath $Path -Filter $Filter -File -Force -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
  $freed = $before - (Get-FolderSize $Path)
  $script:Freed += $freed
  Write-Host ("[OK]    cleaned: {0,-50} freed {1,9:N1} MB" -f $Description, ($freed / 1MB))
}

function Remove-ChromiumCaches {
  param([string]$UserDataDir, [string]$Browser)
  if (-not (Test-Path -LiteralPath $UserDataDir)) { return }
  Get-ChildItem -LiteralPath $UserDataDir -Directory -Force -ErrorAction SilentlyContinue |
    ForEach-Object {
      $profile = $_.FullName
      foreach ($sub in @('Cache', 'Cache_Data', 'Code Cache', 'GPUCache', 'ShaderCache')) {
        $p = Join-Path $profile $sub
        if (Test-Path -LiteralPath $p) { Remove-Target $p "$Browser $($_.Name) - $sub" }
      }
    }
}

# ---- Elevate if not admin (needed for C:\Windows items) ---------------------
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
  Write-Host "(CLEAN) Not elevated - re-launching as Administrator for system paths..."
  $scriptPath = $MyInvocation.MyCommand.Path
  $argList = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
  if ($DryRun) { $argList += " -DryRun" }
  if ($CleanMore) { $argList += " -CleanMore" }
  if ($Deep) { $argList += " -Deep" }
  if ($IncludeDevCaches) { $argList += " -IncludeDevCaches" }
  if ($LoopMinutes -gt 0) { $argList += " -LoopMinutes $LoopMinutes" }
  Start-Process powershell.exe -ArgumentList $argList -Verb RunAs
  exit
}

function Invoke-Clean {
  $script:RunCount++
  Write-Host "=== C-drive cleaner pass $($script:RunCount) started $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==="

  # ---- Default: ONLY stale temp + Recycle Bin -------------------------------
  # Use LOCALAPPDATA\Temp, NOT $env:TEMP — keep-alive redirects TEMP to D:\temp
  # and the DVR's temp files live there and must never be touched. The 1-day age
  # filter keeps any recently-used file, so nothing active is ever deleted.
  $userTemp = Join-Path $env:LOCALAPPDATA "Temp"
  Remove-Target $userTemp "User temp (stale, >1 day)" -MaxAgeHours 24
  Remove-Target "C:\Windows\Temp" "Windows temp (stale, >1 day)" -MaxAgeHours 24

  # ---- Opt-in: system & browser caches (-CleanMore) -------------------------
  if ($CleanMore) {
    Remove-MatchingFiles "$env:LOCALAPPDATA\Microsoft\Windows\Explorer" "thumbcache_*.db" "Explorer thumbnail cache"
    Remove-MatchingFiles "$env:LOCALAPPDATA\Microsoft\Windows\Explorer" "iconcache_*.db" "Explorer icon cache"
    Remove-Target "$env:LOCALAPPDATA\Microsoft\Windows\INetCache" "IE / WinINet cache"
    Remove-Target "$env:LOCALAPPDATA\D3DSCache" "DirectX shader cache"
    Remove-Target "$env:LOCALAPPDATA\CrashDumps" "Local crash dumps"
    Remove-Target "C:\Windows\Minidump" "Windows minidumps"
    Remove-Target "C:\ProgramData\Microsoft\Windows\WER\ReportArchive" "WER report archive"
    Remove-Target "C:\ProgramData\Microsoft\Windows\WER\ReportQueue" "WER report queue"
    Remove-Target "C:\Windows\Prefetch" "Prefetch contents (rebuilds on next boot)"
    Remove-ChromiumCaches "$env:LOCALAPPDATA\Google\Chrome\User Data" "Chrome"
    Remove-ChromiumCaches "$env:LOCALAPPDATA\Microsoft\Edge\User Data" "Edge"
    Remove-ChromiumCaches "$env:LOCALAPPDATA\BraveSoftware\Brave-Browser\User Data" "Brave"
    $ffProfiles = Get-ChildItem "$env:APPDATA\Mozilla\Firefox\Profiles" -Directory -ErrorAction SilentlyContinue
    foreach ($fp in $ffProfiles) {
      Remove-Target (Join-Path $fp.FullName "cache2") "Firefox $($fp.Name) - cache2"
      Remove-Target (Join-Path $fp.FullName "startupCache") "Firefox $($fp.Name) - startupCache"
    }
  }

  # ---- Opt-in: Windows Update / Delivery Optimization / Windows.old (-Deep) --
  if ($Deep) {
    # Windows Update download cache: stop wuauserv, clear, restart (MS-recommended
    # method). Safe on GitHub runners since no pending updates are being staged.
    if (Test-Path "C:\Windows\SoftwareDistribution\Download") {
      Write-Host "(CLEAN) Stopping Windows Update service..."
      $null = net stop wuauserv 2>&1
      Remove-Target "C:\Windows\SoftwareDistribution\Download" "Windows Update download cache"
      Write-Host "(CLEAN) Restarting Windows Update service..."
      $null = net start wuauserv 2>&1
    }
    Remove-Target "C:\Windows\ServiceProfiles\NetworkService\AppData\Local\Microsoft\Windows\DeliveryOptimization\Cache" "Delivery Optimization cache"
    if (Test-Path "C:\Windows.old") { Remove-Target "C:\Windows.old" "Windows.old (old install)" }
  }

  # ---- Opt-in: dev build caches (-IncludeDevCaches) -------------------------
  if ($IncludeDevCaches) {
    Remove-Target "$env:LOCALAPPDATA\pip\cache" "pip cache"
    Remove-Target "$env:LOCALAPPDATA\uv\cache" "uv cache"
    Remove-Target "$env:LOCALAPPDATA\go-build" "Go build cache"
    Remove-Target "$env:LOCALAPPDATA\NuGet\v3-cache" "NuGet cache"
    Remove-Target "$env:LOCALAPPDATA\npm-cache" "npm cache"
    Remove-Target "$env:LOCALAPPDATA\Yarn\Cache" "Yarn cache"
  }

  # ---- Recycle bin ----------------------------------------------------------
  if ($DryRun) {
    Write-Host "[DRY-RUN] would empty the Recycle Bin"
  } else {
    try { Clear-RecycleBin -Force -ErrorAction Stop; Write-Host "[OK]    emptied Recycle Bin" }
    catch { Write-Host "[SKIP]  Recycle Bin: $($_.Exception.Message)" }
  }

  Write-Host ""
  if ($DryRun) {
    Write-Host "=== DRY RUN COMPLETE (pass $($script:RunCount)) - nothing was deleted ==="
  } else {
    Write-Host ("=== PASS $($script:RunCount) DONE: freed {0:N1} MB, removed {1} files / {2} folders, {3} blocked ===" -f ($script:Freed / 1MB), $script:RemovedFiles, $script:RemovedDirs, $script:Blocked)
  }
}

if ($LoopMinutes -gt 0) {
  Write-Host "(CLEAN) Background loop mode: cleaning every $LoopMinutes minute(s)."
  while ($true) {
    Invoke-Clean
    Start-Sleep -Seconds ($LoopMinutes * 60)
  }
} else {
  Invoke-Clean
}
