# Diagnostic Script for Recording Issues
# Checks all common problems and provides solutions

Write-Host "🔍 GoOnDVR Recording Diagnostics" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

$issues = @()
$warnings = @()

# Check 1: Settings File
Write-Host "[1/6] Checking settings.json..." -ForegroundColor Yellow
$settingsPath = "settings.json"
if (-not (Test-Path $settingsPath)) {
    $settingsPath = "conf/settings.json"
}

if (Test-Path $settingsPath) {
    Write-Host "  ✅ Settings file found: $settingsPath" -ForegroundColor Green
    
    $settings = Get-Content $settingsPath -Raw | ConvertFrom-Json
    
    # Check cookies
    if ($settings.cookies) {
        $cookieLength = $settings.cookies.Length
        Write-Host "  📊 Cookies: $cookieLength characters" -ForegroundColor White
        
        if ($settings.cookies -match "csrftoken=") {
            Write-Host "     ✅ csrftoken found" -ForegroundColor Green
        } else {
            $warnings += "Missing csrftoken cookie (may cause API errors)"
            Write-Host "     ⚠️  csrftoken missing" -ForegroundColor Yellow
        }
        
        # Check for newlines (corruption)
        if ($settings.cookies -match "`n" -or $settings.cookies -match "`r") {
            $issues += "Cookies contain newline characters (corrupted)"
            Write-Host "     ❌ Cookies contain newlines (corrupted)" -ForegroundColor Red
        }
    } else {
        $warnings += "No cookies configured"
        Write-Host "  ⚠️  No cookies configured" -ForegroundColor Yellow
    }
    
    # Check user agent
    if ($settings.user_agent) {
        Write-Host "  ✅ User-Agent configured" -ForegroundColor Green
    } else {
        $warnings += "No User-Agent configured (will use default)"
        Write-Host "  ⚠️  No User-Agent configured" -ForegroundColor Yellow
    }
} else {
    $issues += "Settings file not found"
    Write-Host "  ❌ Settings file not found" -ForegroundColor Red
}

# Check 2: Channels Configuration
Write-Host "[2/6] Checking channels.json..." -ForegroundColor Yellow
$channelsPath = "conf/channels.json"
if (Test-Path $channelsPath) {
    $channels = Get-Content $channelsPath -Raw | ConvertFrom-Json
    $activeChannels = ($channels | Where-Object { -not $_.is_paused }).Count
    $totalChannels = $channels.Count
    
    Write-Host "  ✅ Channels file found" -ForegroundColor Green
    Write-Host "  📊 Total channels: $totalChannels" -ForegroundColor White
    Write-Host "  📊 Active channels: $activeChannels" -ForegroundColor White
    Write-Host "  📊 Paused channels: $($totalChannels - $activeChannels)" -ForegroundColor White
} else {
    $warnings += "No channels configured"
    Write-Host "  ⚠️  Channels file not found" -ForegroundColor Yellow
}

# Check 3: GoOnDVR Executable
Write-Host "[3/6] Checking GoOnDVR executable..." -ForegroundColor Yellow
if (Test-Path "goondvr.exe") {
    Write-Host "  ✅ goondvr.exe found" -ForegroundColor Green
} else {
    $warnings += "goondvr.exe not found in current directory"
    Write-Host "  ⚠️  goondvr.exe not found" -ForegroundColor Yellow
}

# Check 4: Videos Directory
Write-Host "[4/6] Checking videos directory..." -ForegroundColor Yellow
if (Test-Path "videos") {
    $videoFiles = Get-ChildItem -Path "videos" -Recurse -File | Where-Object { $_.Extension -in @('.mp4', '.ts', '.mkv') }
    Write-Host "  ✅ Videos directory exists" -ForegroundColor Green
    Write-Host "  📊 Recorded files: $($videoFiles.Count)" -ForegroundColor White
    
    if ($videoFiles.Count -gt 0) {
        $totalSize = ($videoFiles | Measure-Object -Property Length -Sum).Sum
        $totalSizeGB = [math]::Round($totalSize / 1GB, 2)
        Write-Host "  📊 Total size: $totalSizeGB GB" -ForegroundColor White
    }
} else {
    Write-Host "  ⚠️  Videos directory not found (will be created)" -ForegroundColor Yellow
}

# Check 5: Internet Connectivity
Write-Host "[5/6] Checking internet connectivity..." -ForegroundColor Yellow
$siteDomain = "https://www.cb.xxx/"
if (Test-Path ".env") {
    $dom = (Get-Content ".env" | Where-Object { $_ -match '^DOMAIN=' } | Select-Object -First 1)
    if ($dom) { $siteDomain = $dom -replace '^DOMAIN=', '' }
}
try {
    $chaturbateResponse = Invoke-WebRequest -Uri $siteDomain -Method GET -TimeoutSec 10 -ErrorAction Stop
    Write-Host "  ✅ Can reach $siteDomain" -ForegroundColor Green
    
    if ($chaturbateResponse.Content -match "Just a moment") {
        $warnings += "$siteDomain returned challenge page (need fresh cookies)"
        Write-Host "  ⚠️  Challenge page detected" -ForegroundColor Yellow
    }
} catch {
    $issues += "Cannot reach $siteDomain (check internet/firewall)"
    Write-Host "  ❌ Cannot reach $siteDomain" -ForegroundColor Red
    Write-Host "     Error: $($_.Exception.Message)" -ForegroundColor Red
}

# Summary
Write-Host ""
Write-Host "=================================" -ForegroundColor Cyan
Write-Host "📊 DIAGNOSTIC SUMMARY" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

if ($issues.Count -eq 0 -and $warnings.Count -eq 0) {
    Write-Host "✅ All checks passed! Your setup looks good." -ForegroundColor Green
    Write-Host ""
    Write-Host "If recording still doesn't work:" -ForegroundColor Yellow
    Write-Host "1. Check if channels are actually online" -ForegroundColor White
    Write-Host "2. Run with --debug flag: .\goondvr.exe --debug" -ForegroundColor White
    Write-Host "3. Check logs for specific errors" -ForegroundColor White
} else {
    if ($issues.Count -gt 0) {
        Write-Host "❌ CRITICAL ISSUES FOUND:" -ForegroundColor Red
        $issues | ForEach-Object { Write-Host "   • $_" -ForegroundColor Red }
        Write-Host ""
    }
    
    if ($warnings.Count -gt 0) {
        Write-Host "⚠️  WARNINGS:" -ForegroundColor Yellow
        $warnings | ForEach-Object { Write-Host "   • $_" -ForegroundColor Yellow }
        Write-Host ""
    }
    
    Write-Host "🔧 RECOMMENDED ACTIONS:" -ForegroundColor Cyan
    Write-Host ""
    
    if ($warnings -match "cookies") {
        Write-Host "1. Get fresh cookies:" -ForegroundColor White
        Write-Host "   Extract cookies from browser after logging into Chaturbate" -ForegroundColor Gray
        Write-Host ""
    }
    
    if ($issues -match "Settings file") {
        Write-Host "2. Create settings file:" -ForegroundColor White
        Write-Host "   Copy settings.json.example to settings.json" -ForegroundColor Gray
        Write-Host ""
    }
}

Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📚 For detailed solutions, see:" -ForegroundColor Yellow
Write-Host "   RECORDING_FIX_GUIDE.md" -ForegroundColor White
Write-Host ""

Write-Host "Press Enter to exit..." -ForegroundColor Green
Read-Host
