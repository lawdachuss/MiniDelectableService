# Comprehensive thumbnail diagnostic script
# Usage: .\scripts\diagnose-thumbnails.ps1

$ErrorActionPreference = "Continue"

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║           📸 THUMBNAIL DIAGNOSTIC TOOL                     ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Load environment variables
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

$SUPABASE_URL = $env:SUPABASE_URL
$SUPABASE_API_KEY = $env:SUPABASE_API_KEY

Write-Host "[1/5] Checking Supabase Connection..." -ForegroundColor Yellow

if (-not $SUPABASE_URL -or -not $SUPABASE_API_KEY) {
    Write-Host "   ❌ Supabase credentials not configured" -ForegroundColor Red
    Write-Host "   Set SUPABASE_URL and SUPABASE_API_KEY in .env" -ForegroundColor Gray
    $supabaseOk = $false
} else {
    Write-Host "   ✅ Credentials found" -ForegroundColor Green
    $supabaseOk = $true
}

Write-Host ""
Write-Host "[2/5] Fetching Videos from API..." -ForegroundColor Yellow

try {
    $apiResponse = Invoke-RestMethod -Uri "http://localhost:8080/api/videos" -Method Get -ErrorAction Stop
    
    $totalVideos = $apiResponse.count
    $videos = $apiResponse.videos
    
    Write-Host "   ✅ Found $totalVideos videos" -ForegroundColor Green
    Write-Host "   Debug info:" -ForegroundColor Gray
    Write-Host "     - Supabase client: $($apiResponse.debug.supabase_client_initialized)" -ForegroundColor Gray
    Write-Host "     - Supabase count: $($apiResponse.debug.supabase_count)" -ForegroundColor Gray
    Write-Host "     - Local count: $($apiResponse.debug.local_count)" -ForegroundColor Gray
    
    if ($apiResponse.debug.supabase_error) {
        Write-Host "     - Supabase error: $($apiResponse.debug.supabase_error)" -ForegroundColor Yellow
    }
    
} catch {
    Write-Host "   ❌ Failed to fetch videos from API" -ForegroundColor Red
    Write-Host "   Error: $_" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Make sure the app is running on http://localhost:8080" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "[3/5] Analyzing Thumbnail Data..." -ForegroundColor Yellow

$withThumbnails = 0
$withoutThumbnails = 0
$thumbnailUrls = @()

foreach ($video in $videos) {
    if ($video.thumbnail_link -and $video.thumbnail_link -ne "") {
        $withThumbnails++
        $thumbnailUrls += $video.thumbnail_link
    } else {
        $withoutThumbnails++
    }
}

Write-Host "   Videos with thumbnails: $withThumbnails" -ForegroundColor $(if ($withThumbnails -gt 0) { "Green" } else { "Gray" })
Write-Host "   Videos without thumbnails: $withoutThumbnails" -ForegroundColor $(if ($withoutThumbnails -gt 0) { "Yellow" } else { "Gray" })

if ($withoutThumbnails -gt 0) {
    Write-Host ""
    Write-Host "   Videos missing thumbnails:" -ForegroundColor Yellow
    foreach ($video in $videos) {
        if (-not $video.thumbnail_link -or $video.thumbnail_link -eq "") {
            Write-Host "     • $($video.streamer_name) - $($video.filename)" -ForegroundColor Gray
        }
    }
}

Write-Host ""
Write-Host "[4/5] Testing Thumbnail Accessibility..." -ForegroundColor Yellow

if ($thumbnailUrls.Count -eq 0) {
    Write-Host "   ⚠️  No thumbnail URLs to test" -ForegroundColor Yellow
} else {
    $accessible = 0
    $inaccessible = 0
    
    # Test first 5 thumbnails
    $testUrls = $thumbnailUrls | Select-Object -First 5
    
    foreach ($url in $testUrls) {
        try {
            $response = Invoke-WebRequest -Uri $url -Method Head -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
            if ($response.StatusCode -eq 200) {
                $accessible++
                Write-Host "   ✅ $url" -ForegroundColor Green
            } else {
                $inaccessible++
                Write-Host "   ❌ $url (HTTP $($response.StatusCode))" -ForegroundColor Red
            }
        } catch {
            $inaccessible++
            Write-Host "   ❌ $url (Failed to connect)" -ForegroundColor Red
        }
    }
    
    if ($thumbnailUrls.Count -gt 5) {
        Write-Host "   ... and $($thumbnailUrls.Count - 5) more" -ForegroundColor Gray
    }
    
    Write-Host ""
    Write-Host "   Summary: $accessible accessible, $inaccessible inaccessible" -ForegroundColor $(if ($inaccessible -eq 0) { "Green" } else { "Yellow" })
}

Write-Host ""
Write-Host "[5/5] Checking Pixhost.to Service..." -ForegroundColor Yellow

try {
    $pixhostResponse = Invoke-WebRequest -Uri "https://pixhost.to" -Method Get -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
    Write-Host "   ✅ Pixhost.to is accessible" -ForegroundColor Green
} catch {
    Write-Host "   ❌ Pixhost.to is not accessible" -ForegroundColor Red
    Write-Host "   This may affect thumbnail uploads" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "DIAGNOSIS COMPLETE" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Provide recommendations
if ($withoutThumbnails -gt 0) {
    Write-Host "💡 RECOMMENDATIONS:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Videos without thumbnails detected. Possible causes:" -ForegroundColor White
    Write-Host ""
    Write-Host "1. FFmpeg not available during recording" -ForegroundColor Gray
    Write-Host "   Solution: Ensure FFmpeg is installed and in PATH" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "2. Pixhost.to upload failed" -ForegroundColor Gray
    Write-Host "   Solution: Check logs for 'thumbnail upload' errors" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "3. Videos were recorded before thumbnail feature was added" -ForegroundColor Gray
    Write-Host "   Solution: Re-record or manually re-upload the affected videos; thumbnails" -ForegroundColor Cyan
    Write-Host "   are generated automatically on the next successful upload." -ForegroundColor Cyan
    Write-Host ""
} elseif ($totalVideos -eq 0) {
    Write-Host "💡 NO VIDEOS FOUND" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "No videos in the database yet. Thumbnails will be generated" -ForegroundColor White
    Write-Host "automatically when videos are recorded and uploaded." -ForegroundColor White
    Write-Host ""
} else {
    Write-Host "✅ ALL VIDEOS HAVE THUMBNAILS!" -ForegroundColor Green
    Write-Host ""
    if ($inaccessible -gt 0) {
        Write-Host "⚠️  However, some thumbnail URLs are not accessible." -ForegroundColor Yellow
        Write-Host "This could be:" -ForegroundColor White
        Write-Host "  • Temporary network issue" -ForegroundColor Gray
        Write-Host "  • Pixhost.to deleted old images" -ForegroundColor Gray
        Write-Host "  • Firewall blocking Pixhost.to" -ForegroundColor Gray
        Write-Host ""
    }
}

Write-Host "To view videos UI: http://localhost:8080/videos" -ForegroundColor Cyan
Write-Host ""
