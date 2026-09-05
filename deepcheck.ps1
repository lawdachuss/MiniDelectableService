$ErrorActionPreference = "Stop"

# Load .env into process env
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$') {
        Set-Item -Path ("env:" + $matches[1]) -Value ($matches[2].Trim('"').Trim("'"))
    }
}

$base = $env:SUPABASE_URL.TrimEnd('/')
$key  = $env:SUPABASE_SERVICE_ROLE_KEY

$files = @(
'liaglamour_2026-09-01_09-52-43.mp4','BlueandJen_2026-09-01_23-10-21_2.mp4','ameliabiers_2026-08-31_07-45-03_2.mp4','eve_kiwi100_2026-09-02_22-41-00.mp4','alicesternley_2026-08-31_07-20-25_1.mp4','goddessxenvy_2026-09-03_02-00-45.mp4','lovydede_2026-09-02_17-33-14.mp4','emiilycampbell_2026-09-02_23-51-35.mp4','urneighbors_2026-09-02_17-39-09.mp4','dongfatherproductions_2026-09-03_02-18-54.mp4','tomikavilandre_2026-08-30_19-37-34_2.mp4','blow2job_lat_2026-09-03_16-15-45.mp4','molly_p_2026-09-02_17-04-58.mp4','lau__1_2026-08-31_01-35-41_3.mp4','terrryliq_2026-08-31_04-25-12.mp4','annabellecroft_2026-08-31_07-20-26_2.mp4','ManaMi-maru_2026-09-01_17-29-29.mp4','wendyoliver_2026-09-01_19-40-41_1.mp4','yournotmysupervisor_2026-09-02_05-37-19_1.mp4','sisichki_pisichki_2026-09-02_17-12-33.mp4','angelsofaurora_2026-09-02_20-55-29.mp4','KathyAiden_2026-09-02_19-21-32.mp4','jacksoncooper1_2026-09-02_21-33-59.mp4.merged.mp4','Horny_Sanjana_2026-09-02_23-23-11.mp4','baeasian_2026-09-03_02-58-56.mp4','better_togetherx_2026-09-03_02-41-12.mp4','cassianesta_2026-09-03_00-35-57.mp4','BlueandJen_2026-09-02_23-49-59.mp4.merged.mp4','bridgetjean_2026-09-03_03-22-01.mp4','sammyvxoo_2026-09-03_06-51-35.mp4','moon_and_liam_2026-09-03_04-51-45.mp4','partymonsterxxx_2026-09-03_07-30-15.mp4','jully_lov_2026-09-03_02-41-04.mp4','julsweet_2026-09-03_08-34-51_1.mp4','di_n_alex_2026-09-03_13-52-09.mp4','bloomyogi_2026-09-03_21-53-37.mp4'
)

function Invoke-SB($url) {
    $headers = @{ apikey = $key; Authorization = "Bearer $key" }
    try {
        $r = Invoke-RestMethod -Uri $url -Headers $headers -TimeoutSec 60
        return @{ ok = $true; data = $r }
    } catch {
        return @{ ok = $false; err = $_.Exception.Message }
    }
}

$sel = "select=filename,thumbnail_url,sprite_url,preview_url,thumbnail_mirrors,sprite_mirrors,preview_mirrors,embed_url,upload_links(host,url),created_at"

Write-Host "=== DEEP CHECK: 36 recordings with NULL thumbnail_url ==="
foreach ($f in $files) {
    $esc = [uri]::EscapeDataString($f)
    $rec = Invoke-SB ("$base/rest/v1/recordings?filename=eq.$esc&$sel")
    $pi  = Invoke-SB ("$base/rest/v1/preview_images?filename=eq.$esc&select=filename,thumbnail_url,sprite_url,preview_url,image_links,hosts")
    $line = "$f"
    if ($rec.ok) {
        $rows = @($rec.data)
        if ($rows.Count -eq 0) { $line += " | RECORDING_MISSING" }
        else {
            $r = $rows[0]
            $thumb = if ($r.thumbnail_url) { "TH=$($r.thumbnail_url)" } else { "TH=_" }
            $spr   = if ($r.sprite_url)   { "SP=$($r.sprite_url)" }   else { "SP=_" }
            $prev  = if ($r.preview_url)  { "PV=$($r.preview_url)" }  else { "PV=_" }
            $emb   = if ($r.embed_url)    { "EMB=$($r.embed_url)" }   else { "EMB=_" }
            $thMir = if ($r.thumbnail_mirrors) { ($r.thumbnail_mirrors | ConvertTo-Json -Compress) } else { "_-" }
            $ul    = if ($r.upload_links) { (($r.upload_links | ForEach-Object { $_.host + "=" + $_.url }) -join ";") } else { "_" }
            $line += " | $thumb $spr $prev $emb | mirrors=$thMir | upload_links=$ul"
        }
    } else {
        $line += " | REC_ERR: $($rec.err)"
    }
    if ($pi.ok) {
        $prows = @($pi.data)
        if ($prows.Count -eq 0) { $line += " | preview_images=0" }
        else {
            $p = $prows[0]
            $pt = if ($p.thumbnail_url) { "TH=$($p.thumbnail_url)" } else { "TH=_" }
            $ps = if ($p.sprite_url)    { "SP=$($p.sprite_url)" }   else { "SP=_" }
            $pp = if ($p.preview_url)   { "PV=$($p.preview_url)" }  else { "PV=_" }
            $ph = if ($p.hosts) { ($p.hosts | ConvertTo-Json -Compress) } else { "_-" }
            $pl = if ($p.image_links) { ($p.image_links | ConvertTo-Json -Compress) } else { "_-" }
            $line += " | PI: $pt $ps $pp hosts=$ph links=$pl"
        }
    } else {
        $line += " | PI_ERR: $($pi.err)"
    }
    Write-Output $line
}
Write-Host "=== done ==="
