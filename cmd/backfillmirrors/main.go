// Command backfillmirrors re-uploads images from Catbox to all hosts
// and stores all mirror URLs in the database.
//
// It finds all recordings whose thumbnail_url, sprite_url, or preview_url
// points to Catbox, downloads the image, re-uploads it to all hosts
// (Catbox, Pixhost, freeimage.host, ImgChest, Imgbox), and updates the
// database with the primary URL and all mirror URLs for ALL three assets.
//
// Usage:
//
//	go run ./cmd/backfillmirrors [flags]
//
// Flags:
//
//	-dry-run    Show what would be done without making changes
//	-limit N    Process at most N recordings (0 = all)
//	-delay D    Delay between records (default 2s)
//	-proxy URL  HTTP proxy URL for Catbox downloads
//	-token T    Cloudflare Access token for proxy auth
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

var proxyFlag string

func main() {
	dryRun := flag.Bool("dry-run", false, "Show what would be done without making changes")
	limit := flag.Int("limit", 0, "Process at most N recordings (0 = all)")
	delay := flag.Duration("delay", 2*time.Second, "Delay between records")
	flag.StringVar(&proxyFlag, "proxy", "", "HTTP proxy URL for Catbox downloads (e.g. http://127.0.0.1:7890)")
	flag.StringVar(&proxyToken, "token", "", "Cloudflare Access token for proxy auth (or set CF_ACCESS_TOKEN env)")
	skipImgBB := flag.Bool("skip-imgbb", false, "Skip ImgBB uploads (use when keys are rate-limited)")
	flag.Parse()

	// Fall back to environment proxy if not specified via flag
	if proxyFlag == "" {
		proxyFlag = os.Getenv("HTTP_PROXY")
	}
	if proxyFlag == "" {
		proxyFlag = os.Getenv("HTTPS_PROXY")
	}
	if proxyFlag == "" {
		proxyFlag = os.Getenv("ALL_PROXY")
	}

	log.SetFlags(log.Ltime)
	log.Printf("backfillmirrors: dry-run=%v limit=%d delay=%v proxy=%q", *dryRun, *limit, *delay, proxyFlag)

	initDownloadClient()

	// Load config from environment
	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()

	client := server.GetDBClient()
	if client == nil {
		log.Fatal("Supabase not configured")
	}

	// Get recordings with ANY Catbox URL (thumbnail, sprite, or preview)
	log.Printf("querying recordings with Catbox URLs...")
	candidates, err := client.GetRecordingsWithCatboxURLs()
	if err != nil {
		log.Fatalf("get catbox recordings: %v", err)
	}
	log.Printf("found %d recordings with Catbox URLs (any asset type)", len(candidates))

	// Also query for recordings with catbox sprites or previews
	// that might not have catbox thumbnails
	candidateMap := make(map[string]*database.Recording)
	for i := range candidates {
		candidateMap[candidates[i].Filename] = &candidates[i]
	}

	if *limit > 0 {
		// Apply limit
		var limited []*database.Recording
		for _, rec := range candidateMap {
			limited = append(limited, rec)
			if len(limited) >= *limit {
				break
			}
		}
		candidateMap = make(map[string]*database.Recording)
		for _, rec := range limited {
			candidateMap[rec.Filename] = rec
		}
	}

	// Convert to sorted slice
	var candidatePtrs []*database.Recording
	for _, rec := range candidateMap {
		candidatePtrs = append(candidatePtrs, rec)
	}
	candidateMap = nil // free

	log.Printf("processing %d recordings", len(candidatePtrs))

	// Create the image uploader
	imgUploader := uploader.NewMultiImageUploader()

	// Pre-check ImgBB availability
	if *skipImgBB {
		log.Printf("skipping ImgBB uploads (--skip-imgbb)")
		imgUploader.SetSkipHosts("ImgBB")
	} else {
		log.Printf("pre-checking ImgBB availability...")
		tmpImg, _ := os.CreateTemp("", "imgbb-check-*.jpg")
		tmpImg.Write([]byte{0xFF, 0xD8, 0xFF, 0xD9})
		tmpImg.Close()
		// Test ImgBB directly (not the full fallback chain)
		results := imgUploader.UploadToAll(tmpImg.Name())
		os.Remove(tmpImg.Name())
		imgbbOK := false
		for _, r := range results {
			if r.Host == "ImgBB" {
				if r.Err != nil {
					log.Printf("WARNING: ImgBB pre-check failed: %v", r.Err)
				} else {
					imgbbOK = true
				}
			}
		}
		if !imgbbOK {
			log.Printf("ImgBB not available, skipping ImgBB uploads")
			*skipImgBB = true
			imgUploader.SetSkipHosts("ImgBB")
		} else {
			log.Printf("ImgBB is available")
		}
	}

	var (
		processed  int
		succeeded  int
		failed     int
		totalAsset int
		mu         sync.Mutex
	)

	for i, rec := range candidatePtrs {
		log.Printf("[%d/%d] processing %s", i+1, len(candidatePtrs), rec.Filename)

		// Collect all Catbox URLs for this recording
		type asset struct {
			name     string
			url      string
			field    string // "thumbnail", "sprite", "preview"
		}
		var assets []asset

		if strings.Contains(rec.ThumbnailURL, "catbox.moe") {
			assets = append(assets, asset{"thumbnail", rec.ThumbnailURL, "thumbnail"})
		}
		if strings.Contains(rec.SpriteURL, "catbox.moe") {
			assets = append(assets, asset{"sprite", rec.SpriteURL, "sprite"})
		}
		if strings.Contains(rec.PreviewURL, "catbox.moe") {
			assets = append(assets, asset{"preview", rec.PreviewURL, "preview"})
		}

		if len(assets) == 0 {
			log.Printf("  SKIP: no Catbox URLs found")
			continue
		}

		log.Printf("  found %d Catbox assets to mirror: %v", len(assets), func() []string {
			var names []string
			for _, a := range assets {
				names = append(names, a.name)
			}
			return names
		}())

		// Process each asset
		for _, asset := range assets {
			mu.Lock()
			totalAsset++
			mu.Unlock()

			// Download from Catbox
			tmpFile, err := downloadThumbnail(asset.url, rec.Filename+"_"+asset.name)
			if err != nil {
				log.Printf("  ERROR: download %s failed: %v", asset.name, err)
				failed++
				continue
			}
			defer os.Remove(tmpFile)

			if *dryRun {
				log.Printf("  DRY-RUN: would upload %s to all hosts", filepath.Base(tmpFile))
				processed++
				continue
			}

			// Upload to all hosts in parallel
			results := imgUploader.UploadToAll(tmpFile)

			// Collect successful URLs and log failures
			successURLs := make(map[string]string)
			var primaryURL string
			for _, r := range results {
				if r.Err == nil && r.URL != "" {
					successURLs[r.Host] = r.URL
					if primaryURL == "" {
						primaryURL = r.URL
					}
				} else if r.Err != nil {
					log.Printf("  WARN: %s failed: %v", r.Host, r.Err)
				}
			}

			if len(successURLs) == 0 {
				log.Printf("  ERROR: all hosts failed for %s", asset.name)
				failed++
				continue
			}

			log.Printf("  %s: uploaded to %d hosts: %v", asset.name, len(successURLs), successURLs)

			// Update the appropriate field in the recording
			switch asset.field {
			case "thumbnail":
				rec.ThumbnailURL = primaryURL
				rec.ThumbnailMirrors = successURLs
			case "sprite":
				rec.SpriteURL = primaryURL
				rec.SpriteMirrors = successURLs
			case "preview":
				rec.PreviewURL = primaryURL
				rec.PreviewMirrors = successURLs
			}

			mu.Lock()
			processed++
			mu.Unlock()

			// Small delay between asset uploads
			time.Sleep(500 * time.Millisecond)
		}

		// Save the recording with all updated mirrors
		if !*dryRun {
			if err := client.SaveRecording(rec); err != nil {
				log.Printf("  ERROR: save recording: %v", err)
				failed++
				continue
			}

			// Update preview_images table
			img := &database.PreviewImage{
				Filename:          rec.Filename,
				ThumbnailURL:      rec.ThumbnailURL,
				ThumbnailMirrors:  rec.ThumbnailMirrors,
				SpriteURL:         rec.SpriteURL,
				SpriteMirrors:     rec.SpriteMirrors,
				PreviewURL:        rec.PreviewURL,
				PreviewMirrors:    rec.PreviewMirrors,
			}
			if err := client.SavePreviewImage(img); err != nil {
				log.Printf("  ERROR: save preview image: %v", err)
				failed++
				continue
			}

			log.Printf("  OK: saved recording with mirrors (thumb=%d, sprite=%d, preview=%d)",
				len(rec.ThumbnailMirrors), len(rec.SpriteMirrors), len(rec.PreviewMirrors))
			succeeded++
		}

		if *delay > 0 && i < len(candidatePtrs)-1 {
			time.Sleep(*delay)
		}
	}

	log.Printf("backfillmirrors: done — %d recordings, %d assets processed, %d succeeded, %d failed",
		len(candidatePtrs), totalAsset, succeeded, failed)
}

// downloadClient is an HTTP client that optionally uses a proxy for Catbox downloads.
var downloadClient = &http.Client{
	Timeout: 120 * time.Second,
	Transport: &http.Transport{
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
	},
}

var proxyToken string

func initDownloadClient() {
	if proxyFlag != "" {
		parsedProxy, err := url.Parse(proxyFlag)
		if err != nil {
			log.Printf("WARN: invalid proxy URL %q: %v, using direct connection", proxyFlag, err)
		} else {
			downloadClient.Transport = &http.Transport{
				Proxy: http.ProxyURL(parsedProxy),
			}
			log.Printf("using proxy %s for Catbox downloads", proxyFlag)
		}
	}
	if proxyToken == "" {
		proxyToken = os.Getenv("CF_ACCESS_TOKEN")
	}
}

// downloadThumbnail downloads an image from a URL to a temporary file.
func downloadThumbnail(imgURL, filename string) (string, error) {
	imgURL = strings.Replace(imgURL, "http://", "https://", 1)

	req, err := http.NewRequest("GET", imgURL, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	if proxyToken != "" {
		req.Header.Set("Authorization", "Bearer "+proxyToken)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "image/webp,image/apng,image/*,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := downloadClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", imgURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: status %d", imgURL, resp.StatusCode)
	}

	ext := filepath.Ext(filename)
	if ext == "" {
		ext = ".jpg"
	}

	tmpFile, err := os.CreateTemp("", "backfill-mirrors-*"+ext)
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("download: %w", err)
	}

	return tmpFile.Name(), nil
}

func configFromEnv() *entity.Config {
	return &entity.Config{
		SupabaseURL:           env("SUPABASE_URL"),
		SupabaseAPIKey:        env("SUPABASE_API_KEY"),
		SupabaseServiceRoleKey: env("SUPABASE_SERVICE_ROLE_KEY"),
	}
}

func env(k string) string { return os.Getenv(k) }

func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		if exe, e2 := os.Executable(); e2 == nil {
			f, err = os.Open(filepath.Join(filepath.Dir(exe), path))
		}
		if err != nil {
			return
		}
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, `"`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
