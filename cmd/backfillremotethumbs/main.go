// Command backfillremotethumbs restores missing static thumbnails for
// recordings whose local video has already been deleted ("videos" page shows
// no thumbnail).
//
// Unlike backfillst (Streamtape's token-free API lets us Range-download the
// video head), the hosts that actually stored these recordings — GoFile,
// Mixdrop, Vidara, VOE.sx — gate direct video downloads behind
// Cloudflare/turnstile sessions and obfuscated players, so we cannot pull the
// video file to render a frame. However, Vidara renders its own thumbnail for
// each uploaded video and exposes it as the page's <meta property="og:image">,
// which we can fetch with a plain HTTP GET and re-host via the standard
// MultiImageUploader.
//
// Flow for each missing-thumbnail recording:
//  1. load all upload_links and recordings; keep recordings whose
//     thumbnail_url is empty AND that have a Vidara link,
//  2. GET the Vidara embed page (vidarae.live/e/<code>) and extract the
//     og:image thumbnail URL,
//  3. download that thumbnail,
//  4. re-host it via MultiImageUploader (Pixhost -> ImgBB -> Catbox), keeping
//     the host mirror map,
//  5. PATCH recordings.thumbnail_url and save a preview_images row,
//  6. log a per-host summary.
//
// Usage:
//
//	go run ./cmd/backfillremotethumbs [-dry] [-max N]
//
// Flags:
//
//	-dry   resolve + report thumbnail URLs only, do not upload or PATCH
//	-max N process at most N recordings (all by default)
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

var (
	// ogImageRe matches <meta property="og:image" content="..."/> on Vidara's
	// embed page. The thumbnail is video-specific (pointing at the CDN path).
	ogImageRe = regexp.MustCompile(`(?i)<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']`)
)

// downloadClient reuses a single HTTP client with browser-like headers so the
// Vidara thumbnail CDN serves us the real JPEG (it 403s default curl UAs).
var downloadClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 45 * time.Second,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
	},
}

func main() {
	dry := flag.Bool("dry", false, "resolve + report thumbnail URLs only")
	max := flag.Int("max", 0, "max recordings to process (0 = all)")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Ldate)
	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	if !*dry && cfg.SupabaseServiceRoleKey == "" {
		log.Fatal("SUPABASE_SERVICE_ROLE_KEY not set (required for writes)")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()

	client := server.GetDBClient()
	if client == nil {
		log.Fatal("Supabase not configured")
	}

	// 1. Load all upload links and group them by recording_id.
	links, err := client.GetAllUploadLinks()
	if err != nil {
		log.Fatalf("get upload links: %v", err)
	}
	linksByRec := map[string][]database.UploadLink{}
	for _, l := range links {
		linksByRec[l.RecordingID] = append(linksByRec[l.RecordingID], l)
	}
	log.Printf("loaded %d upload links across %d recordings", len(links), len(linksByRec))

	// 2. Load all recordings and pick those with an empty thumbnail_url.
	recs, err := client.GetAllRecordings()
	if err != nil {
		log.Fatalf("get recordings: %v", err)
	}
	var missing []database.Recording
	for i := range recs {
		if strings.TrimSpace(recs[i].ThumbnailURL) == "" {
			missing = append(missing, recs[i])
		}
	}
	log.Printf("%d recordings total, %d missing a thumbnail", len(recs), len(missing))

	// 3. Build work items: for each missing recording, find its Vidara link.
	type workItem struct {
		rec  *database.Recording
		link database.UploadLink
	}
	var work []workItem
	for i := range missing {
		rec := &missing[i]
		for _, l := range linksByRec[rec.ID] {
			if strings.EqualFold(l.Host, "Vidara") && strings.TrimSpace(l.URL) != "" {
				work = append(work, workItem{rec, l})
				break
			}
		}
	}
	log.Printf("of %d missing-thumbnail recordings, %d have a Vidara link to recover from", len(missing), len(work))

	if *max > 0 && len(work) > *max {
		work = work[:*max]
	}

	// 4+5. Resolve + optionally upload + PATCH.
	imgUploader := uploader.NewMultiImageUploader()
	var (
		ok       int
		failed   int
		noThumb  int
		rehosted int
	)
	for i, w := range work {
		code := vidaraCode(w.link.URL)
		if code == "" {
			log.Printf("[%d/%d] %s: bad Vidara URL %q", i+1, len(work), w.rec.Filename, w.link.URL)
			failed++
			continue
		}
		thumbURL, err := resolveVidaraThumb(code)
		if err != nil {
			log.Printf("[%d/%d] %s: resolve thumb: %v", i+1, len(work), w.rec.Filename, err)
			failed++
			continue
		}
		if thumbURL == "" {
			log.Printf("[%d/%d] %s: no og:image on page", i+1, len(work), w.rec.Filename)
			noThumb++
			continue
		}
		log.Printf("[%d/%d] %s: host thumbnail %s", i+1, len(work), w.rec.Filename, thumbURL)

		if *dry {
			ok++
			continue
		}

		// Download the host thumbnail.
		imgPath, mime, err := downloadThumb(thumbURL, w.rec.Filename)
		if err != nil {
			log.Printf("  ERROR download: %v", err)
			failed++
			continue
		}
		var ext string
		switch mime {
		case "image/webp":
			ext = ".webp"
		default:
			ext = ".jpg"
		}
		finalPath := imgPath
		if strings.ToLower(filepath.Ext(imgPath)) != ext {
			finalPath = imgPath + ext
			if err := os.Rename(imgPath, finalPath); err != nil {
				finalPath = imgPath
			}
		}

		// Re-host via the standard image pipeline.
		results := imgUploader.UploadToAll(finalPath, nil)
		os.Remove(finalPath)
		mirrors := map[string]string{}
		var primary string
		for _, r := range results {
			if r.Err == nil && r.URL != "" {
				mirrors[r.Host] = r.URL
				if primary == "" {
					primary = r.URL
				}
			}
		}
		if primary == "" {
			log.Printf("  ERROR: no image host accepted the thumbnail")
			failed++
			continue
		}

		// 6. PATCH recordings + preview_images.
		if err := server.UpdateRecordingThumbnails(w.rec.Filename, primary, "", ""); err != nil {
			log.Printf("  ERROR patch recordings: %v", err)
			failed++
			continue
		}
		if err := server.SavePreviewLinks(w.rec.Filename, primary, "", "", mirrors, nil, nil); err != nil {
			log.Printf("  ERROR patch preview_images: %v", err)
		}
		log.Printf("  OK -> %s (%d host mirrors)", primary, len(mirrors))
		ok++
		rehosted++
		time.Sleep(2 * time.Second) // pace image-host uploads
	}

	log.Printf("done: %d ok (%d re-hosted), %d failed, %d no-thumbnail-page", ok, rehosted, failed, noThumb)
}

// resolveVidaraThumb fetches the Vidara embed page and returns the og:image
// thumbnail URL, or "" if none is present.
func resolveVidaraThumb(code string) (string, error) {
	req, err := http.NewRequest("GET", "https://vidarae.live/e/"+code, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", browserUA)
	resp, err := downloadClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("page status %d", resp.StatusCode)
	}
	m := ogImageRe.FindSubmatch(body)
	if len(m) < 2 {
		return "", nil
	}
	u := string(m[1])
	// The og:image may be protocol-relative; normalise to https.
	u = strings.TrimPrefix(u, "//")
	if !strings.HasPrefix(u, "http") {
		u = "https://" + u
	}
	return u, nil
}

// downloadThumb downloads a thumbnail to a temp file and returns its path and
// content type.
func downloadThumb(imgURL, name string) (string, string, error) {
	req, err := http.NewRequest("GET", imgURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "image/webp,image/apng,image/*,*/*;q=0.8")
	resp, err := downloadClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("status %d", resp.StatusCode)
	}
	tmp, err := os.CreateTemp("", "bfr-"+sanitizeName(name)+"-*")
	if err != nil {
		return "", "", err
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", "", err
	}
	tmp.Close()
	mime := resp.Header.Get("Content-Type")
	if i := strings.Index(mime, ";"); i >= 0 {
		mime = mime[:i]
	}
	return tmp.Name(), mime, nil
}

// vidaraCode extracts the trailing file code from a Vidara share/embed URL
// (anything after the last "/", 6+ alphanumeric chars).
func vidaraCode(u string) string {
	u = strings.TrimSpace(u)
	if i := strings.IndexAny(u, "?#"); i >= 0 {
		u = u[:i]
	}
	u = strings.TrimRight(u, "/")
	if i := strings.LastIndex(u, "/"); i >= 0 {
		u = u[i+1:]
	}
	if len(u) < 6 {
		return ""
	}
	for _, r := range u {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return ""
		}
	}
	return u
}

func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"

func configFromEnv() *entity.Config {
	return &entity.Config{
		SupabaseURL:            env("SUPABASE_URL"),
		SupabaseAPIKey:         env("SUPABASE_API_KEY"),
		SupabaseServiceRoleKey: env("SUPABASE_SERVICE_ROLE_KEY"),
		FFmpegPath:             env("FFMPEG_PATH"),
		Domain:                 or(env("DOMAIN"), "https://www.cb.xxx/"),
	}
}

func env(k string) string { return os.Getenv(k) }

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

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
		v = strings.Trim(v, `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
