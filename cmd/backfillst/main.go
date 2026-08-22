// Command backfillst regenerates missing static thumbnails for recordings
// whose local video is gone but whose file still exists on Streamtape.
//
// Streamtape's official API (api.streamtape.com) provides download tickets
// (file/dlticket) and direct download links (file/dl) that support HTTP Range
// requests. Cam recordings are remuxed with the moov atom near the front, so
// downloading only the first few MB is enough to seek to an early frame and
// render a thumbnail — no need to pull the whole (sometimes 700MB+) file.
//
// For each manifest entry it:
//  1. requests a dlticket, waits the required wait_time, then gets a dl URL,
//  2. downloads only the first -partial bytes via Range (resuming across
//     fresh tickets when the slow CDN link expires mid-transfer),
//  3. extracts a frame at -seek seconds, scaled+padded to 1280x720,
//  4. if the partial head is unreadable, falls back to a tail Range grab
//     (moov-at-end mp4s), then gives up (corrupt file) if that fails too,
//  5. uploads through the standard MultiImageUploader
//     (Pixhost -> ImgBB -> Catbox fallback),
//  6. PATCHes thumbnail_url onto the recordings row AND the preview_images row.
//
// Usage:
//
//	go run ./cmd/backfillst [-partial 10485760] [-seek 2] backfill_manifest.json
//
// Manifest format (JSON array):
//
//	[{"filename": "user_2026-08-14_18-23-39.mp4", "filecode": "P3zVwykJMlF0723"}]
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

const (
	thumbWidth  = 1280
	thumbHeight = 720
	streamAPI   = "https://api.streamtape.com"
)

// cloudflareIPs are known-good Anycast addresses for api.streamtape.com.
// Some ISP resolvers return a stale origin IP (49.44.79.236) that no longer
// responds; the Cloudflare-fronted addresses are the durable path.
var cloudflareIPs = []string{"104.21.96.46", "172.67.173.3"}

type manifestEntry struct {
	Filename  string `json:"filename"`
	Filecode  string `json:"filecode"`
	PartialMB int    `json:"partial_mb,omitempty"` // per-entry override (MB)
}

func main() {
	partialMB := flag.Int("partial", 4, "MB to download from the start of each file")
	seek := flag.Float64("seek", 2, "seconds into the video to grab the thumbnail frame")
	tailMB := flag.Int("tail", 0, "MB to grab from the end as fallback for moov-at-end mp4s (0 = off)")
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: go run ./cmd/backfillst [-partial 10] [-seek 2] <manifest.json>")
	}

	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	if cfg.SupabaseServiceRoleKey == "" {
		log.Fatal("SUPABASE_SERVICE_ROLE_KEY not set (required for preview_images writes)")
	}
	if cfg.StreamtapeLogin == "" || cfg.StreamtapeKey == "" {
		log.Fatal("STREAMTAPE_LOGIN / STREAMTAPE_API_KEY not set")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()

	data, err := os.ReadFile(flag.Arg(0))
	if err != nil {
		log.Fatalf("read manifest: %v", err)
	}
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF}) // strip UTF-8 BOM
	var entries []manifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Fatalf("parse manifest: %v", err)
	}
	if len(entries) == 0 {
		log.Fatal("manifest is empty")
	}
	log.Printf("backfilling %d thumbnails from Streamtape (partial=%dMB seek=%gs)", len(entries), *partialMB, *seek)

	dir, err := os.MkdirTemp("", "backfillst")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	okCount := 0
	for i, e := range entries {
		mb := *partialMB
		if e.PartialMB > 0 {
			mb = e.PartialMB
		}
		log.Printf("[%d/%d] %s (filecode %s, partial %dMB)", i+1, len(entries), e.Filename, e.Filecode, mb)
		start := time.Now()
		thumbURL, err := backfillOne(e, mb, *seek, *tailMB, dir)
		if err != nil {
			log.Printf("  FAIL after %s: %v", time.Since(start).Round(time.Second), err)
			continue
		}
		log.Printf("  OK in %s: %s", time.Since(start).Round(time.Second), thumbURL)
		okCount++
		time.Sleep(2 * time.Second) // pace image-host uploads
	}
	log.Printf("done: %d/%d recordings backfilled", okCount, len(entries))
}

// backfillOne handles a single recording: download a partial of the video,
// render a frame, upload it, and patch both DB tables.
func backfillOne(e manifestEntry, partialMB int, seek float64, tailMB int, workDir string) (string, error) {
	fn := e.Filename
	partPath := filepath.Join(workDir, "part_"+sanitizeName(fn)+".mp4")
	tailPath := filepath.Join(workDir, "tail_"+sanitizeName(fn)+".mp4")
	thumbPath := filepath.Join(workDir, "thumb_"+sanitizeName(fn)+".jpg")

	// 1. Grab the head of the video (moov is at the front for faststart files).
	headBytes := int64(partialMB) * 1024 * 1024
	start := time.Now()
	if err := streamGrab(e.Filecode, partPath, 0, headBytes); err != nil {
		return "", fmt.Errorf("download head: %w", err)
	}
	log.Printf("  head: %d bytes in %s", mustSize(partPath), time.Since(start).Round(time.Second))
	videoPath := partPath

	// 2. Try to render a frame from the head.
	if err := extractThumb(videoPath, thumbPath, seek); err != nil {
		// Fall back to the tail (moov-at-end or sparse head).
		log.Printf("  head unreadable (%v) — trying tail grab", err)
		if tailMB <= 0 {
			return "", fmt.Errorf("frame extraction failed on head (corrupt file?) and tail fallback disabled: %v", err)
		}
		tailBytes := int64(tailMB) * 1024 * 1024
		// Streamtape does not report file size upfront; grab a tail range and
		// let ffmpeg probe it. Tail ranges are addressed from the end.
		if err := streamGrabTail(e.Filecode, tailPath, tailBytes); err != nil {
			return "", fmt.Errorf("download tail: %w", err)
		}
		log.Printf("  tail: %d bytes", mustSize(tailPath))
		videoPath = tailPath
		if err := extractThumb(videoPath, thumbPath, seek); err != nil {
			return "", fmt.Errorf("frame extraction failed on head and tail (corrupt file?): %v", err)
		}
	}
	if _, err := os.Stat(thumbPath); err != nil {
		return "", fmt.Errorf("thumbnail file not produced: %v", err)
	}
	log.Printf("  frame: %s", thumbPath)

	// 3. Upload through the standard image pipeline.
	imgUploader := uploader.NewMultiImageUploader()
	thumbURL, _, err := imgUploader.Upload(thumbPath)
	if err != nil {
		return "", fmt.Errorf("upload thumbnail: %w", err)
	}

	// 4. Patch both tables (these files have no sprite/preview to preserve).
	if err := server.UpdateRecordingThumbnails(fn, thumbURL, "", ""); err != nil {
		return thumbURL, fmt.Errorf("patch recordings row: %w", err)
	}
	if err := server.SavePreviewLinks(fn, thumbURL, "", ""); err != nil {
		return thumbURL, fmt.Errorf("patch preview_images row: %w", err)
	}
	return thumbURL, nil
}

// extractThumb renders a 1280x720 padded JPEG from an early frame.
func extractThumb(videoPath, thumbPath string, seek float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-ss", fmt.Sprintf("%g", seek),
		"-i", videoPath,
		"-vf", fmt.Sprintf(
			"scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
			thumbWidth, thumbHeight, thumbWidth, thumbHeight),
		"-frames:v", "1",
		"-c:v", "mjpeg",
		"-q:v", "5",
		thumbPath,
	}
	if out, err := config.FFmpegCommandContext(ctx, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	if fi, err := os.Stat(thumbPath); err != nil || fi.Size() < 5000 {
		return fmt.Errorf("thumbnail too small (%v)", err)
	}
	return nil
}

// newHTTPClient builds an HTTP client whose transport dials api.streamtape.com
// through the Cloudflare edge (bypassing the ISP resolver's stale origin IP)
// and caps TLS at 1.2 — Cloudflare resets the TLS 1.3 handshake for this host.
// All other hosts resolve and connect normally.
func newHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				if host == "api.streamtape.com" {
					var lastErr error
					for _, ip := range cloudflareIPs {
						c, e := dialer.DialContext(ctx, "tcp4", net.JoinHostPort(ip, port))
						if e == nil {
							return c, nil
						}
						lastErr = e
					}
					return nil, fmt.Errorf("all cloudflare IPs unreachable: %w", lastErr)
				}
				return dialer.DialContext(ctx, network, addr)
			},
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12},
		},
	}
}

// streamGrab downloads bytes [start, start+length) of the file to dstPath,
// resuming across fresh download tickets when a CDN link dies mid-transfer.
func streamGrab(filecode, dstPath string, start, length int64) error {
	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer f.Close()

	client := newHTTPClient(600 * time.Second)
	got := int64(0)
	lastProgress := time.Now()
	for tries := 0; tries < 8; tries++ {
		if got >= length {
			return nil
		}
		dlURL, err := freshDLURL(filecode)
		if err != nil {
			if tries == 7 {
				return err
			}
			time.Sleep(3 * time.Second)
			continue
		}
		req, err := http.NewRequest("GET", dlURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start+got, start+length-1))
		resp, err := client.Do(req)
		if err != nil {
			if tries == 7 {
				return err
			}
			time.Sleep(3 * time.Second)
			continue
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			// Range is beyond the file size — we already hold the whole file.
			resp.Body.Close()
			if got > 0 {
				return nil
			}
			return fmt.Errorf("file is empty")
		}
		n, copyErr := io.Copy(f, resp.Body)
		resp.Body.Close()
		if n > 0 {
			got += n
			lastProgress = time.Now()
			log.Printf("  dl: +%d MB -> %d MB total", n>>20, got>>20)
		}
		// Successful range read that filled the request is complete.
		if copyErr == nil && got >= length {
			return nil
		}
		if n == 0 && copyErr == nil {
			return fmt.Errorf("server returned no data for range (file may be smaller than requested)")
		}
		// Link died or timed out — keep whatever we got and resume on a new URL.
		if time.Since(lastProgress) > 140*time.Second && tries == 7 {
			return fmt.Errorf("download stalled after %d bytes", got)
		}
		if tries == 7 {
			return fmt.Errorf("gave up after 8 attempts at offset %d", got)
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

// streamGrabTail downloads the last `length` bytes of the file (moov-at-end
// fallback). The total size is discovered with a 1-byte range request.
func streamGrabTail(filecode, dstPath string, length int64) error {
	client := newHTTPClient(60 * time.Second)
	dlURL, err := freshDLURL(filecode)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("GET", dlURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	var total int64
	if cr := resp.Header.Get("Content-Range"); cr != "" && strings.Contains(cr, "/") {
		totalStr := cr[strings.LastIndex(cr, "/")+1:]
		fmt.Sscanf(totalStr, "%d", &total)
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if total <= 0 {
		return fmt.Errorf("could not determine file size")
	}
	return streamGrab(filecode, dstPath, total-length, length)
}

// freshDLURL performs the dlticket -> (wait) -> dl dance and returns a direct
// download URL for the filecode. The Cloudflare edge intermittently resets the
// TLS handshake, so the whole sequence is retried a few times.
func freshDLURL(filecode string) (string, error) {
	login := server.Config.StreamtapeLogin
	key := server.Config.StreamtapeKey

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		url, err := oneTicket(filecode, login, key)
		if err == nil {
			return url, nil
		}
		lastErr = err
		log.Printf("  streamtape ticket attempt %d/4 failed: %v", attempt+1, err)
		time.Sleep(3 * time.Second)
	}
	return "", lastErr
}

func oneTicket(filecode, login, key string) (string, error) {
	client := newHTTPClient(40 * time.Second)

	ticketURL := fmt.Sprintf("%s/file/dlticket?file=%s&login=%s&key=%s", streamAPI, filecode, login, key)
	resp, err := client.Get(ticketURL)
	if err != nil {
		return "", fmt.Errorf("dlticket: %w", err)
	}
	var tr struct {
		Status int `json:"status"`
		Result struct {
			Ticket   string `json:"ticket"`
			WaitTime int    `json:"wait_time"`
		} `json:"result"`
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("dlticket decode: %w", err)
	}
	if tr.Status != 200 || tr.Result.Ticket == "" {
		return "", fmt.Errorf("dlticket failed: %s", strings.TrimSpace(string(body)))
	}
	time.Sleep(time.Duration(tr.Result.WaitTime+1) * time.Second)

	dlURL := fmt.Sprintf("%s/file/dl?file=%s&ticket=%s", streamAPI, filecode, tr.Result.Ticket)
	resp, err = client.Get(dlURL)
	if err != nil {
		return "", fmt.Errorf("dl: %w", err)
	}
	var dr struct {
		Status int `json:"status"`
		Result struct {
			URL string `json:"url"`
		} `json:"result"`
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err := json.Unmarshal(body, &dr); err != nil {
		return "", fmt.Errorf("dl decode: %w", err)
	}
	if dr.Status != 200 || dr.Result.URL == "" {
		return "", fmt.Errorf("dl failed: %s", strings.TrimSpace(string(body)))
	}
	return dr.Result.URL, nil
}

func mustSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return fi.Size()
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

// configFromEnv mirrors cmd/backfillthumbs' minimal config wiring.
func configFromEnv() *entity.Config {
	cfg := &entity.Config{
		SupabaseURL:             env("SUPABASE_URL"),
		SupabaseAPIKey:          env("SUPABASE_API_KEY"),
		SupabaseServiceRoleKey:  env("SUPABASE_SERVICE_ROLE_KEY"),
		VoeSXAPIKey:             env("VOESX_API_KEY"),
		StreamtapeLogin:         env("STREAMTAPE_LOGIN"),
		StreamtapeKey:           or(env("STREAMTAPE_KEY"), env("STREAMTAPE_API_KEY")),
		MixdropEmail:            env("MIXDROP_EMAIL"),
		MixdropToken:            or(env("MIXDROP_TOKEN"), env("MIXDROP_KEY")),
		VidaraKey:               env("VIDARA_KEY"),
		Domain:                  or(env("DOMAIN"), "https://www.cb.xxx/"),
		FFmpegPath:              env("FFMPEG_PATH"),
	}
	if cfg.FFmpegPath != "" {
		config.SetFFmpegPath(cfg.FFmpegPath)
	}
	return cfg
}

func env(k string) string { return os.Getenv(k) }

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// loadDotEnv loads KEY=VALUE pairs from a .env file into the environment,
// without overwriting existing variables. Falls back to the executable dir.
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
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		v = strings.Trim(v, `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}