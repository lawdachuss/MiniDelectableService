package uploader

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// MultiImageUploader uploads images to configured hosts in linear fallback
// order: Catbox.moe → Pixhost.to → freeimage.host.  Each host gets at most
// 3 attempts.
//
// Sequential fallback is preferred over parallel upload because:
//   - Pixhost supports JPEG, PNG, GIF, WebP, and AVIF (all formats we
//     generate), so it succeeds in the vast majority of cases without
//     wasting API calls.
//   - Parallel uploads triggered unnecessary rate limiting and made errors
//     harder to diagnose.
//
// All three hosts support adult/NSFW content and provide permanent hosting.
// freeimage.host is the last resort before giving up (64 MB limit, no API
// key needed, permanent storage, adult-friendly).
//
// Format-specific host skipping:
//   - Imgbox only accepts JPG, PNG, GIF — .webp files are automatically
//     skipped to avoid wasted API calls.
type MultiImageUploader struct {
	catbox    *CatboxUploader
	pixhost   *ThumbnailUploader
	freeimage *FreeImageHostUploader
	imgchest  *ImgChestUploader
	imgbox    *ImgboxUploader
	imgbb     *ImgBBUploader
	imgpile   *ImgPileUploader
	skipHosts map[string]bool // hosts to skip in UploadToAll
}

// SetSkipHosts marks specific hosts to skip in UploadToAll.
func (m *MultiImageUploader) SetSkipHosts(hosts ...string) {
	if m.skipHosts == nil {
		m.skipHosts = make(map[string]bool)
	}
	for _, h := range hosts {
		m.skipHosts[h] = true
	}
}

// hostSupportsExtension reports whether the given host accepts files with
// the specified extension.  This avoids wasting API calls on hosts that
// are known to reject certain formats.
func hostSupportsExtension(host, ext string) bool {
	// Imgbox only accepts JPG, PNG, GIF — no WebP.
	if host == "Imgbox" && strings.EqualFold(ext, ".webp") {
		return false
	}
	return true
}

// NewMultiImageUploader creates a new image uploader that uploads to
// Catbox.moe, Pixhost.to, and freeimage.host (fallback order).
func NewMultiImageUploader() *MultiImageUploader {
	return &MultiImageUploader{
		catbox:    NewCatboxUploader(),
		pixhost:   NewThumbnailUploader(""),
		freeimage: NewFreeImageHostUploader(),
		imgchest:  NewImgChestUploader(),
		imgbox:    NewImgboxUploader(),
		imgbb:     NewImgBBUploader(),
		imgpile:   NewImgPileUploader(),
	}
}

// uploadWithRetries tries fn up to maxAttempts times with exponential
// backoff and returns the result of the first successful call.  If a single
// attempt fails because the host is dead or rate-limiting us, it bails
// (no pointless retries on that host) so the caller's fallback chain can
// move on to the next host.
//
// Rate limits are time-windowed (e.g. ImgBB = N uploads/hour, Pixhost =
// burst limits), so a rate-limited attempt is retried with a short delay the
// first time to give the window a chance to open — but never more than
// maxRateRetry attempts, after which the host is abandoned for this file.
func uploadWithRetries(maxAttempts int, label string, fn func() (string, error)) (url string, err error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
		u, e := fn()
		if e == nil {
			return u, nil
		}
		lastErr = e
		// Replace an instant-bail on a rate limit with a single short-delay
		// retry: rate limits are transient and the delay may let the window
		// reopen, avoiding a spurious "all hosts rejected" for the whole file.
		if isFailFastError(e) && !isUploadRateLimited(e) {
			return "", e
		}
	}
	return "", lastErr
}

// ImageUploadResult contains the result of an image upload to a specific host.
type ImageUploadResult struct {
	Host string
	URL  string
	Err  error
}

// Upload returns the first successful direct image URL. Order: Catbox.moe
// FIRST, then Pixhost, then freeimage.host as fallbacks.
//
// All three hosts support adult/NSFW content and provide permanent hosting.
// On the GitHub Actions runner Catbox MUST be reached through CATBOX_PROXY_URL
// (a Cloudflare Worker), which leaves from Cloudflare's edge IPs that Catbox
// does not block — making it the only image host that reliably succeeds from
// the datacenter runner IPs (Pixhost.to and freeimage.host otherwise
// block/rate-limit the runner). They are kept only as fallbacks for the rare
// case the proxy/Catbox is unavailable.
func (m *MultiImageUploader) Upload(filePath string) (url, host string, err error) {
	// Catbox.moe first — routed through CATBOX_PROXY_URL on the runner so it
	// succeeds from datacenter IPs where other hosts are blocked.
	url, err = uploadWithRetries(3, "Catbox", func() (string, error) {
		return m.catbox.Upload(filePath)
	})
	if err == nil {
		return url, "Catbox", nil
	}
	catboxErr := err

	url, err = uploadWithRetries(3, "Pixhost", func() (string, error) {
		return m.pixhost.Upload(filePath)
	})
	if err == nil {
		return url, "Pixhost", nil
	}
	pixhostErr := err

	url, err = uploadWithRetries(3, "freeimage.host", func() (string, error) {
		return m.freeimage.Upload(filePath)
	})
	if err == nil {
		return url, "freeimage.host", nil
	}
	freeimageErr := err

	// ImgChest — only if token is configured
	if m.imgchest.HasToken() {
		url, err = uploadWithRetries(3, "ImgChest", func() (string, error) {
			return m.imgchest.Upload(filePath)
		})
		if err == nil {
			return url, "ImgChest", nil
		}
		imgchestErr := err

		// ImgPile — token host, NSFW-friendly (only if key is configured)
		imgpileErr := fmt.Errorf("imgpile: IMGPILE_KEY not set")
		if m.imgpile.HasToken() {
			url, err = uploadWithRetries(3, "ImgPile", func() (string, error) {
				return m.imgpile.Upload(filePath)
			})
			if err == nil {
				return url, "ImgPile", nil
			}
			imgpileErr = err
		}

		// Imgbox — last resort (skip for .webp which Imgbox rejects)
		if hostSupportsExtension("Imgbox", filepath.Ext(filePath)) {
			url, err = uploadWithRetries(3, "Imgbox", func() (string, error) {
				return m.imgbox.Upload(filePath)
			})
			if err == nil {
				return url, "Imgbox", nil
			}
		}
		imgboxErr := err

		// ImgBB — absolute last resort
		if m.imgbb.keys.count() > 0 {
			url, err = uploadWithRetries(3, "ImgBB", func() (string, error) {
				return m.imgbb.Upload(filePath)
			})
			if err == nil {
				return url, "ImgBB", nil
			}
			imgbbErr := err
			return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgchest: %v, imgpile: %v, imgbox: %v, imgbb: %v)", catboxErr, pixhostErr, freeimageErr, imgchestErr, imgpileErr, imgboxErr, imgbbErr)
		}
		return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgchest: %v, imgpile: %v, imgbox: %v)", catboxErr, pixhostErr, freeimageErr, imgchestErr, imgpileErr, imgboxErr)
	}

	// ImgPile — token host, tried when ImgChest is not configured
	imgpileErr := fmt.Errorf("imgpile: IMGPILE_KEY not set")
	if m.imgpile.HasToken() {
		url, err = uploadWithRetries(3, "ImgPile", func() (string, error) {
			return m.imgpile.Upload(filePath)
		})
		if err == nil {
			return url, "ImgPile", nil
		}
		imgpileErr = err
	}

	// Imgbox — fallback when ImgChest is not configured (skip for .webp)
	if hostSupportsExtension("Imgbox", filepath.Ext(filePath)) {
		url, err = uploadWithRetries(3, "Imgbox", func() (string, error) {
			return m.imgbox.Upload(filePath)
		})
		if err == nil {
			return url, "Imgbox", nil
		}
	}
	imgboxErr := err

	// ImgBB — absolute last resort
	if m.imgbb.keys.count() > 0 {
		url, err = uploadWithRetries(3, "ImgBB", func() (string, error) {
			return m.imgbb.Upload(filePath)
		})
		if err == nil {
			return url, "ImgBB", nil
		}
		imgbbErr := err
		return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgpile: %v, imgbox: %v, imgbb: %v)", catboxErr, pixhostErr, freeimageErr, imgpileErr, imgboxErr, imgbbErr)
	}

	return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgpile: %v, imgbox: %v)", catboxErr, pixhostErr, freeimageErr, imgpileErr, imgboxErr)
}

// OnSuccessFunc is called the moment a single host finishes uploading,
// receiving the host name and URL.  Used to persist metadata (Supabase
// PATCH) immediately instead of waiting for every host to finish.
type OnSuccessFunc func(host, url string)

// UploadToAll uploads the image to ALL configured hosts in parallel.
// Returns a slice of results, one per host. The caller should pick the
// first successful URL for the primary thumbnail_url, and can optionally
// store additional mirror URLs for redundancy.
//
// If onHost is non-nil it is called the instant each host succeeds,
// before the next host finishes — so the caller can persist the URL
// (e.g. Supabase upsert) without waiting for slower hosts.
//
// This provides maximum redundancy: even if one host goes down, the
// thumbnail is still available on the others.
func (m *MultiImageUploader) UploadToAll(filePath string, onHost OnSuccessFunc) []ImageUploadResult {
	type hostJob struct {
		name string
		fn   func(string) (string, error)
	}

	jobs := []hostJob{}
	// Add hosts, skipping any in skipHosts
	addJob := func(name string, fn func(string) (string, error)) {
		if m.skipHosts != nil && m.skipHosts[name] {
			return
		}
		jobs = append(jobs, hostJob{name, fn})
	}
	addJob("Catbox", m.catbox.Upload)
	addJob("Pixhost", m.pixhost.Upload)
	addJob("freeimage.host", m.freeimage.Upload)
	if m.imgchest.HasToken() {
		addJob("ImgChest", m.imgchest.Upload)
	}
	if m.imgpile.HasToken() {
		addJob("ImgPile", m.imgpile.Upload)
	}
	// Skip Imgbox for .webp files — Imgbox only accepts JPG, PNG, GIF.
	if hostSupportsExtension("Imgbox", filepath.Ext(filePath)) {
		addJob("Imgbox", m.imgbox.Upload)
	}
	if m.imgbb.keys.count() > 0 {
		addJob("ImgBB", m.imgbb.Upload)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]ImageUploadResult, 0, len(jobs))

	for _, job := range jobs {
		wg.Add(1)
		go func(j hostJob) {
			defer wg.Done()
			url, err := uploadWithRetries(3, j.name, func() (string, error) {
				return j.fn(filePath)
			})
			if err != nil {
				log.Printf("UploadToAll: %s failed for %s: %v", j.name, filepath.Base(filePath), err)
			}
			if err == nil && url != "" && onHost != nil {
				onHost(j.name, url)
			}
			mu.Lock()
			results = append(results, ImageUploadResult{
				Host: j.name,
				URL:  url,
				Err:  err,
			})
			mu.Unlock()
		}(job)
	}

	wg.Wait()
	return results
}

// UploadToAllURLs is a convenience wrapper that returns only the successful
// URLs from UploadToAll, keyed by host name.  If onHost is non-nil it is
// called the instant each host succeeds (see UploadToAll).
func (m *MultiImageUploader) UploadToAllURLs(filePath string, onHost OnSuccessFunc) map[string]string {
	results := m.UploadToAll(filePath, onHost)
	urls := make(map[string]string)
	for _, r := range results {
		if r.Err == nil && r.URL != "" {
			urls[r.Host] = r.URL
		}
	}
	return urls
}
