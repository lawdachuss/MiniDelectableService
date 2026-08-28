package uploader

import (
	"fmt"
	"time"
)

// MultiImageUploader uploads images to configured hosts in linear fallback
// order: Pixhost.to → ImgBB → Catbox.moe.  Each host gets at most 3 attempts.
//
// Sequential fallback is preferred over parallel upload because:
//   - Pixhost supports JPEG, PNG, and GIF (all formats we generate), so it
//     succeeds in the vast majority of cases without wasting API calls.
//   - Parallel upload to Pixhost + ImgBB triggered unnecessary rate limiting
//     on ImgBB and made errors harder to diagnose.
type MultiImageUploader struct {
	pixhost *ThumbnailUploader
	imgbb   *ImgBBUploader
	catbox  *CatboxUploader
}

// NewMultiImageUploader creates a new image uploader that uploads to
// Pixhost.to, ImgBB, and Catbox.moe (fallback order).
func NewMultiImageUploader() *MultiImageUploader {
	return &MultiImageUploader{
		pixhost: NewThumbnailUploader(""),
		imgbb:   NewImgBBUploader(),
		catbox:  NewCatboxUploader(),
	}
}

// uploadWithRetries tries fn up to maxAttempts times with exponential
// backoff and returns the result of the first successful call.  If a single
// attempt fails because the host is dead or rate-limiting us, it bails
// immediately (no pointless retries on that host) so the caller's fallback
// chain can move on to the next host.
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
		// Dead or rate-limiting host — don't keep hammering it; let the
		// fallback (Pixhost → ImgBB → Catbox) try the next host now.
		if isFailFastError(e) {
			return "", e
		}
	}
	return "", lastErr
}

// Upload returns the first successful direct image URL. Order: Catbox.moe
// FIRST, then Pixhost, then ImgBB as fallbacks.
//
// On the GitHub Actions runner Catbox MUST be reached through CATBOX_PROXY_URL
// (a Cloudflare Worker), which leaves from Cloudflare's edge IPs that Catbox
// does not block — making it the only image host that reliably succeeds from
// the datacenter runner IPs (Pixhost.to and ImgBB otherwise block/rate-limit
// the runner). They are kept only as fallbacks for the rare case the
// proxy/Catbox is unavailable.
func (m *MultiImageUploader) Upload(filePath string) (url, host string, err error) {
	// Catbox.moe first — routed through CATBOX_PROXY_URL on the runner so it
	// succeeds from datacenter IPs where Pixhost/ImgBB are blocked.
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

	url, err = uploadWithRetries(3, "ImgBB", func() (string, error) {
		return m.imgbb.Upload(filePath)
	})
	if err == nil {
		return url, "ImgBB", nil
	}

	return "", "", fmt.Errorf("catbox: %w (pixhost: %v, imgbb: %v)", catboxErr, pixhostErr, err)
}
