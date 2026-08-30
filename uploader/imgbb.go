package uploader

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const imgbbAPIURL = "https://api.imgbb.com/1/upload"

// imgbbGlobal throttling: ImgBB's free tier rate-limits by IP address, not
// by API key.  All keys from the same server share one rate-limit bucket.
// The exact limit is undocumented but empirically ~50–100 uploads/h per IP;
// exceeding it returns HTTP 400 {"error":{"message":"Rate limit reached.","code":100}}.
// During a host outage or mass backfill every preview falls back to ImgBB;
// an unthrottled burst exhausts the bucket in seconds and all keys fail
// simultaneously.  Spacing calls process-wide keeps the throughput within
// quota.
var (
	imgbbMu         sync.Mutex
	imgbbLastUpload time.Time
	// imgbbBackoffUntil records when a rate-limit error was last seen.
	// While active, throttleImgBB uses the longer imgbbCooldownInterval
	// instead of imgbbMinInterval to give the IP-level bucket time to
	// refill.
	imgbbBackoffUntil time.Time
)

// imgbbMinInterval is the normal minimum spacing between ImgBB API calls.
// ~50–100 uploads/h per IP → 36–72 s between calls; 12 s gives ~300/h
// ceiling which stays well under quota during normal (non-backfill) usage.
const imgbbMinInterval = 12 * time.Second

// imgbbCooldownInterval is the spacing used after a rate-limit error.
// It is intentionally longer than imgbbMinInterval to let the IP-level
// bucket recover before resuming normal pacing.
const imgbbCooldownInterval = 72 * time.Second

// markImgBBRateLimited records a rate-limit hit so throttleImgBB uses the
// longer cooldown interval for subsequent calls.
func markImgBBRateLimited() {
	imgbbMu.Lock()
	defer imgbbMu.Unlock()
	// Keep the cooldown window alive: extend it on every hit so a sustained
	// burst doesn't resume too early.
	imgbbBackoffUntil = time.Now().Add(imgbbCooldownInterval)
}

// throttleImgBB sleeps until the appropriate interval has elapsed since the
// previous ImgBB API call, then records the call time.  Safe for concurrent
// callers.  After a rate-limit error the longer cooldown interval is used.
func throttleImgBB() {
	imgbbMu.Lock()
	defer imgbbMu.Unlock()
	interval := imgbbMinInterval
	if time.Now().Before(imgbbBackoffUntil) {
		interval = imgbbCooldownInterval
	}
	if d := interval - time.Since(imgbbLastUpload); d > 0 {
		time.Sleep(d)
	}
	imgbbLastUpload = time.Now()
}

type imgbbResponse struct {
	Data struct {
		URL string `json:"url"`
	} `json:"data"`
	Status int             `json:"status"`
	Error  json.RawMessage `json:"error,omitempty"`
}

// imgbbKeyRing manages multiple API keys and rotates through them on
// rate-limit errors.  Keys are read from the IMGBB_API_KEY env var,
// which may be a comma-separated list (e.g. "key1,key2,key3").
//
// The ring is a package-level singleton so that the rotation index persists
// across NewImgBBUploader instances — every uploader shares the same ring
// and continues where the previous one left off.
type imgbbKeyRing struct {
	mu    sync.Mutex
	keys  []string
	index int
}

var globalImgbbKeyRing = sync.OnceValue(func() *imgbbKeyRing {
	raw := os.Getenv("IMGBB_API_KEY")
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			keys = append(keys, k)
		}
	}
	return &imgbbKeyRing{keys: keys}
})

func (kr *imgbbKeyRing) current() string {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	if len(kr.keys) == 0 {
		return ""
	}
	return kr.keys[kr.index]
}

func (kr *imgbbKeyRing) rotate() {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	if len(kr.keys) > 1 {
		kr.index = (kr.index + 1) % len(kr.keys)
	}
}

func (kr *imgbbKeyRing) count() int {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	return len(kr.keys)
}

// ImgBBUploader handles uploading images to ImgBB with automatic
// key rotation on rate-limit errors.
type ImgBBUploader struct {
	keys   *imgbbKeyRing
	client *http.Client
}

func NewImgBBUploader() *ImgBBUploader {
	return &ImgBBUploader{
		keys:   globalImgbbKeyRing(),
		client: newNoProxyClient(60 * time.Second),
	}
}

// isRateLimitError returns true if the response indicates a rate-limit hit
// (HTTP 429 or "rate limit" in the error message).
func isRateLimitError(statusCode int, body []byte) bool {
	if statusCode == 429 {
		return true
	}
	return strings.Contains(strings.ToLower(string(body)), "rate limit")
}

// Upload uploads an image file to ImgBB.  On rate-limit errors the key ring
// is rotated and the upload retried with the next key.  Each key is tried at
// most once per call to avoid hammering a rate-limited key with backoff.
func (u *ImgBBUploader) Upload(filePath string) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("imgbb: IMGBB_API_KEY not set")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("imgbb: read file: %w", err)
	}

	encoded := base64.StdEncoding.EncodeToString(data)

	// Try each key at most once (rotate on rate-limit).
	attempts := u.keys.count()
	var lastErr error
	for i := 0; i < attempts; i++ {
		// Space API calls process-wide so a burst can't exhaust every key
		// in a single tick (see throttleImgBB).
		throttleImgBB()
		key := u.keys.current()
		form := url.Values{
			"key":   {key},
			"image": {encoded},
		}

		resp, err := u.client.PostForm(imgbbAPIURL, form)
		if err != nil {
			lastErr = fmt.Errorf("imgbb: post: %w", err)
			u.keys.rotate()
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()

		if readErr != nil {
			lastErr = fmt.Errorf("imgbb: read response: %w", readErr)
			u.keys.rotate()
			continue
		}

		if resp.StatusCode == 429 {
			lastErr = fmt.Errorf("imgbb: rate limited (HTTP 429)")
			markImgBBRateLimited()
			u.keys.rotate()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("imgbb: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			// Rotate on rate-limit messages (429 or body text) or 403
			// (key-specific "Forbidden" / "Invalid API key").  Do NOT
			// rotate on generic 400 — that's usually a bad request
			// (invalid image format, file too large) which fails for
			// every key and rotating would waste attempts.
			if isRateLimitError(resp.StatusCode, body) || resp.StatusCode == 403 {
				markImgBBRateLimited()
				u.keys.rotate()
				continue
			}
			return "", lastErr
		}

		var result imgbbResponse
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = fmt.Errorf("imgbb: parse response: %w", err)
			u.keys.rotate()
			continue
		}

		if result.Status != 200 {
			msg := string(result.Error)
			var errObj struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(result.Error, &errObj) == nil && errObj.Message != "" {
				msg = errObj.Message
			}
			if msg == "" || msg == "null" {
				msg = string(body)
			}
			err = fmt.Errorf("imgbb: error: %s", msg)
			lastErr = err
			if isRateLimitError(result.Status, []byte(msg)) {
				markImgBBRateLimited()
				u.keys.rotate()
				continue
			}
			// Rotate on key-specific errors so the next key gets a chance.
			lower := strings.ToLower(msg)
			if strings.Contains(lower, "invalid") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "suspended") {
				u.keys.rotate()
				continue
			}
			return "", err
		}

		if result.Data.URL == "" {
			return "", fmt.Errorf("imgbb: empty image URL in response")
		}

		return result.Data.URL, nil
	}

	return "", lastErr
}
