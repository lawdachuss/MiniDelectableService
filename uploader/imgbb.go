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

// imgbbGlobal throttling: ImgBB's free tier rate-limits bursts (~1 upload/s
// per key, ~50/h per key).  During a host outage (e.g. Catbox 520) or a mass
// thumbnail backfill, every preview falls back to ImgBB; an unthrottled burst
// exhausts the key ring in seconds (the fleet-wide "imgbb: HTTP 400: rate
// limit reached" failures).  Spacing calls process-wide turns the burst into
// a steady trickle that stays within quota.
var (
	imgbbMu         sync.Mutex
	imgbbLastUpload time.Time
)

// imgbbMinInterval is the minimum spacing between ImgBB API calls.  With a
// 6-key ring and ~50 uploads/h per key the aggregate quota is ~300/h, so 1s
// spacing (3600/h ceiling) still exceeds quota during an outage — the uploads
// then fail fast and are retried later by the throttled ScanThumbnails pass.
// The point is to stop the second-long burst that burns every key at once.
const imgbbMinInterval = time.Second

// throttleImgBB sleeps until imgbbMinInterval has elapsed since the previous
// ImgBB API call, then records the call time.  Safe for concurrent callers.
func throttleImgBB() {
	imgbbMu.Lock()
	defer imgbbMu.Unlock()
	if d := imgbbMinInterval - time.Since(imgbbLastUpload); d > 0 {
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
type imgbbKeyRing struct {
	mu    sync.Mutex
	keys  []string
	index int
}

func newImgbbKeyRing() *imgbbKeyRing {
	raw := os.Getenv("IMGBB_API_KEY")
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			keys = append(keys, k)
		}
	}
	return &imgbbKeyRing{keys: keys}
}

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
		keys:   newImgbbKeyRing(),
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
			u.keys.rotate()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("imgbb: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			// Rotate on rate-limit messages
			if isRateLimitError(resp.StatusCode, body) {
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
