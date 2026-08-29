package uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// ThumbnailUploader handles uploading thumbnail images to Pixhost.to
type ThumbnailUploader struct {
	apiKey string // Not used for Pixhost, kept for compatibility
	client *http.Client
}

// pixhostResponse is the JSON response from the Pixhost.to API
type pixhostResponse struct {
	Name    string `json:"name"`
	ShowURL string `json:"show_url"`
	ThURL   string `json:"th_url"`
	ImgURL  string `json:"img_url"` // Direct image URL (may be empty for NSFW)
}

// NewThumbnailUploader creates a new Pixhost.to thumbnail uploader.
// apiKey parameter is ignored (Pixhost doesn't require API keys)
func NewThumbnailUploader(apiKey string) *ThumbnailUploader {
	return &ThumbnailUploader{
		apiKey: apiKey,
		client: newNoProxyClient(2 * time.Minute),
	}
}

// Upload uploads a thumbnail image to Pixhost.to and returns the direct image URL.
// Pixhost supports JPEG, PNG, and GIF.
//
// The file is opened once and streamed through both upload strategies without
// loading the entire image into RAM.  Only the multipart preamble (headers +
// form fields, < 512 B) is buffered in memory.
func (t *ThumbnailUploader) Upload(thumbnailPath string) (string, error) {
	release := acquireHostSem("Pixhost")
	defer release()

	log.Printf("Uploading thumbnail to Pixhost.to: %s", thumbnailPath)

	fi, err := os.Stat(thumbnailPath)
	if err != nil {
		return "", fmt.Errorf("stat file: %w", err)
	}
	fileSize := fi.Size()

	file, err := os.Open(thumbnailPath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Build the multipart preamble once — shared across strategy retries.
	// Contains form fields + file part header but NOT the file bytes.
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)
	if err := mw.WriteField("content_type", "1"); err != nil {
		return "", fmt.Errorf("write content_type: %w", err)
	}
	if err := mw.WriteField("max_th_size", "420"); err != nil {
		return "", fmt.Errorf("write max_th_size: %w", err)
	}
	if _, err := mw.CreateFormFile("img", filepath.Base(thumbnailPath)); err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()

	// MIME type for the raw body fallback strategy.
	ext := strings.ToLower(filepath.Ext(thumbnailPath))
	mimeType := "application/octet-stream"
	switch ext {
	case ".webp":
		mimeType = "image/webp"
	case ".jpg", ".jpeg":
		mimeType = "image/jpeg"
	case ".png":
		mimeType = "image/png"
	}

	// Body reader helpers — seek the file back before each use.
	multipartBody := func() io.Reader {
		file.Seek(0, io.SeekStart)
		return io.MultiReader(
			bytes.NewReader(preamble.Bytes()),
			file,
			bytes.NewReader([]byte(closing)),
		)
	}
	rawBody := func() io.Reader {
		file.Seek(0, io.SeekStart)
		return file
	}

	var lastErr error
	strategies := []struct {
		name string
		fn   func() (*http.Response, error)
	}{
		{"multipart", func() (*http.Response, error) {
			totalLen := int64(preamble.Len()) + fileSize + int64(len(closing))
			req, err := http.NewRequest("POST", "https://api.pixhost.to/images", multipartBody())
			if err != nil {
				return nil, fmt.Errorf("create request: %w", err)
			}
			req.ContentLength = totalLen
			req.Header.Set("Content-Type", contentType)
			req.Header.Set("Accept", "application/json")
			req.Header.Set("User-Agent", defaultUserAgent)
			return t.client.Do(req)
		}},
		{"raw body", func() (*http.Response, error) {
			req, err := http.NewRequest("POST", "https://api.pixhost.to/images", rawBody())
			if err != nil {
				return nil, fmt.Errorf("create raw request: %w", err)
			}
			req.ContentLength = fileSize
			req.Header.Set("Content-Type", mimeType)
			req.Header.Set("Accept", "application/json")
			req.Header.Set("User-Agent", defaultUserAgent)
			return t.client.Do(req)
		}},
	}

	var resp *http.Response
	for _, strat := range strategies {
		resp, err = strat.fn()
		if err != nil {
			lastErr = fmt.Errorf("%s: send request: %w", strat.name, err)
			log.Printf("Pixhost: %s failed (request error) — trying next strategy: %v", strat.name, err)
			continue
		}
		// Only fall through to next strategy on 414 — other status codes are handled downstream
		if resp.StatusCode == 414 {
			bodyDump, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("%s: Pixhost returned status 414: %s", strat.name, strings.TrimSpace(string(bodyDump)))
			log.Printf("Pixhost: %s got 414 — trying next strategy", strat.name)
			continue
		}
		break
	}

	if resp == nil {
		return "", fmt.Errorf("all upload strategies failed, last: %w", lastErr)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Pixhost returned status %d: %s", resp.StatusCode, string(body))
	}

	var result pixhostResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	// For NSFW uploads img_url is always empty. Derive the full-size CDN URL
	// from th_url by replacing the thumbnail path with the images path:
	//   https://t2.pixhost.to/thumbs/ID/file.jpg
	//   → https://img2.pixhost.to/images/ID/file.jpg
	// This gives the original full-resolution image without any age-gate.
	// th_url is only used as a last resort because it is capped at max_th_size.
	imageURL := strings.TrimSpace(result.ImgURL)
	if imageURL == "" {
		if th := strings.TrimSpace(result.ThURL); th != "" {
			imageURL = pixhostThumbToFull(th)
		}
	}
	if imageURL == "" && strings.Contains(result.ShowURL, "/show/") {
		imageURL = strings.Replace(result.ShowURL, "/show/", "/images/", 1)
	}
	if imageURL == "" {
		return "", fmt.Errorf("Pixhost returned no image URL (response: %s)", string(body))
	}
	log.Printf("Pixhost response: img_url=%q show_url=%q th_url=%q → using %q",
		result.ImgURL, result.ShowURL, result.ThURL, imageURL)

	log.Printf("Thumbnail uploaded to Pixhost: %s", imageURL)
	return imageURL, nil
}

// pixhostThToFull re-derives the full-resolution CDN URL from a Pixhost
// thumbnail URL.
//
//	https://t2.pixhost.to/thumbs/8020/file.jpg
//	→ https://img2.pixhost.to/images/8020/file.jpg
//
// If the URL doesn't match the expected pattern, it is returned unchanged so
// we always have something to store rather than an empty string.
var pixhostThRe = regexp.MustCompile(`^https?://t(\d+)\.pixhost\.to/thumbs/`)

func pixhostThumbToFull(thURL string) string {
	loc := pixhostThRe.FindStringIndex(thURL)
	if loc == nil {
		return thURL
	}
	// Extract the server number from the match (group 1)
	sub := pixhostThRe.FindStringSubmatch(thURL)
	if len(sub) < 2 {
		return thURL
	}
	n := sub[1]
	full := "https://img" + n + ".pixhost.to/images/" + thURL[loc[1]:]
	return full
}
