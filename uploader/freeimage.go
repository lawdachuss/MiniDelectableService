package uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FreeImageHostUploader handles uploading images to freeimage.host.
// No account required — uses the guest API key.
// Supports adult/NSFW content. Permanent hosting (no inactivity deletion).
// Max file size: 64 MB. Supports JPG, PNG, BMP, GIF, WEBP.
type FreeImageHostUploader struct {
	client  *http.Client
	apiKey  string
}

// freeImageHostResponse is the JSON response from the freeimage.host API.
type freeImageHostResponse struct {
	StatusCode int `json:"status_code"`
	Success    struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"success"`
	Image struct {
		URL      string `json:"url"`
		URLViewer string `json:"url_viewer"`
		Thumb    struct {
			URL string `json:"url"`
		} `json:"thumb"`
		Medium struct {
			URL string `json:"url"`
		} `json:"medium"`
		DisplayURL string `json:"display_url"`
	} `json:"image"`
	StatusTxt string `json:"status_txt"`
	Error     *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// NewFreeImageHostUploader creates a new freeimage.host uploader.
// Uses the guest API key (no account required).
func NewFreeImageHostUploader() *FreeImageHostUploader {
	return &FreeImageHostUploader{
		client: newNoProxyClient(2 * time.Minute),
		// Guest API key — public, documented, no account needed.
		apiKey: "6d207e02198a847aa98d0a2a901485a5",
	}
}

// Upload uploads an image file to freeimage.host and returns the direct image URL.
// Retries up to 3 times with exponential backoff on transient errors.
//
// API: POST https://freeimage.host/api/1/upload
// Fields: key, source (file), format=json
// Response: JSON with image.url containing the direct link.
func (u *FreeImageHostUploader) Upload(filePath string) (string, error) {
	release := acquireHostSem("freeimage.host")
	defer release()

	var lastErr error

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second // 2s, 4s
			time.Sleep(backoff)
		}

		url, err := u.uploadOnce(filePath)
		if err == nil {
			return url, nil
		}

		lastErr = err

		// Dead or rate-limiting host — bail so the fallback chain can move on.
		if isFailFastError(err) {
			return "", err
		}
	}

	return "", fmt.Errorf("freeimage.host: all 3 attempts failed, last: %w", lastErr)
}

// uploadOnce performs a single upload attempt without retry logic.
// Uses multipart form upload with streaming (no full file read into RAM).
func (u *FreeImageHostUploader) uploadOnce(filePath string) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("freeimage.host: stat file: %w", err)
	}

	// Build the multipart preamble (headers + form fields, NOT file bytes).
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)

	if err := mw.WriteField("key", u.apiKey); err != nil {
		return "", fmt.Errorf("freeimage.host: write key: %w", err)
	}
	if err := mw.WriteField("action", "upload"); err != nil {
		return "", fmt.Errorf("freeimage.host: write action: %w", err)
	}
	if err := mw.WriteField("format", "json"); err != nil {
		return "", fmt.Errorf("freeimage.host: write format: %w", err)
	}
	if _, err := mw.CreateFormFile("source", filepath.Base(filePath)); err != nil {
		return "", fmt.Errorf("freeimage.host: create form file: %w", err)
	}
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("freeimage.host: open file: %w", err)
	}
	defer file.Close()

	totalLen := int64(preamble.Len()) + fi.Size() + int64(len(closing))
	body := io.MultiReader(&preamble, file, bytes.NewReader([]byte(closing)))

	req, err := http.NewRequest("POST", "https://freeimage.host/api/1/upload", body)
	if err != nil {
		return "", fmt.Errorf("freeimage.host: create request: %w", err)
	}
	req.ContentLength = totalLen
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("freeimage.host: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max response
	if err != nil {
		return "", fmt.Errorf("freeimage.host: read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("freeimage.host: rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("freeimage.host: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var result freeImageHostResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("freeimage.host: parse response: %w", err)
	}

	if result.StatusCode != 200 {
		msg := result.StatusTxt
		if result.Error != nil && result.Error.Message != "" {
			msg = result.Error.Message
		}
		return "", fmt.Errorf("freeimage.host: API error: %s", msg)
	}

	// For animated .webp files, freeimage.host's display_url returns the
	// .th.webp thumbnail which is a static single frame — always use
	// image.url (the original uploaded file) to preserve animation.
	// For other formats (jpg, png) display_url returns a medium-sized
	// version that's fine for static thumbnails.
	var imageURL string
	if strings.EqualFold(filepath.Ext(filePath), ".webp") {
		imageURL = strings.TrimSpace(result.Image.URL)
	} else {
		imageURL = strings.TrimSpace(result.Image.DisplayURL)
		if imageURL == "" {
			imageURL = strings.TrimSpace(result.Image.URL)
		}
	}
	if imageURL == "" {
		return "", fmt.Errorf("freeimage.host: no image URL in response")
	}

	// Ensure HTTPS (API may return HTTP URLs).
	imageURL = strings.Replace(imageURL, "http://", "https://", 1)

	return imageURL, nil
}
