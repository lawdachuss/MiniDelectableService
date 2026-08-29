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
	"strings"
	"time"
)

// ImgChestUploader handles uploading images to imgchest.com.
// Requires a personal access token (created from profile > security tab).
// Supports adult/NSFW content (nsfw flag in API). Permanent hosting.
// CDN URLs: https://cdn.imgchest.com/files/{id}.{ext}
type ImgChestUploader struct {
	client *http.Client
	token  string
}

// imgChestResponse is the JSON response from the imgchest.com API.
type imgChestResponse struct {
	Data struct {
		ID        string `json:"id"`
		Title     string `json:"title"`
		Images    []struct {
			ID   string `json:"id"`
			Link string `json:"link"`
		} `json:"images"`
	} `json:"data"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// NewImgChestUploader creates a new imgchest.com uploader.
// Token is read from IMGCHEST_TOKEN environment variable.
func NewImgChestUploader() *ImgChestUploader {
	token := os.Getenv("IMGCHEST_TOKEN")
	return &ImgChestUploader{
		client: newNoProxyClient(2 * time.Minute),
		token:  token,
	}
}

// HasToken returns true if an imgchest token is configured.
func (u *ImgChestUploader) HasToken() bool {
	return u.token != ""
}

// Upload uploads an image file to imgchest.com and returns the direct CDN URL.
// Requires a valid personal access token.
//
// API: POST https://api.imgchest.com/v1/post
// Auth: Bearer <token>
// Fields: title, privacy=hidden, anonymous=true, nsfw=true, images[]=(file)
// Response: JSON with data.images[0].link containing the CDN URL.
func (u *ImgChestUploader) Upload(filePath string) (string, error) {
	if u.token == "" {
		return "", fmt.Errorf("imgchest: no token configured (set IMGCHEST_TOKEN)")
	}

	release := acquireHostSem("ImgChest")
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

		if isFailFastError(err) {
			return "", err
		}
	}
	return "", fmt.Errorf("imgchest: all 3 attempts failed, last: %w", lastErr)
}

// uploadOnce performs a single upload attempt without retry logic.
func (u *ImgChestUploader) uploadOnce(filePath string) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("imgchest: stat file: %w", err)
	}

	// Build multipart preamble with form fields.
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)

	if err := mw.WriteField("title", filepath.Base(filePath)); err != nil {
		return "", fmt.Errorf("imgchest: write title: %w", err)
	}
	if err := mw.WriteField("privacy", "hidden"); err != nil {
		return "", fmt.Errorf("imgchest: write privacy: %w", err)
	}
	// Note: anonymous field removed - API returns 500 with it on some accounts
	if err := mw.WriteField("nsfw", "true"); err != nil {
		return "", fmt.Errorf("imgchest: write nsfw: %w", err)
	}
	if _, err := mw.CreateFormFile("images[]", filepath.Base(filePath)); err != nil {
		return "", fmt.Errorf("imgchest: create form file: %w", err)
	}
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("imgchest: open file: %w", err)
	}
	defer file.Close()

	totalLen := int64(preamble.Len()) + fi.Size() + int64(len(closing))
	body := io.MultiReader(&preamble, file, bytes.NewReader([]byte(closing)))

	req, err := http.NewRequest("POST", "https://api.imgchest.com/v1/post", body)
	if err != nil {
		return "", fmt.Errorf("imgchest: create request: %w", err)
	}
	req.ContentLength = totalLen
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+u.token)
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("imgchest: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max response
	if err != nil {
		return "", fmt.Errorf("imgchest: read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("imgchest: rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("imgchest: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var result imgChestResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("imgchest: parse response: %w", err)
	}

	if !result.Success && result.Error != "" {
		return "", fmt.Errorf("imgchest: API error: %s", result.Error)
	}

	// Extract the first image URL from the response.
	if len(result.Data.Images) == 0 {
		return "", fmt.Errorf("imgchest: no images in response")
	}

	imageURL := strings.TrimSpace(result.Data.Images[0].Link)
	if imageURL == "" {
		return "", fmt.Errorf("imgchest: empty image URL in response")
	}

	// Ensure HTTPS.
	imageURL = strings.Replace(imageURL, "http://", "https://", 1)

	log.Printf("ImgChest: uploaded %s → %s", filepath.Base(filePath), imageURL)
	return imageURL, nil
}
