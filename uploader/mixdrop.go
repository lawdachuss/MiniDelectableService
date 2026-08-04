package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// MixdropUploader handles uploading files to Mixdrop
type MixdropUploader struct {
	email  string
	token  string
	client *http.Client
}

// NewMixdropUploader creates a new Mixdrop uploader instance
func NewMixdropUploader(email, token string) *MixdropUploader {
	return &MixdropUploader{
		email: email,
		token: token,
		client: &http.Client{
			Timeout: 120 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:          10,
				MaxIdleConnsPerHost:   2,
				IdleConnTimeout:       90 * time.Second,
				DisableCompression:    true,
				TLSHandshakeTimeout:   30 * time.Second,
				ResponseHeaderTimeout: 120 * time.Second,
				DisableKeepAlives:     true,
				DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

type mixdropUploadResp struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
	Result  struct {
		Fileref string `json:"fileref"`
		Title   string `json:"title"`
		Name    string `json:"name"`
	} `json:"result"`
}

// Upload uploads a file to Mixdrop and returns the embed link
func (u *MixdropUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to Mixdrop and reports progress through fn.
func (u *MixdropUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release := acquireHostSem("Mixdrop")
	defer release()

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		link, err := u.uploadFile(filePath, progress)
		if err != nil {
			lastErr = fmt.Errorf("upload file: %w", err)
			if isUploadRateLimited(err) {
				time.Sleep(uploadBackoff(attempt, err))
				lastErr = nil
				continue
			}
			if attempt < 3 {
				continue
			}
			return "", lastErr
		}
		return link, nil
	}
	return "", lastErr
}

func (u *MixdropUploader) uploadFile(filePath string, progress ProgressFunc) (string, error) {
	// Credentials go in form fields only — no Authorization header.
	// The API field is "key" (matches the env-var MIXDROP_KEY), not "token".
	// Build multipart body with exact Content-Length; Mixdrop's nginx proxy
	// rejects chunked transfer encoding with a 400.
	fields := map[string]string{
		"email": u.email,
		"key":   u.token,
	}
	body, contentLen, contentType, closer, err := multipartStreamWithProgress(fields, "file", filePath, "Mixdrop", progress)
	if err != nil {
		return "", fmt.Errorf("build multipart: %w", err)
	}
	defer closer.Close()

	req, err := http.NewRequest("POST", "https://ul.mixdrop.ag/api", body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.ContentLength = contentLen
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	rawBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests {
		return "", fmt.Errorf("upload status 429: rate limit — %s", strings.TrimSpace(string(rawBody)))
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upload status %d: %s", resp.StatusCode, string(rawBody))
	}

	var uploadResp mixdropUploadResp
	if err := json.Unmarshal(rawBody, &uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w (body: %s)", err, string(rawBody))
	}
	if !uploadResp.Success {
		errMsg := uploadResp.Error
		if errMsg == "" {
			errMsg = string(rawBody)
		}
		return "", fmt.Errorf("upload failed: %s", errMsg)
	}
	if uploadResp.Result.Fileref == "" {
		return "", fmt.Errorf("empty fileref in upload response (body: %s)", string(rawBody))
	}

	return fmt.Sprintf("https://mixdrop.ag/e/%s", uploadResp.Result.Fileref), nil
}
