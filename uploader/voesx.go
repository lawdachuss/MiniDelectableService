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

const (
	voeSXAPIBase = "https://voe.sx/api"
)

// VoeSXUploader handles uploading files to VOE.sx
type VoeSXUploader struct {
	apiKey string
	client *http.Client
}

// NewVoeSXUploader creates a new VOE.sx uploader instance
func NewVoeSXUploader(apiKey string) *VoeSXUploader {
	return &VoeSXUploader{
		apiKey: apiKey,
		client: &http.Client{
			Timeout: 120 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				DialContext:         (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

type voeSXServerResponse struct {
	ServerTime string `json:"server_time"`
	Msg        string `json:"msg"`
	Message    string `json:"message"`
	Status     int    `json:"status"`
	Success    bool   `json:"success"`
	Result     string `json:"result"`
}

type voeSXUploadResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	File    struct {
		ID                int    `json:"id"`
		FileCode          string `json:"file_code"`
		FileTitle         string `json:"file_title"`
		EncodingNecessary bool   `json:"encoding_necessary"`
	} `json:"file"`
}

// Upload uploads a file to VOE.sx and returns the view link
func (u *VoeSXUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to VOE.sx and reports progress through fn.
func (u *VoeSXUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.apiKey == "" {
		return "", fmt.Errorf("VOE.sx API key not configured")
	}

	release := acquireHostSem("VOE.sx")
	defer release()

	var lastErr error

	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		downloadLink, err := u.uploadFile(filePath, progress)
		if err != nil {
			lastErr = fmt.Errorf("upload file: %w", err)
			if isUploadRateLimited(err) {
				time.Sleep(uploadBackoff(attempt, err))
				lastErr = nil
				continue
			}
			// Storage-full is unrecoverable — don't waste time retrying.
			if isVoeStorageFull(err) {
				return "", lastErr
			}
			if attempt < maxAttempts {
				continue
			}
			return "", lastErr
		}

		return downloadLink, nil
	}

	return "", lastErr
}

// getUploadServer gets the upload server URL from VOE.sx API
func (u *VoeSXUploader) getUploadServer() (string, error) {
	url := fmt.Sprintf("%s/upload/server?key=%s", voeSXAPIBase, u.apiKey)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request upload server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get upload server failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var serverResp voeSXServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	if !serverResp.Success || serverResp.Status != 200 {
		return "", fmt.Errorf("server status not ok: %s (msg: %s)", serverResp.Msg, serverResp.Message)
	}

	if serverResp.Result == "" {
		return "", fmt.Errorf("no upload server URL in response")
	}

	return serverResp.Result, nil
}

func (u *VoeSXUploader) uploadFile(filePath string, progress ProgressFunc) (string, error) {
	// Step 1: Get upload server
	uploadServer, err := u.getUploadServer()
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"key": u.apiKey},
		"file", filePath, "VOE.sx", progress,
	)
	if err != nil {
		return "", fmt.Errorf("multipart stream: %w", err)
	}
	defer file.Close()

	req, err := http.NewRequest("POST", uploadServer, body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = contentLen

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var uploadResp voeSXUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	if !uploadResp.Success {
		return "", fmt.Errorf("upload failed: %s", uploadResp.Message)
	}

	if uploadResp.File.FileCode == "" {
		return "", fmt.Errorf("no file code in response")
	}

	viewURL := fmt.Sprintf("https://voe.sx/%s", uploadResp.File.FileCode)
	return viewURL, nil
}

// isVoeStorageFull returns true when the VOE.sx error indicates the account
// storage is exhausted. This is unrecoverable until the user frees space or
// provides a new API key, so callers should fail immediately instead of retrying.
func isVoeStorageFull(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "storage space") ||
		strings.Contains(msg, "maximum storage") ||
		strings.Contains(msg, "storage full") ||
		strings.Contains(msg, "quota")
}
