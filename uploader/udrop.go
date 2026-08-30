package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	udropAPIBase = "https://www.udrop.com/api/v2"
)

// UDropUploader handles uploading files to udrop.com
type UDropUploader struct {
	key1   string
	key2   string
	client *http.Client

	// Cached auth token (valid ~1 hour)
	tokenMu      sync.Mutex
	accessToken  string
	accountID    string
	tokenExpiry  time.Time
}

// NewUDropUploader creates a new UDrop uploader instance.
// Keys are read from UDROP_KEY1 and UDROP_KEY2 environment variables.
func NewUDropUploader() *UDropUploader {
	return &UDropUploader{
		key1: os.Getenv("UDROP_KEY1"),
		key2: os.Getenv("UDROP_KEY2"),
		client: &http.Client{
			Timeout: 120 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				TLSHandshakeTimeout: 30 * time.Second,
				DialContext:         (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

// HasKeys returns true if both UDrop API keys are configured.
func (u *UDropUploader) HasKeys() bool {
	return u.key1 != "" && u.key2 != ""
}

// udropAuthResponse is the JSON response from /authorize
type udropAuthResponse struct {
	Data struct {
		AccessToken string `json:"access_token"`
		AccountID   string `json:"account_id"`
	} `json:"data"`
	Status   string `json:"_status"`
	Error    string `json:"response,omitempty"`
}

// udropUploadResponse is the JSON response from /file/upload
type udropUploadResponse struct {
	Response string `json:"response"`
	Data     []struct {
		Name    string `json:"name"`
		Size    string `json:"size"`
		URL     string `json:"url"`
		Error   string `json:"error"`
		ShortURL string `json:"short_url"`
	} `json:"data"`
	Status string `json:"_status"`
	Error  string `json:"response,omitempty"`
}

// authenticate obtains or refreshes the access token
func (u *UDropUploader) authenticate() (string, string, error) {
	u.tokenMu.Lock()
	defer u.tokenMu.Unlock()

	// Return cached token if still valid (with 5 min buffer)
	if u.accessToken != "" && time.Now().Before(u.tokenExpiry) {
		return u.accessToken, u.accountID, nil
	}

	// Build form body
	body := &multipartBody{
		fields: map[string]string{
			"key1": u.key1,
			"key2": u.key2,
		},
	}

	bodyReader, _, contentType, err := buildMultipartBody(body)
	if err != nil {
		return "", "", fmt.Errorf("udrop: build auth body: %w", err)
	}
	defer bodyReader.Close()

	req, err := http.NewRequest("POST", udropAPIBase+"/authorize", bodyReader)
	if err != nil {
		return "", "", fmt.Errorf("udrop: create auth request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("udrop: auth request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return "", "", fmt.Errorf("udrop: read auth response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("udrop: auth HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var result udropAuthResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", "", fmt.Errorf("udrop: parse auth response: %w", err)
	}

	if result.Status != "success" || result.Data.AccessToken == "" {
		msg := result.Error
		if msg == "" {
			msg = "authentication failed"
		}
		return "", "", fmt.Errorf("udrop: auth error: %s", msg)
	}

	u.accessToken = result.Data.AccessToken
	u.accountID = result.Data.AccountID
	u.tokenExpiry = time.Now().Add(55 * time.Minute) // Token valid ~1 hour

	return u.accessToken, u.accountID, nil
}

// Upload uploads a file to UDrop and returns the direct link
func (u *UDropUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to UDrop and reports progress through fn
func (u *UDropUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if !u.HasKeys() {
		return "", fmt.Errorf("udrop: no keys configured (set UDROP_KEY1 and UDROP_KEY2)")
	}

	release := acquireHostSem("UDrop")
	defer release()

	var lastErr error

	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		url, err := u.uploadOnce(filePath, progress)
		if err == nil {
			return url, nil
		}
		lastErr = err

		if isFailFastError(err) {
			return "", err
		}
	}

	return "", fmt.Errorf("udrop: all %d attempts failed, last: %w", maxAttempts, lastErr)
}

// uploadOnce performs a single upload attempt
func (u *UDropUploader) uploadOnce(filePath string, progress ProgressFunc) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("udrop: stat file: %w", err)
	}

	// Authenticate
	accessToken, accountID, err := u.authenticate()
	if err != nil {
		return "", err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("udrop: open file: %w", err)
	}
	defer file.Close()

	// Build multipart body
	body := &multipartBody{
		fields: map[string]string{
			"access_token": accessToken,
			"account_id":   accountID,
		},
		fileField: &multipartFileField{
			name:     "upload_file",
			fileName: filepath.Base(filePath),
			reader:   file,
			size:     fi.Size(),
		},
	}

	bodyReader, contentLength, contentType, err := buildMultipartBody(body)
	if err != nil {
		return "", fmt.Errorf("udrop: build multipart: %w", err)
	}
	defer bodyReader.Close()

	// Wrap reader with progress tracking if provided
	var uploadReader io.Reader = bodyReader
	if progress != nil {
		progressFile := NewProgressReaderWithCallback(bodyReader, contentLength, "UDrop", progress)
		uploadReader = progressFile
	}

	req, err := http.NewRequest("POST", udropAPIBase+"/file/upload", uploadReader)
	if err != nil {
		return "", fmt.Errorf("udrop: create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.ContentLength = contentLength

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("udrop: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max response
	if err != nil {
		return "", fmt.Errorf("udrop: read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("udrop: rate limited (HTTP 429)")
	}

	if resp.StatusCode == 401 {
		// Token expired, clear cache and retry
		u.tokenMu.Lock()
		u.accessToken = ""
		u.tokenMu.Unlock()
		return "", fmt.Errorf("udrop: auth expired (HTTP 401)")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("udrop: HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var result udropUploadResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("udrop: parse response: %w", err)
	}

	if result.Status != "success" {
		msg := result.Error
		if msg == "" {
			msg = result.Response
		}
		return "", fmt.Errorf("udrop: upload error: %s", msg)
	}

	if len(result.Data) == 0 {
		return "", fmt.Errorf("udrop: no file data in response")
	}

	fileData := result.Data[0]
	if fileData.Error != "" {
		return "", fmt.Errorf("udrop: file error: %s", fileData.Error)
	}

	// Return the URL
	url := fileData.URL
	if url == "" {
		return "", fmt.Errorf("udrop: no URL in response")
	}

	// Ensure HTTPS
	url = strings.Replace(url, "http://", "https://", 1)

	return url, nil
}
