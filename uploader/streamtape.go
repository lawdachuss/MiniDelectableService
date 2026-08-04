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

// StreamtapeUploader handles uploading files to Streamtape
type StreamtapeUploader struct {
	login  string
	key    string
	client *http.Client
}

// NewStreamtapeUploader creates a new Streamtape uploader instance
func NewStreamtapeUploader(login, key string) *StreamtapeUploader {
	return &StreamtapeUploader{
		login: login,
		key:   key,
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

type streamtapeServerResp struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
	Result struct {
		URL string `json:"url"`
	} `json:"result"`
}

type streamtapeUploadResp struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
	Result struct {
		ID    string `json:"id"`
		URL   string `json:"url"`
		Embed string `json:"embed"`
	} `json:"result"`
}

// Upload uploads a file to Streamtape and returns the embed/view link
func (u *StreamtapeUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to Streamtape and reports progress through fn.
func (u *StreamtapeUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release := acquireHostSem("Streamtape")
	defer release()

	uploadURL, err := u.getUploadURL()
	if err != nil {
		return "", fmt.Errorf("get upload URL: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		link, err := u.uploadFile(filePath, uploadURL, progress)
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

func (u *StreamtapeUploader) getUploadURL() (string, error) {
	url := fmt.Sprintf("https://api.streamtape.com/file/ul?login=%s&key=%s", u.login, u.key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var serverResp streamtapeServerResp
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if serverResp.Status != 200 {
		return "", fmt.Errorf("API error %d: %s", serverResp.Status, serverResp.Msg)
	}
	if serverResp.Result.URL == "" {
		return "", fmt.Errorf("empty upload URL in response")
	}
	return serverResp.Result.URL, nil
}

func (u *StreamtapeUploader) uploadFile(filePath, uploadURL string, progress ProgressFunc) (string, error) {
	// Build multipart body with exact Content-Length — Streamtape rejects chunked encoding.
	body, contentLen, contentType, closer, err := multipartStreamWithProgress(nil, "file", filePath, "Streamtape", progress)
	if err != nil {
		return "", fmt.Errorf("build multipart: %w", err)
	}
	defer closer.Close()

	req, err := http.NewRequest("POST", uploadURL, body)
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

	if resp.StatusCode == http.StatusTooManyRequests {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload status 429: rate limit — %s", strings.TrimSpace(string(body)))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload status %d: %s", resp.StatusCode, string(body))
	}

	var uploadResp streamtapeUploadResp
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}
	if uploadResp.Status != 200 {
		return "", fmt.Errorf("upload API error %d: %s", uploadResp.Status, uploadResp.Msg)
	}
	if uploadResp.Result.ID == "" {
		return "", fmt.Errorf("empty file ID in upload response")
	}

	embedURL := uploadResp.Result.Embed
	if embedURL == "" {
		embedURL = fmt.Sprintf("https://streamtape.com/e/%s/", uploadResp.Result.ID)
	}
	return embedURL, nil
}
