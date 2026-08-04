package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const (
	vidaraAPIBase = "https://api.vidara.so/v1"
)

// VidaraUploader handles uploading files to vidara.so
type VidaraUploader struct {
	apiKey string
	client *http.Client
}

// NewVidaraUploader creates a new Vidara uploader instance
func NewVidaraUploader(apiKey string) *VidaraUploader {
	return &VidaraUploader{
		apiKey: apiKey,
		client: &http.Client{
			Timeout: 120 * time.Minute, // Long timeout for large video uploads
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

type vidaraServerResponse struct {
	Msg    string `json:"msg"`
	Status int    `json:"status"`
	Result struct {
		UploadServer string `json:"upload_server"`
	} `json:"result"`
}

type vidaraUploadResponse struct {
	URL      string `json:"url"`
	Title    string `json:"title"`
	VideoID  int64  `json:"video_id"`
	Filecode string `json:"filecode"`
	Msg      string `json:"msg"`
	Status   int    `json:"status"`
}

// Upload uploads a file to Vidara and returns the view link
func (u *VidaraUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to Vidara and reports progress through fn.
func (u *VidaraUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.apiKey == "" {
		return "", fmt.Errorf("Vidara API key not configured")
	}

	release := acquireHostSem("Vidara")
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
			if attempt < maxAttempts {
				continue
			}
			return "", lastErr
		}

		return downloadLink, nil
	}

	return "", lastErr
}

// getUploadServer gets the upload server URL from the Vidara API
func (u *VidaraUploader) getUploadServer() (string, error) {
	req, err := http.NewRequest("GET", vidaraAPIBase+"/upload/server?api_key="+u.apiKey, nil)
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

	var serverResp vidaraServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	if serverResp.Status != 200 || serverResp.Result.UploadServer == "" {
		return "", fmt.Errorf("server status not ok: %d (msg: %s)", serverResp.Status, serverResp.Msg)
	}

	return serverResp.Result.UploadServer, nil
}

func (u *VidaraUploader) uploadFile(filePath string, progress ProgressFunc) (string, error) {
	uploadServer, err := u.getUploadServer()
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"api_key": u.apiKey},
		"file", filePath, "Vidara", progress,
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

	var uploadResp vidaraUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	if uploadResp.URL == "" && uploadResp.Filecode == "" {
		return "", fmt.Errorf("no file URL in response")
	}

	if uploadResp.URL != "" {
		return uploadResp.URL, nil
	}
	return fmt.Sprintf("https://vidara.so/v/%s", uploadResp.Filecode), nil
}
