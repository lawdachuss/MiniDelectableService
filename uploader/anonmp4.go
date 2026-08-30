package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	anonmp4APIBase = "https://anonmp4api.xyz"
)

// AnonMP4Uploader handles uploading videos to anonmp4.to
type AnonMP4Uploader struct {
	client *http.Client
}

// NewAnonMP4Uploader creates a new AnonMP4 uploader instance
func NewAnonMP4Uploader() *AnonMP4Uploader {
	return &AnonMP4Uploader{
		client: &http.Client{
			Timeout: 120 * time.Minute, // Long timeout for large video uploads
			Transport: &http.Transport{
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				DisableCompression:    true,
				TLSHandshakeTimeout:   30 * time.Second,
				ResponseHeaderTimeout: 90 * time.Second,
				DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

// anonmp4UploadResponse is the JSON response from the AnonMP4 API
type anonmp4UploadResponse struct {
	Success   bool   `json:"success"`
	VideoID   string `json:"video_id"`
	Title     string `json:"title"`
	Thumbnail string `json:"thumbnail"`
	WatchURL  string `json:"watch_url"`
	EmbedURL  string `json:"embed_url"`
	DeleteURL string `json:"delete_url"`
	UploadDate string `json:"upload_date"`
	Message   string `json:"message"`
	Error     *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Upload uploads a video file to AnonMP4 and returns the embed link
func (u *AnonMP4Uploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a video file to AnonMP4 and reports progress through fn
func (u *AnonMP4Uploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release := acquireHostSem("AnonMP4")
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

	return "", fmt.Errorf("anonmp4: all %d attempts failed, last: %w", maxAttempts, lastErr)
}

// uploadOnce performs a single upload attempt
func (u *AnonMP4Uploader) uploadOnce(filePath string, progress ProgressFunc) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("anonmp4: stat file: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("anonmp4: open file: %w", err)
	}
	defer file.Close()

	// Build multipart body
	body := &multipartBody{
		fields: map[string]string{},
		fileField: &multipartFileField{
			name:     "file",
			fileName: filepath.Base(filePath),
			reader:   file,
			size:     fi.Size(),
		},
	}

	bodyReader, contentLength, contentType, err := buildMultipartBody(body)
	if err != nil {
		return "", fmt.Errorf("anonmp4: build multipart: %w", err)
	}
	defer bodyReader.Close()

	// Wrap reader with progress tracking if provided
	var uploadReader io.Reader = bodyReader
	if progress != nil {
		progressFile := NewProgressReaderWithCallback(bodyReader, contentLength, "AnonMP4", progress)
		uploadReader = progressFile
	}

	req, err := http.NewRequest("POST", anonmp4APIBase+"/upload", uploadReader)
	if err != nil {
		return "", fmt.Errorf("anonmp4: create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.ContentLength = contentLength

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("anonmp4: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max response
	if err != nil {
		return "", fmt.Errorf("anonmp4: read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("anonmp4: rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("anonmp4: HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var result anonmp4UploadResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("anonmp4: parse response: %w", err)
	}

	if !result.Success {
		if result.Error != nil {
			return "", fmt.Errorf("anonmp4: API error %s: %s", result.Error.Code, result.Error.Message)
		}
		return "", fmt.Errorf("anonmp4: upload failed: %s", result.Message)
	}

	// Return the embed URL
	var url string
	if result.EmbedURL != "" {
		url = result.EmbedURL
	} else {
		url = result.WatchURL
	}

	if url == "" {
		return "", fmt.Errorf("anonmp4: no URL in response")
	}

	// Ensure HTTPS
	url = stringReplace(url, "http://", "https://")

	return url, nil
}

// multipartBody represents the fields and file for a multipart upload
type multipartBody struct {
	fields    map[string]string
	fileField *multipartFileField
}

// multipartFileField represents a file field in a multipart form
type multipartFileField struct {
	name     string
	fileName string
	reader   io.Reader
	size     int64
}

// buildMultipartBody builds a multipart form body and returns the reader, content length, and content type
func buildMultipartBody(body *multipartBody) (io.ReadCloser, int64, string, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()

		// Write form fields
		for key, value := range body.fields {
			if err := mw.WriteField(key, value); err != nil {
				pw.CloseWithError(fmt.Errorf("write field: %w", err))
				return
			}
		}

		// Write file field
		if body.fileField != nil {
			part, err := mw.CreateFormFile(body.fileField.name, body.fileField.fileName)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("create form file: %w", err))
				return
			}
			if _, err := io.Copy(part, body.fileField.reader); err != nil {
				pw.CloseWithError(fmt.Errorf("copy file: %w", err))
				return
			}
		}

		// Close the multipart writer
		if err := mw.Close(); err != nil {
			pw.CloseWithError(fmt.Errorf("close multipart: %w", err))
			return
		}
	}()

	return pr, -1, mw.FormDataContentType(), nil
}

// stringReplace is a helper to replace strings
func stringReplace(s, old, new string) string {
	for i := 0; i < len(s)-len(old); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}
