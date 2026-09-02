package uploader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/sardanioss/httpcloak"
)

const (
	// filemoon.org/api/v1 is the canonical FileMoon upload API. byse.sx was
	// tried as a mirror but no longer serves the API (returns 404 on
	// /api/v1/files/upload), so uploads must go to filemoon.org directly.
	filemoonAPIBase = "https://filemoon.org/api/v1"

	// filemoonHLSMaxWait is the maximum time to wait for HLS conversion
	// after upload. If conversion doesn't complete in this window, the
	// embed URL is returned anyway (it works immediately for basic playback).
	filemoonHLSMaxWait = 5 * time.Minute

	// filemoonHLSPollInterval is how often to check conversion status.
	filemoonHLSPollInterval = 10 * time.Second
)

// FileMoonUploader handles uploading videos to filemoon.org
type FileMoonUploader struct {
	apiToken string
	client   *http.Client
}

// NewFileMoonUploader creates a new FileMoon uploader instance.
// Token is read from FILEMOON_API_TOKEN environment variable.
func NewFileMoonUploader() *FileMoonUploader {
	token := os.Getenv("FILEMOON_API_TOKEN")
	// Use a dedicated httpcloak client with Chrome 146 TLS fingerprint to
	// bypass Cloudflare bot detection on filemoon.org. The plain Go TLS
	// fingerprint triggers Cloudflare challenges (502/302 redirects).
	//
	// Unlike the shared httpcloak transport, this streams the request body
	// directly through httpcloak (no io.ReadAll into memory) and uses a
	// 2-hour timeout for large video uploads.
	cloakClient := httpcloak.New("chrome-146-windows", httpcloak.WithTimeout(120*time.Minute))
	return &FileMoonUploader{
		apiToken: token,
		client:   &http.Client{Transport: &filemoonTransport{client: cloakClient}},
	}
}

// filemoonTransport wraps httpcloak.Client as an http.RoundTripper that
// streams the request body directly through httpcloak (no full-body buffering)
// and supports long timeouts for large video uploads.
type filemoonTransport struct {
	client *httpcloak.Client
}

func (t *filemoonTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	// Use a long timeout for upload requests (default 30s is too short).
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 120*time.Minute)
		defer cancel()
	}

	// Build the httpcloak request, streaming the body directly.
	cloakReq := &httpcloak.Request{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header,
	}
	if req.Body != nil {
		// Stream body directly — do NOT io.ReadAll into memory.
		// For large video uploads (2-5 GB) this avoids OOM.
		cloakReq.Body = req.Body
	}

	cloakResp, err := t.client.Do(ctx, cloakReq)
	if err != nil {
		return nil, err
	}

	// Response body is small (JSON), safe to buffer.
	body, bodyErr := cloakResp.Bytes()
	if bodyErr != nil {
		cloakResp.Close()
		return nil, bodyErr
	}
	cloakResp.Close()

	resp := &http.Response{
		StatusCode: cloakResp.StatusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}
	if cloakResp.Headers != nil {
		for k, vs := range cloakResp.Headers {
			for _, v := range vs {
				resp.Header.Add(k, v)
			}
		}
	}
	return resp, nil
}

// HasToken returns true if a FileMoon API token is configured.
func (u *FileMoonUploader) HasToken() bool {
	return u.apiToken != ""
}

// fileMoonUploadResponse is the JSON response from the FileMoon upload API
type fileMoonUploadResponse struct {
	Success bool `json:"success"`
	Data    struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Filename string `json:"filename"`
		Size     int64  `json:"size_bytes"`
		URLs     struct {
			Page  string `json:"page"`
			Watch string `json:"watch"`
			Embed string `json:"embed"`
		} `json:"urls"`
	} `json:"data"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// fileMoonStatusResponse is the JSON response from GET /files/{id}/status
type fileMoonStatusResponse struct {
	Success bool `json:"success"`
	Data    struct {
		File struct {
			ID               string `json:"id"`
			Name             string `json:"name"`
			AllowOnlineWatch bool   `json:"allow_online_watch"`
		} `json:"file"`
		Conversion struct {
			Status          string `json:"status"`
			Stage           string `json:"stage"`
			Attempts        int    `json:"attempts"`
			DurationSeconds int    `json:"duration_seconds"`
			Error           string `json:"error"`
			UpdatedAt       string `json:"updated_at"`
		} `json:"conversion"`
	} `json:"data"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Upload uploads a video file to FileMoon and returns the embed link
func (u *FileMoonUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a video file to FileMoon and reports progress through fn
func (u *FileMoonUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.apiToken == "" {
		return "", fmt.Errorf("filemoon: no token configured (set FILEMOON_API_TOKEN)")
	}

	release := acquireHostSem("FileMoon")
	defer release()

	var lastErr error

	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		url, fileID, err := u.uploadOnce(filePath, progress)
		if err == nil {
			// Poll HLS conversion status in background (non-blocking)
			if fileID != "" {
				go u.pollHLSConversion(fileID)
			}
			return url, nil
		}
		lastErr = err

		if isFailFastError(err) {
			return "", err
		}
	}

	return "", fmt.Errorf("filemoon: all %d attempts failed, last: %w", maxAttempts, lastErr)
}

// uploadOnce performs a single upload attempt. Returns embed URL and file ID.
func (u *FileMoonUploader) uploadOnce(filePath string, progress ProgressFunc) (string, string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", "", fmt.Errorf("filemoon: stat file: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", "", fmt.Errorf("filemoon: open file: %w", err)
	}
	defer file.Close()

	// Build multipart body
	body := &multipartBody{
		fields: map[string]string{
			"visibility": "1", // public
		},
		fileField: &multipartFileField{
			name:     "file",
			fileName: filepath.Base(filePath),
			reader:   file,
			size:     fi.Size(),
		},
	}

	bodyReader, contentLength, contentType, err := buildMultipartBody(body)
	if err != nil {
		return "", "", fmt.Errorf("filemoon: build multipart: %w", err)
	}
	defer bodyReader.Close()

	// Wrap reader with progress tracking if provided
	var uploadReader io.Reader = bodyReader
	if progress != nil {
		progressFile := NewProgressReaderWithCallback(bodyReader, contentLength, "FileMoon", progress)
		uploadReader = progressFile
	}

	req, err := http.NewRequest("POST", filemoonAPIBase+"/files/upload", uploadReader)
	if err != nil {
		return "", "", fmt.Errorf("filemoon: create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+u.apiToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.ContentLength = contentLength

	resp, err := u.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("filemoon: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max response
	if err != nil {
		return "", "", fmt.Errorf("filemoon: read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", "", fmt.Errorf("filemoon: rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("filemoon: HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var result fileMoonUploadResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", "", fmt.Errorf("filemoon: parse response: %w", err)
	}

	if !result.Success {
		if result.Error != nil {
			return "", "", fmt.Errorf("filemoon: API error %s: %s", result.Error.Code, result.Error.Message)
		}
		return "", "", fmt.Errorf("filemoon: upload failed")
	}

	// Return the embed URL
	embedURL := result.Data.URLs.Embed
	if embedURL == "" {
		embedURL = result.Data.URLs.Watch
	}

	if embedURL == "" {
		return "", "", fmt.Errorf("filemoon: no URL in response")
	}

	// Ensure HTTPS
	embedURL = stringReplace(embedURL, "http://", "https://")

	return embedURL, result.Data.ID, nil
}

// pollHLSConversion checks HLS conversion status until completed/failed or timeout.
// Runs in a goroutine after upload — the embed URL works immediately, but HLS
// adaptive streaming quality isn't available until conversion finishes.
func (u *FileMoonUploader) pollHLSConversion(fileID string) {
	deadline := time.Now().Add(filemoonHLSMaxWait)

	for time.Now().Before(deadline) {
		status, stage, err := u.getConversionStatus(fileID)
		if err != nil {
			log.Printf("FileMoon HLS: status check failed for %s: %v", fileID, err)
			return
		}

		switch status {
		case "completed":
			log.Printf("FileMoon HLS: conversion completed for %s (stage=%s)", fileID, stage)
			return
		case "failed":
			log.Printf("FileMoon HLS: conversion failed for %s", fileID)
			return
		default:
			// pending or running — keep polling
		}

		time.Sleep(filemoonHLSPollInterval)
	}

	log.Printf("FileMoon HLS: conversion polling timed out for %s after %v", fileID, filemoonHLSMaxWait)
}

// getConversionStatus queries GET /files/{id}/status and returns (status, stage, error).
func (u *FileMoonUploader) getConversionStatus(fileID string) (string, string, error) {
	req, err := http.NewRequest("GET", filemoonAPIBase+"/files/"+fileID+"/status", nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", "Bearer "+u.apiToken)
	req.Header.Set("Accept", "application/json")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return "", "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var result fileMoonStatusResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", "", fmt.Errorf("parse: %w", err)
	}

	if !result.Success {
		if result.Error != nil {
			return "", "", fmt.Errorf("%s: %s", result.Error.Code, result.Error.Message)
		}
		return "", "", fmt.Errorf("unknown error")
	}

	return result.Data.Conversion.Status, result.Data.Conversion.Stage, nil
}
