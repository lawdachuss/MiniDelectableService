package uploader

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AnonMP4 upload flow (as of Sep 2026).
//
// The long-documented API base (https://anonmp4api.xyz, a direct IONOS IP at
// 74.208.192.119) has been unreachable since ~Aug 2026 — TCP 443 silently
// drops from every fleet node — while the public site anonmp4.to keeps
// working behind Cloudflare. The site's own uploader no longer uses the
// documented API either; it assigns a signed upload node and streams the file
// in 2 MiB chunks to a Cloudflare Worker:
//
//  1. POST anonmp4.to/get-upload-node   {client_token, filename}
//     -> {status, node_id, upload_url, node_token}
//  2. POST upload_url?file_id=<node_token>&chunkIndex=&totalChunks=&filename=
//     with raw binary chunk bodies + Content-Range headers, up to 5 in
//     parallel (2 MiB chunks)
//  3. POST anonmp4.to/get-video-node    {node_id, node_token, client_token, filename}
//     -> final watch/embed links
//
// client_token is a static value embedded in the public site (no account or
// API key required — uploads are anonymous, matching the old API contract).
// anonmp4SiteBase is a var (not const) so tests can point it at an httptest
// server.
var anonmp4SiteBase = "https://anonmp4.to"

const (
	// anonmp4ClientToken mirrors the token the anonmp4.to homepage embeds in
	// its upload JS. If the site ever rotates it, uploads fail with a clear
	// node-assignment error and the value needs updating here.
	anonmp4ClientToken  = "TVRBYTRMcTBFNE5TNGI1TVR0M2Y5T1RWOE1UdDMzTVRZYTROVHg3M01kNw"
	anonmp4ChunkSize    = 2 * 1024 * 1024 // the site uploads 2 MiB chunks
	anonmp4MaxParallel  = 5               // site's per-batch chunk parallelism
	anonmp4MaxAttempts  = 3               // outer attempts (full node cycle)
	anonmp4MaxChunkHits = 3               // per-chunk retries
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

// anonmp4NodeResponse is the JSON returned by /get-upload-node
type anonmp4NodeResponse struct {
	Status    bool   `json:"status"`
	NodeID    string `json:"node_id"`
	UploadURL string `json:"upload_url"`
	NodeToken string `json:"node_token"`
	Message   string `json:"message"`
}

// anonmp4RejectedError marks a functional rejection (bad client_token, non-
// success status, 4xx) that retrying seconds later cannot fix — the caller
// should fail fast and let the host fallback chain move on.
type anonmp4RejectedError struct {
	msg string
}

func (e *anonmp4RejectedError) Error() string { return e.msg }

// anonmp4UploadResponse is the JSON returned by /get-video-node (final links).
// Mirrors the shape the old API documented; newer endpoints may answer with
// "status":"success" instead of "success":true, so both are tolerated.
type anonmp4UploadResponse struct {
	Success    bool   `json:"success"`
	Status     string `json:"status"`
	VideoID    string `json:"video_id"`
	Title      string `json:"title"`
	Thumbnail  string `json:"thumbnail"`
	WatchURL   string `json:"watch_url"`
	EmbedURL   string `json:"embed_url"`
	DeleteURL  string `json:"delete_url"`
	UploadDate string `json:"upload_date"`
	Message    string `json:"message"`
	Error      *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Upload uploads a video file to AnonMP4 and returns the embed link
func (u *AnonMP4Uploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a video file to AnonMP4 and reports progress
// through fn (called with host "AnonMP4" and cumulative bytes).
func (u *AnonMP4Uploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release := acquireHostSem("AnonMP4")
	defer release()

	fileName := filepath.Base(filePath)
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("anonmp4: stat file: %w", err)
	}
	if fi.Size() == 0 {
		return "", fmt.Errorf("anonmp4: refusing to upload empty file %s", fileName)
	}

	var lastErr error
	for attempt := 1; attempt <= anonmp4MaxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		link, err := u.uploadFlow(filePath, fileName, fi.Size(), progress)
		if err == nil {
			return link, nil
		}
		lastErr = err

		// Fail fast on dial failures / HTTP 5xx (service unreachable) and on
		// functional rejections (bad client_token, non-success status) — in
		// both cases burning the remaining attempts can't help.
		if isFailFastError(err) {
			return "", err
		}
		var rejected *anonmp4RejectedError
		if errors.As(err, &rejected) {
			return "", err
		}
	}

	return "", fmt.Errorf("anonmp4: all %d attempts failed, last: %w", anonmp4MaxAttempts, lastErr)
}

// uploadFlow runs one full node cycle: assign node -> stream chunks -> fetch
// final links.
func (u *AnonMP4Uploader) uploadFlow(filePath, fileName string, size int64, progress ProgressFunc) (string, error) {
	node, err := u.assignUploadNode(fileName)
	if err != nil {
		return "", fmt.Errorf("anonmp4: node assignment: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("anonmp4: open file: %w", err)
	}
	defer file.Close()

	if err := u.uploadChunked(file, size, node.UploadURL, node.NodeToken, fileName, progress); err != nil {
		return "", fmt.Errorf("anonmp4: chunk upload: %w", err)
	}

	res, err := u.fetchVideoNode(node.NodeID, node.NodeToken, fileName)
	if err != nil {
		return "", fmt.Errorf("anonmp4: fetch video info: %w", err)
	}

	var link string
	if res.EmbedURL != "" {
		link = res.EmbedURL
	} else {
		link = res.WatchURL
	}
	if link == "" {
		return "", fmt.Errorf("anonmp4: no watch/embed URL in finalize response (video_id=%s)", res.VideoID)
	}
	// Ensure HTTPS
	link = strings.Replace(link, "http://", "https://", 1)
	return link, nil
}

// assignUploadNode asks the site for a signed upload node for this filename.
func (u *AnonMP4Uploader) assignUploadNode(fileName string) (*anonmp4NodeResponse, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	_ = w.WriteField("client_token", anonmp4ClientToken)
	_ = w.WriteField("filename", fileName)
	if err := w.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", anonmp4SiteBase+"/get-upload-node", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limited (HTTP 429)")
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized ||
			resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return nil, &anonmp4RejectedError{msg: fmt.Sprintf("node assignment rejected (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(raw)))}
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var node anonmp4NodeResponse
	if err := json.Unmarshal(raw, &node); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if !node.Status || node.UploadURL == "" || node.NodeToken == "" {
		msg := node.Message
		if msg == "" {
			msg = "node assignment returned no upload_url"
		}
		return nil, &anonmp4RejectedError{msg: fmt.Sprintf("node assignment failed: %s", msg)}
	}
	return &node, nil
}

// uploadChunked streams the file in 2 MiB chunks, up to 5 at a time, exactly
// like the site's browser uploader. Each chunk is retried up to
// anonmp4MaxChunkHits times with 1s/2s backoff before the whole cycle fails.
func (u *AnonMP4Uploader) uploadChunked(file *os.File, size int64, uploadURL, nodeToken, fileName string, progress ProgressFunc) error {
	totalChunks := int((size + anonmp4ChunkSize - 1) / anonmp4ChunkSize)

	var (
		mu          sync.Mutex
		uploaded    int64
		firstErr    error
		progressNow bool
	)
	if progress != nil {
		progressNow = true
	}
	report := func() {
		if progressNow {
			progress("AnonMP4", uploaded, size)
		}
	}

	for batchStart := 1; batchStart <= totalChunks; batchStart += anonmp4MaxParallel {
		batchEnd := batchStart + anonmp4MaxParallel - 1
		if batchEnd > totalChunks {
			batchEnd = totalChunks
		}

		var wg sync.WaitGroup
		errs := make(chan error, batchEnd-batchStart+1)
		for idx := batchStart; idx <= batchEnd; idx++ {
			wg.Add(1)
			go func(chunkIdx int) {
				defer wg.Done()
				if err := u.uploadChunkWithRetries(file, size, uploadURL, nodeToken, fileName, chunkIdx, totalChunks); err != nil {
					errs <- err
					return
				}
				chunkBytes := int64(anonmp4ChunkSize)
				if int64(chunkIdx)*anonmp4ChunkSize > size {
					chunkBytes = size - int64(chunkIdx-1)*anonmp4ChunkSize
				}
				mu.Lock()
				uploaded += chunkBytes
				mu.Unlock()
				report()
			}(idx)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			return firstErr
		}
	}
	return nil
}

// uploadChunkWithRetries sends one chunk, retrying on transient failures.
func (u *AnonMP4Uploader) uploadChunkWithRetries(file *os.File, size int64, uploadURL, nodeToken, fileName string, chunkIdx, totalChunks int) error {
	var lastErr error
	for attempt := 1; attempt <= anonmp4MaxChunkHits; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt-1) * time.Second) // 1s, 2s
		}
		err := u.uploadChunk(file, size, uploadURL, nodeToken, fileName, chunkIdx, totalChunks)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("chunk %d/%d failed after %d attempts: %w", chunkIdx, totalChunks, anonmp4MaxChunkHits, lastErr)
}

// uploadChunk POSTs one raw-binary chunk with Content-Range metadata as query
// params (mirrors the site: body stays pure binary, no multipart boundary).
func (u *AnonMP4Uploader) uploadChunk(file *os.File, size int64, uploadURL, nodeToken, fileName string, chunkIdx, totalChunks int) error {
	start := int64(chunkIdx-1) * anonmp4ChunkSize
	end := start + anonmp4ChunkSize
	if end > size {
		end = size
	}
	chunk := make([]byte, end-start)
	if _, err := file.ReadAt(chunk, start); err != nil {
		return fmt.Errorf("read chunk: %w", err)
	}

	params := url.Values{}
	params.Set("file_id", nodeToken)
	params.Set("chunkIndex", strconv.Itoa(chunkIdx))
	params.Set("totalChunks", strconv.Itoa(totalChunks))
	params.Set("filename", fileName)

	req, err := http.NewRequest("POST", uploadURL+"?"+params.Encode(), bytes.NewReader(chunk))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, url.QueryEscape(fileName)))
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, size))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Uploading-Mode", "parallel")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.ContentLength = int64(len(chunk))

	resp, err := u.client.Do(req)
	if err != nil {
		return fmt.Errorf("send chunk: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return fmt.Errorf("read chunk response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return fmt.Errorf("rate limited (HTTP 429)")
		}
		msg := strings.TrimSpace(string(raw))
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)
	}
	// Chunk responses are plain acks; some carry {"status":"error",...}.
	var ack map[string]any
	if err := json.Unmarshal(raw, &ack); err == nil {
		if s, _ := ack["status"].(string); s == "error" {
			msg, _ := ack["message"].(string)
			return fmt.Errorf("chunk rejected: %s", msg)
		}
	}
	return nil
}

// fetchVideoNode retrieves the final watch/embed links for the assembled file.
func (u *AnonMP4Uploader) fetchVideoNode(nodeID, nodeToken, fileName string) (*anonmp4UploadResponse, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	_ = w.WriteField("node_id", nodeID)
	_ = w.WriteField("node_token", nodeToken)
	_ = w.WriteField("client_token", anonmp4ClientToken)
	_ = w.WriteField("filename", fileName)
	if err := w.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", anonmp4SiteBase+"/get-video-node", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limited (HTTP 429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var res anonmp4UploadResponse
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if !res.Success && !strings.EqualFold(res.Status, "success") {
		if res.Error != nil {
			return nil, &anonmp4RejectedError{msg: fmt.Sprintf("finalize rejected: API error %s: %s", res.Error.Code, res.Error.Message)}
		}
		msg := res.Message
		if msg == "" {
			msg = "finalize returned non-success status"
		}
		return nil, &anonmp4RejectedError{msg: fmt.Sprintf("finalize failed: %s", msg)}
	}
	return &res, nil
}
