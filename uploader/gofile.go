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
	gofileAPIBase = "https://api.gofile.io"
)

// GoFileUploader handles uploading files to GoFile.io
type GoFileUploader struct {
	client *http.Client
}

// NewGoFileUploader creates a new GoFile uploader instance
func NewGoFileUploader() *GoFileUploader {
	return &GoFileUploader{
		client: &http.Client{
			Timeout: 120 * time.Minute, // Long timeout for large video uploads
			Transport: &http.Transport{
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				DisableCompression:    true,
				TLSHandshakeTimeout:   30 * time.Second,
				ResponseHeaderTimeout: 90 * time.Second, // fail fast if server accepts but never responds
				DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

type getServerResponse struct {
	Status string `json:"status"`
	Data   struct {
		Servers []struct {
			Name string `json:"name"`
			Zone string `json:"zone"`
		} `json:"servers"`
	} `json:"data"`
}

type uploadResponse struct {
	Status string `json:"status"`
	Data   struct {
		DownloadPage string `json:"downloadPage"`
		Code         string `json:"code"`
		ParentFolder string `json:"parentFolder"`
		FileID       string `json:"fileId"`
		FileName     string `json:"fileName"`
		MD5          string `json:"md5"`
	} `json:"data"`
}

// Upload uploads a file to GoFile and returns the download link
func (u *GoFileUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to GoFile and reports progress through fn.
func (u *GoFileUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release := acquireHostSem("GoFile")
	defer release()

	var downloadLink string
	var lastErr error

	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		// Step 1: Get the best server
		server, err := u.getBestServer()
		if err != nil {
			lastErr = fmt.Errorf("get best server: %w", err)
			if attempt < maxAttempts {
				continue
			}
			return "", lastErr
		}

		// Step 2: Upload the file
		downloadLink, err = u.uploadFile(server, filePath, progress)
		if err != nil {
			lastErr = fmt.Errorf("upload file: %w", err)
			if isUploadRateLimited(err) {
				// Rate limit — backoff and clear lastErr to avoid double-sleep next iteration
				time.Sleep(uploadBackoff(attempt, err))
				lastErr = nil
				continue
			}
			if attempt < maxAttempts {
				continue
			}
			return "", lastErr
		}

		// Success!
		return downloadLink, nil
	}

	return "", lastErr
}

func (u *GoFileUploader) getBestServer() (string, error) {
	req, err := http.NewRequest("GET", gofileAPIBase+"/servers", nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request servers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var serverResp getServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	if serverResp.Status != "ok" {
		return "", fmt.Errorf("server status not ok: %s", serverResp.Status)
	}

	if len(serverResp.Data.Servers) == 0 {
		return "", fmt.Errorf("no servers available")
	}

	// Return the first server (you could add logic to pick based on zone)
	return serverResp.Data.Servers[0].Name, nil
}

func (u *GoFileUploader) uploadFile(server, filePath string, progress ProgressFunc) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Use pipe to stream the file without loading it all into memory
	pipeReader, pipeWriter := io.Pipe()
	writer := multipart.NewWriter(pipeWriter)

	// Start writing in a goroutine
	errChan := make(chan error, 1)
	go func() {
		defer pipeWriter.Close()

		part, err := writer.CreateFormFile("file", filepath.Base(filePath))
		if err != nil {
			errChan <- fmt.Errorf("create form file: %w", err)
			writer.Close()
			return
		}

		// Wrap file with ProgressReader for live upload tracking
		fi, _ := file.Stat()
		var fileSize int64
		if fi != nil {
			fileSize = fi.Size()
		}
		progressFile := NewProgressReaderWithCallback(file, fileSize, "GoFile", progress)

		// Use a larger buffer for faster copying (4MB chunks)
		buf := make([]byte, 4*1024*1024)
		if _, err := io.CopyBuffer(part, progressFile, buf); err != nil {
			errChan <- fmt.Errorf("copy file: %w", err)
			writer.Close()
			return
		}

		// Close writer before signaling success to flush multipart boundary
		if err := writer.Close(); err != nil {
			errChan <- fmt.Errorf("close writer: %w", err)
			return
		}

		errChan <- nil
	}()

	uploadURL := fmt.Sprintf("https://%s.gofile.io/contents/uploadfile", server)
	req, err := http.NewRequest("POST", uploadURL, pipeReader)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := u.client.Do(req)
	if err != nil {
		pipeReader.CloseWithError(err) // unblock the writer goroutine
		// Drain error channel to avoid goroutine leak
		select {
		case <-errChan:
		case <-time.After(5 * time.Second):
			// Timeout waiting for goroutine - it may be stuck
		}
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	// Check for errors from the goroutine
	select {
	case err := <-errChan:
		if err != nil {
			return "", err
		}
	case <-time.After(30 * time.Second):
		// Goroutine took too long - this shouldn't happen but prevents deadlock
		return "", fmt.Errorf("timeout waiting for file copy to complete")
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var uploadResp uploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	if uploadResp.Status != "ok" {
		return "", fmt.Errorf("upload status not ok: %s", uploadResp.Status)
	}

	return uploadResp.Data.DownloadPage, nil
}
