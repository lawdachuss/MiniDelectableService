package uploader

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// newNoProxyClient returns an http.Client that explicitly bypasses any
// environment-configured proxy (ALL_PROXY / HTTP_PROXY / HTTPS_PROXY).
// All connections are direct; image/thumbnail upload services must reach
// the public internet directly.
func newNoProxyClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: nil, // never use environment proxy
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// multipartStream builds a multipart request body that streams the file without
// loading it into RAM, while still setting an exact Content-Length so servers
// that reject chunked transfer encoding (Streamtape, Mixdrop) work.
//
// fields is written before the file part (may be nil).
// If host is non-empty the file part is wrapped with a ProgressReader.
// Returns: body reader, content-length, multipart content-type, closer (the opened file), error.
func multipartStream(fields map[string]string, fileField, filePath, host string) (io.Reader, int64, string, io.Closer, error) {
	return multipartStreamWithProgress(fields, fileField, filePath, host, nil)
}

func multipartStreamWithProgress(fields map[string]string, fileField, filePath, host string, progress ProgressFunc) (io.Reader, int64, string, io.Closer, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return nil, 0, "", nil, fmt.Errorf("stat: %w", err)
	}

	// Build the preamble (all multipart headers, but NOT the file bytes).
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)

	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return nil, 0, "", nil, fmt.Errorf("write field %s: %w", k, err)
		}
	}

	// CreateFormFile writes the part header into preamble; we do NOT write file
	// bytes through this writer — they come from the file directly.
	if _, err := mw.CreateFormFile(fileField, filepath.Base(filePath)); err != nil {
		return nil, 0, "", nil, fmt.Errorf("create form file: %w", err)
	}

	// Closing boundary that would normally be written by mw.Close().
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()
	totalLen := int64(preamble.Len()) + fi.Size() + int64(len(closing))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, "", nil, fmt.Errorf("open: %w", err)
	}

	var fileReader io.Reader = file
	if host != "" {
		fileReader = NewProgressReaderWithCallback(file, fi.Size(), host, progress)
	}

	body := io.MultiReader(&preamble, fileReader, bytes.NewReader([]byte(closing)))
	return body, totalLen, contentType, file, nil
}

// Logger is the interface for logging upload events.
// The channel package implements this with ch.Info/ch.Error.
type Logger interface {
	Info(format string, a ...any)
	Error(format string, a ...any)
}

// UploadResult contains the result of an upload to a specific host
type UploadResult struct {
	Host         string
	DownloadLink string
	Error        error
}

// MultiHostUploader handles uploading to multiple hosts simultaneously
type MultiHostUploader struct {
	gofile     *GoFileUploader
	voesx      *VoeSXUploader
	streamtape *StreamtapeUploader
	mixdrop    *MixdropUploader
	vidara     *VidaraUploader
	log           Logger
	hostInitOnce  sync.Once
	hosts         map[string]uploaderFunc // host name -> upload function, lazy-init
	progress      ProgressFunc
}

type uploaderFunc func(string, ProgressFunc) (string, error)

func (m *MultiHostUploader) initHosts() {
	m.hostInitOnce.Do(func() {
		// Don't clobber a hosts map that was pre-populated (e.g. by tests that
		// inject fakes).  Only build the default host set when none was provided.
		if m.hosts != nil {
			return
		}
		m.hosts = map[string]uploaderFunc{}
		m.hosts["GoFile"] = m.gofile.UploadWithProgress
		if m.voesx != nil && m.voesx.apiKey != "" {
			m.hosts["VOE.sx"] = m.voesx.UploadWithProgress
		}
		if m.streamtape != nil && m.streamtape.login != "" && m.streamtape.key != "" {
			m.hosts["Streamtape"] = m.streamtape.UploadWithProgress
		}
		if m.mixdrop != nil && m.mixdrop.email != "" && m.mixdrop.token != "" {
			m.hosts["Mixdrop"] = m.mixdrop.UploadWithProgress
		}
		if m.vidara != nil && m.vidara.apiKey != "" {
			m.hosts["Vidara"] = m.vidara.UploadWithProgress
		}
	})
}

// NewMultiHostUploader creates a new multi-host uploader
func NewMultiHostUploader(voeSXAPIKey, streamtapeLogin, streamtapeKey, mixdropEmail, mixdropToken, vidaraKey string, log Logger) *MultiHostUploader {
	if log == nil {
		log = &nilLogger{}
	}
	return &MultiHostUploader{
		gofile:     NewGoFileUploader(),
		voesx:      NewVoeSXUploader(voeSXAPIKey),
		streamtape: NewStreamtapeUploader(streamtapeLogin, streamtapeKey),
		mixdrop:    NewMixdropUploader(mixdropEmail, mixdropToken),
		vidara:     NewVidaraUploader(vidaraKey),
		log:        log,
	}
}

// SetProgressCallback sets an upload-local progress callback for this uploader.
func (m *MultiHostUploader) SetProgressCallback(fn ProgressFunc) {
	m.progress = fn
}

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

// isUploadRateLimited returns true if the error indicates a rate-limit hit
// (429 Too Many Requests or similar). Uses a different name than imgbb.go's
// isRateLimitError to avoid redeclaration.
func isUploadRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests")
}

// isFailFastError reports whether an upload error means the host is dead
// (DNS failure, connection refused/reset, timeout) or actively rate-limiting
// us.  In either case retrying the SAME host is futile and, for rate limits,
// actually makes things worse (it extends the rate-limit window).  Callers
// should bail on the current host and let their fallback chain (Pixhost →
// ImgBB → Catbox, or Catbox → ImgBB) try the next host instead.
func isFailFastError(err error) bool {
	if err == nil {
		return false
	}
	if isUploadRateLimited(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "dial tcp") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "eof")
}

// uploadBackoff returns the appropriate backoff duration based on whether
// the error was a rate-limit hit. Rate limits get a longer 30s+10s/attempt,
// while other errors use standard exponential delay.
func uploadBackoff(attempt int, err error) time.Duration {
	if isUploadRateLimited(err) {
		// Long backoff for rate limits — wait 30s + 10s per retry
		return 30*time.Second + time.Duration(attempt)*10*time.Second
	}
	// Standard exponential backoff: 5s, 10s, 20s, 40s...
	return time.Duration((1<<uint(attempt))*5) * time.Second
}

// nilLogger discards all log messages when no logger is provided.
type nilLogger struct{}

func (n *nilLogger) Info(format string, a ...any)  {}
func (n *nilLogger) Error(format string, a ...any) {}

// UploadToAll uploads a file to all configured hosts in parallel.
// Returns a slice of results, one for each host.
func (m *MultiHostUploader) UploadToAll(filePath string) []UploadResult {
	m.initHosts()
	hosts := make([]string, 0, len(m.hosts))
	for name := range m.hosts {
		hosts = append(hosts, name)
	}
	return m.UploadSelected(filePath, hosts)
}

// UploadSelected uploads a file to the specified hosts in parallel.
// Host names that are not configured are silently skipped.
func (m *MultiHostUploader) UploadSelected(filePath string, hosts []string) []UploadResult {
	m.initHosts()
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := []UploadResult{}

	progressFn := m.progress
	for _, name := range hosts {
		uploadFn, ok := m.hosts[name]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(host string, fn uploaderFunc) {
			defer wg.Done()
			m.log.Info("upload: starting %s upload for %s", host, filePath)
			link, err := fn(filePath, progressFn)
			mu.Lock()
			results = append(results, UploadResult{
				Host:         host,
				DownloadLink: link,
				Error:        err,
			})
			mu.Unlock()
			if err != nil {
				m.log.Error("upload: %s failed for %s: %v", host, filePath, err)
			} else {
				m.log.Info("upload: %s successful for %s: %s", host, filePath, link)
			}
		}(name, uploadFn)
	}

	wg.Wait()
	return results
}

// UploadSelectedPriority uploads to the priority host first (sequentially),
// then to remaining hosts in parallel. This ensures the priority host gets
// full bandwidth during shutdown when time is limited.
func (m *MultiHostUploader) UploadSelectedPriority(filePath string, hosts []string, priorityHost string) []UploadResult {
	m.initHosts()

	var priorityHosts []string
	var otherHosts []string
	for _, host := range hosts {
		if host == priorityHost {
			priorityHosts = append(priorityHosts, host)
		} else {
			otherHosts = append(otherHosts, host)
		}
	}

	var results []UploadResult
	progressFn := m.progress

	for _, host := range priorityHosts {
		fn, ok := m.hosts[host]
		if !ok {
			continue
		}
		m.log.Info("upload: priority upload to %s for %s", host, filePath)
		link, err := fn(filePath, progressFn)
		results = append(results, UploadResult{Host: host, DownloadLink: link, Error: err})
		if err != nil {
			m.log.Error("upload: %s (priority) failed for %s: %v", host, filePath, err)
		} else {
			m.log.Info("upload: %s (priority) successful for %s: %s", host, filePath, link)
		}
	}

	if len(otherHosts) > 0 {
		otherResults := m.UploadSelected(filePath, otherHosts)
		results = append(results, otherResults...)
	}

	return results
}

// AvailableHosts returns the names of all configured upload hosts.
func (m *MultiHostUploader) AvailableHosts() []string {
	m.initHosts()
	hosts := make([]string, 0, len(m.hosts))
	for name := range m.hosts {
		hosts = append(hosts, name)
	}
	return hosts
}

// GetSuccessfulUploads returns only the successful upload results
func GetSuccessfulUploads(results []UploadResult) []UploadResult {
	var successful []UploadResult
	for _, result := range results {
		if result.Error == nil && result.DownloadLink != "" {
			successful = append(successful, result)
		}
	}
	return successful
}

// FormatResults formats upload results into a readable string
func FormatResults(results []UploadResult) string {
	var output string
	successCount := 0

	for _, result := range results {
		if result.Error == nil && result.DownloadLink != "" {
			output += fmt.Sprintf("✓ %s: %s\n", result.Host, result.DownloadLink)
			successCount++
		} else {
			output += fmt.Sprintf("✗ %s: %v\n", result.Host, result.Error)
		}
	}

	output = fmt.Sprintf("Upload completed: %d/%d successful\n%s", successCount, len(results), output)
	return output
}
