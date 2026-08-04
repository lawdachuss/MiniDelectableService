package uploader

import (
	"io"
	"sync"
	"time"
)

// ProgressFunc is called with the host name, bytes read so far, and total bytes.
type ProgressFunc func(host string, current, total int64)

var (
	progressMu sync.Mutex
	progressFn ProgressFunc
)

// SetProgressCallback sets the global progress callback for upload operations.
func SetProgressCallback(fn ProgressFunc) {
	progressMu.Lock()
	progressFn = fn
	progressMu.Unlock()
}

// ClearProgressCallback clears the global progress callback.
func ClearProgressCallback() {
	progressMu.Lock()
	progressFn = nil
	progressMu.Unlock()
}

// reportProgress calls the global callback if set.
func reportProgress(host string, current, total int64) {
	progressMu.Lock()
	fn := progressFn
	progressMu.Unlock()
	if fn != nil {
		fn(host, current, total)
	}
}

// ProgressReader wraps an io.Reader and reports read progress via a callback.
type ProgressReader struct {
	reader io.Reader
	total  int64
	read   int64
	host   string
	fn     ProgressFunc
	called bool // whether we've already sent the initial (0, total) report

	lastReport time.Time
}

// progressThrottle is the minimum interval between progress callbacks.
// Progress fires on every read (the HTTP transport reads in 32KB chunks), and
// each callback is dispatched under a lock and may rebuild UI state — reporting
// every few milliseconds for dozens of concurrent uploads wastes CPU and can
// measurably reduce upload throughput.  The first and final reports are always
// sent regardless of throttle state.
const progressThrottle = 200 * time.Millisecond

// NewProgressReader creates a ProgressReader that reports progress to the
// global callback. Prefer NewProgressReaderWithCallback for per-upload progress.
func NewProgressReader(r io.Reader, total int64, host string) *ProgressReader {
	return NewProgressReaderWithCallback(r, total, host, nil)
}

// NewProgressReaderWithCallback creates a ProgressReader with an upload-local
// callback. If fn is nil, it falls back to the legacy package callback.
func NewProgressReaderWithCallback(r io.Reader, total int64, host string, fn ProgressFunc) *ProgressReader {
	return &ProgressReader{reader: r, total: total, host: host, fn: fn}
}

func (pr *ProgressReader) report(current, total int64) {
	now := time.Now()
	if pr.lastReport.IsZero() || current >= total || now.Sub(pr.lastReport) >= progressThrottle {
		pr.lastReport = now
		if pr.fn != nil {
			pr.fn(pr.host, current, total)
			return
		}
		reportProgress(pr.host, current, total)
	}
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	if !pr.called {
		pr.called = true
		pr.report(0, pr.total)
	}
	n, err := pr.reader.Read(p)
	pr.read += int64(n)
	if n > 0 {
		pr.report(pr.read, pr.total)
	}
	return n, err
}
