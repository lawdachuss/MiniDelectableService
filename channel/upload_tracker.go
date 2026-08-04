package channel

import (
	"path/filepath"
	"sync"
)

var (
	pendingUploadsMu sync.Mutex
	pendingUploads   = make(map[string]struct{})
)

// MarkUploadInFlight records that a file is currently being uploaded by the
// channel system.  The watcher calls IsUploadInFlight before processing a
// file and skips it when this returns true, preventing duplicate uploads
// and the "file not found" race when DeleteLocalAfterUpload fires.
func MarkUploadInFlight(filePath string) {
	filePath = normalizeUploadPath(filePath)
	pendingUploadsMu.Lock()
	pendingUploads[filePath] = struct{}{}
	pendingUploadsMu.Unlock()
}

// MarkUploadDone removes a file from the in-flight set.  Called via defer in
// upload goroutines so the set is always cleaned up.
func MarkUploadDone(filePath string) {
	filePath = normalizeUploadPath(filePath)
	pendingUploadsMu.Lock()
	delete(pendingUploads, filePath)
	pendingUploadsMu.Unlock()
}

// IsUploadInFlight returns true if the file is currently being uploaded.
func IsUploadInFlight(filePath string) bool {
	filePath = normalizeUploadPath(filePath)
	pendingUploadsMu.Lock()
	_, ok := pendingUploads[filePath]
	pendingUploadsMu.Unlock()
	return ok
}

// InFlightCount returns the number of files currently marked in-flight.
// Exposed for diagnostics: a count far larger than the number of active
// pipelines suggests stale markers were left behind by a crashed flow — the
// "already uploading, skipping duplicate" failure mode.
func InFlightCount() int {
	pendingUploadsMu.Lock()
	defer pendingUploadsMu.Unlock()
	return len(pendingUploads)
}

func normalizeUploadPath(filePath string) string {
	if abs, err := filepath.Abs(filePath); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(filePath)
}
