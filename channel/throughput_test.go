package channel

import (
	"testing"
	"time"
)

// TestEstimatedUploadThroughput verifies the tracker converts the windowed
// byte count into a rate, using direct state injection for determinism.
func TestEstimatedUploadThroughput(t *testing.T) {
	tpMu.Lock()
	tpStart = time.Now().Add(-2 * time.Minute)
	tpBytes = 120_000_000 // 120 MB over 2 min = 1 MB/s
	tpMu.Unlock()

	got := EstimatedUploadThroughput()
	if got < 999_000 || got > 1_001_000 {
		t.Fatalf("throughput = %v, want ~1000000 bytes/s", got)
	}
}

// TestRecordUploadBytesResetsStaleWindow verifies a sample arriving long
// after the window start (uploads idle between bursts) starts a fresh window
// instead of averaging the new rate with stale bytes.
func TestRecordUploadBytesResetsStaleWindow(t *testing.T) {
	tpMu.Lock()
	tpStart = time.Now().Add(-10 * time.Minute)
	tpBytes = 1_000_000_000
	tpMu.Unlock()

	RecordUploadBytes(4096)

	tpMu.Lock()
	defer tpMu.Unlock()
	if tpBytes != 4096 {
		t.Fatalf("stale window not reset: bytes = %d, want 4096", tpBytes)
	}
	if tpStart.IsZero() || time.Since(tpStart) > time.Second {
		t.Fatalf("window start not refreshed: %v", tpStart)
	}
}

// TestRecordUploadBytesIgnoresNonPositive ensures zero/negative deltas (the
// initial 0/total report, or a regress) never pollute the estimate.
func TestRecordUploadBytesIgnoresNonPositive(t *testing.T) {
	tpMu.Lock()
	tpStart = time.Now()
	tpBytes = 1000
	tpMu.Unlock()

	RecordUploadBytes(0)
	RecordUploadBytes(-5)

	tpMu.Lock()
	defer tpMu.Unlock()
	if tpBytes != 1000 {
		t.Fatalf("non-positive samples changed the counter: %d", tpBytes)
	}
}
