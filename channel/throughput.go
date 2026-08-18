package channel

import (
	"sync"
	"time"
)

// Node-wide upload throughput estimate, used by the session loop's early
// final-drain decision: if the pending upload backlog can't finish before the
// runner's hard deadline (the 6h job cap on GitHub-hosted VMs), the session
// stops recording early so the drain gets the time the multi-GB files need.
//
// The window resets whenever a sample arrives more than throughputWindow after
// the window start (uploads idle between bursts), so the estimate tracks
// current conditions instead of a stale average across quiet periods.
const throughputWindow = 3 * time.Minute

var (
	tpMu    sync.Mutex
	tpBytes int64
	tpStart time.Time
)

// RecordUploadBytes feeds bytes-sent samples (positive deltas from upload
// progress callbacks) into the node-wide estimate.
func RecordUploadBytes(n int64) {
	if n <= 0 {
		return
	}
	tpMu.Lock()
	now := time.Now()
	if tpStart.IsZero() || now.Sub(tpStart) > throughputWindow {
		tpStart = now
		tpBytes = 0
	}
	tpBytes += n
	tpMu.Unlock()
}

// EstimatedUploadThroughput returns the current aggregate upload rate in
// bytes/sec (0 when no uploads have been observed recently).
func EstimatedUploadThroughput() float64 {
	tpMu.Lock()
	defer tpMu.Unlock()
	if tpStart.IsZero() {
		return 0
	}
	elapsed := time.Since(tpStart).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(tpBytes) / elapsed
}
