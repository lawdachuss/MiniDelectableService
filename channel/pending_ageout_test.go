package channel

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDeleteStalePendingSegments verifies that only pending segments older
// than pendingMaxAge are deleted: fresh segments (still being extended by a
// live stream) survive, stale sub-threshold segments (the stream is not
// coming back) are removed, and sidecars are never touched.
func TestDeleteStalePendingSegments(t *testing.T) {
	dir := t.TempDir()

	fresh := filepath.Join(dir, "alice_2026-08-13_10-00-00.mp4")
	mustWriteFile(t, fresh)
	mustTouch(t, fresh, time.Now())

	stale := filepath.Join(dir, "alice_2026-08-11_09-00-00.mp4")
	mustWriteFile(t, stale)
	mustTouch(t, stale, time.Now().Add(-48*time.Hour))

	sidecar := filepath.Join(dir, "alice_2026-08-11_09-00-00.thumb.jpg")
	mustWriteFile(t, sidecar)
	mustTouch(t, sidecar, time.Now().Add(-48*time.Hour))

	deleteStalePendingSegments(dir)

	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh segment was deleted: %v", err)
	}
	if _, err := os.Stat(stale); err == nil {
		t.Error("stale segment was not deleted")
	}
	if _, err := os.Stat(sidecar); err != nil {
		t.Errorf("sidecar was deleted alongside the segment: %v", err)
	}
}
