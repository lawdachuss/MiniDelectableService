package channel

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const scratchName = ".merging-1723600000000000000-merged-alice_2026-08-14_04-16-00.mp4"

// TestCollectPendingSegmentsInDirExcludesMergeScratch verifies that crash-left
// ".merging-*" scratch files (and their ".normalized.mp4" temps) are never
// collected as real pending segments, so a partial mid-merge encode can never
// be concatenated into the next merged output.
func TestCollectPendingSegmentsInDirExcludesMergeScratch(t *testing.T) {
	dir := t.TempDir()

	real := filepath.Join(dir, "alice_2026-08-14_05-00-00.mp4")
	mustWriteFile(t, real)

	// Crash mid-merge leftovers.
	mustWriteFile(t, filepath.Join(dir, scratchName))
	mustWriteFile(t, filepath.Join(dir, scratchName+".normalized.mp4"))

	got := collectPendingSegmentsInDir(dir)
	if !reflect.DeepEqual(got, []string{real}) {
		t.Errorf("collectPendingSegmentsInDir = %v, want only the real segment %s", got, real)
	}
}

// TestRecoverMergeScratch verifies crash-recovery of ".merging-*" scratch:
//   - a sole scratch (inputs already consumed by a finished merge) is renamed
//     to its stable "merged-*" name so the content is uploaded exactly once;
//   - a scratch alongside real segments is mid-merge garbage and is deleted;
//   - unrecognized scratch names and real segments are left untouched.
func TestRecoverMergeScratch(t *testing.T) {
	t.Run("sole scratch is recovered to stable name", func(t *testing.T) {
		dir := t.TempDir()
		mustWriteFile(t, filepath.Join(dir, scratchName))

		recoverMergeScratch(dir)

		if _, err := os.Stat(filepath.Join(dir, scratchName)); err == nil {
			t.Error("scratch was not renamed away")
		}
		stable := filepath.Join(dir, "merged-alice_2026-08-14_04-16-00.mp4")
		if _, err := os.Stat(stable); err != nil {
			t.Errorf("finished merge was not recovered to stable name: %v", err)
		}
	})

	t.Run("scratch alongside segments is garbage", func(t *testing.T) {
		dir := t.TempDir()
		real := filepath.Join(dir, "alice_2026-08-14_05-00-00.mp4")
		mustWriteFile(t, real)
		scratch := filepath.Join(dir, scratchName)
		mustWriteFile(t, scratch)

		recoverMergeScratch(dir)

		if _, err := os.Stat(scratch); err == nil {
			t.Error("mid-merge scratch was not deleted while its inputs still exist")
		}
		if _, err := os.Stat(real); err != nil {
			t.Errorf("real segment was touched: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, "merged-alice_2026-08-14_04-16-00.mp4")); err == nil {
			t.Error("recovered a scratch that still has its inputs — would double-merge")
		}
	})

	t.Run("unrecognized scratch is left alone", func(t *testing.T) {
		dir := t.TempDir()
		mustWriteFile(t, filepath.Join(dir, ".merging-1723600000000000000-foo.mp4"))

		recoverMergeScratch(dir)

		if _, err := os.Stat(filepath.Join(dir, ".merging-1723600000000000000-foo.mp4")); err != nil {
			t.Errorf("unrecognized scratch was modified: %v", err)
		}
	})

	t.Run("multiple scratches are left for stale aging", func(t *testing.T) {
		dir := t.TempDir()
		mustWriteFile(t, filepath.Join(dir, scratchName))
		mustWriteFile(t, filepath.Join(dir, ".merging-1723600000000000001-merged-bob_2026-08-14_04-16-00.mp4"))

		recoverMergeScratch(dir)

		if _, err := os.Stat(filepath.Join(dir, scratchName)); err != nil {
			t.Errorf("first scratch was modified: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, ".merging-1723600000000000001-merged-bob_2026-08-14_04-16-00.mp4")); err != nil {
			t.Errorf("second scratch was modified: %v", err)
		}
	})
}

// TestDeleteStalePendingSegmentsScratch verifies that only stale ".merging-*"
// scratch files are aged out: a fresh scratch (live encode) survives, a stale
// one (crash leftover) is removed, and real segments are never touched.
func TestDeleteStalePendingSegmentsScratch(t *testing.T) {
	dir := t.TempDir()

	fresh := filepath.Join(dir, ".merging-1723600000000000002-merged-alice_2026-08-14_04-16-00.mp4")
	mustWriteFile(t, fresh)
	mustTouch(t, fresh, time.Now())

	stale := filepath.Join(dir, ".merging-1723600000000000003-merged-alice_2026-08-12_04-16-00.mp4")
	mustWriteFile(t, stale)
	mustTouch(t, stale, time.Now().Add(-48*time.Hour))

	real := filepath.Join(dir, "alice_2026-08-14_04-16-00.mp4")
	mustWriteFile(t, real)
	mustTouch(t, real, time.Now())

	deleteStalePendingSegments(dir)

	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh merge scratch was deleted: %v", err)
	}
	if _, err := os.Stat(stale); err == nil {
		t.Error("stale merge scratch was not deleted")
	}
	if _, err := os.Stat(real); err != nil {
		t.Errorf("real segment was deleted: %v", err)
	}
}
