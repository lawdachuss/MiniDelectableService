package channel

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestIsFinalizingTemp verifies the ffmpeg finalizer scratch-file detector:
// a crash mid-finalize leaves "<base>.finalizing<ext>" behind, which must
// never be treated as a real video (uploaded, thumbnailed, or merged).
func TestIsFinalizingTemp(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"foo.finalizing.mp4", true},   // remux of foo.ts / foo.mp4 in progress
		{"foo.finalizing.mkv", true},   // mkv-container finalize scratch
		{"foo.mp4", false},             // plain recording
		{"foo.ts", false},              // raw HLS recording
		{"foo.video.muxed.mp4", false}, // final muxed output
		{"foo.preview.webp", false},    // preview sidecar (handled elsewhere)
		{"alice_2026-08-08_10-00-00.finalizing.mp4", true},
	}
	for _, c := range cases {
		if got := IsFinalizingTemp(c.name); got != c.want {
			t.Errorf("IsFinalizingTemp(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestIsSidecarExcludesScratch verifies the pipeline/orphan sidecar filter
// rejects finalizer scratch files, merge scratch files, and deletion-in-progress
// leftovers, so enqueueFile, collectPendingSegmentsInDir, and the watcher never
// pick them up as videos.
func TestIsSidecarExcludesScratch(t *testing.T) {
	for _, name := range []string{
		"foo.finalizing.mp4",
		"foo.finalizing.mkv",
		"foo.mp4.deleting.3",
		".merging-1723600000000000000-merged-alice_2026-08-14_04-16-00.mp4",
		".merging-1723600000000000000-merged-alice_2026-08-14_04-16-00.mp4.normalized.mp4",
	} {
		if !isSidecar(name) {
			t.Errorf("isSidecar(%q) = false, want true (scratch file must never be treated as video)", name)
		}
	}
	for _, name := range []string{
		"foo.mp4",
		"foo.video.muxed.mp4",
		"alice_2026-08-08_10-00-00.mp4",
		"merged-alice_2026-08-08_10-00-00.mp4",
	} {
		if isSidecar(name) {
			t.Errorf("isSidecar(%q) = true, want false (real video)", name)
		}
	}
}

// TestRemoveStaleFinalizingScratch verifies that only old scratch files are
// deleted: a fresh scratch (live finalize) is preserved, a stale one
// (crash leftover) is removed, and real videos are never touched.
func TestRemoveStaleFinalizingScratch(t *testing.T) {
	dir := t.TempDir()

	// Fresh scratch — belongs to a live finalize, must survive.
	fresh := filepath.Join(dir, "live.finalizing.mp4")
	mustWriteFile(t, fresh)
	past := time.Now().Add(-24 * time.Hour)
	mustTouch(t, fresh, past.Add(1*time.Minute)) // 23h59m old → still stale? no: 23h59m > 35m
	// Reset to actually-fresh for the assertion below.
	mustTouch(t, fresh, time.Now())

	// Stale scratch — crash leftover, must be deleted.
	stale := filepath.Join(dir, "dead.finalizing.mp4")
	mustWriteFile(t, stale)
	mustTouch(t, stale, past)

	// Real video next to the stale scratch — must never be deleted.
	real := filepath.Join(dir, "dead.ts")
	mustWriteFile(t, real)

	removeStaleFinalizingScratch(dir)

	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh scratch was deleted: %v", err)
	}
	if _, err := os.Stat(stale); err == nil {
		t.Error("stale scratch was not deleted")
	}
	if _, err := os.Stat(real); err != nil {
		t.Errorf("real video was deleted: %v", err)
	}
}

func mustWriteFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustTouch(t *testing.T, path string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}
