package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
)

// TestThumbFailureCounter verifies the persistent corrupt-file eviction
// counter: failures accumulate across calls, reset on success, and the
// counter file survives in the journal dir.
func TestThumbFailureCounter(t *testing.T) {
	if Config != nil {
		old := Config.OutputDir
		t.Cleanup(func() { Config.OutputDir = old })
	} else {
		t.Cleanup(func() { Config = nil })
	}
	Config = &entity.Config{OutputDir: t.TempDir()}
	defer ClearThumbFailure("v:\\fake\\path.mp4")

	if c := RecordThumbFailure("v:\\fake\\path.mp4"); c != 1 {
		t.Fatalf("first failure count = %d, want 1", c)
	}
	if c := RecordThumbFailure("v:\\fake\\path.mp4"); c != 2 {
		t.Fatalf("second failure count = %d, want 2", c)
	}
	if got := ThumbFailures()["v:\\fake\\path.mp4"]; got != 2 {
		t.Fatalf("ThumbFailures count = %d, want 2", got)
	}

	ClearThumbFailure("v:\\fake\\path.mp4")
	if _, ok := ThumbFailures()["v:\\fake\\path.mp4"]; ok {
		t.Fatal("failure entry not cleared")
	}

	// The counter file must exist on disk (persistence across restarts).
	journal := filepath.Join(Config.OutputDir, ".journal", "thumb_failures.json")
	if _, err := os.Stat(journal); err != nil {
		t.Errorf("counter file not persisted: %v", err)
	}
}
