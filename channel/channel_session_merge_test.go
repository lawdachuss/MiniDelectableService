package channel

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// TestMergeTwoFiles validates that two consecutive same-session recordings are
// joined into one continuous video (timeline re-based) and the originals are
// removed on success.
func TestMergeTwoFiles(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
	server.Config = &entity.Config{FinalizeMode: "remux", FFmpegContainer: "mp4"}

	dir := t.TempDir()
	a := filepath.Join(dir, "cam_2026-01-01_00-00-00.mp4")
	b := filepath.Join(dir, "cam_2026-01-01_00-02-05.mp4")
	gen := func(out, freq string) {
		cmd := exec.Command("ffmpeg", "-y",
			"-f", "lavfi", "-i", "testsrc=duration=2:size=320x240:rate=15",
			"-f", "lavfi", "-i", "sine=frequency="+freq+":duration=2",
			"-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac",
			"-f", "mp4", "-movflags", "+frag_keyframe+empty_moov", out)
		if outb, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("generate %s: %v\n%s", out, err, outb)
		}
	}
	gen(a, "440")
	gen(b, "660")

	merged, err := mergeTwoFiles(a, b)
	if err != nil {
		t.Fatalf("mergeTwoFiles: %v", err)
	}
	da, _ := VideoDurationSeconds(a)
	db, _ := VideoDurationSeconds(b)
	dm, err := VideoDurationSeconds(merged)
	if err != nil {
		t.Fatalf("probe merged: %v", err)
	}
	if dm < (da+db)*0.85 {
		t.Fatalf("merged duration %.1fs < 85%% of inputs %.1fs", dm, da+db)
	}
	if _, err := os.Stat(a); err == nil {
		t.Errorf("input %s was not removed after merge", a)
	}
	if _, err := os.Stat(b); err == nil {
		t.Errorf("input %s was not removed after merge", b)
	}
	// Re-merge the merged file with a third cycle to ensure stable naming.
	c := filepath.Join(dir, "cam_2026-01-01_00-04-10.mp4")
	gen(c, "880")
	merged2, err := mergeTwoFiles(merged, c)
	if err != nil {
		t.Fatalf("second merge: %v", err)
	}
	dc, _ := VideoDurationSeconds(c)
	dm2, err := VideoDurationSeconds(merged2)
	if err != nil {
		t.Fatalf("probe merged2: %v", err)
	}
	if dm2 < (dm+dc)*0.85 {
		t.Fatalf("merged2 duration %.1fs < 85%% of %.1fs", dm2, dm+dc)
	}
}

// TestContinuationClassification locks the fix for ~20-minute fragmentation:
// Chaturbate's HLS token rotates every ~20 minutes, surfacing as a stall whose
// reason is "stream session expired (no new segments)" (or "...— reconnecting").
// Both must be treated as a continuation of the SAME live session (so fragments
// merge into one long recording), NOT as a session end. Definitive ends
// (offline, private show, max duration, handoff, unknown) must remain stops so a
// merge flushes instead of holding forever.
func TestContinuationClassification(t *testing.T) {
	continuations := []string{
		"stream session expired (no new segments)",
		"stream session expired (HLS session/token) — reconnecting",
		"stream session expired (HLS session/token) — reconnecting (site probe failed: 502)",
	}
	for _, r := range continuations {
		if !isContinuationReason(r) {
			t.Errorf("isContinuationReason(%q) = false, want true (fragment would be uploaded alone)", r)
		}
		if isDefinitiveStop(r) {
			t.Errorf("isDefinitiveStop(%q) = true, want false (would not merge with next cycle)", r)
		}
	}

	stops := []string{
		"channel went offline",
		"channel entered a private show",
		"max duration or filesize reached",
		"channel stopped (handoff)",
		"stream ended normally",
		"unknown",
	}
	for _, r := range stops {
		if isContinuationReason(r) {
			t.Errorf("isContinuationReason(%q) = true, want false", r)
		}
		if !isDefinitiveStop(r) {
			t.Errorf("isDefinitiveStop(%q) = false, want true (merge would never flush)", r)
		}
	}
}
