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
