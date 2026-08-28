package channel

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func TestUploadTrackerNormalizesPaths(t *testing.T) {
	path := filepath.Join("videos", "..", "videos", "tracker-test.mp4")
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("abs path: %v", err)
	}

	MarkUploadInFlight(path)
	t.Cleanup(func() { MarkUploadDone(abs) })

	if !IsUploadInFlight(abs) {
		t.Fatal("absolute path was not recognized as in-flight")
	}

	MarkUploadDone(abs)
	if IsUploadInFlight(path) {
		t.Fatal("path remained in-flight after MarkUploadDone")
	}
}

func TestPipelineCleanupDeletesWithLinksAndThumbnail(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{
		DeleteLocalAfterUpload: true,
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, "partial.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o666); err != nil {
		t.Fatalf("write video: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "tester"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	p := &Pipeline{
		FilePath:   filePath,
		Filename:   filepath.Base(filePath),
		Links:      map[string]string{"GoFile": "https://gofile.example/video"},
		ThumbURL:   "https://pixhost.example/thumb.jpg",
		SpriteURL:  "https://pixhost.example/sprite.jpg",
		PreviewURL: "https://files.catbox.moe/preview.webp",
	}

	if err := p.stageCleanup(ch); err != nil {
		t.Fatalf("stageCleanup: %v", err)
	}
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("file should have been removed with links and all presentation assets: %v", err)
	}
}

func TestPipelineCleanupKeepsWhenNoThumbnails(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{
		DeleteLocalAfterUpload: true,
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, "nothumbs.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o666); err != nil {
		t.Fatalf("write video: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "tester"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	p := &Pipeline{
		FilePath: filePath,
		Filename: filepath.Base(filePath),
		Links:    map[string]string{"GoFile": "https://gofile.example/video"},
	}

	if err := p.stageCleanup(ch); err != nil {
		t.Fatalf("stageCleanup: %v", err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should be kept when no thumbnails were generated: %v", err)
	}
}

// TestPipelineCleanupKeepsWhenSpriteButNoThumbnail is the regression test for
// the case that orphaned recordings in production: the sprite (and sometimes
// the preview) uploaded successfully but the THUMBNAIL failed.  The old
// "all three assets empty" gate deleted the local file anyway, making the
// thumbnail permanently un-recoverable (ScanThumbnails needs the source
// video).  The file must be kept whenever the thumbnail is missing.
func TestPipelineCleanupKeepsWhenSpriteButNoThumbnail(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{
		DeleteLocalAfterUpload: true,
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sprite-only.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o666); err != nil {
		t.Fatalf("write video: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "tester"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	p := &Pipeline{
		FilePath:   filePath,
		Filename:   filepath.Base(filePath),
		Links:      map[string]string{"GoFile": "https://gofile.example/video"},
		SpriteURL:  "https://pixhost.example/sprite.jpg",
		PreviewURL: "https://files.catbox.moe/preview.webp",
	}

	if err := p.stageCleanup(ch); err != nil {
		t.Fatalf("stageCleanup: %v", err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should be kept when only the thumbnail is missing (sprite/preview exist): %v", err)
	}
}

func TestPipelineCleanupKeepsWhenPreviewMissing(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{DeleteLocalAfterUpload: true}

	filePath := filepath.Join(t.TempDir(), "preview-missing.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o666); err != nil {
		t.Fatalf("write video: %v", err)
	}
	ch := &Channel{Config: &entity.ChannelConfig{Username: "tester"}, LogCh: make(chan string, 20), UpdateCh: make(chan bool, 1)}
	p := &Pipeline{
		FilePath:  filePath,
		Filename:  filepath.Base(filePath),
		Links:     map[string]string{"GoFile": "https://gofile.example/video"},
		ThumbURL:  "https://pixhost.example/thumb.jpg",
		SpriteURL: "https://pixhost.example/sprite.jpg",
	}
	if err := p.stageCleanup(ch); err != nil {
		t.Fatalf("stageCleanup: %v", err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should be kept when hover preview is missing: %v", err)
	}
}

func TestPipelineCleanupKeepsWhenNoLinks(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{
		DeleteLocalAfterUpload: true,
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, "nolinks.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o666); err != nil {
		t.Fatalf("write video: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "tester"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	p := &Pipeline{
		FilePath: filePath,
		Filename: filepath.Base(filePath),
		Links:    map[string]string{},
	}

	if err := p.stageCleanup(ch); err != nil {
		t.Fatalf("stageCleanup: %v", err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should be kept when no links exist: %v", err)
	}
}

func TestPipelineCleanupWhenDisabled(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{
		DeleteLocalAfterUpload: false,
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, "disabled.mp4")
	if err := os.WriteFile(filePath, []byte("video"), 0o666); err != nil {
		t.Fatalf("write video: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "tester"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	p := &Pipeline{
		FilePath: filePath,
		Filename: filepath.Base(filePath),
		Links:    map[string]string{"GoFile": "https://gofile.example/video"},
	}

	if err := p.stageCleanup(ch); err != nil {
		t.Fatalf("stageCleanup: %v", err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should be kept when delete after upload is disabled: %v", err)
	}
}
