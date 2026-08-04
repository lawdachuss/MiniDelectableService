// Command uploadtest generates a test video of a given size and pushes it
// through the real upload pipeline: thumbnails → upload to every configured
// host → metadata save to Supabase → optional cleanup.  It is a manual
// end-to-end smoke test for the upload system.
//
// Usage:
//
//	go run ./cmd/uploadtest -username alice -size 20
//
// The video is enqueued directly (bypassing the min-duration merge gate) so a
// short synthetic file is uploaded immediately.  Config is read from .env.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

func main() {
	var (
		username = flag.String("username", "testuser", "channel username used in the filename and metadata")
		sizeMB   = flag.Float64("size", 20, "target test video size in megabytes")
		dir      = flag.String("dir", "", "directory for the test video (default: temp dir)")
		timeout  = flag.Duration("timeout", 20*time.Minute, "how long to wait for uploads to finish")
	)
	flag.Parse()

	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set — metadata cannot be saved to Supabase")
	}
	server.Config = cfg
	channel.SetUploadConcurrency(cfg.UploadMaxConcurrent)
	uploader.SetHostConcurrency(cfg.UploadHostConcurrency)
	channel.SetPipelineWorkers(cfg.PipelineWorkers)

	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey, cfg.StreamtapeLogin, cfg.StreamtapeKey,
		cfg.MixdropEmail, cfg.MixdropToken, cfg.VidaraKey, nil,
	)
	hosts := upl.AvailableHosts()
	if len(hosts) == 0 {
		log.Fatal("no upload hosts configured")
	}
	fmt.Printf("configured upload hosts: %s\n", strings.Join(hosts, ", "))

	outDir := *dir
	if outDir == "" {
		var err error
		outDir, err = os.MkdirTemp("", "uploadtest")
		if err != nil {
			log.Fatalf("temp dir: %v", err)
		}
	}
	if err := os.MkdirAll(outDir, 0o777); err != nil {
		log.Fatalf("mkdir %s: %v", outDir, err)
	}

	filename := fmt.Sprintf("%s_%s.mp4", *username, time.Now().Format("2006-01-02_15-04-05"))
	videoPath := filepath.Join(outDir, filename)
	if err := generateTestVideo(videoPath, *sizeMB); err != nil {
		log.Fatalf("generate test video: %v", err)
	}

	ch := &channel.Channel{
		Config: &entity.ChannelConfig{
			Username:  *username,
			RoomTitle: "uploadtest",
		},
		LogCh:      make(chan string, 256),
		UpdateCh:   make(chan bool, 64),
		RoomTitle:  "uploadtest",
		Tags:       []string{"uploadtest"},
		Gender:     "f",
		Resolution: "1280x720",
		Framerate:  30,
	}
	ch.PipelineQueue = channel.NewPipelineQueue(ch)

	fmt.Printf("enqueueing %s\n", videoPath)
	ch.PipelineQueue.EnqueueFile(videoPath)

	deadline := time.Now().Add(*timeout)
	var lastStatus string
	for time.Now().Before(deadline) {
		for _, e := range ch.PipelineQueue.HistoryEntries() {
			if e.Filename != filename {
				continue
			}
			if e.Failed {
				log.Fatalf("pipeline FAILED: %s", e.Error)
			}
			if e.Stage == "done" {
				fmt.Println()
				fmt.Printf("SUCCESS: uploaded %s to all hosts and saved metadata to Supabase\n", videoPath)
				return
			}
		}
		status := fmt.Sprintf("status: %s (%.0f%%) %d/%d hosts",
			ch.UploadStatus, ch.UploadProgress, ch.UploadHostCount, ch.UploadHostTotal)
		if status != lastStatus {
			fmt.Println(status)
			lastStatus = status
		}
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("timed out after %s waiting for uploads", *timeout)
}

// generateTestVideo renders an H.264 test pattern with audio via ffmpeg,
// sized approximately to the requested megabytes.
func generateTestVideo(path string, sizeMB float64) error {
	const totalBitsPerSec = 2_328_000 // 2200k video + 128k audio
	dur := int(sizeMB * 8_000_000 / totalBitsPerSec)
	if dur < 10 {
		dur = 10
	}
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc2=duration=%d:size=1280x720:rate=30", dur),
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100",
		"-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
		"-b:v", "2200k",
		"-c:a", "aac", "-b:a", "128k",
		"-shortest",
		path,
	}
	if out, err := config.FFmpegCommand(args...).CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg: %v\n%s", err, out)
	}
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	fmt.Printf("test video: %s (%.1f MB, ~%ds)\n", path, float64(st.Size())/1024/1024, dur)
	return nil
}

func configFromEnv() *entity.Config {
	cfg := &entity.Config{
		VoeSXAPIKey:             env("VOESX_API_KEY"),
		StreamtapeLogin:         env("STREAMTAPE_LOGIN"),
		StreamtapeKey:           or(env("STREAMTAPE_KEY"), env("STREAMTAPE_API_KEY")),
		MixdropEmail:            env("MIXDROP_EMAIL"),
		MixdropToken:            or(env("MIXDROP_TOKEN"), env("MIXDROP_KEY")),
		VidaraKey:               env("VIDARA_KEY"),
		SupabaseURL:             env("SUPABASE_URL"),
		SupabaseAPIKey:          env("SUPABASE_API_KEY"),
		UploadMaxConcurrent:     intEnv("UPLOAD_MAX_CONCURRENT", 100),
		UploadHostConcurrency:   intEnv("UPLOAD_HOST_CONCURRENCY", 8),
		PipelineWorkers:         intEnv("PIPELINE_WORKERS", 3),
		DeleteLocalAfterUpload:  strings.EqualFold(env("DELETE_LOCAL_AFTER_UPLOAD"), "true"),
		MinDurationBeforeUpload: intEnv("MIN_DURATION_BEFORE_UPLOAD", 0),
		FFmpegPath:              env("FFMPEG_PATH"),
		Domain:                  or(env("DOMAIN"), "https://www.cb.xxx/"),
	}
	if cfg.FFmpegPath != "" {
		config.SetFFmpegPath(cfg.FFmpegPath)
	}
	return cfg
}

func env(k string) string { return os.Getenv(k) }

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func intEnv(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// loadDotEnv loads KEY=VALUE pairs from a .env file into the environment,
// without overwriting existing variables. Falls back to the executable dir.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		if exe, e2 := os.Executable(); e2 == nil {
			f, err = os.Open(filepath.Join(filepath.Dir(exe), path))
		}
		if err != nil {
			return
		}
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		v = strings.Trim(v, `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
